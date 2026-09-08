//! Ownership resolution on the ingress path.
//!
//! Every publish asks one question before anything else happens: is this shard
//! mine? The answer is `Local`, `Forward`, or a typed refusal — never a
//! shrug. A broker that treats an unresolved shard as its own is a broker
//! writing data another node owns.
//!
//! **Single-node brokers are unaffected.** A broker with no cluster identity has
//! no router to consult and no assignments to honour, so dispatch is `Local` by
//! construction. Clustering is opt-in, and a broker that never joined one must
//! behave exactly as it did before this existed.
use std::net::SocketAddr;
use std::sync::Arc;

use felix_router::{Resolution, ShardRouter, Unavailable};
use tokio::sync::Mutex;

use crate::shard_lifecycle::ShardLifecycle;
use crate::shard_watch::ShardKey;

/// What ingress should do with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatch {
    /// Serve it here.
    Local,
    /// Another node owns it. M4 forwards; until then the caller refuses with
    /// this attached, so "someone else's shard" stays distinguishable from
    /// "something went wrong".
    Forward {
        node_id: String,
        advertise_addr: SocketAddr,
    },
    /// Nobody can serve it right now, and this says why.
    Unavailable(Reason),
}

/// Why a request cannot be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// Placement has not assigned the shard.
    NotAssigned,
    /// The owner is known but unreachable or not live.
    OwnerUnavailable(String),
    /// This node owns the shard but has not finished opening it.
    ///
    /// Deliberately not `Local`: serving now would acknowledge a write against
    /// a log that is not yet recovered.
    NotReady,
    /// This node's routing view is behind the caller's.
    Stale { have: u64, wanted: u64 },
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAssigned => write!(f, "shard is not assigned to any broker"),
            Self::OwnerUnavailable(detail) => write!(f, "shard owner is unavailable: {detail}"),
            Self::NotReady => write!(f, "this broker is still opening the shard"),
            Self::Stale { have, wanted } => {
                write!(
                    f,
                    "routing view is behind: have generation {have}, caller has {wanted}"
                )
            }
        }
    }
}

/// Map a request to a shard number.
///
/// Deterministic, and a pure function of its inputs, so the same key lands on
/// the same shard on every broker and across restarts.
///
/// **The wire protocol carries no routing key today**, so `routing_key` is
/// always `None` and every record of a stream lands on shard 0. That makes a
/// stream's configured `shards` count metadata the data path does not yet use.
/// Adding a negotiated key field is what makes `shards > 1` mean anything; this
/// function is the place it plugs in, and the rest of the path is already
/// shard-aware.
pub fn shard_for(shards: u32, routing_key: Option<&[u8]>) -> u32 {
    if shards <= 1 {
        return 0;
    }
    match routing_key {
        // Same construction as placement's: FNV-1a with a finalizer, written
        // out so the mapping cannot shift with a toolchain change.
        Some(key) => (finalize(fnv1a(key)) % u64::from(shards)) as u32,
        None => 0,
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Resolves ingress requests against cluster ownership.
///
/// Absent on a single-node broker — see [`dispatch`].
pub struct IngressRouter {
    router: Arc<ShardRouter>,
    lifecycle: Arc<Mutex<ShardLifecycle>>,
}

impl IngressRouter {
    pub fn new(router: Arc<ShardRouter>, lifecycle: Arc<Mutex<ShardLifecycle>>) -> Self {
        Self { router, lifecycle }
    }

    /// Decide what to do with a request for `key`.
    ///
    /// Two sources have to agree. The router says who the cluster believes owns
    /// the shard; the lifecycle says whether this broker has actually opened it.
    /// Trusting only the router would serve writes during recovery; trusting
    /// only the lifecycle would keep serving a shard that has been reassigned.
    pub async fn dispatch(&self, key: &ShardKey) -> Dispatch {
        let route = self.router.resolve(&to_router_key(key));
        match route {
            Resolution::Local { generation } => {
                // The cluster says ours. Local state has the deciding vote on
                // whether it is servable yet.
                if self.lifecycle.lock().await.may_serve_at(key, generation) {
                    Dispatch::Local
                } else {
                    Dispatch::Unavailable(Reason::NotReady)
                }
            }
            Resolution::Remote {
                node_id,
                advertise_addr,
                ..
            } => Dispatch::Forward {
                node_id,
                advertise_addr,
            },
            Resolution::Stale { have, wanted } => {
                Dispatch::Unavailable(Reason::Stale { have, wanted })
            }
            Resolution::Unavailable(Unavailable::NoAssignment) => {
                Dispatch::Unavailable(Reason::NotAssigned)
            }
            Resolution::Unavailable(other) => {
                Dispatch::Unavailable(Reason::OwnerUnavailable(other.to_string()))
            }
        }
    }
}

/// Decide what to do with a request, for a broker that may or may not be in a
/// cluster.
///
/// `None` is a single-node broker: no identity, no assignments, nothing to
/// resolve against. Everything is local, exactly as it was before clustering
/// existed.
pub async fn dispatch(ingress: Option<&IngressRouter>, key: &ShardKey) -> Dispatch {
    match ingress {
        None => Dispatch::Local,
        Some(ingress) => ingress.dispatch(key).await,
    }
}

fn to_router_key(key: &ShardKey) -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
    }
}

/// Build a routing table from what the watch currently holds.
///
/// Lives here rather than in the watch because it is the point where two
/// separate views -- ownership and node addresses -- are joined into the one
/// the hot path reads.
pub fn routing_table_from(
    assignments: &std::collections::HashMap<ShardKey, crate::shard_watch::ShardAssignment>,
    nodes: &std::collections::HashMap<String, felix_router::NodeRef>,
) -> felix_router::RoutingTable {
    felix_router::RoutingTable::build(
        assignments.values().map(|assignment| {
            (
                to_router_key(&assignment.key),
                assignment.leader.clone(),
                assignment.replicas.clone(),
                assignment.generation,
            )
        }),
        nodes,
    )
}

#[cfg(test)]
#[path = "shard_routing_tests.rs"]
mod tests;
