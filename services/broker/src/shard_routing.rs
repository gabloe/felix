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
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use felix_router::{Resolution, ShardRouter, Unavailable};

use crate::shard_watch::ShardKey;

/// What ingress should do with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatch {
    /// Serve it here.
    Local,
    /// Another node owns it. The publish is forwarded there over the internal
    /// protocol and its answer is relayed back to the client.
    Forward {
        node_id: String,
        advertise_addr: SocketAddr,
        /// The generation this broker resolved against.
        ///
        /// Carried to the owner, which compares it with its own. A mismatch in
        /// either direction is a typed answer and never a successful ownership
        /// claim — see `docs/internal-protocol.md`.
        generation: u64,
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

/// Shards this broker has opened, and the generation each was opened at.
///
/// Published as an immutable snapshot for the same reason routes are: the
/// publish path reads this on every request, and it must not take a lock a
/// writer can hold -- least of all an async one, which would put an await into
/// the hot path for a lookup that is two loads.
pub type ServableShards = HashMap<ShardKey, u64>;

/// Resolves ingress requests against cluster ownership.
///
/// Absent on a single-node broker — see [`dispatch`].
///
/// Both reads are `ArcSwap` loads, so `dispatch` is synchronous and allocation
/// free. That is deliberate: it sits in front of every publish, and a resolver
/// that cost a lock or an await would show up in p999 long before it showed up
/// in a correctness test.
pub struct IngressRouter {
    router: Arc<ShardRouter>,
    servable: ArcSwap<ServableShards>,
}

impl IngressRouter {
    pub fn new(router: Arc<ShardRouter>) -> Self {
        Self {
            router,
            servable: ArcSwap::from_pointee(ServableShards::new()),
        }
    }

    /// Replace the set of shards this broker can serve.
    ///
    /// Called after each lifecycle reconcile, which is the only thing that
    /// changes it.
    pub fn publish_servable(&self, servable: ServableShards) {
        self.servable.store(Arc::new(servable));
    }

    /// Decide what to do with a request for `key`.
    ///
    /// Two sources have to agree. The router says who the cluster believes owns
    /// the shard; the servable set says whether this broker has actually opened
    /// it. Trusting only the router would serve writes during recovery;
    /// trusting only local state would keep serving a shard that has been
    /// reassigned.
    /// The generation this broker currently leads `key` at, if it does.
    ///
    /// A publish waiting for a quorum needs it: the majority is over the
    /// replica set of *that* generation, and an acknowledgement from an older
    /// one does not count toward a newer one's quorum.
    pub fn generation(&self, key: &ShardKey) -> Option<u64> {
        match self.dispatch(key) {
            Dispatch::Local => match self.router.resolve(&to_router_key(key)) {
                Resolution::Local { generation } => Some(generation),
                _ => None,
            },
            _ => None,
        }
    }

    /// How many shards this stream was placed with.
    ///
    /// Read from the routing snapshot, which is an `ArcSwap` load and no lock.
    pub fn shards_for(&self, tenant_id: &str, namespace: &str, stream: &str) -> u32 {
        self.router
            .snapshot()
            .shards_for(tenant_id, namespace, stream)
    }

    pub fn dispatch(&self, key: &ShardKey) -> Dispatch {
        match self.router.resolve(&to_router_key(key)) {
            Resolution::Local { generation } => {
                // The cluster says ours. Local readiness has the deciding vote,
                // and only at this exact generation: an older one means we have
                // not caught up with a reassignment that already happened.
                if self.servable.load().get(key) == Some(&generation) {
                    Dispatch::Local
                } else {
                    Dispatch::Unavailable(Reason::NotReady)
                }
            }
            Resolution::Remote {
                node_id,
                advertise_addr,
                generation,
            } => Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
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
/// existed, and it costs one branch.
#[inline]
pub fn dispatch(ingress: Option<&IngressRouter>, key: &ShardKey) -> Dispatch {
    match ingress {
        None => Dispatch::Local,
        Some(ingress) => ingress.dispatch(key),
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

/// The cluster state one task keeps in step. Grouped because they are only ever
/// used together, and only in the order this feed applies them.
pub struct FeedState {
    pub ownership: Arc<tokio::sync::RwLock<crate::shard_watch::ShardOwnership>>,
    pub lifecycle: Arc<tokio::sync::Mutex<crate::shard_lifecycle::ShardLifecycle>>,
    pub store: Arc<dyn crate::shard_lifecycle::ShardStore>,
    pub ingress: Arc<IngressRouter>,
    pub router: Arc<ShardRouter>,
    /// Refreshed on the same tick as the catalog it is derived from, so what a
    /// client is told and what this broker forwards to cannot come from
    /// different fetches.
    pub client_endpoints: Option<Arc<crate::client_endpoints::ClientEndpoints>>,
}

/// What the feed needs to read the node catalog.
pub struct CatalogSource {
    pub client: reqwest::Client,
    pub base_url: String,
    /// The same credential the assignment feed uses: `/v1/nodes` and
    /// `/v1/shard-assignments` both require `node.view:cluster:*`.
    pub token: Option<String>,
}

/// Keep local shard state and the routing table in step with the watch.
///
/// One task owns the sequence, so the two never disagree: reconcile local state
/// first, then publish what is servable, then publish the routes. Publishing
/// routes first would advertise this node as the owner of a shard it has not
/// opened.
///
/// The node catalog is refreshed on the same tick, because a route is only
/// usable when both halves are known: an assignment names an owner by id, and
/// only the catalog turns that into an address to forward to.
pub fn spawn_feed(
    state: FeedState,
    catalog_source: Option<CatalogSource>,
    interval: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let FeedState {
        ownership,
        lifecycle,
        store,
        ingress,
        router,
        client_endpoints,
    } = state;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut catalog = HashMap::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            if let Some(source) = &catalog_source {
                match crate::node_catalog::fetch(
                    &source.client,
                    &source.base_url,
                    source.token.as_deref(),
                )
                .await
                {
                    Ok(fetched) => {
                        if let Some(endpoints) = &client_endpoints {
                            endpoints.publish(fetched.client_endpoints);
                        }
                        catalog = fetched.nodes;
                    }
                    // The previous catalog is kept: a control-plane blip must
                    // not erase every address this broker can forward to and
                    // turn a healthy cluster into one that refuses every remote
                    // publish.
                    Err(err) => {
                        tracing::warn!(error = %err, "node catalog refresh failed; keeping the last one");
                        crate::shard_watch_metrics::record_catalog_refresh_failure();
                    }
                }
            }

            let assignments = ownership.read().await.assignments().clone();
            crate::shard_lifecycle::reconcile(&lifecycle, store.as_ref(), &assignments).await;

            let servable = lifecycle.lock().await.servable();
            ingress.publish_servable(servable);
            router.publish(routing_table_from(&assignments, &catalog), &catalog);
            crate::shard_watch_metrics::set_catalog_nodes(catalog.len());
        }
    })
}

#[cfg(test)]
#[path = "shard_routing_tests.rs"]
mod tests;
