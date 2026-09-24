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

/// Map a request to a shard number.
///
/// Re-exported from `felix_wire::routing` rather than defined here: a client
/// that routes its publishes to the shard's owner has to reach the same answer
/// this broker does, and two copies of a hash are two things that can drift.
/// The wire crate is where both sides already meet.
pub use felix_wire::routing::shard_for;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use felix_router::{Resolution, ShardRouter, Unavailable};

use crate::shards::lifecycle::fence::ShardFence;
use crate::shards::{ShardKey, ShardKind};

/// What ingress should do with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatch {
    /// Serve it here, at this generation.
    ///
    /// A write carries the generation to its claim, where the fence refuses it
    /// if the shard stopped serving in between. Zero on a single-node broker,
    /// which has no generations and no fence.
    Local { generation: u64 },
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

impl Reason {
    /// The `reason` a `shard_unavailable` error carries to the client.
    pub fn wire_name(&self) -> &'static str {
        use felix_wire::shard_unavailable_reason as wire;
        match self {
            Self::NotAssigned => wire::NOT_ASSIGNED,
            Self::OwnerUnavailable(_) => wire::OWNER_UNAVAILABLE,
            Self::NotReady => wire::NOT_READY,
            Self::Stale { .. } => wire::STALE,
        }
    }
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
/// One `ArcSwap` load per request, so `dispatch` is synchronous and allocation
/// free. That is deliberate: it sits in front of every publish, and a resolver
/// that cost a lock or an await would show up in p999 long before it showed up
/// in a correctness test.
pub struct IngressRouter {
    router: Arc<ShardRouter>,
    view: ArcSwap<View>,
    fence: Arc<ShardFence>,
}

/// Routes and the servable set, published as one value.
///
/// Two swaps would leave a moment where one is updated and the other is not:
/// a new owner that has opened a shard but still routes it to the old leader,
/// or an old leader that has stopped serving but still routes it to itself.
struct View {
    routes: Arc<felix_router::Routes>,
    servable: ServableShards,
}

impl IngressRouter {
    /// `fence` is the one the shard lifecycle opens and closes; see
    /// [`crate::shards::lifecycle::ShardLifecycle::fence`].
    pub fn new(router: Arc<ShardRouter>, fence: Arc<ShardFence>) -> Self {
        let view = ArcSwap::from_pointee(View {
            routes: router.routes(),
            servable: ServableShards::new(),
        });
        Self {
            router,
            view,
            fence,
        }
    }

    /// The write fence every local write enters when it claims its place in
    /// the log. Here because every write path already holds this router.
    pub fn fence(&self) -> &Arc<ShardFence> {
        &self.fence
    }

    /// Publish new routes and the shards this broker can serve, together.
    ///
    /// The feed calls this after each lifecycle reconcile, which is the only
    /// thing that changes the servable set.
    pub fn publish(
        &self,
        table: felix_router::RoutingTable,
        nodes: &HashMap<String, felix_router::NodeRef>,
        servable: ServableShards,
    ) {
        let routes = self.router.publish(table, nodes);
        self.view.store(Arc::new(View { routes, servable }));
    }

    /// Replace the servable set, keeping the routes last published to the
    /// router.
    pub fn publish_servable(&self, servable: ServableShards) {
        self.view.store(Arc::new(View {
            routes: self.router.routes(),
            servable,
        }));
    }

    /// The generation this broker currently leads `key` at, if it does.
    ///
    /// A publish waiting for a quorum needs it: the majority is over the
    /// replica set of *that* generation, and an acknowledgement from an older
    /// one does not count toward a newer one's quorum.
    pub fn generation(&self, key: &ShardKey) -> Option<u64> {
        match self.dispatch(key) {
            Dispatch::Local { generation } => Some(generation),
            _ => None,
        }
    }

    /// How many shards this stream or cache was placed with.
    ///
    /// Read from the published routes, which is an `ArcSwap` load and no lock.
    pub fn shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> u32 {
        self.view.load().routes.table().shards_for(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
        )
    }

    /// The placed shard count, or `None` if the routing snapshot does not know
    /// this stream. See [`felix_router::RoutingTable::placed_shards_for`].
    pub fn placed_shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<u32> {
        self.view.load().routes.table().placed_shards_for(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
        )
    }

    /// Decide what to do with a request for `key`.
    ///
    /// Two sources have to agree. The routes say who the cluster believes owns
    /// the shard; the servable set says whether this broker has actually opened
    /// it. Trusting only the routes would serve writes during recovery;
    /// trusting only local state would keep serving a shard that has been
    /// reassigned.
    pub fn dispatch(&self, key: &ShardKey) -> Dispatch {
        let view = self.view.load();
        match self.router.resolve_with(&view.routes, &to_router_key(key)) {
            Resolution::Local { generation } => {
                // The cluster says ours. Local readiness has the deciding vote,
                // and only at this exact generation: an older one means we have
                // not caught up with a reassignment that already happened.
                if view.servable.get(key) == Some(&generation) {
                    Dispatch::Local { generation }
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
        None => Dispatch::Local { generation: 0 },
        Some(ingress) => ingress.dispatch(key),
    }
}

pub(crate) fn to_router_key(key: &ShardKey) -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: to_router_kind(key.kind),
    }
}

/// The watch's kind and the router's kind are separate types on purpose — the
/// router is a library that does not know about the control plane's wire
/// format — so the two are mapped here, in one place.
pub(crate) fn to_router_kind(kind: ShardKind) -> felix_router::ShardKind {
    match kind {
        ShardKind::Stream => felix_router::ShardKind::Stream,
        ShardKind::Cache => felix_router::ShardKind::Cache,
    }
}

/// Build a routing table from what the watch currently holds.
///
/// Lives here rather than in the watch because it is the point where two
/// separate views -- ownership and node addresses -- are joined into the one
/// the hot path reads.
pub fn routing_table_from(
    assignments: &std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    nodes: &std::collections::HashMap<String, felix_router::NodeRef>,
) -> felix_router::RoutingTable {
    felix_router::RoutingTable::build_with(
        assignments.values().map(|assignment| felix_router::Placed {
            key: to_router_key(&assignment.key),
            leader: assignment.leader.clone(),
            replicas: assignment.replicas.clone(),
            generation: assignment.generation,
            draining: assignment.is_draining(),
            successor: assignment.successor.clone(),
        }),
        nodes,
    )
}

/// The cluster state one task keeps in step. Grouped because they are only ever
/// used together, and only in the order this feed applies them.
pub struct FeedState {
    pub ownership: Arc<tokio::sync::RwLock<crate::shards::watch::ShardOwnership>>,
    pub lifecycle: Arc<tokio::sync::Mutex<crate::shards::lifecycle::ShardLifecycle>>,
    pub store: Arc<dyn crate::shards::lifecycle::ShardStore>,
    pub ingress: Arc<IngressRouter>,
    /// Refreshed on the same tick as the catalog it is derived from, so what a
    /// client is told and what this broker forwards to cannot come from
    /// different fetches.
    pub client_endpoints: Option<Arc<crate::cluster::client_endpoints::ClientEndpoints>>,
    /// Notified by the watch when ownership changed. The feed acts on it at
    /// once instead of at its next tick.
    pub assignments_changed: Arc<tokio::sync::Notify>,
    /// Notified by the feed after it acts on a change, so replication ships a
    /// newly fenced or newly led shard without waiting for its own tick.
    pub routes_changed: Arc<tokio::sync::Notify>,
}

/// What the feed needs to read the node catalog.
pub struct CatalogSource {
    pub client: reqwest::Client,
    pub base_url: String,
    /// The same credential the assignment feed uses: `/v1/nodes` and
    /// `/v1/shard-assignments` both require `node.view:cluster:*`. Held, not
    /// copied, so a refresh reaches this feed too.
    pub token: Option<crate::cluster::credential::NodeCredential>,
}

/// Keep local shard state and the routing table in step with the watch.
///
/// One task owns the sequence, so the two never disagree: reconcile local state
/// first, then publish what is servable and the routes together.
///
/// Runs on a tick and whenever the watch reports a change. The tick refreshes
/// the node catalog, because a route is only usable when both halves are
/// known: an assignment names an owner by id, and only the catalog turns that
/// into an address to forward to. A wake skips the refresh unless an
/// assignment names a node the catalog lacks, so a move is not held up by a
/// round trip for addresses this broker already has.
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
        client_endpoints,
        assignments_changed,
        routes_changed,
    } = state;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut catalog = HashMap::new();
        loop {
            let woken = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => false,
                _ = assignments_changed.notified() => true,
            };

            let assignments = ownership.read().await.assignments().clone();

            if let Some(source) = &catalog_source
                && (!woken || names_unknown_node(&assignments, &catalog))
            {
                // Read on every refresh rather than once, so a refreshed token
                // is in use from the next poll.
                let bearer = source.token.as_ref().map(|token| token.bearer());
                match crate::cluster::node_catalog::fetch(
                    &source.client,
                    &source.base_url,
                    bearer.as_deref().map(String::as_str),
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
                        crate::shards::watch::metrics::record_catalog_refresh_failure();
                    }
                }
            }

            crate::shards::lifecycle::reconcile(&lifecycle, store.as_ref(), &assignments).await;

            // Read after reconcile, so an open that just finished is included:
            // publishing the routes first would advertise this node as the
            // owner of a shard it has not opened.
            let servable = lifecycle.lock().await.servable();
            ingress.publish(
                routing_table_from(&assignments, &catalog),
                &catalog,
                servable,
            );
            crate::shards::watch::metrics::set_catalog_nodes(catalog.len());
            if woken {
                routes_changed.notify_one();
            }
        }
    })
}

/// Whether an assignment names a node the catalog has no entry for.
fn names_unknown_node(
    assignments: &HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    catalog: &HashMap<String, felix_router::NodeRef>,
) -> bool {
    assignments.values().any(|assignment| {
        std::iter::once(&assignment.leader)
            .chain(&assignment.replicas)
            .chain(&assignment.successor)
            .any(|node| !catalog.contains_key(node))
    })
}

#[cfg(test)]
mod tests;
