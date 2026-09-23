//! The routing table: an immutable snapshot of where every shard is served.

use std::collections::HashMap;
use std::net::SocketAddr;

use super::{ShardKey, ShardKind};

/// An immutable set of routes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RoutingTable {
    routes: HashMap<ShardKey, Route>,
    /// How many shards each stream was placed with, by `tenant/namespace/stream`.
    ///
    /// Derived once when the table is built rather than counted per publish: a
    /// publish needs it before it can resolve a routing key to a shard, and
    /// that is the hottest question the router is asked.
    shards_per_stream: HashMap<String, u32>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a table from assignments and the nodes they name.
    ///
    /// An assignment whose leader is absent from `nodes` still produces a route,
    /// carrying [`Unavailable::LeaderUnknown`](crate::Unavailable::LeaderUnknown) at lookup time. Dropping it here
    /// instead would make a missing address indistinguishable from a missing
    /// assignment, and those are different problems.
    pub fn build(
        assignments: impl IntoIterator<Item = (ShardKey, String, Vec<String>, u64)>,
        nodes: &HashMap<String, NodeRef>,
    ) -> Self {
        Self::build_with(
            assignments
                .into_iter()
                .map(|(key, leader, replicas, generation)| Placed {
                    key,
                    leader,
                    replicas,
                    generation,
                    draining: false,
                }),
            nodes,
        )
    }

    /// [`RoutingTable::build`], with each assignment's draining flag.
    pub fn build_with(
        assignments: impl IntoIterator<Item = Placed>,
        nodes: &HashMap<String, NodeRef>,
    ) -> Self {
        let mut routes = HashMap::new();
        let mut shards_per_stream: HashMap<String, u32> = HashMap::new();
        for Placed {
            key,
            leader,
            replicas,
            generation,
            draining,
        } in assignments
        {
            // The count is the highest shard index placed plus one, not the
            // number of assignments: placement may not have managed to place
            // every shard, and a publish must still resolve keys against the
            // stream's real width or the same key would move as placement
            // catches up.
            let stream_id = stream_id(key.kind, &key.tenant_id, &key.namespace, &key.stream);
            let width = shards_per_stream.entry(stream_id).or_insert(0);
            *width = (*width).max(key.shard + 1);
            let leader_ref = nodes.get(&leader).cloned().unwrap_or(NodeRef {
                node_id: leader.clone(),
                // A placeholder that can never be dialled, paired with
                // `live: false` so it is refused rather than attempted.
                advertise_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                region: String::new(),
                live: false,
            });
            let known = nodes.contains_key(&leader);
            routes.insert(
                key,
                Route {
                    leader: NodeRef {
                        // Recorded so a lookup can say "unknown" rather than
                        // "not live", which sends an operator somewhere else.
                        live: known && leader_ref.live,
                        ..leader_ref
                    },
                    replicas: replicas
                        .into_iter()
                        .filter_map(|id| nodes.get(&id).cloned())
                        .collect(),
                    generation,
                    draining,
                },
            );
        }
        Self {
            routes,
            shards_per_stream,
        }
    }

    /// How many shards this stream was placed with, for routing.
    ///
    /// `1` when the table has never heard of the stream, which is the answer a
    /// publish needs: an unplaced stream has one shard as far as routing is
    /// concerned, and `shard_for` sends every key to shard 0.
    ///
    /// **Not the answer to give a client.** That fallback makes an unknown
    /// stream indistinguishable from a single-shard one, and a client told
    /// "one shard" reads shard 0 and calls it the stream. Use
    /// [`RoutingTable::placed_shards_for`] where the answer leaves the broker.
    pub fn shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> u32 {
        self.placed_shards_for(kind, tenant_id, namespace, stream)
            .unwrap_or(1)
    }

    /// How many shards this stream was placed with, or `None` if this table has
    /// never heard of it.
    ///
    /// The distinction [`RoutingTable::shards_for`] deliberately collapses: "I do
    /// not know this stream" and "exactly one shard" are different answers, and
    /// only one of them is safe to act on.
    pub fn placed_shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<u32> {
        let id = stream_id(kind, tenant_id, namespace, stream);
        // A placement of zero shards is not a placement; treating it as unknown
        // keeps every caller from having to special-case a count it cannot use.
        self.shards_per_stream
            .get(&id)
            .copied()
            .filter(|shards| *shards > 0)
    }

    pub fn get(&self, key: &ShardKey) -> Option<&Route> {
        self.routes.get(key)
    }

    /// Every route in the table.
    ///
    /// For a caller that has to visit shards rather than look one up -- the
    /// replication driver walks the table to find the shards this node leads.
    pub fn iter(&self) -> impl Iterator<Item = (&ShardKey, &Route)> {
        self.routes.iter()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

/// A node a shard can be served from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRef {
    pub node_id: String,
    pub advertise_addr: SocketAddr,
    pub region: String,
    /// Whether the cluster currently considers this node able to serve.
    pub live: bool,
}

/// Where one shard is served, as of the snapshot that carried it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub leader: NodeRef,
    pub replicas: Vec<NodeRef>,
    pub generation: u64,
    /// The leader has been told to stop serving at this generation so the
    /// shard can move. It still leads for replication: the followers are
    /// caught up from it before anyone else takes over.
    pub draining: bool,
}

/// One assignment as the table takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub key: ShardKey,
    pub leader: String,
    pub replicas: Vec<String>,
    pub generation: u64,
    pub draining: bool,
}

/// The key `shards_per_stream` is built and looked up under.
///
/// One function so the two cannot disagree, and the kind leads because a cache
/// and a stream may share every other part of it.
fn stream_id(kind: ShardKind, tenant_id: &str, namespace: &str, stream: &str) -> String {
    let mut id = String::with_capacity(tenant_id.len() + namespace.len() + stream.len() + 4);
    id.push_str(kind.prefix());
    id.push('/');
    id.push_str(tenant_id);
    id.push('/');
    id.push_str(namespace);
    id.push('/');
    id.push_str(stream);
    id
}
