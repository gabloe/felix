//! Where a shard lives, resolved without touching the control plane.
//!
//! The publish path asks this on every request, so a lookup must not take a
//! lock a writer can hold and must never make a network call. Routes are
//! published as an immutable snapshot and swapped in whole: readers take a
//! cheap atomic load and read a table nobody can mutate underneath them, and an
//! update costs one pointer swap regardless of how many readers are in flight.
//!
//! Every outcome is explicit. There is deliberately no "not sure, handle it
//! locally": a broker that treats an unknown route as its own is a broker
//! writing a shard it does not own.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::RegionRouter;

/// One shard of one stream.
///
/// Defined here rather than shared with the control plane or the storage layer,
/// both of which have their own: the router is a library that must not depend on
/// either, and the duplication is three fields and a number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
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
}

/// Why a shard cannot be routed.
///
/// Separate variants because they need different responses: a missing
/// assignment waits for placement, a dead leader waits for failover, and a
/// blocked region is a policy decision that will not resolve on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unavailable {
    /// Placement has not assigned this shard.
    NoAssignment,
    /// The assignment names a node this router has no address for.
    LeaderUnknown { node_id: String },
    /// The leader is registered but not live.
    LeaderNotLive { node_id: String },
    /// Region policy forbids reaching the leader's region from here.
    RegionNotRoutable { region: String },
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAssignment => write!(f, "shard has no assignment"),
            Self::LeaderUnknown { node_id } => {
                write!(f, "leader {node_id} is not in the routing table")
            }
            Self::LeaderNotLive { node_id } => write!(f, "leader {node_id} is not live"),
            Self::RegionNotRoutable { region } => {
                write!(f, "region {region} is not routable from here")
            }
        }
    }
}

/// What to do with a request for a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// This node leads the shard. Handle it here.
    Local {
        generation: u64,
    },
    /// Another node leads it. M4 forwards; until then this is a typed refusal
    /// rather than an error, so the two are distinguishable.
    Remote {
        node_id: String,
        advertise_addr: SocketAddr,
        generation: u64,
    },
    /// The caller's view of the assignment is newer than this router's.
    ///
    /// Distinct from unavailable: the route is not wrong, this node's copy is
    /// behind, and the answer is to wait for the watch rather than to fail the
    /// stream.
    Stale {
        have: u64,
        wanted: u64,
    },
    Unavailable(Unavailable),
}

impl Resolution {
    pub fn is_local(&self) -> bool {
        matches!(self, Resolution::Local { .. })
    }
}

/// An immutable set of routes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RoutingTable {
    routes: HashMap<ShardKey, Route>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a table from assignments and the nodes they name.
    ///
    /// An assignment whose leader is absent from `nodes` still produces a route,
    /// carrying [`Unavailable::LeaderUnknown`] at lookup time. Dropping it here
    /// instead would make a missing address indistinguishable from a missing
    /// assignment, and those are different problems.
    pub fn build(
        assignments: impl IntoIterator<Item = (ShardKey, String, Vec<String>, u64)>,
        nodes: &HashMap<String, NodeRef>,
    ) -> Self {
        let mut routes = HashMap::new();
        for (key, leader, replicas, generation) in assignments {
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
                },
            );
        }
        Self { routes }
    }

    pub fn get(&self, key: &ShardKey) -> Option<&Route> {
        self.routes.get(key)
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

/// Resolves shards to nodes, hot-path cheap and updated by whole-table swap.
#[derive(Debug)]
pub struct ShardRouter {
    local_node_id: String,
    local_region: String,
    table: ArcSwap<RoutingTable>,
    /// Which regions this node may reach. Policy, not placement.
    regions: RegionRouter<String>,
    /// Node ids this router has ever been told about, so an assignment naming
    /// one it has never seen is reported as unknown rather than not live.
    known_nodes: ArcSwap<std::collections::HashSet<String>>,
}

impl ShardRouter {
    pub fn new(
        local_node_id: impl Into<String>,
        local_region: impl Into<String>,
        regions: RegionRouter<String>,
    ) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            local_region: local_region.into(),
            table: ArcSwap::from_pointee(RoutingTable::new()),
            regions,
            known_nodes: ArcSwap::from_pointee(std::collections::HashSet::new()),
        }
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Replace every route in one swap.
    ///
    /// Whole-table rather than per-shard: a partial update would let a reader
    /// see half of a rebalance, and routes are small enough that rebuilding is
    /// cheaper than reasoning about that.
    pub fn publish(&self, table: RoutingTable, nodes: &HashMap<String, NodeRef>) {
        self.known_nodes
            .store(Arc::new(nodes.keys().cloned().collect()));
        self.table.store(Arc::new(table));
    }

    /// The current table, for a caller that wants to read several routes
    /// against one consistent snapshot.
    pub fn snapshot(&self) -> Arc<RoutingTable> {
        self.table.load_full()
    }
}

/// Whether this node may store records another broker replicates to it.
///
/// Separate from [`Resolution`], which answers "who serves reads and writes".
/// A follower serves neither and still has to store, so the two questions have
/// different answers for the same shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaRole {
    /// This node is in the shard's replica set at the epoch the leader named.
    Follower,
    /// The leader named an epoch older than this node's. It has been
    /// superseded, and a superseded leader's records must not be stored: it may
    /// have written them after losing the shard.
    Fenced { have: u64, named: u64 },
    /// This node's view is older than the epoch named. Its copy is behind, so
    /// it cannot yet tell whether it is a replica at that epoch.
    Behind { have: u64, named: u64 },
    /// This node is not in the shard's replica set. Includes leading it: a
    /// leader does not replicate to itself.
    NotAReplica,
}

impl ShardRouter {
    /// Whether this node should store records for `key` shipped at `generation`.
    pub fn replica_role(&self, key: &ShardKey, generation: u64) -> ReplicaRole {
        let table = self.table.load();
        let Some(route) = table.get(key) else {
            // Nothing known about the shard at all, so this node cannot confirm
            // membership. Behind rather than NotAReplica: the assignment may
            // simply not have arrived, and refusing permanently would strand a
            // follower whose watch is a moment late.
            return ReplicaRole::Behind {
                have: 0,
                named: generation,
            };
        };
        if generation < route.generation {
            return ReplicaRole::Fenced {
                have: route.generation,
                named: generation,
            };
        }
        if generation > route.generation {
            return ReplicaRole::Behind {
                have: route.generation,
                named: generation,
            };
        }
        if route
            .replicas
            .iter()
            .any(|replica| replica.node_id == self.local_node_id)
        {
            ReplicaRole::Follower
        } else {
            ReplicaRole::NotAReplica
        }
    }
}

impl ShardRouter {
    /// Resolve a shard against the current table.
    pub fn resolve(&self, key: &ShardKey) -> Resolution {
        self.resolve_in(&self.table.load(), key, None)
    }

    /// Resolve a shard, rejecting a table older than the caller already knows
    /// about.
    ///
    /// The caller has seen generation `wanted`; if this router is behind, the
    /// honest answer is that its copy is stale, not that the shard is somewhere
    /// it has since moved from.
    pub fn resolve_at(&self, key: &ShardKey, wanted: u64) -> Resolution {
        self.resolve_in(&self.table.load(), key, Some(wanted))
    }

    fn resolve_in(&self, table: &RoutingTable, key: &ShardKey, wanted: Option<u64>) -> Resolution {
        let Some(route) = table.get(key) else {
            // A caller expecting a generation for a shard this table has never
            // heard of is ahead of us, not looking at a missing assignment.
            return match wanted {
                Some(wanted) => Resolution::Stale { have: 0, wanted },
                None => Resolution::Unavailable(Unavailable::NoAssignment),
            };
        };

        if let Some(wanted) = wanted
            && route.generation < wanted
        {
            return Resolution::Stale {
                have: route.generation,
                wanted,
            };
        }

        // Local first: this node's own liveness is not something it needs the
        // catalog's opinion on, and a broker that stopped serving its own shards
        // because it had not yet seen its own heartbeat land would be absurd.
        if route.leader.node_id == self.local_node_id {
            return Resolution::Local {
                generation: route.generation,
            };
        }

        if !self.known_nodes.load().contains(&route.leader.node_id) {
            return Resolution::Unavailable(Unavailable::LeaderUnknown {
                node_id: route.leader.node_id.clone(),
            });
        }
        if !route.leader.live {
            return Resolution::Unavailable(Unavailable::LeaderNotLive {
                node_id: route.leader.node_id.clone(),
            });
        }
        // Policy last: a blocked region is a real refusal, but saying so about a
        // leader that is also dead would be the less useful of the two facts.
        if !self
            .regions
            .can_route(&self.local_region, &route.leader.region)
        {
            return Resolution::Unavailable(Unavailable::RegionNotRoutable {
                region: route.leader.region.clone(),
            });
        }

        Resolution::Remote {
            node_id: route.leader.node_id.clone(),
            advertise_addr: route.leader.advertise_addr,
            generation: route.generation,
        }
    }
}

#[cfg(test)]
#[path = "shard_tests.rs"]
mod tests;
