//! Deterministic shard placement.
//!
//! **Bounded-load rendezvous hashing.** Score every eligible node against the
//! shard and take the highest — but skip a node already carrying its balanced
//! share, so the shard spills to the next node under its share. Rendezvous is
//! chosen over a consistent-hash ring because it needs no ring state, no
//! virtual-node tuning, and removing a node only moves the shards that node
//! held; the load bound is added because plain highest-random-weight balances
//! only in the limit of many keys, and a cluster starts at a handful of shards
//! (24 over three brokers skewed 11/5/8) where the bound is the difference
//! between even use and single-node saturation. See `choose`.
//!
//! Everything here is a pure function of a metadata snapshot. That is what makes
//! "the same snapshot always yields the same placement" testable rather than
//! hoped for, and it keeps the algorithm out of the store.
//!
//! **Moves.** A shard whose leader is alive is moved, not reassigned: the
//! destination is staged as a replica, caught up, and made leader only after
//! the old leader has stopped and said so. Each step is an assignment write,
//! so any instance resumes a half-done move from the store. See `move_step`
//! and `docs/replication-design.md`. Two triggers: a draining node gives up
//! what it leads, and a node over its share gives shards to one under it,
//! bounded by `MovePolicy`.
//!
//! `NodeCapacity::weight` is ignored: weighted rendezvous needs a logarithm,
//! and floating point that must agree bit-for-bit across instances is a bad
//! foundation for a decision that has to be identical everywhere.
mod replica_positions;

pub use replica_positions::ReplicaPositions;

use std::collections::HashMap;

use crate::model::{
    Cache, Node, NodeLifecycle, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
};

/// Why a shard could not be placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unplaceable {
    /// No node is live.
    NoEligibleNode,
    /// Every live node is at its `max_shards` cap.
    AllNodesAtCapacity,
    /// The shard was replicated, its leader is gone, and no replica that holds
    /// the log can take over.
    ///
    /// Deliberately unavailable rather than placed elsewhere. A node that has
    /// never seen the shard would serve an empty log at a new generation while
    /// the records sat on replicas that were not chosen — the failover would
    /// *be* the data loss, and nothing downstream would report it as one. This
    /// is visible, and it resolves on its own when a replica catches up or the
    /// old leader returns.
    NoCaughtUpReplica,
}

impl std::fmt::Display for Unplaceable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEligibleNode => write!(f, "no live node is available to lead this shard"),
            Self::AllNodesAtCapacity => {
                write!(f, "every live node is at its max_shards capacity")
            }
            Self::NoCaughtUpReplica => write!(
                f,
                "the leader is gone and no replica holding this shard's log can take over"
            ),
        }
    }
}

/// What placement decided for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The existing assignment is still valid. Nothing to write.
    Kept,
    /// This shard needs an assignment written: a leader, and the followers that
    /// will hold a copy of it.
    Place(String, Vec<String>),
    /// One step of a planned move, as the assignment to write for it.
    Move(MoveStep, ShardAssignment),
    /// A move is in progress and this pass can do nothing for it yet.
    Waiting(Blocked),
    Unplaceable(Unplaceable),
}

/// The steps of a planned move. Each is one assignment write at a new
/// generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveStep {
    /// The destination joins the replica set as `successor`.
    Stage { successor: String },
    /// The destination is caught up: the assignment goes `Draining` and the
    /// leader stops serving.
    Fence,
    /// The leader has stopped. `to` leads from the next generation.
    CutOver { from: String, to: String },
    /// The destination stopped being live before it led; its staging is undone.
    Abandon { successor: String },
    /// A follower on a draining node is replaced by one that is staying.
    Reseat { from: String, to: String },
}

impl MoveStep {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Stage { .. } => "stage",
            Self::Fence => "fence",
            Self::CutOver { .. } => "cut_over",
            Self::Abandon { .. } => "abandon",
            Self::Reseat { .. } => "reseat",
        }
    }
}

/// Why a move could not advance this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocked {
    /// The successor is a replica but has not reported caught up.
    DestinationCatchingUp { successor: String },
    /// The assignment is `Draining` and the leader has not reported drained.
    LeaderStopping,
    /// A move is wanted, and `MovePolicy::max_concurrent` is reached.
    MoveLimit,
    /// The leader is draining and no live node can take the shard.
    NoDestination,
}

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DestinationCatchingUp { successor } => {
                write!(f, "waiting for {successor} to catch up")
            }
            Self::LeaderStopping => write!(f, "waiting for the leader to stop serving"),
            Self::MoveLimit => write!(f, "waiting for a move slot"),
            Self::NoDestination => write!(f, "no live node can take this shard"),
        }
    }
}

/// How many moves may be in progress at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MovePolicy {
    /// Cluster-wide. A move holds a slot from staging to cut-over. `0` starts
    /// nothing: a drain waits and an imbalance stays, both visibly.
    pub max_concurrent: usize,
}

/// One at a time, like the broker's rebuild limit: a move is a full copy of
/// a shard's log.
pub const DEFAULT_MAX_CONCURRENT_MOVES: usize = 1;

impl Default for MovePolicy {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT_MOVES,
        }
    }
}

/// One shard's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPlan {
    pub key: ShardKey,
    pub decision: Decision,
}

/// The full result of one reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub shards: Vec<ShardPlan>,
}

impl Plan {
    pub fn to_place(&self) -> impl Iterator<Item = (&ShardKey, &str, &[String])> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Place(leader, replicas) => {
                Some((&plan.key, leader.as_str(), replicas.as_slice()))
            }
            _ => None,
        })
    }

    pub fn unplaceable(&self) -> impl Iterator<Item = (&ShardKey, &Unplaceable)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Unplaceable(reason) => Some((&plan.key, reason)),
            _ => None,
        })
    }

    pub fn kept(&self) -> usize {
        self.shards
            .iter()
            .filter(|plan| plan.decision == Decision::Kept)
            .count()
    }

    pub fn moves(&self) -> impl Iterator<Item = (&ShardKey, &MoveStep, &ShardAssignment)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Move(step, assignment) => Some((&plan.key, step, assignment)),
            _ => None,
        })
    }

    pub fn waiting(&self) -> impl Iterator<Item = (&ShardKey, &Blocked)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Waiting(reason) => Some((&plan.key, reason)),
            _ => None,
        })
    }
}

/// Decide where every shard of every stream belongs.
///
/// Independent of the order `streams`, `nodes`, and `existing` arrive in: shards
/// are planned in sorted order and candidates are chosen by score, so two
/// control-plane instances reading the same rows agree without coordinating.
/// Which replicas hold enough of a shard's log to lead it.
///
/// Promotion is gated on this, and the gate is the difference between a failover
/// and data loss: a replica that holds nothing can be promoted perfectly well
/// and will serve an empty shard.
pub trait CaughtUp {
    /// Whether `node_id` is within the catch-up bound for `key`.
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool;

    /// How far `node_id` had got, as last reported.
    ///
    /// Used to choose *between* caught-up replicas. "Caught up" is only ever
    /// true of the tail it was measured against, so a report made before the
    /// leader's last writes can call two replicas level when one holds more.
    /// Preferring the higher offset picks the replica a quorum-acknowledged
    /// record is guaranteed to be on.
    ///
    /// `None` means nothing is known, which orders below any known offset.
    fn reported_offset(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
        None
    }

    /// Whether the leader of `key` has reported, at exactly `generation`, that
    /// it has stopped serving and its log will not grow. A report from an
    /// earlier generation describes a leader that was still writing.
    fn is_drained(&self, _key: &ShardKey, _generation: u64) -> bool {
        false
    }

    /// The generation the report for `key` was made at, if there is a fresh
    /// one.
    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        None
    }
}

/// `inner`, believed only where its report is from `generation`.
///
/// A move is decided on the assignment's own generation. The report the store
/// holds may be the previous leader's, still fresh and still listing the
/// replicas it had level -- and a fence made on that would stop a leader for
/// a destination that may hold nothing of what it has written since.
struct AtGeneration<'a> {
    inner: &'a dyn CaughtUp,
    generation: u64,
}

impl AtGeneration<'_> {
    fn current(&self, key: &ShardKey) -> bool {
        self.inner.reported_generation(key) == Some(self.generation)
    }
}

impl CaughtUp for AtGeneration<'_> {
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.current(key) && self.inner.is_caught_up(key, node_id)
    }

    fn reported_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.reported_offset(key, node_id))
            .flatten()
    }

    fn is_drained(&self, key: &ShardKey, generation: u64) -> bool {
        self.inner.is_drained(key, generation)
    }

    fn reported_generation(&self, key: &ShardKey) -> Option<u64> {
        self.inner.reported_generation(key)
    }
}

/// Nothing is caught up.
///
/// What the cluster can honestly report until records are actually replicated
/// (#112). With this, promotion never fires and placement behaves exactly as it
/// did — which is correct, because a promotion today would hand the shard to a
/// broker holding none of it.
pub struct NothingCaughtUp;

impl CaughtUp for NothingCaughtUp {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        false
    }
}

/// One thing with shards to place, viewed the only way placement cares about.
///
/// Streams and caches are placed by the same algorithm because they are the
/// same log underneath. Collapsing them here rather than running two passes is
/// what makes the `max_shards` cap and the load counting apply across both — two
/// passes would each believe it had the whole cluster to itself.
#[derive(Debug, Clone, Copy)]
struct Placeable<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    name: &'a str,
    kind: ShardKind,
    shards: u32,
    replication_factor: u32,
    /// Whether there is a log to hand off. An ephemeral stream is reassigned
    /// outright.
    durable: bool,
}

impl<'a> Placeable<'a> {
    fn of_stream(stream: &'a Stream) -> Self {
        Self {
            tenant_id: &stream.tenant_id,
            namespace: &stream.namespace,
            name: &stream.stream,
            kind: ShardKind::Stream,
            shards: stream.shards,
            replication_factor: stream.replication_factor.max(1),
            durable: stream.durable,
        }
    }

    fn of_cache(cache: &'a Cache) -> Self {
        Self {
            tenant_id: &cache.tenant_id,
            namespace: &cache.namespace,
            name: &cache.cache,
            kind: ShardKind::Cache,
            shards: cache.shards,
            replication_factor: cache.replication_factor.max(1),
            // A cache is durable wherever the broker is; assume it is.
            durable: true,
        }
    }

    fn key(&self, shard: u32) -> ShardKey {
        ShardKey {
            tenant_id: self.tenant_id.to_string(),
            namespace: self.namespace.to_string(),
            stream: self.name.to_string(),
            shard,
            kind: self.kind,
        }
    }
}

pub fn plan(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
) -> Plan {
    plan_with(
        streams,
        caches,
        nodes,
        existing,
        caught_up,
        MovePolicy::default(),
    )
}

/// [`plan`] under an explicit move policy.
pub fn plan_with(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
    policy: MovePolicy,
) -> Plan {
    let placeables: Vec<Placeable<'_>> = streams
        .iter()
        .map(Placeable::of_stream)
        .chain(caches.iter().map(Placeable::of_cache))
        .collect();

    // A live node may be given shards. A draining node keeps serving what it
    // has until each shard is moved off it, so its assignments go through the
    // move path, not the failover path.
    let eligible: Vec<&Node> = {
        let mut live: Vec<&Node> = nodes
            .iter()
            .filter(|node| node.status.lifecycle == NodeLifecycle::Live)
            .collect();
        // Sorted so the tie-break below is stable regardless of input order.
        live.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        live
    };
    let is_live = |id: &str| eligible.iter().any(|node| node.node_id == id);
    let is_draining = |id: &str| {
        nodes
            .iter()
            .any(|node| node.node_id == id && node.status.lifecycle == NodeLifecycle::Draining)
    };
    let is_serving = |id: &str| is_live(id) || is_draining(id);

    let current: HashMap<&ShardKey, &ShardAssignment> =
        existing.iter().map(|a| (&a.key, a)).collect();

    // Load counts every assignment we intend to exist after this pass, so a
    // cap is respected across kept and newly placed shards alike.
    let mut load: HashMap<&str, u32> = HashMap::new();
    // Leaders per live node as they will stand once every move in flight
    // completes: a staged successor already counts. Otherwise an over-share
    // node stages more moves than it needs and the destination ends up over.
    let mut leaders: HashMap<&str, u32> = HashMap::new();
    for assignment in existing {
        if is_live(&assignment.leader) {
            *load.entry(assignment.leader.as_str()).or_default() += 1;
        }
        let will_lead = assignment
            .successor
            .as_deref()
            .filter(|successor| is_live(successor))
            .unwrap_or(assignment.leader.as_str());
        if is_live(will_lead) {
            *leaders.entry(will_lead).or_default() += 1;
        }
    }

    // Keyed by the same string `owner_of` builds, so the lookup below cannot
    // disagree with the key it is derived from.
    let placeable_of: HashMap<String, &Placeable<'_>> = placeables
        .iter()
        .map(|placeable| {
            (
                format!(
                    "{}/{}/{}/{}",
                    placeable.kind, placeable.tenant_id, placeable.namespace, placeable.name
                ),
                placeable,
            )
        })
        .collect();

    let mut keys: Vec<ShardKey> = placeables
        .iter()
        .flat_map(|placeable| (0..placeable.shards).map(move |shard| placeable.key(shard)))
        .collect();
    keys.sort_by(|a, b| order(a).cmp(&order(b)));

    // The balanced share for bounded-load rendezvous: total roles (leaders and
    // replicas) spread evenly across the eligible nodes. Counting replicas keeps
    // a replicated stream's replica load from starving a node of leadership.
    let total_roles: u32 = placeables
        .iter()
        .map(|p| p.shards.saturating_mul(p.replication_factor.max(1)))
        .sum();
    let cap = if eligible.is_empty() {
        u32::MAX
    } else {
        total_roles.div_ceil(eligible.len() as u32).max(1)
    };
    // Fair share of leadership, for the rebalance trigger. Leaders only: that
    // is the load a client feels.
    let leader_share = if eligible.is_empty() {
        u32::MAX
    } else {
        (keys.len() as u32).div_ceil(eligible.len() as u32).max(1)
    };

    let mut moves = Moves {
        in_flight: existing
            .iter()
            .filter(|a| a.successor.is_some() || a.state == ShardState::Draining)
            .count(),
        policy,
    };

    let mut shards = Vec::with_capacity(keys.len());
    for key in keys {
        let placeable = placeable_of.get(owner_of(&key).as_str()).copied();
        let replication_factor = placeable.map_or(1, |p| p.replication_factor);
        let durable = placeable.is_none_or(|p| p.durable);

        // The leader is still serving: a move, not a reassignment, unless
        // there is no log to move.
        if let Some(existing) = current.get(&key)
            && is_serving(&existing.leader)
            && (durable || is_live(&existing.leader))
        {
            let decision = move_step(
                &key,
                existing,
                replication_factor,
                &eligible,
                &is_live,
                &is_draining,
                caught_up,
                &mut load,
                &mut leaders,
                leader_share,
                &mut moves,
            );
            shards.push(ShardPlan { key, decision });
            continue;
        }

        // The leader is gone. Prefer one of its followers -- but only one that
        // actually holds the log, or the failover is the data loss.
        if let Some(previous) = current.get(&key)
            && let Some(promoted) = promote(&key, previous, &eligible, caught_up)
        {
            *load.entry(promoted).or_default() += 1;
            let replicas = choose_replicas(
                &key,
                &eligible,
                &mut load,
                promoted,
                replication_factor.saturating_sub(1),
            );
            shards.push(ShardPlan {
                key,
                decision: Decision::Place(promoted.to_string(), replicas),
            });
            continue;
        }

        // The shard was replicated and nothing that holds it can lead. Placing
        // it on a node that has never seen it is not a failover, it is a
        // silently empty shard: the records stay on the replicas, unreachable,
        // while a new leader serves nothing at a newer generation.
        //
        // A stream that never asked for replication is untouched by this — it
        // has no replicas, so there was never a copy to prefer, and a fresh
        // placement remains the only thing available.
        if let Some(previous) = current.get(&key)
            && !previous.replicas.is_empty()
            && durable
        {
            shards.push(ShardPlan {
                key,
                decision: Decision::Unplaceable(Unplaceable::NoCaughtUpReplica),
            });
            continue;
        }

        let decision = match choose(&key, &eligible, &load, cap, &leaders, leader_share) {
            Some(leader) => {
                *load.entry(leader).or_default() += 1;
                *leaders.entry(leader).or_default() += 1;
                // Followers are the next best-scoring nodes for this shard,
                // excluding the leader. Chosen by the same score so the whole
                // replica set is a deterministic function of the shard key and
                // the cluster -- two control-plane instances planning the same
                // cluster produce the same set.
                let replicas = choose_replicas(
                    &key,
                    &eligible,
                    &mut load,
                    leader,
                    replication_factor.saturating_sub(1),
                );
                Decision::Place(leader.to_string(), replicas)
            }
            None if eligible.is_empty() => Decision::Unplaceable(Unplaceable::NoEligibleNode),
            None => Decision::Unplaceable(Unplaceable::AllNodesAtCapacity),
        };
        shards.push(ShardPlan { key, decision });
    }

    Plan { shards }
}

/// The move slots one pass hands out.
struct Moves {
    in_flight: usize,
    policy: MovePolicy,
}

impl Moves {
    /// Take a slot for a new move, if one is free.
    fn begin(&mut self) -> bool {
        if self.in_flight < self.policy.max_concurrent {
            self.in_flight += 1;
            true
        } else {
            false
        }
    }
}

/// What to do about a shard whose leader is still serving.
///
/// Stage the destination as a replica; fence once it is caught up, which
/// stops the leader; cut over once the leader reports drained. A leader that
/// dies mid-move is handled by the failover path, where the successor is a
/// candidate like any other replica. Every input is in the store or a fresh
/// report, so any pass resumes where the last left off.
#[allow(clippy::too_many_arguments)]
fn move_step<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    is_draining: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    load: &mut HashMap<&'a str, u32>,
    leaders: &mut HashMap<&'a str, u32>,
    leader_share: u32,
    moves: &mut Moves,
) -> Decision {
    let leader = existing.leader.as_str();
    let leader_live = is_live(leader);
    let caught_up = &AtGeneration {
        inner: caught_up,
        generation: existing.generation,
    };
    // The maps are keyed by catalog-lifetime strings.
    let catalog_id = |id: &str| -> Option<&'a str> {
        eligible
            .iter()
            .find(|node| node.node_id == id)
            .map(|node| node.node_id.as_str())
    };

    if existing.state == ShardState::Draining {
        if !caught_up.is_drained(key, existing.generation) {
            return Decision::Waiting(Blocked::LeaderStopping);
        }
        // Whoever leads next must hold everything the leader held.
        let target = existing
            .successor
            .as_deref()
            .filter(|successor| is_live(successor) && caught_up.is_caught_up(key, successor))
            .or_else(|| promote(key, existing, eligible, caught_up))
            // Nothing else holds the log; the leader takes it back at a new
            // generation and the move is chosen again.
            .unwrap_or(leader);
        let replicas = replicas_after_cut_over(
            key,
            existing,
            target,
            replication_factor,
            eligible,
            is_live,
            load,
        );
        if let Some(from) = catalog_id(leader) {
            leaders
                .entry(from)
                .and_modify(|count| *count = count.saturating_sub(1));
        }
        if let Some(to) = catalog_id(target) {
            *leaders.entry(to).or_default() += 1;
        }
        return Decision::Move(
            MoveStep::CutOver {
                from: leader.to_string(),
                to: target.to_string(),
            },
            ShardAssignment {
                key: key.clone(),
                leader: target.to_string(),
                replicas,
                generation: 0,
                state: ShardState::Assigning,
                successor: None,
            },
        );
    }

    if let Some(successor) = existing.successor.as_deref() {
        if !is_live(successor) {
            // Gone before it led. Undone rather than waited out, or the move
            // holds its slot for as long as the node is away.
            let mut replicas = existing.replicas.clone();
            replicas.retain(|replica| replica != successor);
            return Decision::Move(
                MoveStep::Abandon {
                    successor: successor.to_string(),
                },
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas,
                    generation: 0,
                    state: existing.state,
                    successor: None,
                },
            );
        }
        if caught_up.is_caught_up(key, successor) {
            return Decision::Move(
                MoveStep::Fence,
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas: existing.replicas.clone(),
                    generation: 0,
                    state: ShardState::Draining,
                    successor: Some(successor.to_string()),
                },
            );
        }
        return Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: successor.to_string(),
        });
    }

    // Nothing in progress. Should a move start?
    let over_share = leaders.get(leader).copied().unwrap_or(0) > leader_share;
    let wanted = if !leader_live {
        // Draining: a caught-up live replica under its share is cheapest, the
        // copy already exists; otherwise the bounded choice, or any live node
        // with room.
        promote_under_share(key, existing, eligible, caught_up, leaders, leader_share)
            .or_else(|| choose_destination(key, eligible, leaders, leader_share, leader, false))
    } else if leader_live && over_share {
        // Only from over share to under share, so moves converge instead of
        // trading shards back and forth.
        choose_destination(key, eligible, leaders, leader_share, leader, true)
    } else {
        None
    };

    match wanted {
        Some(destination) => {
            if !moves.begin() {
                return Decision::Waiting(Blocked::MoveLimit);
            }
            if let Some(from) = catalog_id(leader) {
                leaders
                    .entry(from)
                    .and_modify(|count| *count = count.saturating_sub(1));
            }
            *leaders.entry(destination).or_default() += 1;
            let already_a_replica = existing.replicas.iter().any(|r| r == destination);
            if already_a_replica && caught_up.is_caught_up(key, destination) {
                // The copy is already there and level: straight to the fence.
                return Decision::Move(
                    MoveStep::Fence,
                    ShardAssignment {
                        key: key.clone(),
                        leader: leader.to_string(),
                        replicas: existing.replicas.clone(),
                        generation: 0,
                        state: ShardState::Draining,
                        successor: Some(destination.to_string()),
                    },
                );
            }
            let mut replicas = existing.replicas.clone();
            if !already_a_replica {
                replicas.push(destination.to_string());
                *load.entry(destination).or_default() += 1;
            }
            Decision::Move(
                MoveStep::Stage {
                    successor: destination.to_string(),
                },
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas,
                    generation: 0,
                    state: existing.state,
                    successor: Some(destination.to_string()),
                },
            )
        }
        None if !leader_live => Decision::Waiting(Blocked::NoDestination),
        None => reseat(key, existing, eligible, is_draining, load, moves),
    }
}

/// Replace a follower on a draining node with one that is staying.
///
/// Draining only, never merely down: a rolling restart takes every node down
/// in turn, and reseating each would copy every shard once per restart.
fn reseat<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    is_draining: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Decision {
    let Some(departing) = existing
        .replicas
        .iter()
        .find(|replica| is_draining(replica))
    else {
        return Decision::Kept;
    };
    // Swapped only when someone can take its place; dropping it costs a copy.
    let mut taken: Vec<&str> = existing.nodes().map(String::as_str).collect();
    taken.retain(|node| node != departing);
    let Some(replacement) = eligible
        .iter()
        .filter(|node| !taken.contains(&node.node_id.as_str()))
        .filter(|node| match node.spec.capacity.max_shards {
            Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
            None => true,
        })
        .max_by(|a, b| {
            score(key, &a.node_id)
                .cmp(&score(key, &b.node_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
    else {
        return Decision::Kept;
    };
    if !moves.begin() {
        return Decision::Waiting(Blocked::MoveLimit);
    }
    *load.entry(replacement.node_id.as_str()).or_default() += 1;
    let replicas = existing
        .replicas
        .iter()
        .map(|replica| {
            if replica == departing {
                replacement.node_id.clone()
            } else {
                replica.clone()
            }
        })
        .collect();
    Decision::Move(
        MoveStep::Reseat {
            from: departing.clone(),
            to: replacement.node_id.clone(),
        },
        ShardAssignment {
            key: key.clone(),
            leader: existing.leader.clone(),
            replicas,
            generation: 0,
            state: existing.state,
            successor: None,
        },
    )
}

/// The replica set once `target` leads: nodes that already hold a copy first
/// (the old leader if it is staying, then live followers), topped up by
/// score, and cut to the replication factor.
fn replicas_after_cut_over<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    target: &str,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
) -> Vec<String> {
    let wanted = replication_factor.saturating_sub(1) as usize;
    let mut replicas: Vec<String> = existing
        .nodes()
        .filter(|node| node.as_str() != target && is_live(node))
        .cloned()
        .collect();
    replicas.truncate(wanted);
    if replicas.len() < wanted {
        let taken: Vec<String> = replicas.clone();
        let mut chosen = choose_replicas(
            key,
            eligible,
            load,
            target,
            (wanted - replicas.len()) as u32,
        );
        // `choose_replicas` does not know about the ones kept above.
        chosen.retain(|node| !taken.contains(node));
        replicas.extend(chosen);
        replicas.truncate(wanted);
    }
    replicas
}

/// A caught-up live follower under its leadership share, furthest ahead first.
fn promote_under_share<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    caught_up: &dyn CaughtUp,
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
) -> Option<&'a str> {
    let under_share: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| leaders.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share)
        .collect();
    promote(key, existing, &under_share, caught_up)
}

/// The live node to move a shard to, by score, preferring nodes under their
/// leadership share. `balanced_only` answers `None` rather than spill over.
fn choose_destination<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
    exclude: &str,
    balanced_only: bool,
) -> Option<&'a str> {
    let candidates: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| node.node_id != exclude)
        .collect();
    let leader_load: HashMap<&str, u32> = candidates
        .iter()
        .map(|node| {
            (
                node.node_id.as_str(),
                leaders.get(node.node_id.as_str()).copied().unwrap_or(0),
            )
        })
        .collect();
    let under_share = candidates
        .iter()
        .copied()
        .filter(|node| leader_load.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share)
        .collect::<Vec<_>>();
    let pick = |from: &[&'a Node]| {
        from.iter()
            .copied()
            .filter(|node| match node.spec.capacity.max_shards {
                Some(max) => leader_load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
                None => true,
            })
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
            .map(|node| node.node_id.as_str())
    };
    if balanced_only {
        pick(&under_share)
    } else {
        pick(&under_share).or_else(|| pick(&candidates))
    }
}

/// The best eligible follower that is caught up, if any.
///
/// Ties break on score then node id, the same way leadership does, so the choice
/// is a deterministic function of the shard and the cluster.
fn promote<'a>(
    key: &ShardKey,
    previous: &ShardAssignment,
    eligible: &[&'a Node],
    caught_up: &dyn CaughtUp,
) -> Option<&'a str> {
    eligible
        .iter()
        .filter(|node| previous.replicas.iter().any(|r| r == &node.node_id))
        .filter(|node| caught_up.is_caught_up(key, &node.node_id))
        .max_by(|a, b| {
            // Furthest ahead first. Score only breaks ties, so a replica that
            // holds more is never passed over for one that merely scores
            // better — the failover would otherwise discard the difference.
            caught_up
                .reported_offset(key, &a.node_id)
                .cmp(&caught_up.reported_offset(key, &b.node_id))
                .then_with(|| score(key, &a.node_id).cmp(&score(key, &b.node_id)))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
        .map(|node| node.node_id.as_str())
}

/// The stream or cache a shard belongs to, as `kind/tenant/namespace/name`.
///
/// The kind leads because a cache and a stream may share every other field.
fn owner_of(key: &ShardKey) -> String {
    format!(
        "{}/{}/{}/{}",
        key.kind, key.tenant_id, key.namespace, key.stream
    )
}

/// The next best-scoring nodes for a shard, after the leader.
///
/// Fewer than `wanted` is normal and not an error: a three-node cluster cannot
/// hold four copies. Placement records what it could achieve rather than
/// claiming a replica set it did not create, because an assignment that names a
/// node holding nothing is exactly the lie failover would act on.
fn choose_replicas<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
    leader: &str,
    wanted: u32,
) -> Vec<String> {
    let mut chosen = Vec::new();
    for _ in 0..wanted {
        let Some(node) = eligible
            .iter()
            .filter(|node| node.node_id != leader)
            .filter(|node| !chosen.iter().any(|taken: &String| taken == &node.node_id))
            .filter(|node| match node.spec.capacity.max_shards {
                // A replica holds a copy, so it counts against capacity just as
                // leadership does.
                Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
                None => true,
            })
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
        else {
            break;
        };
        *load.entry(node.node_id.as_str()).or_default() += 1;
        chosen.push(node.node_id.clone());
    }
    chosen
}

/// Highest-scoring node that still keeps the cluster balanced.
///
/// **Bounded-load rendezvous.** Pure highest-random-weight balances only in the
/// limit of many keys; at the handful-of-shards scale a cluster actually starts
/// at, its variance is large — 24 shards on three nodes landed 11/5/8, and a
/// staggered start can funnel *everything* onto the first broker to register.
/// That is single-node saturation while the rest idle. So a shard goes to its
/// highest-scoring node *unless that node is already carrying its balanced share*
/// (`cap`), in which case it spills to the next-highest node under its share.
/// Locality is nearly preserved (a shard only moves off its top choice when that
/// node is full) and placement stays a deterministic function of the snapshot,
/// because `plan` walks the shards in sorted order and `load` accumulates the
/// same way on every instance. `None` only when no node has capacity at all — the
/// second pass drops the balance cap so a full-but-uncapped cluster still places.
///
/// Ties break on `node_id`, which cannot itself tie: node identity is unique.
fn choose<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    load: &HashMap<&str, u32>,
    cap: u32,
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
) -> Option<&'a str> {
    let has_capacity = |node: &Node| match node.spec.capacity.max_shards {
        Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
        None => true,
    };
    let under_share = |node: &Node| load.get(node.node_id.as_str()).copied().unwrap_or(0) < cap;
    // Leaders are bounded as well as roles. With a replication factor equal
    // to the node count every node holds a role for every shard, so the role
    // cap says nothing about who leads -- and a cluster placed with every
    // leader on one node would be moved apart again by the next pass.
    let under_leader_share =
        |node: &Node| leaders.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share;
    let best = |balanced: bool, led: bool| {
        eligible
            .iter()
            .filter(|node| has_capacity(node))
            .filter(|node| !balanced || under_share(node))
            .filter(|node| !led || under_leader_share(node))
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
    };
    best(true, true)
        .or_else(|| best(true, false))
        .or_else(|| best(false, false))
        .map(|n| n.node_id.as_str())
}

/// Score a (shard, node) pair.
///
/// The shard key and the node id are hashed independently and then mixed,
/// rather than run through one hash over the concatenation. Both converge to an
/// even split eventually, but the single-pass form is badly behaved at the size
/// a cluster actually is: over 300 shards on four nodes it put 43 on one and 94
/// on another, a 43% deviation, against 8% for this. The difference is entirely
/// in the small-sample behaviour, which is the only sample there is.
///
/// FNV-1a is written out rather than taken from `DefaultHasher`: the standard
/// hasher's seeding is not part of its contract, and a placement decision that
/// changed with the Rust version -- or differed between two control-plane
/// instances -- would be silently catastrophic.
fn score(key: &ShardKey, node_id: &str) -> u64 {
    /// Fractional part of the golden ratio, the usual choice for spreading a
    /// multiplicative mix across the whole word.
    const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

    // A stream contributes no kind field, so every score computed before caches
    // were placed is unchanged bit-for-bit: adding the field unconditionally
    // would reshuffle the placement of every stream in every fresh cluster for
    // no gain. A cache does contribute one, which is what stops a cache and a
    // stream of the same name from scoring identically and tracking each other
    // onto the same node forever.
    let shard = finalize(match key.kind {
        ShardKind::Stream => fnv1a(&[
            key.tenant_id.as_bytes(),
            key.namespace.as_bytes(),
            key.stream.as_bytes(),
            &key.shard.to_be_bytes(),
        ]),
        ShardKind::Cache => fnv1a(&[
            key.tenant_id.as_bytes(),
            key.namespace.as_bytes(),
            key.stream.as_bytes(),
            &key.shard.to_be_bytes(),
            key.kind.as_str().as_bytes(),
        ]),
    });
    let node = finalize(fnv1a(&[node_id.as_bytes()]));
    finalize(shard.wrapping_mul(GOLDEN) ^ node.rotate_left(32))
}

/// FNV-1a over several fields, with a separator between them so ("ab", "c") and
/// ("a", "bc") cannot collapse into the same byte stream.
fn fnv1a(fields: &[&[u8]]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for field in fields {
        for byte in *field {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// splitmix64's finalizer. FNV-1a alone avalanches poorly on short inputs, and
/// rendezvous hashing needs the high bits well mixed or placement skews.
fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Kind sorts last so the relative order of stream shards is what it always
/// was, and a cache sharing a stream's name is never an unstable tie.
fn order(key: &ShardKey) -> (&str, &str, &str, u32, ShardKind) {
    (
        &key.tenant_id,
        &key.namespace,
        &key.stream,
        key.shard,
        key.kind,
    )
}

/// Build the assignment a `Place` decision calls for.
///
/// New assignments start `Assigning`: placement has decided, and the broker has
/// not yet confirmed it is serving. Generation is the store's.
pub fn assignment_for(key: &ShardKey, leader: &str, replicas: Vec<String>) -> ShardAssignment {
    ShardAssignment {
        key: key.clone(),
        leader: leader.to_string(),
        replicas,
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
    }
}

/// Plan against the current catalog and write what the plan calls for.
///
/// Reads the whole catalog rather than a delta: placement is a function of the
/// snapshot, and reconstructing it incrementally would be a second
/// implementation of the same decision that could disagree with the first.
///
/// Idempotent. A pass over an already-placed cluster writes nothing, so running
/// it on a timer does not churn the persisted rows or the changefeed.
pub async fn reconcile_once(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
) -> ReconcileOutcome {
    let (streams, caches, nodes, existing) = match load(store).await {
        Ok(loaded) => loaded,
        Err(err) => {
            tracing::error!(error = %err, "could not read the catalog to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return ReconcileOutcome::default();
        }
    };

    // Read once, as of the store's clock: one instant for the whole pass, so a
    // report cannot be fresh for one shard and stale for the next within the
    // same plan, and the same clock the reports were stamped with.
    let caught_up = match ReplicaPositions::load(store, liveness).await {
        Ok(positions) => positions,
        Err(err) => {
            tracing::error!(error = %err, "could not read replica reports to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return ReconcileOutcome::default();
        }
    };
    let plan = plan_with(&streams, &caches, &nodes, &existing, &caught_up, policy);
    let mut outcome = ReconcileOutcome {
        kept: plan.kept(),
        ..ReconcileOutcome::default()
    };

    for (key, step, assignment) in plan.moves() {
        match store.put_shard_assignment(assignment.clone()).await {
            Ok(written) => {
                outcome.moved += 1;
                metrics::counter!(SHARD_MOVE_STEPS_TOTAL, "step" => step.label()).increment(1);
                tracing::info!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    step = step.label(),
                    detail = ?step,
                    leader = %written.leader,
                    successor = ?written.successor,
                    generation = written.generation,
                    "shard move advanced",
                );
            }
            Err(err) => {
                outcome.failed += 1;
                tracing::warn!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    step = step.label(),
                    error = %err,
                    "could not persist a shard move step; retrying next pass",
                );
            }
        }
    }

    for (key, reason) in plan.waiting() {
        outcome.waiting += 1;
        // Waiting on a catch-up or a drain is what a move in progress looks
        // like; the gauge below is what an operator watches.
        tracing::debug!(
            kind = %key.kind,
            name = %key.stream,
            shard = key.shard,
            reason = %reason,
            "shard move waiting",
        );
    }

    for (key, leader, replicas) in plan.to_place() {
        match store
            .put_shard_assignment(assignment_for(key, leader, replicas.to_vec()))
            .await
        {
            Ok(assignment) => {
                outcome.placed += 1;
                tracing::info!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    leader = %leader,
                    replicas = replicas.len(),
                    generation = assignment.generation,
                    "shard placed",
                );
            }
            Err(err) => {
                // One shard failing must not abandon the rest: the next pass
                // retries it, and the others are placed now rather than later.
                outcome.failed += 1;
                tracing::warn!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    leader = %leader,
                    error = %err,
                    "could not persist a shard placement; retrying next pass",
                );
            }
        }
    }

    for (key, reason) in plan.unplaceable() {
        outcome.unplaceable += 1;
        // Warn rather than error: an empty or full cluster is an operational
        // state to fix, not a control-plane fault.
        tracing::warn!(
            kind = %key.kind,
            name = %key.stream,
            shard = key.shard,
            reason = %reason,
            "shard has no eligible leader",
        );
    }

    metrics::counter!(SHARDS_PLACED_TOTAL).increment(outcome.placed as u64);
    metrics::gauge!(SHARDS_UNPLACEABLE).set(outcome.unplaceable as f64);
    metrics::gauge!(SHARD_MOVES_WAITING).set(outcome.waiting as f64);
    outcome
}

async fn load(
    store: &dyn crate::store::ControlPlaneStore,
) -> crate::store::StoreResult<(Vec<Stream>, Vec<Cache>, Vec<Node>, Vec<ShardAssignment>)> {
    let streams = store.stream_snapshot().await?.items;
    let caches = store.cache_snapshot().await?.items;
    let nodes = store.list_nodes().await?;
    let existing = store.list_shard_assignments().await?;
    Ok((streams, caches, nodes, existing))
}

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub placed: usize,
    pub kept: usize,
    pub unplaceable: usize,
    pub failed: usize,
    /// Move steps written.
    pub moved: usize,
    /// Moves that could not advance this pass.
    pub waiting: usize,
}

/// Shards assigned a leader.
pub const SHARDS_PLACED_TOTAL: &str = "felix_shards_placed_total";
/// Shards with no eligible leader right now. Non-zero means capacity or
/// liveness needs attention.
pub const SHARDS_UNPLACEABLE: &str = "felix_shards_unplaceable";
/// Passes that could not read the catalog at all.
pub const RECONCILE_FAILURES_TOTAL: &str = "felix_shard_reconcile_failures_total";
/// Move steps written, by step.
pub const SHARD_MOVE_STEPS_TOTAL: &str = "felix_shard_move_steps_total";
/// Moves that could not advance in the last pass.
pub const SHARD_MOVES_WAITING: &str = "felix_shard_moves_waiting";

/// Place shards on an interval until `shutdown` fires.
pub fn spawn_reconciler(
    store: std::sync::Arc<dyn crate::store::ControlPlaneStore + Send + Sync>,
    liveness: crate::config::NodeLivenessConfig,
    policy: MovePolicy,
    interval: std::time::Duration,
    gate: crate::raft::LeadershipGate,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // A pass that overruns must not then run back-to-back catching up.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    // Placement decides from what it reads; under Raft the
                    // gate's linearizable check also guarantees those reads
                    // are current before any assignment is proposed.
                    if !gate.holds().await {
                        continue;
                    }
                    reconcile_once(store.as_ref(), &liveness, policy).await;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests;
