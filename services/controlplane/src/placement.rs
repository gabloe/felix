//! Deterministic shard placement.
//!
//! Rendezvous hashing (highest random weight): score every eligible node
//! against the shard, take the highest. Chosen over a consistent-hash ring
//! because it needs no ring state, no virtual-node tuning, and distributes
//! noticeably better at the handful-of-brokers scale a cluster actually starts
//! at — and because removing a node only moves the shards that node held.
//!
//! Everything here is a pure function of a metadata snapshot. That is what makes
//! "the same snapshot always yields the same placement" testable rather than
//! hoped for, and it keeps the algorithm out of the store.
//!
//! Two things are deliberately *not* here. There is no online rebalancing: an
//! assignment whose leader is still eligible is kept, however uneven that
//! leaves the cluster, because moving a shard costs a log handoff and v1 does
//! not have one. And `NodeCapacity::weight` is ignored — weighted rendezvous
//! needs a logarithm, and floating point that must agree bit-for-bit across
//! every control-plane instance is a bad foundation for a decision that has to
//! be identical everywhere.
use std::collections::HashMap;

use crate::model::{Node, NodeLifecycle, ShardAssignment, ShardKey, ShardState, Stream};

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
    Unplaceable(Unplaceable),
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

pub fn plan(
    streams: &[Stream],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
) -> Plan {
    let eligible: Vec<&Node> = {
        let mut live: Vec<&Node> = nodes
            .iter()
            .filter(|node| node.status.lifecycle == NodeLifecycle::Live)
            .collect();
        // Sorted so the tie-break below is stable regardless of input order.
        live.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        live
    };

    let current: HashMap<&ShardKey, &ShardAssignment> =
        existing.iter().map(|a| (&a.key, a)).collect();

    // Load counts every assignment we intend to exist after this pass, so a cap
    // is respected across kept and newly placed shards alike.
    let mut load: HashMap<&str, u32> = HashMap::new();
    for assignment in existing {
        if eligible
            .iter()
            .any(|node| node.node_id == assignment.leader)
        {
            *load.entry(assignment.leader.as_str()).or_default() += 1;
        }
    }

    // Keyed by the same string `stream_of` builds, so the lookup below cannot
    // disagree with the key it is derived from.
    let factors: HashMap<String, u32> = streams
        .iter()
        .map(|stream| {
            (
                format!(
                    "{}/{}/{}",
                    stream.tenant_id, stream.namespace, stream.stream
                ),
                stream.replication_factor.max(1),
            )
        })
        .collect();

    let mut keys: Vec<ShardKey> = streams
        .iter()
        .flat_map(|stream| {
            (0..stream.shards).map(move |shard| ShardKey {
                tenant_id: stream.tenant_id.clone(),
                namespace: stream.namespace.clone(),
                stream: stream.stream.clone(),
                shard,
            })
        })
        .collect();
    keys.sort_by(|a, b| order(a).cmp(&order(b)));

    let mut shards = Vec::with_capacity(keys.len());
    for key in keys {
        // An assignment whose leader is still live is kept. Rebalancing it would
        // cost a log handoff that does not exist yet, and churn the persisted
        // record for no gain.
        if let Some(existing) = current.get(&key)
            && eligible.iter().any(|node| node.node_id == existing.leader)
        {
            shards.push(ShardPlan {
                key,
                decision: Decision::Kept,
            });
            continue;
        }

        let replication_factor = factors.get(stream_of(&key).as_str()).copied().unwrap_or(1);

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
        {
            shards.push(ShardPlan {
                key,
                decision: Decision::Unplaceable(Unplaceable::NoCaughtUpReplica),
            });
            continue;
        }

        let decision = match choose(&key, &eligible, &load) {
            Some(leader) => {
                *load.entry(leader).or_default() += 1;
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
            score(key, &a.node_id)
                .cmp(&score(key, &b.node_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
        .map(|node| node.node_id.as_str())
}

/// The stream a shard belongs to, as `tenant/namespace/stream`.
fn stream_of(key: &ShardKey) -> String {
    format!("{}/{}/{}", key.tenant_id, key.namespace, key.stream)
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

/// Highest-scoring node with capacity left.
///
/// Ties break on `node_id`, which cannot itself tie: node identity is unique.
fn choose<'a>(key: &ShardKey, eligible: &[&'a Node], load: &HashMap<&str, u32>) -> Option<&'a str> {
    eligible
        .iter()
        .filter(|node| match node.spec.capacity.max_shards {
            Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
            None => true,
        })
        .max_by(|a, b| {
            score(key, &a.node_id)
                .cmp(&score(key, &b.node_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
        .map(|node| node.node_id.as_str())
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

    let shard = finalize(fnv1a(&[
        key.tenant_id.as_bytes(),
        key.namespace.as_bytes(),
        key.stream.as_bytes(),
        &key.shard.to_be_bytes(),
    ]));
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

fn order(key: &ShardKey) -> (&str, &str, &str, u32) {
    (&key.tenant_id, &key.namespace, &key.stream, key.shard)
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
    positions: &crate::replica_positions::ReplicaPositions,
) -> ReconcileOutcome {
    let (streams, nodes, existing) = match load(store).await {
        Ok(loaded) => loaded,
        Err(err) => {
            tracing::error!(error = %err, "could not read the catalog to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return ReconcileOutcome::default();
        }
    };

    // One instant for the whole pass, so a report cannot be fresh for one shard
    // and stale for the next within the same plan.
    let caught_up = crate::replica_positions::CaughtUpAt {
        positions,
        now_millis: crate::api::nodes::now_millis(),
    };
    let plan = plan(&streams, &nodes, &existing, &caught_up);
    let mut outcome = ReconcileOutcome {
        kept: plan.kept(),
        ..ReconcileOutcome::default()
    };

    for (key, leader, replicas) in plan.to_place() {
        match store
            .put_shard_assignment(assignment_for(key, leader, replicas.to_vec()))
            .await
        {
            Ok(assignment) => {
                outcome.placed += 1;
                tracing::info!(
                    stream = %key.stream,
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
                    stream = %key.stream,
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
            stream = %key.stream,
            shard = key.shard,
            reason = %reason,
            "shard has no eligible leader",
        );
    }

    metrics::counter!(SHARDS_PLACED_TOTAL).increment(outcome.placed as u64);
    metrics::gauge!(SHARDS_UNPLACEABLE).set(outcome.unplaceable as f64);
    outcome
}

async fn load(
    store: &dyn crate::store::ControlPlaneStore,
) -> crate::store::StoreResult<(Vec<Stream>, Vec<Node>, Vec<ShardAssignment>)> {
    let streams = store.stream_snapshot().await?.items;
    let nodes = store.list_nodes().await?;
    let existing = store.list_shard_assignments().await?;
    Ok((streams, nodes, existing))
}

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub placed: usize,
    pub kept: usize,
    pub unplaceable: usize,
    pub failed: usize,
}

/// Shards assigned a leader.
pub const SHARDS_PLACED_TOTAL: &str = "felix_shards_placed_total";
/// Shards with no eligible leader right now. Non-zero means capacity or
/// liveness needs attention.
pub const SHARDS_UNPLACEABLE: &str = "felix_shards_unplaceable";
/// Passes that could not read the catalog at all.
pub const RECONCILE_FAILURES_TOTAL: &str = "felix_shard_reconcile_failures_total";

/// Place shards on an interval until `shutdown` fires.
pub fn spawn_reconciler(
    store: std::sync::Arc<dyn crate::store::ControlPlaneStore + Send + Sync>,
    positions: std::sync::Arc<crate::replica_positions::ReplicaPositions>,
    interval: std::time::Duration,
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
                    reconcile_once(store.as_ref(), positions.as_ref()).await;
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "placement_tests.rs"]
mod tests;
