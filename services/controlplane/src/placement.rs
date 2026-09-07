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
}

impl std::fmt::Display for Unplaceable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEligibleNode => write!(f, "no live node is available to lead this shard"),
            Self::AllNodesAtCapacity => {
                write!(f, "every live node is at its max_shards capacity")
            }
        }
    }
}

/// What placement decided for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The existing assignment is still valid. Nothing to write.
    Kept,
    /// This shard needs an assignment written, to this leader.
    Place(String),
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
    pub fn to_place(&self) -> impl Iterator<Item = (&ShardKey, &str)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Place(leader) => Some((&plan.key, leader.as_str())),
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
pub fn plan(streams: &[Stream], nodes: &[Node], existing: &[ShardAssignment]) -> Plan {
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

        let decision = match choose(&key, &eligible, &load) {
            Some(leader) => {
                *load.entry(leader).or_default() += 1;
                Decision::Place(leader.to_string())
            }
            None if eligible.is_empty() => Decision::Unplaceable(Unplaceable::NoEligibleNode),
            None => Decision::Unplaceable(Unplaceable::AllNodesAtCapacity),
        };
        shards.push(ShardPlan { key, decision });
    }

    Plan { shards }
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
pub fn assignment_for(key: &ShardKey, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: key.clone(),
        leader: leader.to_string(),
        replicas: Vec::new(),
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
pub async fn reconcile_once(store: &dyn crate::store::ControlPlaneStore) -> ReconcileOutcome {
    let (streams, nodes, existing) = match load(store).await {
        Ok(loaded) => loaded,
        Err(err) => {
            tracing::error!(error = %err, "could not read the catalog to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return ReconcileOutcome::default();
        }
    };

    let plan = plan(&streams, &nodes, &existing);
    let mut outcome = ReconcileOutcome {
        kept: plan.kept(),
        ..ReconcileOutcome::default()
    };

    for (key, leader) in plan.to_place() {
        match store
            .put_shard_assignment(assignment_for(key, leader))
            .await
        {
            Ok(assignment) => {
                outcome.placed += 1;
                tracing::info!(
                    stream = %key.stream,
                    shard = key.shard,
                    leader = %leader,
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
                    reconcile_once(store.as_ref()).await;
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "placement_tests.rs"]
mod tests;
