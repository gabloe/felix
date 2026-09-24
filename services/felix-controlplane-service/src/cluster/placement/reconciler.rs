//! Applying a plan to the store, on a timer.
use std::collections::HashMap;

use super::{MovePolicy, PlacementWakes, Plan, ReplicaPositions, assignment_for, plan_with};
use crate::model::{Cache, Node, ShardAssignment, ShardKey, Stream};
use crate::store::AssignmentWrite;

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

/// Placements and move steps not written because the shard changed after the
/// pass read it. Expected now and then with several instances running
/// placement; the next pass re-plans from the new state.
pub const SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL: &str =
    "felix_shard_assignment_write_conflicts_total";

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
    /// Writes skipped because another writer changed the shard after this
    /// pass read it.
    pub conflicts: usize,
}

/// A plan, and the generation of every assignment it was planned from.
pub(super) struct PlannedPass {
    plan: Plan,
    /// Absent for a shard that had no assignment when the pass read.
    read: HashMap<ShardKey, u64>,
}

impl PlannedPass {
    #[cfg(test)]
    pub(super) fn plan(&self) -> &Plan {
        &self.plan
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
///
/// Safe to run on several instances at once: each write lands only if the
/// shard is still at the generation this pass read, and one that is not is
/// counted in `conflicts` and left to the next pass.
pub async fn reconcile_once(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
) -> ReconcileOutcome {
    match plan_pass(store, liveness, policy).await {
        Some(pass) => apply_pass(store, &pass, &PlacementWakes::default()).await,
        None => ReconcileOutcome::default(),
    }
}

/// Read the catalog and plan against it. `None` when it could not be read.
pub(super) async fn plan_pass(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
) -> Option<PlannedPass> {
    let (streams, caches, nodes, existing) = match load(store).await {
        Ok(loaded) => loaded,
        Err(err) => {
            tracing::error!(error = %err, "could not read the catalog to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return None;
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
            return None;
        }
    };
    let plan = plan_with(&streams, &caches, &nodes, &existing, &caught_up, policy);
    let read = existing
        .iter()
        .map(|assignment| (assignment.key.clone(), assignment.generation))
        .collect();
    Some(PlannedPass { plan, read })
}

/// Write what a planned pass calls for, waking this instance's long-polls on
/// every write.
pub(super) async fn apply_pass(
    store: &dyn crate::store::ControlPlaneStore,
    pass: &PlannedPass,
    wakes: &PlacementWakes,
) -> ReconcileOutcome {
    let plan = &pass.plan;
    let mut outcome = ReconcileOutcome {
        kept: plan.kept(),
        ..ReconcileOutcome::default()
    };

    // Every write is conditional on the generation the plan was made from:
    // another instance may have moved the shard on since, and a step planned
    // from the old state must not undo a newer one.
    for (key, step, assignment) in plan.moves() {
        match store
            .put_shard_assignment_if(assignment.clone(), pass.read.get(key).copied())
            .await
        {
            Ok(AssignmentWrite::Stale { current }) => {
                outcome.conflicts += 1;
                conflict(key, step.label(), pass.read.get(key).copied(), current);
            }
            Ok(AssignmentWrite::Written(written)) => {
                wakes.assignment_written();
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
            .put_shard_assignment_if(
                assignment_for(key, leader, replicas.to_vec()),
                pass.read.get(key).copied(),
            )
            .await
        {
            Ok(AssignmentWrite::Stale { current }) => {
                outcome.conflicts += 1;
                conflict(key, "place", pass.read.get(key).copied(), current);
            }
            Ok(AssignmentWrite::Written(assignment)) => {
                wakes.assignment_written();
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
    metrics::counter!(SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL).increment(outcome.conflicts as u64);
    metrics::gauge!(SHARDS_UNPLACEABLE).set(outcome.unplaceable as f64);
    metrics::gauge!(SHARD_MOVES_WAITING).set(outcome.waiting as f64);
    outcome
}

/// Place shards on an interval until `shutdown` fires. Every assignment it
/// writes wakes this instance's long-polls through `wakes`.
pub fn spawn_reconciler(
    store: std::sync::Arc<dyn crate::store::ControlPlaneStore + Send + Sync>,
    liveness: crate::config::NodeLivenessConfig,
    policy: MovePolicy,
    interval: std::time::Duration,
    gate: crate::raft::LeadershipGate,
    wakes: std::sync::Arc<PlacementWakes>,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // A pass that overruns must not then run back-to-back catching up.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            // Placement decides from what it reads; under Raft the gate's
            // linearizable check also guarantees those reads are current
            // before any assignment is proposed.
            if !gate.holds().await {
                continue;
            }
            if let Some(pass) = plan_pass(store.as_ref(), &liveness, policy).await {
                apply_pass(store.as_ref(), &pass, &wakes).await;
            }
        }
    })
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

fn conflict(key: &ShardKey, step: &str, planned_from: Option<u64>, current: Option<u64>) {
    tracing::info!(
        kind = %key.kind,
        name = %key.stream,
        shard = key.shard,
        step,
        planned_from = ?planned_from,
        current = ?current,
        "shard changed since this pass read it; not writing, the next pass re-plans",
    );
}
