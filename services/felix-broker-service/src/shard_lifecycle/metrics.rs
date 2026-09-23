//! Metrics for local shard ownership.
//!
//! The question: is this broker serving what the cluster thinks it serves? A
//! shard stuck in `opening` is one the control plane believes is live; a shard
//! in `failed` is one nobody is serving.
use std::collections::HashMap;

use crate::shard_lifecycle::Phase;

/// Shards this broker holds, by phase. Bounded at six series.
pub const PHASE: &str = "felix_broker_shard_phase";
/// Local ownership transitions, by direction.
pub const TRANSITIONS_TOTAL: &str = "felix_broker_shard_transitions_total";
/// Events ignored for naming a generation at or below the one already acted on.
pub const STALE_EVENTS_TOTAL: &str = "felix_broker_shard_stale_events_total";
/// Local logs that could not be opened. Non-zero means a shard the cluster
/// believes is placed here is not being served.
pub const OPEN_FAILURES_TOTAL: &str = "felix_broker_shard_open_failures_total";

const ALL_PHASES: [Phase; 6] = [
    Phase::Unassigned,
    Phase::Opening,
    Phase::Active,
    Phase::Draining,
    Phase::Closed,
    Phase::Failed,
];

pub fn record_transition(from: Phase, to: Phase) {
    metrics::counter!(TRANSITIONS_TOTAL, "from" => from.label(), "to" => to.label()).increment(1);
}

pub fn record_stale_event() {
    metrics::counter!(STALE_EVENTS_TOTAL).increment(1);
}

pub fn record_open_failure() {
    metrics::counter!(OPEN_FAILURES_TOTAL).increment(1);
}

/// Publish the phase census.
///
/// Every phase is written, including the empty ones: a gauge that stops being
/// set keeps its last value, and "no shards are failed any more" is exactly the
/// fact an operator needs to see.
pub fn record_phase_counts(counts: HashMap<Phase, usize>) {
    for phase in ALL_PHASES {
        metrics::gauge!(PHASE, "phase" => phase.label())
            .set(counts.get(&phase).copied().unwrap_or(0) as f64);
    }
}
