//! Metrics for the leader's side of replication.
//!
//! Unlabelled by shard on purpose. A label per shard is a label per stream per
//! tenant, which is unbounded by design in a multi-tenant broker; these answer
//! "is replication healthy on this node" and the shard in question is in the
//! log line that accompanies a halt.

/// Replication batches this broker shipped as a leader, by `outcome`.
pub const SHIPPED_TOTAL: &str = "felix_broker_replication_shipped_total";

/// How far the slowest follower is behind, in records, across every shard this
/// broker leads.
///
/// This is the `ConsistencyLevel::Leader` loss window. Records acknowledged to
/// a client but not yet on a follower are the ones a total loss of this broker
/// would take with it.
pub const LAG_RECORDS: &str = "felix_broker_replication_lag_records";

/// Followers replication has stopped for, because their log diverged or this
/// broker was superseded. Not a rate -- any non-zero value is worth waking
/// someone for.
pub const HALTED: &str = "felix_broker_replication_halted";

pub const OUTCOME_OK: &str = "ok";
/// The follower asked to be sent a different offset. Ordinary during catch-up.
pub const OUTCOME_RESUMED: &str = "resumed";
/// The follower refused for a reason that resolves on its own.
pub const OUTCOME_REFUSED: &str = "refused";
/// The leader could not read its own log.
pub const OUTCOME_READ_FAILED: &str = "read_failed";
pub const OUTCOME_UNREACHABLE: &str = "unreachable";
pub const OUTCOME_TIMEOUT: &str = "timeout";
pub const OUTCOME_DISCONNECTED: &str = "disconnected";
/// The two logs disagree about bytes both sides hold.
pub const OUTCOME_DIVERGED: &str = "diverged";
/// The follower knows a newer generation than this broker is shipping at.
pub const OUTCOME_FENCED: &str = "fenced";

pub fn record_shipped(outcome: &'static str) {
    metrics::counter!(SHIPPED_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_lag(records: u64) {
    metrics::gauge!(LAG_RECORDS).set(records as f64);
}

pub fn record_halted(count: usize) {
    metrics::gauge!(HALTED).set(count as f64);
}
