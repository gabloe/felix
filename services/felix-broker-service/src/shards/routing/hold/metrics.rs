//! Held writes. A hold that ends in a refusal is a move that took longer
//! than the window, or a burst larger than the cap.
use std::time::Duration;

use super::HoldRefused;

/// Writes held for a moving shard.
pub const HELD_TOTAL: &str = "felix_broker_shard_move_held_total";
/// Seconds each held write waited, however it ended.
pub const HOLD_SECONDS: &str = "felix_broker_shard_move_hold_seconds";
/// Writes to a moving shard refused, by `reason`: `timed_out` or `full`.
pub const HOLD_REFUSED_TOTAL: &str = "felix_broker_shard_move_hold_refused_total";

/// Bucket bounds for [`HOLD_SECONDS`]: a switch-over is milliseconds, and
/// the default window is two seconds.
pub const HOLD_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
];

pub(super) fn record_held() {
    metrics::counter!(HELD_TOTAL).increment(1);
}

pub(super) fn record_hold_seconds(held: Duration) {
    metrics::histogram!(HOLD_SECONDS).record(held.as_secs_f64());
}

pub(super) fn record_refused(reason: HoldRefused) {
    metrics::counter!(HOLD_REFUSED_TOTAL, "reason" => reason.label()).increment(1);
}
