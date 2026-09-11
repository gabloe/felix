//! Which replicas hold a shard's log, as their leaders last reported.
//!
//! Promotion is gated on this. Without it a lost leader cannot be replaced at
//! all — see `docs/replication-design.md` — and with a *wrong* answer it can be
//! replaced by a broker holding less than it claims, which is worse.
//!
//! # Why the leader reports it
//!
//! The leader is the only party that knows both ends of the comparison: its own
//! tail, and how far each follower has acknowledged. A follower knows only where
//! it is, not whether that is caught up.
//!
//! # Why reports expire
//!
//! A report says a follower *was* caught up at the moment it was made. The
//! leader then keeps writing, and the follower may fall behind. Promoting on a
//! stale report is how a failover silently loses the records written after it.
//!
//! So a report is only believed for a bounded time — long enough to outlive the
//! window in which a leader's death is noticed, and no longer. The bound is
//! derived from the liveness settings rather than configured separately: those
//! already say how quickly a dead leader is detected, and a report has to
//! survive exactly that long to be usable.
use std::collections::{BTreeSet, HashMap};

use std::sync::Mutex;

use crate::model::ShardKey;
use crate::placement::CaughtUp;

/// One leader's account of its followers for one shard.
#[derive(Debug, Clone)]
struct Report {
    /// The assignment generation the leader held when it reported. A report
    /// from an older generation says nothing about this one: the replica set
    /// may be different.
    generation: u64,
    caught_up: BTreeSet<String>,
    reported_at_millis: u64,
}

/// The control plane's view of which replicas can take over.
#[derive(Debug)]
pub struct ReplicaPositions {
    shards: Mutex<HashMap<ShardKey, Report>>,
    /// How long a report is believed.
    ttl_millis: u64,
}

impl ReplicaPositions {
    /// Believe a report for `expiry_timeout_ms` plus one heartbeat interval.
    ///
    /// It has to outlive the detection window: the last report a leader makes
    /// is from just before it dies, and promotion happens only after the
    /// cluster has noticed, which takes the expiry timeout. A margin of one
    /// heartbeat covers the gap between a leader's final report and its final
    /// heartbeat.
    ///
    /// Not longer, deliberately. Every extra second is a second of writes a
    /// promoted follower might be missing.
    pub fn new(liveness: &crate::config::NodeLivenessConfig) -> Self {
        Self {
            shards: Mutex::new(HashMap::new()),
            ttl_millis: liveness.expiry_timeout_ms + liveness.heartbeat_interval_ms,
        }
    }

    /// Record what a leader reports about one shard.
    ///
    /// A report at an older generation than the one already held is dropped:
    /// leadership has moved on, and the old leader's view of its followers is
    /// no longer about the current replica set.
    pub fn record(
        &self,
        key: ShardKey,
        generation: u64,
        caught_up: BTreeSet<String>,
        now_millis: u64,
    ) {
        let mut shards = self.shards.lock().expect("replica positions lock");
        if let Some(held) = shards.get(&key)
            && held.generation > generation
        {
            return;
        }
        shards.insert(
            key,
            Report {
                generation,
                caught_up,
                reported_at_millis: now_millis,
            },
        );
    }

    /// Forget everything about a shard.
    pub fn forget(&self, key: &ShardKey) {
        self.shards
            .lock()
            .expect("replica positions lock")
            .remove(key);
    }

    fn is_caught_up_at(&self, key: &ShardKey, node_id: &str, now_millis: u64) -> bool {
        let shards = self.shards.lock().expect("replica positions lock");
        let Some(report) = shards.get(key) else {
            return false;
        };
        if now_millis.saturating_sub(report.reported_at_millis) > self.ttl_millis {
            return false;
        }
        report.caught_up.contains(node_id)
    }
}

/// The reports as of `now_millis`.
///
/// A snapshot rather than a live view, so every shard in one planning pass is
/// judged against the same instant. Planning that read the clock per shard
/// could promote on one side of a report's expiry and refuse on the other.
pub struct CaughtUpAt<'a> {
    pub positions: &'a ReplicaPositions,
    pub now_millis: u64,
}

impl CaughtUp for CaughtUpAt<'_> {
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.positions
            .is_caught_up_at(key, node_id, self.now_millis)
    }
}

#[cfg(test)]
#[path = "replica_positions_tests.rs"]
mod tests;
