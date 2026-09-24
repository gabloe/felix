//! What a publish waits on before a `Quorum` stream acknowledges it.
//!
//! The replication driver owns the cursors; a publish must not. So the driver
//! publishes one number per shard — the highest offset a majority of the
//! replica set holds durably — and a publish waits for that number to reach
//! past its own last offset.
//!
//! A `watch` channel rather than a lock and a condition variable: every waiter
//! wants the same number, latest-wins is exactly right for a monotonic
//! high-water mark, and a waiter that arrives after the mark has already passed
//! sees the current value immediately rather than waiting for the next change.
//!
//! # Generations
//!
//! The mark is reset to zero when a shard's generation changes. An offset that
//! a majority held under the previous leadership says nothing about the current
//! one: the replica set may be different, and the design note is explicit that
//! an acknowledgement from a replica at an older generation does not count
//! toward a newer generation's quorum. Resetting is what enforces that here.
use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::watch;

use super::FollowerCursor;
use crate::shards::ShardKey;

/// The quorum-durable high-water mark for each shard this broker leads.
#[derive(Debug, Default)]
pub struct QuorumMarks {
    shards: Mutex<HashMap<ShardKey, ShardMark>>,
}

#[derive(Debug)]
struct ShardMark {
    generation: u64,
    offset: watch::Sender<u64>,
}

impl QuorumMarks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record how far the majority has got for `key` at `generation`.
    ///
    /// A generation change restarts the mark at zero rather than carrying the
    /// old one forward.
    pub fn publish(&self, key: &ShardKey, generation: u64, offset: u64) {
        let mut shards = self.shards.lock();
        match shards.get_mut(key) {
            Some(mark) if mark.generation == generation => {
                // Monotonic within a generation: the mark is a high-water mark,
                // and a pass that saw less than the last one saw a follower
                // mid-answer rather than a record becoming un-stored.
                mark.offset.send_if_modified(|current| {
                    if offset > *current {
                        *current = offset;
                        true
                    } else {
                        false
                    }
                });
            }
            _ => {
                shards.insert(
                    key.clone(),
                    ShardMark {
                        generation,
                        offset: watch::Sender::new(offset),
                    },
                );
            }
        }
    }

    /// Stop tracking a shard this broker no longer leads.
    ///
    /// Dropping the sender ends every wait on it, which is what turns a
    /// leadership loss into a failed publish rather than one that hangs until
    /// its timeout.
    pub fn forget(&self, key: &ShardKey) {
        self.shards.lock().remove(key);
    }

    /// Keep only the shards named, forgetting the rest.
    pub fn retain(&self, live: &[ShardKey]) {
        self.shards.lock().retain(|key, _| live.contains(key));
    }

    /// The mark as it stands for `key` at `generation`, or `None` when this
    /// broker is not tracking the shard at that generation.
    pub fn offset(&self, key: &ShardKey, generation: u64) -> Option<u64> {
        let shards = self.shards.lock();
        let mark = shards.get(key)?;
        (mark.generation == generation).then(|| *mark.offset.borrow())
    }

    /// Wait until a majority holds every offset below `offset`.
    ///
    /// `offset` is one past the last record of the batch, so this returns when
    /// the batch itself is on a majority.
    pub async fn wait_for(
        &self,
        key: &ShardKey,
        generation: u64,
        offset: u64,
        timeout: std::time::Duration,
    ) -> QuorumWait {
        let Some(mut watcher) = self.watcher(key, generation) else {
            // Nothing is tracking this shard at this generation: either this
            // broker does not lead it, or the leadership has already moved.
            // Either way it cannot promise a quorum for this write.
            return QuorumWait::NotLeading;
        };

        if *watcher.borrow_and_update() >= offset {
            return QuorumWait::Reached;
        }

        let reached = tokio::time::timeout(timeout, async {
            // `changed` also errors when the sender is dropped, which is how a
            // shard released mid-publish ends the wait instead of running it
            // out.
            while watcher.changed().await.is_ok() {
                if *watcher.borrow_and_update() >= offset {
                    return true;
                }
            }
            false
        })
        .await;

        match reached {
            Ok(true) => QuorumWait::Reached,
            Ok(false) => QuorumWait::NotLeading,
            Err(_) => QuorumWait::TimedOut,
        }
    }

    /// A receiver for `key` at `generation`, if this broker is tracking it.
    fn watcher(&self, key: &ShardKey, generation: u64) -> Option<watch::Receiver<u64>> {
        let shards = self.shards.lock();
        let mark = shards.get(key)?;
        (mark.generation == generation).then(|| mark.offset.subscribe())
    }
}

/// How a wait for a majority ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumWait {
    /// A majority holds the batch. The publish may be acknowledged.
    Reached,
    /// No majority within the budget. The record may still be on disk here and
    /// may yet reach a majority, so this is not "the write failed" — it is
    /// "this broker cannot say that it succeeded", which is the honest answer
    /// and the one the client can act on.
    TimedOut,
    /// This broker is not the leader of that shard at that generation any more.
    NotLeading,
}

/// Hold a `Quorum` publish until a majority of the shard's replica set has it.
///
/// A `Leader` stream returns at once: local durability is the guarantee it
/// offers, and it has already been reached by the time this is called.
///
/// The wait is bounded. A timeout is **not** "the write failed" — the records
/// are on this broker's disk and may yet reach a majority — it is "this broker
/// cannot say that it succeeded", which is the honest answer and the one a
/// client can act on. Reporting success instead would make an acknowledgement
/// mean less than the stream promises.
pub async fn await_quorum(
    handle: &felix_broker::StreamHandle,
    shard: Option<&crate::shards::ShardKey>,
    outcome: &felix_broker::PublishOutcome,
    marks: Option<&crate::replication::quorum::QuorumMarks>,
    ingress: Option<&crate::shards::routing::IngressRouter>,
    timeout: std::time::Duration,
) -> Result<(), anyhow::Error> {
    if handle.consistency() != felix_broker::ConsistencyLevel::Quorum {
        return Ok(());
    }
    let (Some(shard), Some(marks), Some(ingress)) = (shard, marks, ingress) else {
        // A single-node broker has no replica set. `Quorum` on a stream nobody
        // replicates is satisfied by the leader alone, which has already
        // written the record.
        return Ok(());
    };
    // An ephemeral stream has no offsets, so there is nothing to replicate and
    // nothing to wait for.
    let Some((_, last_offset)) = outcome.offsets else {
        return Ok(());
    };
    let Some(generation) = ingress.generation(shard) else {
        anyhow::bail!("shard ownership changed before the batch could reach a quorum");
    };

    // `last_offset` is inclusive, and the mark is one past what is held.
    match marks
        .wait_for(shard, generation, last_offset + 1, timeout)
        .await
    {
        crate::replication::quorum::QuorumWait::Reached => Ok(()),
        crate::replication::quorum::QuorumWait::TimedOut => {
            crate::replication::metrics::record_quorum(
                crate::replication::metrics::QUORUM_TIMED_OUT,
            );
            Err(anyhow::anyhow!(
                "the batch is durable here but did not reach a majority within {timeout:?}"
            ))
        }
        crate::replication::quorum::QuorumWait::NotLeading => {
            crate::replication::metrics::record_quorum(
                crate::replication::metrics::QUORUM_NOT_LEADING,
            );
            Err(anyhow::anyhow!(
                "shard leadership moved before the batch could reach a quorum"
            ))
        }
    }
}

/// Hold a write to a `Quorum` cache until a majority of its shard's replica set
/// has it. The cache-side mirror of [`await_quorum`].
///
/// A cache put does not report the offset it took, so this waits for the
/// shard's tail as read after the write. That is at or past the write, so a
/// mark at the tail covers it; a concurrent later write can only make the wait
/// longer, never make it end before this write is on a majority.
pub async fn await_cache_quorum(
    broker: &felix_broker::Broker,
    shard: &crate::shards::ShardKey,
    marks: Option<&QuorumMarks>,
    ingress: Option<&crate::shards::routing::IngressRouter>,
    timeout: std::time::Duration,
) -> Result<(), anyhow::Error> {
    let consistency = broker
        .cache_consistency(&shard.tenant_id, &shard.namespace, &shard.stream)
        .await;
    if consistency != Some(felix_broker::ConsistencyLevel::Quorum) {
        return Ok(());
    }
    // As for a stream: a broker with no replica set is satisfied by itself.
    let (Some(marks), Some(ingress)) = (marks, ingress) else {
        return Ok(());
    };
    // A cache with no log keeps nothing to replicate.
    let Some(log) = broker
        .shard_log(
            felix_broker::LogKind::Cache,
            &shard.tenant_id,
            &shard.namespace,
            &shard.stream,
            shard.shard,
        )
        .await
    else {
        return Ok(());
    };
    let tail = log.tail_offset().await?;
    let Some(generation) = ingress.generation(shard) else {
        anyhow::bail!("shard ownership changed before the write could reach a quorum");
    };
    match marks.wait_for(shard, generation, tail, timeout).await {
        QuorumWait::Reached => Ok(()),
        QuorumWait::TimedOut => {
            crate::replication::metrics::record_quorum(
                crate::replication::metrics::QUORUM_TIMED_OUT,
            );
            Err(anyhow::anyhow!(
                "the write is durable here but did not reach a majority within {timeout:?}"
            ))
        }
        QuorumWait::NotLeading => {
            crate::replication::metrics::record_quorum(
                crate::replication::metrics::QUORUM_NOT_LEADING,
            );
            Err(anyhow::anyhow!(
                "shard leadership moved before the write could reach a quorum"
            ))
        }
    }
}

/// How many copies must hold a record for a majority, the leader included.
///
/// `replicas` is the follower count, so the replica set is one larger. A set of
/// three needs two, a set of five needs three — and a set of one needs one,
/// which is the leader alone and is why `replication_factor: 1` costs nothing.
pub fn majority_of(replicas: usize) -> usize {
    replicas.div_ceil(2) + 1
}

/// The highest offset a majority of the replica set holds durably.
///
/// The leader is counted as holding everything up to `leader_tail`: it wrote the
/// records, and a record it has not written is not a candidate for a quorum in
/// the first place.
///
/// **A halted follower counts for nothing.** It is not slow, it has stopped —
/// its log has diverged, or this broker has been superseded — and letting a
/// stale position count toward a majority is how an acknowledgement comes to
/// mean less than it says.
///
/// Cursors belong to one generation. The caller passes the set for the
/// generation it is publishing at, which is what stops an older generation's
/// acknowledgements satisfying a newer generation's quorum.
pub fn quorum_offset(leader_tail: u64, followers: &[FollowerCursor]) -> u64 {
    let needed = majority_of(followers.len());
    // The leader is one of them, and it holds the most.
    let mut held: Vec<u64> = std::iter::once(leader_tail)
        .chain(
            followers
                .iter()
                .filter(|follower| follower.halted.is_none())
                .map(|follower| follower.next_offset.min(leader_tail)),
        )
        .collect();
    // Descending, so the `needed`-th is the highest offset that many hold.
    held.sort_unstable_by(|a, b| b.cmp(a));
    held.get(needed - 1).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests;
