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

use crate::shard_watch::ShardKey;
use parking_lot::Mutex;
use tokio::sync::watch;

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

    /// A receiver for `key` at `generation`, if this broker is tracking it.
    fn watcher(&self, key: &ShardKey, generation: u64) -> Option<watch::Receiver<u64>> {
        let shards = self.shards.lock();
        let mark = shards.get(key)?;
        (mark.generation == generation).then(|| mark.offset.subscribe())
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

#[cfg(test)]
#[path = "quorum_tests.rs"]
mod tests;
