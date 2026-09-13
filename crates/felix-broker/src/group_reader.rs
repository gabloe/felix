//! Reading a stream shard as a consumer group.
//!
//! Joins the three pieces: the shard's log holds the records, the durable
//! cursor in [`crate::consumer_groups`] says where the group has finished, and
//! [`crate::group_delivery::GroupTracker`] holds what is currently handed out.
//!
//! Everything here is about keeping those three consistent. The cursor is
//! written only when a contiguous run of acknowledgements closes, because that
//! is the only moment the group has genuinely finished a prefix — writing it
//! per acknowledgement would either lie about progress or need a second
//! structure on disk to say which of the acknowledged offsets were contiguous.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;

use crate::consumer_groups::ConsumerGroups;
use crate::error::{BrokerError, Result};
use crate::group_delivery::GroupTracker;

/// A group reading one shard of one stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    pub group: String,
}

/// One record handed to a consumer, with the offset it must acknowledge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    pub offset: u64,
    pub payload: bytes::Bytes,
}

/// Every group this broker is serving, and the state each one holds.
///
/// Trackers are built on first touch from the durable cursor, and are in memory
/// only: losing them redelivers whatever was in flight, which is the same thing
/// losing the leader does.
#[derive(Debug)]
pub struct GroupReader {
    cursors: Arc<ConsumerGroups>,
    trackers: Mutex<HashMap<GroupKey, Arc<Mutex<GroupTracker>>>>,
    visibility: Duration,
    /// Records a group was owed and can never receive, because retention
    /// removed them first.
    ///
    /// Counted rather than logged: this crate takes no logging dependency, and
    /// silently skipping them would hide the one case where a queue drops work
    /// nobody asked it to drop. The service layer reports it.
    trimmed: AtomicU64,
}

impl GroupReader {
    pub fn new(cursors: Arc<ConsumerGroups>, visibility: Duration) -> Self {
        Self {
            cursors,
            trackers: Mutex::new(HashMap::new()),
            visibility,
            trimmed: AtomicU64::new(0),
        }
    }

    /// How long a claim stands before the record is owed again.
    pub fn visibility(&self) -> Duration {
        self.visibility
    }

    /// Records skipped because retention removed them before the group got to
    /// them. Non-zero means a queue dropped work; it is a retention setting too
    /// short for how far a group is allowed to fall behind.
    pub fn trimmed_skipped(&self) -> u64 {
        self.trimmed.load(Ordering::Relaxed)
    }

    /// Take up to `max` records for `group`, claimed until the visibility
    /// timeout lapses.
    ///
    /// Reads are by offset one at a time rather than as a range: the offsets a
    /// group is owed are not contiguous once anything has been redelivered, so
    /// a range read would return records the group is not owed and skip ones it
    /// is.
    ///
    /// The one-byte budget asks for exactly one record. It relies on the read
    /// path's rule that a range holding data never answers empty — the first
    /// record is returned whatever the budget, so a record larger than the
    /// budget is delivered rather than skipped. Without that rule a large
    /// record would be owed for ever and the group would stall on it.
    pub async fn poll(
        &self,
        key: &GroupKey,
        log: &crate::durable::StreamLog,
        max: usize,
        now: Instant,
    ) -> Result<Vec<Claimed>> {
        let tail = log.tail_offset().await?;
        let tracker = self.tracker_for(key).await?;
        let offsets = {
            let mut tracker = tracker.lock().await;
            tracker.claim(tail, max, now, self.visibility)
        };

        let mut claimed = Vec::with_capacity(offsets.len());
        for offset in offsets {
            match log.read_from(offset, 1).await {
                Ok(records) => match records.into_iter().next() {
                    Some(record) => claimed.push(Claimed {
                        offset,
                        payload: record.payload,
                    }),
                    // The offset is below the tail and yet holds nothing. Give
                    // the claim back rather than dropping it silently: a record
                    // the group is owed and never receives would stall the
                    // cursor at that offset for ever.
                    None => tracker.lock().await.nack(offset),
                },
                Err(BrokerError::CursorTooOld { .. }) => {
                    // Retention removed it while the group was behind. Nobody
                    // can deliver it, so it is settled rather than left owed --
                    // leaving it owed would stall the group for ever on a record
                    // that no longer exists anywhere.
                    self.trimmed.fetch_add(1, Ordering::Relaxed);
                    self.settle(key, &tracker, offset).await?;
                }
                Err(err) => {
                    // The read failed for a reason that may not repeat. Owed
                    // again, so the next poll tries.
                    tracker.lock().await.nack(offset);
                    return Err(err);
                }
            }
        }
        Ok(claimed)
    }

    /// Finish one record. Persists the cursor when a contiguous run closes.
    pub async fn ack(&self, key: &GroupKey, offset: u64) -> Result<()> {
        let tracker = self.tracker_for(key).await?;
        self.settle(key, &tracker, offset).await
    }

    /// Give one record back, to be handed out again at once.
    pub async fn nack(&self, key: &GroupKey, offset: u64) -> Result<()> {
        let tracker = self.tracker_for(key).await?;
        tracker.lock().await.nack(offset);
        Ok(())
    }

    /// Where `group` has finished, as recorded on disk.
    pub async fn committed(&self, key: &GroupKey) -> Result<Option<u64>> {
        self.cursors
            .committed(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
            )
            .await
    }

    async fn settle(
        &self,
        key: &GroupKey,
        tracker: &Arc<Mutex<GroupTracker>>,
        offset: u64,
    ) -> Result<()> {
        let advanced = tracker.lock().await.ack(offset);
        // Only when the run closed. An acknowledgement above a gap has not
        // finished anything the group can resume from.
        if let Some(committed) = advanced {
            self.cursors
                .commit(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &key.group,
                    committed,
                )
                .await?;
        }
        Ok(())
    }

    async fn tracker_for(&self, key: &GroupKey) -> Result<Arc<Mutex<GroupTracker>>> {
        if let Some(tracker) = self.trackers.lock().await.get(key) {
            return Ok(Arc::clone(tracker));
        }
        // Hydrated from disk outside the map lock is tempting and wrong: two
        // pollers racing would both read the cursor and both insert, and the
        // loser's tracker would hand out records the winner already claimed.
        let mut trackers = self.trackers.lock().await;
        if let Some(tracker) = trackers.get(key) {
            return Ok(Arc::clone(tracker));
        }
        let committed = self
            .cursors
            .committed(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
            )
            .await?
            // A group that has never committed starts at the beginning of what
            // the log still holds. Starting at the tail would silently skip
            // everything published before the group first connected.
            .unwrap_or(0);
        let tracker = Arc::new(Mutex::new(GroupTracker::new(committed)));
        trackers.insert(key.clone(), Arc::clone(&tracker));
        Ok(tracker)
    }
}

#[cfg(test)]
#[path = "group_reader_tests.rs"]
mod tests;
