//! Offsets a consumer group has given up on.
//!
//! Not a copy of the records. They are still in the stream's log, at the
//! offsets recorded here, readable by an ordinary replay — so dead-lettering
//! duplicates nothing and loses nothing. What it stores is the one fact the log
//! does not already hold: that this group tried this record too many times and
//! stopped.
//!
//! Durable, and written before the group's cursor moves past the record. The
//! other order would let a crash in between leave the cursor beyond a record
//! nothing says was ever attempted, which is a silent skip rather than a
//! dead letter.
//!
//! A key → latest-value projection again, like the cursors themselves. **One
//! log per stream shard**, exactly as the cursors are shaped, with the group
//! folded into the entry key rather than into the log's identity: replication
//! ships whole logs, and a log per `(stream, group)` is a set the shipping
//! driver cannot enumerate — groups appear whenever a consumer names one —
//! where a log per shard is exactly the unit the driver already walks. This is
//! what lets a dead-letter list reach a replica and survive its leader.
use std::path::PathBuf;

use felix_storage::LogCache;
use felix_storage::log::LogConfig;

use super::reader::GroupKey;
use crate::error::{BrokerError, Result};

/// Every group's dead letters, on their own root.
#[derive(Debug)]
pub struct DeadLetters {
    entries: LogCache,
    /// Where an earlier layout kept one log per `(stream, group)`. Read-only:
    /// entries recorded before the per-shard layout are still listed and can
    /// still be discarded, and nothing is ever written there again. `None`
    /// once no legacy directory can exist.
    legacy_root: PathBuf,
}

impl DeadLetters {
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        Ok(Self {
            entries: LogCache::open(&root, config).map_err(storage_error)?,
            legacy_root: root,
        })
    }

    /// Record that `group` gave up on `offset`.
    pub async fn record(&self, key: &GroupKey, offset: u64) -> Result<()> {
        self.entries
            .put_checked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                // Scoped by group inside the shard's log: two groups reading
                // one shard fail on different records, and merging their dead
                // letters would have each answering for the other's.
                &entry_key(&key.group, offset),
                bytes::Bytes::new(),
                None,
            )
            .await
            .map_err(storage_error)
    }

    /// Offsets `group` has given up on, lowest first.
    pub async fn list(&self, key: &GroupKey) -> Result<Vec<u64>> {
        let prefix = format!("{}\u{1f}", key.group);
        let mut offsets: Vec<u64> = self
            .entries
            .keys(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter_map(|entry| entry.strip_prefix(&prefix)?.parse().ok())
            .collect();
        if let Some(legacy) = self.legacy(key) {
            offsets.extend(
                legacy
                    .keys(
                        &key.tenant_id,
                        &key.namespace,
                        &legacy_scope(key),
                        key.shard,
                    )
                    .await
                    .map_err(storage_error)?
                    .into_iter()
                    .filter_map(|entry| entry.parse::<u64>().ok()),
            );
        }
        offsets.sort_unstable();
        offsets.dedup();
        Ok(offsets)
    }

    /// Drop one offset from the list.
    ///
    /// Returns whether it was there. Does not touch the record, which stays in
    /// the stream's log — this only says the group has stopped tracking it as
    /// an outstanding problem.
    pub async fn discard(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        let removed = self
            .entries
            .delete_checked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &entry_key(&key.group, offset),
            )
            .await
            .map_err(storage_error)?;
        if removed.is_some() {
            return Ok(true);
        }
        // Recorded before the per-shard layout, perhaps. The legacy log is
        // opened only when its directory already exists, so a miss costs a
        // path check and never creates anything.
        let Some(legacy) = self.legacy(key) else {
            return Ok(false);
        };
        let removed = legacy
            .delete_checked(
                &key.tenant_id,
                &key.namespace,
                &legacy_scope(key),
                key.shard,
                &offset.to_string(),
            )
            .await
            .map_err(storage_error)?;
        Ok(removed.is_some())
    }

    /// The log a shard's dead letters are written to.
    ///
    /// For replication: the list of what a group gave up on has to reach a
    /// replica alongside the cursors that say what it finished.
    pub async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.entries
            .shard_log(tenant_id, namespace, stream, shard)
            .await
            .map_err(storage_error)
    }

    /// [`DeadLetters::shard_log`], created at `base_offset` if absent.
    pub async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.entries
            .shard_log_at(tenant_id, namespace, stream, shard, base_offset)
            .await
            .map_err(storage_error)
    }

    /// Flush every open dead-letter log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.entries.shutdown().await.map_err(storage_error)
    }

    /// The legacy per-`(stream, group)` log, if one is on disk for this key.
    ///
    /// Gated on the directory existing: [`LogCache`] creates a shard directory
    /// on open, and probing for old data must not litter the root with empty
    /// logs for every group that never had any.
    fn legacy(&self, key: &GroupKey) -> Option<&LogCache> {
        let shard_key = felix_storage::log::ShardKey {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: legacy_scope(key),
            shard: key.shard,
        };
        felix_storage::disk_log::layout::shard_dir(&self.legacy_root, &shard_key)
            .exists()
            .then_some(&self.entries)
    }
}

/// One group's claim on one offset, inside the shard's log.
fn entry_key(group: &str, offset: u64) -> String {
    format!("{group}\u{1f}{offset}")
}

/// How the earlier layout named a `(stream, group)` log.
fn legacy_scope(key: &GroupKey) -> String {
    format!("{}\u{1f}{}", key.stream, key.group)
}

fn storage_error(err: felix_storage::StorageError) -> BrokerError {
    BrokerError::Storage(err.to_string())
}

#[cfg(test)]
mod tests;
