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
//! A key → latest-value projection again, like the cursors themselves: the key
//! is the offset, so recording one twice is idempotent.
use std::path::PathBuf;

use felix_storage::LogCache;
use felix_storage::log::LogConfig;

use crate::error::{BrokerError, Result};
use crate::group_reader::GroupKey;

fn storage_error(err: felix_storage::StorageError) -> BrokerError {
    BrokerError::Storage(err.to_string())
}

/// Every group's dead letters, on their own root.
#[derive(Debug)]
pub struct DeadLetters {
    entries: LogCache,
}

impl DeadLetters {
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        Ok(Self {
            entries: LogCache::open(root, config).map_err(storage_error)?,
        })
    }

    /// Record that `group` gave up on `offset`.
    pub async fn record(&self, key: &GroupKey, offset: u64) -> Result<()> {
        self.entries
            .put_checked(
                &key.tenant_id,
                &key.namespace,
                // Scoped by group as well as stream: two groups reading one
                // shard fail on different records, and merging their dead
                // letters would have each answering for the other's.
                &scope(key),
                key.shard,
                &offset.to_string(),
                bytes::Bytes::new(),
                None,
            )
            .await
            .map_err(storage_error)
    }

    /// Offsets `group` has given up on, lowest first.
    pub async fn list(&self, key: &GroupKey) -> Result<Vec<u64>> {
        let mut offsets: Vec<u64> = self
            .entries
            .keys(&key.tenant_id, &key.namespace, &scope(key), key.shard)
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter_map(|entry| entry.parse().ok())
            .collect();
        offsets.sort_unstable();
        Ok(offsets)
    }

    /// Flush every open dead-letter log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.entries.shutdown().await.map_err(storage_error)
    }
}

/// Dead letters are stored per stream *and* group.
fn scope(key: &GroupKey) -> String {
    format!("{}\u{1f}{}", key.stream, key.group)
}

#[cfg(test)]
#[path = "dead_letters_tests.rs"]
mod tests;
