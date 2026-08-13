// Durable storage for streams marked `durable: true`.
//
// A durable stream keeps the in-memory ring buffer — cursor replay and fanout
// still read from it, and it is what keeps non-durable performance intact — but
// every publish is written to a disk-backed log *before* it is fanned out or
// acknowledged.
//
// ## Ordering, and why it is this way
//
// ```text
//   publish → append to the durable log → (fsync, if OnCommit) → fanout → ack
// ```
//
// The append comes first because the alternative is unrecoverable: a record
// delivered to subscribers and acknowledged to the publisher, but lost in a
// crash, is a silent hole in a log that consumers believe they have read. Paying
// the append latency before fanout means a failed write becomes a failed
// publish, which the publisher can retry.
//
// The cost is real and deliberate: a durable publish carries the storage write
// (and under `FsyncMode::OnCommit`, a device flush) inside its latency.
// Non-durable streams never touch this path at all.

use std::sync::Arc;

use bytes::Bytes;
use felix_storage::DiskLogProvider;
use felix_storage::disk_log::DiskLog;
use felix_storage::log::{
    AppendOnlyLog, AppendRecord, AppendResult, LogConfig, LogRecord, Offset, ReadRange, ShardKey,
};

use crate::error::{BrokerError, Result};

/// The broker's handle on durable storage: one provider, many stream logs.
#[derive(Debug, Clone)]
pub struct DurableStorage {
    provider: Arc<DiskLogProvider>,
}

impl DurableStorage {
    /// Open (and recover) durable storage rooted at `root`.
    pub fn open(root: impl Into<std::path::PathBuf>, config: LogConfig) -> Result<Self> {
        let provider = DiskLogProvider::new(root, config).map_err(storage_error)?;
        Ok(Self {
            provider: Arc::new(provider),
        })
    }

    /// Wrap an already constructed provider, for callers that build their own.
    pub fn from_provider(provider: Arc<DiskLogProvider>) -> Self {
        Self { provider }
    }

    pub fn root(&self) -> &std::path::Path {
        self.provider.root()
    }

    pub fn config(&self) -> &LogConfig {
        self.provider.config()
    }

    /// Open the log for one stream shard, recovering whatever is on disk.
    ///
    /// Repeated calls for the same shard return the same log, so re-registering
    /// a stream — which the control-plane watcher does on every restart and
    /// resync — never opens a second writer over the same files.
    pub fn open_stream(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<StreamLog> {
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
        };
        let log = self.provider.open_shard(&key).map_err(storage_error)?;
        Ok(StreamLog { log })
    }

    /// Flush and stop every open log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.provider.shutdown().await.map_err(storage_error)
    }
}

/// One durable stream shard's log, as the broker uses it.
#[derive(Debug, Clone)]
pub struct StreamLog {
    log: DiskLog,
}

impl StreamLog {
    /// Persist a publish batch, returning the offsets it was assigned.
    ///
    /// Returns only once the configured durability policy is satisfied: under
    /// `FsyncMode::OnCommit` the bytes are on the device before this resolves.
    pub async fn append(&self, payloads: &[Bytes]) -> Result<AppendResult> {
        if payloads.is_empty() {
            return Err(BrokerError::Storage(
                "cannot append an empty publish batch".to_string(),
            ));
        }
        // One timestamp for the batch: the records were published together, and
        // reading the clock per record costs more than the precision is worth.
        let timestamp_micros = now_micros();
        let records: Vec<AppendRecord> = payloads
            .iter()
            .map(|payload| AppendRecord {
                payload: payload.clone(),
                timestamp_micros,
            })
            .collect();
        self.log.append(&records).await.map_err(storage_error)
    }

    /// Replay persisted records from `start`, bounded by `max_bytes`.
    pub async fn read_from(&self, start: Offset, max_bytes: usize) -> Result<Vec<LogRecord>> {
        self.log
            .read_range(ReadRange { start, max_bytes })
            .await
            .map_err(storage_error)
    }

    /// Offset the next published record will take.
    pub async fn tail_offset(&self) -> Result<Offset> {
        self.log.tail_offset().await.map_err(storage_error)
    }

    /// Exclusive bound on offsets that survive a crash right now.
    pub fn durable_offset(&self) -> Offset {
        self.log.durable_offset()
    }

    /// Bytes written but not yet flushed to the device.
    pub fn unsynced_bytes(&self) -> u64 {
        self.log.unsynced_bytes()
    }

    /// Force a flush regardless of the configured policy.
    pub async fn sync(&self) -> Result<()> {
        self.log.sync().await.map_err(storage_error)
    }
}

fn storage_error(err: felix_storage::StorageError) -> BrokerError {
    BrokerError::Storage(err.to_string())
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::log::FsyncMode;
    use tempfile::tempdir;

    fn config() -> LogConfig {
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        }
    }

    fn payloads(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_bytes()))
            .collect()
    }

    #[tokio::test]
    async fn appends_are_assigned_contiguous_offsets() {
        let dir = tempdir().expect("dir");
        let storage = DurableStorage::open(dir.path(), config()).expect("open");
        let log = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");

        let first = log.append(&payloads(&["a", "b"])).await.expect("append");
        assert_eq!((first.first_offset, first.last_offset), (0, 1));
        let second = log.append(&payloads(&["c"])).await.expect("append");
        assert_eq!((second.first_offset, second.last_offset), (2, 2));
        assert_eq!(log.tail_offset().await.expect("tail"), 3);
    }

    #[tokio::test]
    async fn an_empty_batch_is_rejected() {
        let dir = tempdir().expect("dir");
        let storage = DurableStorage::open(dir.path(), config()).expect("open");
        let log = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");
        assert!(matches!(
            log.append(&[]).await.expect_err("empty"),
            BrokerError::Storage(_)
        ));
    }

    #[tokio::test]
    async fn reopening_the_same_stream_shares_one_log() {
        let dir = tempdir().expect("dir");
        let storage = DurableStorage::open(dir.path(), config()).expect("open");
        let first = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");
        let again = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");

        first.append(&payloads(&["one"])).await.expect("append");
        let result = again.append(&payloads(&["two"])).await.expect("append");
        // A second writer over the same directory would have restarted at zero.
        assert_eq!(result.first_offset, 1);
    }

    #[tokio::test]
    async fn different_streams_are_isolated() {
        let dir = tempdir().expect("dir");
        let storage = DurableStorage::open(dir.path(), config()).expect("open");
        let orders = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");
        let events = storage
            .open_stream("t1", "default", "events", 0)
            .expect("stream");
        let shard_one = storage
            .open_stream("t1", "default", "orders", 1)
            .expect("stream");

        orders.append(&payloads(&["o"])).await.expect("append");
        assert_eq!(events.tail_offset().await.expect("tail"), 0);
        assert_eq!(shard_one.tail_offset().await.expect("tail"), 0);
    }

    #[tokio::test]
    async fn records_are_readable_after_reopening_storage() {
        let dir = tempdir().expect("dir");
        {
            let storage = DurableStorage::open(dir.path(), config()).expect("open");
            let log = storage
                .open_stream("t1", "default", "orders", 0)
                .expect("stream");
            log.append(&payloads(&["persisted", "twice"]))
                .await
                .expect("append");
            storage.shutdown().await.expect("shutdown");
        }

        let storage = DurableStorage::open(dir.path(), config()).expect("reopen");
        let log = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");
        let records = log.read_from(0, usize::MAX).await.expect("read");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, Bytes::from_static(b"persisted"));
        assert_eq!(records[1].payload, Bytes::from_static(b"twice"));
        assert_eq!(log.tail_offset().await.expect("tail"), 2);
    }

    #[tokio::test]
    async fn on_commit_is_durable_by_the_time_append_returns() {
        let dir = tempdir().expect("dir");
        let storage = DurableStorage::open(
            dir.path(),
            LogConfig {
                fsync_mode: FsyncMode::OnCommit,
                ..config()
            },
        )
        .expect("open");
        let log = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");

        let result = log.append(&payloads(&["a", "b"])).await.expect("append");
        assert!(log.durable_offset() > result.last_offset);
        assert_eq!(log.unsynced_bytes(), 0);
    }
}
