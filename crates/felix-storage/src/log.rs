use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::{Result, StorageError};

pub type Offset = u64;
pub type SegmentId = u64;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
}

#[derive(Debug, Clone)]
pub struct AppendRecord {
    pub payload: Bytes,
    pub timestamp_micros: u64,
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub offset: Offset,
    pub timestamp_micros: u64,
    pub checksum: u32,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    None,
    Periodic { interval: Duration },
    OnCommit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    pub segment_size_bytes: u64,
    pub index_spacing_bytes: u64,
    pub fsync_mode: FsyncMode,
    /// Ceiling on records returned by a single `read_range`, on top of the
    /// caller's byte budget. Payload bytes alone do not bound a response made of
    /// empty records, and an unbounded response is a memory hazard on a path
    /// that will later serve follower catch-up.
    pub max_records_per_read: usize,
    /// Reserve a segment's blocks when it is created. Keeps allocation off the
    /// append path; disable on filesystems where reservations are expensive or
    /// where thin provisioning makes them counter-productive.
    pub preallocate_segments: bool,
    /// Truncate a *complete* trailing record whose checksum does not verify.
    ///
    /// Off by default, and the default is the conservative one. A torn write
    /// and bit rot on an acknowledged record produce identical bytes: a
    /// full-length record that fails its checksum. Recovery cannot tell them
    /// apart, so it refuses to guess and fails to start, naming the segment and
    /// position. Provably *incomplete* writes - a record cut short by end of
    /// file - are still repaired automatically, because nothing could have
    /// acknowledged them.
    ///
    /// Turn this on only where losing the last record is preferable to a broker
    /// that will not start, which is a defensible trade under `FsyncMode::None`
    /// and is not one under `OnCommit`.
    pub repair_checksum_tail: bool,
    /// Checksum every record of every segment at open time.
    ///
    /// Off by default: startup would otherwise cost one full pass over all data
    /// on disk. The active segment is always fully scanned regardless, and every
    /// read verifies the records it returns, so bit rot in cold data is still
    /// caught — just when it is read rather than at boot. Turn this on where a
    /// slow, loud startup is preferable to a late surprise.
    pub verify_all_on_open: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1024 * 1024,
            index_spacing_bytes: 4 * 1024,
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
            max_records_per_read: 10_000,
            preallocate_segments: true,
            repair_checksum_tail: false,
            verify_all_on_open: false,
        }
    }
}

impl LogConfig {
    /// Reject configurations that cannot produce a working log.
    ///
    /// Called once when a log is opened rather than on the append path, so a
    /// misconfiguration fails at startup instead of at the first publish.
    pub fn validate(&self) -> Result<()> {
        // A segment must have room for its header plus at least one record, or
        // rollover would loop without ever making progress.
        let minimum = crate::segment::SEGMENT_HEADER_LEN + crate::segment::RECORD_HEADER_LEN;
        if self.segment_size_bytes < minimum {
            return Err(StorageError::InvalidConfig(
                "segment_size_bytes is too small to hold a single record",
            ));
        }
        if self.index_spacing_bytes == 0 {
            return Err(StorageError::InvalidConfig(
                "index_spacing_bytes must be greater than zero",
            ));
        }
        if self.max_records_per_read == 0 {
            return Err(StorageError::InvalidConfig(
                "max_records_per_read must be greater than zero",
            ));
        }
        if let FsyncMode::Periodic { interval } = self.fsync_mode
            && interval.is_zero()
        {
            // A zero interval is not "sync always" — it is a busy loop. Callers
            // who want per-commit durability must say `OnCommit`.
            return Err(StorageError::InvalidConfig(
                "periodic fsync interval must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Longest window during which an acknowledged record may not be durable.
    ///
    /// `None` means unbounded: with `FsyncMode::None` the log makes no promise
    /// beyond what the operating system decides to do.
    pub fn durability_window(&self) -> Option<Duration> {
        match self.fsync_mode {
            FsyncMode::None => None,
            FsyncMode::Periodic { interval } => Some(interval),
            FsyncMode::OnCommit => Some(Duration::ZERO),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub first_offset: Offset,
    pub last_offset: Offset,
}

#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    pub id: SegmentId,
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ReadRange {
    pub start: Offset,
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SealedSegment {
    pub descriptor: SegmentDescriptor,
    pub checksum: u64,
}

pub trait AppendOnlyLog: Send + Sync {
    fn append(&self, records: &[AppendRecord]) -> BoxFuture<'_, Result<AppendResult>>;
    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>>;
    fn tail_offset(&self) -> BoxFuture<'_, Result<Offset>>;
    fn truncate(&self, offset: Offset) -> BoxFuture<'_, Result<()>>;
    fn seal(&self) -> BoxFuture<'_, Result<SealedSegment>>;
}

pub trait LogProvider: Send + Sync {
    type Log: AppendOnlyLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_log_config_values() {
        let config = LogConfig::default();
        assert_eq!(config.segment_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.index_spacing_bytes, 4 * 1024);
        assert_eq!(
            config.fsync_mode,
            FsyncMode::Periodic {
                interval: Duration::from_millis(250)
            }
        );
    }
}
