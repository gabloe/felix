//! The log every store in this crate is built on: offsets, records, and the
//! [`AppendOnlyLog`] trait that [`crate::DiskLog`] implements.

mod config;

pub use config::{FsyncMode, LogConfig};

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::Result;

/// A record's position in its log. Assigned on append, never reused.
pub type Offset = u64;
/// Names one segment file within a shard's log.
pub type SegmentId = u64;
/// The future every [`AppendOnlyLog`] method returns.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An ordered, append-only sequence of records addressed by offset.
pub trait AppendOnlyLog: Send + Sync {
    fn append(&self, records: &[AppendRecord]) -> BoxFuture<'_, Result<AppendResult>>;
    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>>;
    fn tail_offset(&self) -> BoxFuture<'_, Result<Offset>>;
    fn truncate(&self, offset: Offset) -> BoxFuture<'_, Result<()>>;
    /// Note that `generation` begins at `start_offset`, and persist it.
    ///
    /// Returns whether anything was recorded: a generation at or below the
    /// newest already held is ignored, since a leader re-reporting its own is
    /// ordinary and an older one is stale.
    ///
    /// Default: not recorded. An in-memory log has no divergence to repair,
    /// because it has no follower.
    fn record_generation(&self, _generation: u64, _start_offset: Offset) -> Result<bool> {
        Ok(false)
    }

    /// The generation history, oldest first.
    fn generations(&self) -> Vec<Epoch> {
        Vec::new()
    }

    /// Where `generation` stops, given `tail`. `None` if it is not in the
    /// history — the case that must refuse rather than guess, because the
    /// answer becomes a truncation point.
    fn generation_end(&self, _generation: u64, _tail: Offset) -> Option<Offset> {
        None
    }
    fn seal(&self) -> BoxFuture<'_, Result<SealedSegment>>;
}

/// Opens the log for a shard.
pub trait LogProvider: Send + Sync {
    type Log: AppendOnlyLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>>;
}

/// One record to append. Its offset is assigned by the log.
#[derive(Debug, Clone)]
pub struct AppendRecord {
    pub payload: Bytes,
    pub timestamp_micros: u64,
}

/// The offsets an append was given, first and last inclusive.
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub first_offset: Offset,
    pub last_offset: Offset,
}

/// Where a read starts and how many payload bytes it may return.
#[derive(Debug, Clone)]
pub struct ReadRange {
    pub start: Offset,
    pub max_bytes: usize,
}

/// A record read back from the log.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub offset: Offset,
    pub timestamp_micros: u64,
    pub checksum: u32,
    pub payload: Bytes,
}

/// One leadership generation, and the offset its first record took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epoch {
    pub generation: u64,
    pub start_offset: Offset,
}

/// A segment that will not be written again, with a checksum of its bytes.
#[derive(Debug, Clone)]
pub struct SealedSegment {
    pub descriptor: SegmentDescriptor,
    pub checksum: u64,
}

/// The offset and byte range one segment covers.
#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    pub id: SegmentId,
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub size_bytes: u64,
}

/// Names one shard's log. Caches and counters reuse it, with the cache or
/// scope name in `stream`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
}
