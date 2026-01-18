use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::Result;

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

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub segment_size_bytes: u64,
    pub index_spacing_bytes: u64,
    pub fsync_mode: FsyncMode,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1024 * 1024,
            index_spacing_bytes: 4 * 1024,
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
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
