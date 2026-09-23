//! One [`DiskLog`] per shard under a common root directory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::Result;
use crate::log::{BoxFuture, LogConfig, LogProvider, Offset, ShardKey};

use super::{DiskLog, layout};

/// Opens one [`DiskLog`] per shard under a common root directory.
///
/// Repeated opens of the same shard return the same log. Two independent
/// writers over one directory would interleave offsets and corrupt the segment,
/// so the cache is a correctness requirement, not an optimisation.
#[derive(Debug)]
pub struct DiskLogProvider {
    root: PathBuf,
    config: LogConfig,
    open_logs: Mutex<HashMap<ShardKey, DiskLog>>,
}

impl DiskLogProvider {
    pub fn new(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        config.validate()?;
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            config,
            open_logs: Mutex::new(HashMap::new()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Open or return the cached log for `shard`, creating it to begin at
    /// `base_offset` if it does not exist yet.
    ///
    /// For a replica being given a shard whose early history is already gone.
    /// An existing shard keeps its own base, so this is safe to call on every
    /// contact rather than only the first.
    pub fn open_shard_at(&self, shard: &ShardKey, base_offset: Offset) -> Result<DiskLog> {
        let mut open_logs = self.open_logs.lock();
        if let Some(log) = open_logs.get(shard) {
            return Ok(log.clone());
        }
        let log = DiskLog::open_at(
            layout::shard_dir(&self.root, shard),
            layout::shard_label(shard),
            self.config.clone(),
            base_offset,
        )?;
        open_logs.insert(shard.clone(), log.clone());
        Ok(log)
    }

    /// Open or return the cached log for `shard`.
    pub fn open_shard(&self, shard: &ShardKey) -> Result<DiskLog> {
        // Recovery runs under the lock: two callers racing to open the same new
        // shard must not both scan and both create segment zero.
        let mut open_logs = self.open_logs.lock();
        if let Some(log) = open_logs.get(shard) {
            return Ok(log.clone());
        }
        let log = DiskLog::open(
            layout::shard_dir(&self.root, shard),
            layout::shard_label(shard),
            self.config.clone(),
        )?;
        open_logs.insert(shard.clone(), log.clone());
        Ok(log)
    }

    /// Shard keys this provider currently has open.
    pub fn open_shards(&self) -> Vec<ShardKey> {
        self.open_logs.lock().keys().cloned().collect()
    }

    /// Flush and stop every open log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let logs: Vec<DiskLog> = self.open_logs.lock().values().cloned().collect();
        let mut first_error = None;
        for log in logs {
            if let Err(err) = log.shutdown().await {
                tracing::error!(shard = %log.label(), error = %err, "failed to flush log on shutdown");
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl LogProvider for DiskLogProvider {
    type Log = DiskLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>> {
        let shard = shard.clone();
        Box::pin(async move { self.open_shard(&shard) })
    }
}
