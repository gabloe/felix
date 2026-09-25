//! Making the active segment durable.
//!
//! One flush covers every record written before it, which is what lets group
//! commit in `sync` hand a single flush to many waiting appends.

use std::sync::Arc;

use super::LogInner;
use crate::io::sync_data;
use crate::log::Offset;
use crate::{Result, StorageError, metrics_names};

impl LogInner {
    /// Flush the active segment and report the exclusive offset bound now
    /// durable.
    ///
    /// The file handle and the offset it covers are captured under the lock,
    /// then the lock is released before the flush: an `fsync` must never be held
    /// across the lock that appends need.
    pub(super) async fn flush(self: Arc<Self>) -> Result<Offset> {
        let (handle, segment_id, synced_bytes, durable_upto) = {
            let segments = self.segments.read();
            let active = segments.active();
            (
                active.sync_handle(),
                active.id(),
                active.size_bytes(),
                segments.tail_offset(),
            )
        };
        // Taken after the active handle, so a rollover that lands in between is
        // seen here rather than missed: `commit_roll` sets it before releasing
        // the lock this read just took.
        let retired = self.pending_seal.lock().clone();
        let had_pending_seal = retired.is_some();

        let started = std::time::Instant::now();

        // With `FELIX_STORAGE_IO_URING=1` on Linux the fsync goes into a ring
        // and the kernel completes it. `None` means the ring is not
        // available -- an old kernel, or a container that forbids the syscall --
        // and the flush thread below takes over, because durability must not
        // depend on an optimisation being present (#548).
        #[cfg(target_os = "linux")]
        let via_uring: Option<std::io::Result<()>> = if crate::io::uring_fsync::enabled() {
            use std::os::unix::io::AsRawFd;
            // The retired segment first: `durable_upto` covers records in both,
            // and may not be reported until every one of them is on disk.
            let mut result = Some(Ok(()));
            if let Some(retired) = retired.as_ref() {
                result = crate::io::uring_fsync::fsync(retired.as_raw_fd()).await;
            }
            if matches!(result, Some(Ok(()))) {
                result = crate::io::uring_fsync::fsync(handle.as_raw_fd()).await;
            }
            result
        } else {
            None
        };
        #[cfg(not(target_os = "linux"))]
        let via_uring: Option<std::io::Result<()>> = None;

        let outcome = match via_uring {
            Some(result) => result,
            None => {
                self.flusher
                    .run(move || {
                        // The retired segment first, for the same reason.
                        if let Some(retired) = retired {
                            sync_data(&retired)?;
                        }
                        sync_data(&handle)
                    })
                    .await
            }
        };
        if let Err(err) = outcome {
            let err = StorageError::SyncFailed(err.to_string());
            // A flush that could not cover the retired segment is the same
            // failure as a seal that could not, and is equally terminal.
            if had_pending_seal {
                self.record_roll_failure(&err);
            }
            return Err(err);
        }

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        #[cfg(test)]
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        {
            let mut segments = self.segments.write();
            // A rollover may have swapped the active segment while the flush was
            // in flight. Its records were covered by the `pending_seal` sync
            // above, and the retired writer is owned by the rollover task, so
            // there is nothing to record against it here.
            if segments.active().id() == segment_id {
                segments.active_mut().mark_synced(synced_bytes);
            }
            metrics::gauge!(metrics_names::UNSYNCED_BYTES)
                .set(segments.active().unsynced_bytes() as f64);
        }

        // Rechecked after the flush, not only before it. A rollover can fail
        // while this flush is in flight, and `durable_upto` spans the retired
        // segment as well as the active one -- reporting it would acknowledge
        // records that the failed seal left unflushed.
        self.check_roll_state()?;

        Ok(durable_upto)
    }

    /// Wait until every offset below `target` is durable, flushing if needed.
    pub(super) async fn ensure_durable(self: &Arc<Self>, target: Offset) -> Result<()> {
        self.durability
            .ensure_durable(target, || Arc::clone(self).flush())
            .await
    }
}
