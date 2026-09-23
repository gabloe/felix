//! The set of segments that make up one shard's log: many sealed, exactly one
//! active.
//!
//! This is where rollover, offset-to-segment routing and truncation live. It is
//! entirely synchronous and holds no locks of its own — `DiskLog` owns the lock
//! and calls in.

mod rollover;
mod truncation;

#[cfg(test)]
mod test_support;

pub use rollover::{PreparedSegment, RollOutcome, RollPlan};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::io::read_at;
use crate::log::{AppendRecord, LogConfig, LogRecord, Offset, SegmentDescriptor, SegmentId};
use crate::segment::{
    ReadBudget, SegmentReader, SegmentWriter, SparseIndex, index_file_name, segment_file_name,
};
use crate::{Result, StorageError, metrics_names};

/// Everything on disk for one shard.
#[derive(Debug)]
pub struct SegmentSet {
    dir: PathBuf,
    label: String,
    config: LogConfig,
    /// Ordered by `base_offset`, oldest first.
    sealed: Vec<SealedEntry>,
    active: SegmentWriter,
    /// Read handle on the active segment. Recreated on every roll.
    active_reader: SegmentReader,
    /// Next free segment id.
    ///
    /// Atomic because `roll_plan` runs under a *read* lock — it has to, so
    /// appends continue while a replacement is built — and two plans racing on
    /// the same id would both try to create the same file, with the loser
    /// failing on `create_new`.
    next_segment_id: AtomicU64,
}

impl SegmentSet {
    /// Take ownership of an already recovered set of segments.
    pub fn new(
        dir: PathBuf,
        label: String,
        config: LogConfig,
        sealed: Vec<SealedEntry>,
        active: SegmentWriter,
    ) -> Result<Self> {
        let active_reader = SegmentReader::open(active.path(), active.id(), active.base_offset())?;
        let next_segment_id = AtomicU64::new(active.id() + 1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((sealed.len() + 1) as f64);
        Ok(Self {
            dir,
            label,
            config,
            sealed,
            active,
            active_reader,
            next_segment_id,
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Offset the next appended record will take.
    pub fn tail_offset(&self) -> Offset {
        self.active.next_offset()
    }

    /// Oldest offset still readable. Rises only when segments are deleted.
    pub fn base_offset(&self) -> Offset {
        self.sealed
            .first()
            .map(|entry| entry.descriptor.base_offset)
            .unwrap_or_else(|| self.active.base_offset())
    }

    pub fn active(&self) -> &SegmentWriter {
        &self.active
    }

    pub fn active_mut(&mut self) -> &mut SegmentWriter {
        &mut self.active
    }

    /// Every segment, oldest first.
    pub fn descriptors(&self) -> Vec<SegmentDescriptor> {
        self.sealed
            .iter()
            .map(|entry| entry.descriptor.clone())
            .chain(std::iter::once(self.active.descriptor()))
            .collect()
    }

    /// Append a batch, rolling to a new segment first if it would not fit.
    ///
    /// A batch is never split across segments: offsets stay contiguous either
    /// way, but keeping a batch whole means one `write` call and one index
    /// update per append regardless of where the boundary falls.
    pub fn append(&mut self, records: &[AppendRecord]) -> Result<(Offset, Offset)> {
        // An empty active segment must accept the batch even when it is
        // oversized — otherwise a record larger than `segment_size_bytes` could
        // never be written at all. Such a record gets a segment to itself and
        // the next append rolls again.
        //
        // Normally `DiskLog::append` has already rolled off-thread by this
        // point; this is the fallback for a roll that became necessary in the
        // window since that check, and for callers that drive `SegmentSet`
        // directly.
        if self.would_roll(records) {
            self.roll()?;
        }
        self.active.append(records)
    }

    /// Read records from `start` onward, spending at most `budget`.
    ///
    /// Walks segments in offset order, so results are strictly ascending with no
    /// duplicates and no gaps inside the data that is present.
    pub fn read(&self, start: Offset, mut budget: ReadBudget) -> Result<Vec<LogRecord>> {
        let mut out = Vec::new();
        if start >= self.tail_offset() {
            return Ok(out);
        }

        for entry in &self.sealed {
            if budget.is_spent() {
                return Ok(out);
            }
            // Skip segments entirely below the requested start.
            if entry.next_offset() <= start {
                continue;
            }
            entry.reader.read_from(
                &entry.index,
                start,
                entry.descriptor.size_bytes,
                &mut budget,
                &self.label,
                &mut out,
            )?;
        }

        if !budget.is_spent() && self.active.next_offset() > start {
            self.active_reader.read_from(
                self.active.index(),
                start,
                self.active.size_bytes(),
                &mut budget,
                &self.label,
                &mut out,
            )?;
        }
        Ok(out)
    }

    /// Seal the active segment and report a verifiable summary of it.
    pub fn seal_active(&mut self) -> Result<(SegmentDescriptor, u64)> {
        let descriptor = self.active.seal()?;
        let checksum = checksum_file(self.active.path())?;
        Ok((descriptor, checksum))
    }

    /// Delete whole sealed segments from the head until the configured
    /// retention bounds are satisfied.
    ///
    /// Head-only and whole-segment: partial segments are never rewritten, which
    /// is what lets recovery keep trusting "valid bytes end at EOF". The active
    /// segment is never a candidate, so a log always retains at least the
    /// records written since the last roll.
    ///
    /// Advancing `base_offset` is a side effect of removing the head entry, and
    /// both happen under the caller's write lock — so a reader either sees a
    /// segment and can read it, or sees a raised base offset and gets
    /// `Trimmed`. It never sees a descriptor whose file is gone.
    pub fn enforce_retention(&mut self, now_micros: u64) -> Result<RetentionOutcome> {
        let max_bytes = self.config.retention_bytes;
        let max_age_micros = self
            .config
            .retention_age
            .map(|age| age.as_micros().min(u128::from(u64::MAX)) as u64);
        let mut outcome = RetentionOutcome::default();
        if max_bytes.is_none() && max_age_micros.is_none() {
            outcome.base_offset = self.base_offset();
            return Ok(outcome);
        }

        let mut total_bytes: u64 = self
            .sealed
            .iter()
            .map(|entry| entry.descriptor.size_bytes)
            .sum::<u64>()
            + self.active.size_bytes();

        while let Some(head) = self.sealed.first() {
            let id = head.descriptor.id;
            let size = head.descriptor.size_bytes;

            let over_size = max_bytes.is_some_and(|max| total_bytes > max);
            let too_old = match max_age_micros {
                // The newest record decides: a segment is only expired once
                // every record in it is, so nothing younger than the bound goes.
                Some(max_age) => match self.newest_timestamp(head)? {
                    Some(newest) => now_micros.saturating_sub(newest) > max_age,
                    // A sealed segment always holds a record; treat an
                    // unreadable timestamp as "keep" rather than deleting on
                    // missing evidence.
                    None => false,
                },
                None => false,
            };
            if !over_size && !too_old {
                break;
            }

            // Drop the entry first: it owns the reader's descriptor, and
            // closing before unlinking keeps this correct on platforms that
            // refuse to remove an open file.
            self.sealed.remove(0);
            self.remove_segment_files(id)?;
            total_bytes = total_bytes.saturating_sub(size);
            outcome.segments_deleted += 1;
            outcome.bytes_reclaimed += size;
        }

        outcome.base_offset = self.base_offset();
        if outcome.segments_deleted > 0 {
            metrics::counter!(metrics_names::RETENTION_SEGMENTS_DELETED_TOTAL)
                .increment(outcome.segments_deleted as u64);
            metrics::counter!(metrics_names::RETENTION_BYTES_RECLAIMED_TOTAL)
                .increment(outcome.bytes_reclaimed);
            metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        }
        metrics::gauge!(metrics_names::RETENTION_BASE_OFFSET).set(outcome.base_offset as f64);
        Ok(outcome)
    }

    /// Timestamp of the newest record in a sealed segment.
    fn newest_timestamp(&self, entry: &SealedEntry) -> Result<Option<u64>> {
        let mut out = Vec::new();
        let mut budget = ReadBudget::new(usize::MAX, 1);
        entry.reader.read_from(
            &entry.index,
            entry.descriptor.last_offset,
            entry.descriptor.size_bytes,
            &mut budget,
            &self.label,
            &mut out,
        )?;
        Ok(out.first().map(|record| record.timestamp_micros))
    }

    fn bump_next_segment_id(&self, at_least: SegmentId) {
        self.next_segment_id.fetch_max(at_least, Ordering::AcqRel);
    }

    fn remove_segment_files(&self, id: SegmentId) -> Result<()> {
        for path in [
            self.dir.join(segment_file_name(id)),
            self.dir.join(index_file_name(id)),
        ] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        Ok(())
    }
}

/// A finished segment: immutable bytes plus the index needed to seek into them.
#[derive(Debug)]
pub struct SealedEntry {
    pub descriptor: SegmentDescriptor,
    pub index: SparseIndex,
    pub reader: SegmentReader,
}

impl SealedEntry {
    /// Offset one past the last record, matching `SegmentWriter::next_offset`.
    fn next_offset(&self) -> Offset {
        // A sealed segment always holds at least one record, so `last_offset`
        // is real rather than the empty-segment placeholder.
        self.descriptor.last_offset + 1
    }
}

/// What one retention pass reclaimed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionOutcome {
    pub segments_deleted: usize,
    pub bytes_reclaimed: u64,
    /// Oldest offset still readable after the pass.
    pub base_offset: Offset,
}

/// CRC-32 of an entire file, streamed in fixed chunks.
pub fn checksum_file(path: &Path) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut position = 0u64;
    loop {
        let read = read_at(&file, &mut buf, position)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        position += read as u64;
    }
    Ok(u64::from(hasher.finalize()))
}

#[cfg(test)]
mod tests;
