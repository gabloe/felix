//! Cutting a log back: dropping a suffix for replication, or discarding
//! everything to start again at a new base.

use crate::disk_log::now_micros;
use crate::log::{Offset, SegmentId};
use crate::segment::format::{RECORD_HEADER_LEN, SEGMENT_HEADER_LEN};
use crate::segment::writer::ResumeState;
use crate::segment::{
    ReadBudget, ScanStart, SegmentReader, SegmentWriter, SparseIndex, index_file_name,
    scan_segment, segment_file_name,
};
use crate::{Result, metrics_names};

use super::{SealedEntry, SegmentSet};

impl SegmentSet {
    /// Drop every record at or after `offset`.
    ///
    /// Used by replication to discard a divergent suffix. Truncating to at or
    /// beyond the tail is a no-op; truncating below the base offset empties the
    /// log.
    pub fn truncate(&mut self, offset: Offset) -> Result<()> {
        if offset >= self.tail_offset() {
            return Ok(());
        }

        // Remove whole segments that begin at or after the cut.
        while let Some(entry) = self.sealed.last() {
            if entry.descriptor.base_offset >= offset {
                let id = entry.descriptor.id;
                self.sealed.pop();
                self.remove_segment_files(id)?;
            } else {
                break;
            }
        }

        // The active segment either survives with a shorter tail or is replaced
        // by whichever sealed segment now contains the cut.
        if self.active.base_offset() >= offset {
            let active_id = self.active.id();
            let resume = match self.sealed.pop() {
                Some(entry) => entry,
                None => {
                    // Nothing left at all: restart the log at `offset`.
                    self.replace_active(active_id + 1, offset, SEGMENT_HEADER_LEN, offset, 0)?;
                    self.remove_segment_files(active_id)?;
                    return Ok(());
                }
            };
            self.adopt_sealed_as_active(resume)?;
            self.remove_segment_files(active_id)?;
        }

        self.truncate_active_to(offset)
    }

    /// Discard every record and start again, empty, at `base_offset`.
    ///
    /// For a follower rebuilding a shard whose copy is wrong rather than
    /// merely short: nothing here is worth keeping, and the leader's oldest
    /// record is where the new copy begins. Unlike `truncate`, the base may
    /// move in either direction.
    pub fn reset_to(&mut self, base_offset: Offset) -> Result<()> {
        while let Some(entry) = self.sealed.pop() {
            self.remove_segment_files(entry.descriptor.id)?;
        }
        let active_id = self.active.id();
        self.replace_active(
            active_id + 1,
            base_offset,
            SEGMENT_HEADER_LEN,
            base_offset,
            0,
        )?;
        self.remove_segment_files(active_id)
    }

    /// Reopen a sealed segment as the active one so appends resume inside it.
    fn adopt_sealed_as_active(&mut self, entry: SealedEntry) -> Result<()> {
        let outcome = scan_segment(
            &self.dir.join(segment_file_name(entry.descriptor.id)),
            entry.descriptor.id,
            &self.label,
            self.config.index_spacing_bytes,
            ScanStart::Full,
            self.config.repair_checksum_tail,
        )?;
        self.active = SegmentWriter::reopen(
            &self.dir,
            entry.descriptor.id,
            ResumeState {
                base_offset: entry.descriptor.base_offset,
                valid_bytes: outcome.valid_bytes,
                next_offset: outcome.next_offset,
                record_count: outcome.record_count,
                index: outcome.index,
            },
            self.config.index_spacing_bytes,
        )?;
        self.active_reader = SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?;
        Ok(())
    }

    /// Cut the active segment back to `offset`, keeping records below it.
    fn truncate_active_to(&mut self, offset: Offset) -> Result<()> {
        if offset >= self.active.next_offset() {
            return Ok(());
        }

        // Find the byte position of `offset` by seeking with the index and
        // walking forward — the same path a read takes.
        let mut budget = ReadBudget::unbounded();
        let mut kept = Vec::new();
        self.active_reader.read_from(
            self.active.index(),
            self.active.base_offset(),
            self.active.size_bytes(),
            &mut budget,
            &self.label,
            &mut kept,
        )?;
        let keep_count = kept
            .iter()
            .take_while(|record| record.offset < offset)
            .count();
        let keep_bytes = SEGMENT_HEADER_LEN
            + kept
                .iter()
                .take(keep_count)
                .map(|record| RECORD_HEADER_LEN + record.payload.len() as u64)
                .sum::<u64>();

        let id = self.active.id();
        let base_offset = self.active.base_offset();
        self.replace_active(id, base_offset, keep_bytes, offset, keep_count as u64)
    }

    /// Swap in an active writer over segment `id`, either reopened at
    /// `valid_bytes` or created fresh when the file does not exist.
    fn replace_active(
        &mut self,
        id: SegmentId,
        base_offset: Offset,
        valid_bytes: u64,
        next_offset: Offset,
        record_count: u64,
    ) -> Result<()> {
        let path = self.dir.join(segment_file_name(id));
        self.active = if path.exists() {
            let index = SparseIndex::load(&self.dir.join(index_file_name(id)), base_offset)
                .unwrap_or_else(|| SparseIndex::new(base_offset));
            SegmentWriter::reopen(
                &self.dir,
                id,
                ResumeState {
                    base_offset,
                    valid_bytes,
                    next_offset,
                    record_count,
                    // A truncation invalidates every index entry past the cut;
                    // rebuild from the surviving prefix rather than trusting it.
                    index: rebuild_index_prefix(index, valid_bytes),
                },
                self.config.index_spacing_bytes,
            )?
        } else {
            SegmentWriter::create(
                &self.dir,
                id,
                base_offset,
                now_micros(),
                self.config.preallocate_bytes(),
                self.config.index_spacing_bytes,
            )?
        };
        self.active_reader = SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?;
        self.bump_next_segment_id(id + 1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(())
    }
}

/// Drop index entries that point past `valid_bytes`.
fn rebuild_index_prefix(index: SparseIndex, valid_bytes: u64) -> SparseIndex {
    let mut rebuilt = SparseIndex::new(index.base_offset());
    for entry in index.entries() {
        if entry.position < valid_bytes {
            rebuilt.push(*entry);
        }
    }
    rebuilt
}

#[cfg(test)]
mod tests;
