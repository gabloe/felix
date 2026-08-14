// The append side of a single segment.
//
// A writer owns one data file plus its index and knows nothing about rollover,
// retention, or fsync policy — it exposes `append` and `sync` and lets
// `crate::disk_log` decide when to call them.
//
// Performance shape, and why:
//
// * **One `write` per batch, not per record.** Records are encoded into a
//   reusable staging buffer and handed to the kernel in a single call. Syscall
//   count is the thing that scales with batch size otherwise, and it dominates
//   at small payloads.
// * **`write` and `sync` are separate.** A `write` lands in the page cache and
//   is cheap; only `sync` touches the device. Keeping them apart is what lets
//   the log amortise one device flush across many appends (see
//   `disk_log::sync`), which is the single largest lever on durable throughput.
// * **Blocks are reserved up front.** See `segment::io::preallocate`.
// * **The staging buffer is never freed.** Steady-state appends do no
//   allocation at all beyond growing it once to the high-water batch size.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::Result;
use crate::log::{AppendRecord, Offset, SegmentDescriptor, SegmentId};
use crate::segment::format::{MAX_PAYLOAD_BYTES, SEGMENT_HEADER_LEN, SegmentHeader, encode_record};
use crate::segment::index::{IndexWriter, SparseIndex};
use crate::segment::io::{preallocate, sync_data, sync_dir};
use crate::segment::{index_file_name, segment_file_name};
use crate::{StorageError, metrics_names};

/// State a recovered segment resumes from, as produced by
/// [`crate::segment::scan_segment`].
///
/// Grouped into one type because the fields are only meaningful together: a
/// `valid_bytes` from one scan paired with a `next_offset` from another would
/// silently corrupt the segment.
#[derive(Debug)]
pub struct ResumeState {
    pub base_offset: Offset,
    pub valid_bytes: u64,
    pub next_offset: Offset,
    pub record_count: u64,
    pub index: SparseIndex,
}

/// The active segment: the only file in a shard that accepts writes.
#[derive(Debug)]
pub struct SegmentWriter {
    id: SegmentId,
    base_offset: Offset,
    path: PathBuf,
    file: File,
    index: IndexWriter,
    /// Bytes handed to the kernel, i.e. the file's logical length.
    size_bytes: u64,
    /// Bytes known to be on stable storage.
    synced_bytes: u64,
    /// Offset the next appended record will take.
    next_offset: Offset,
    record_count: u64,
    /// Reused across appends so steady state allocates nothing.
    staging: Vec<u8>,
    /// A duplicate descriptor used only for flushing, so a sync never has to
    /// hold the lock that guards `file`.
    sync_handle: Arc<File>,
    /// Set when a failed append could not be rolled back. The segment's real
    /// length is then unknown, so further appends are refused.
    poisoned: bool,
}

impl SegmentWriter {
    /// Create a brand new segment starting at `base_offset`.
    pub fn create(
        dir: &Path,
        id: SegmentId,
        base_offset: Offset,
        created_at_micros: u64,
        preallocate_bytes: u64,
        index_spacing_bytes: u64,
    ) -> Result<Self> {
        let path = dir.join(segment_file_name(id));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        preallocate(&file, preallocate_bytes)?;
        file.write_all(&SegmentHeader::new(base_offset, created_at_micros).encode())?;
        // The header must be durable, and the directory entry must be durable
        // too, before any record claims to live in this segment.
        sync_data(&file)?;
        sync_dir(dir)?;

        let index = IndexWriter::open(
            &dir.join(index_file_name(id)),
            SparseIndex::new(base_offset),
        )?
        .with_spacing(index_spacing_bytes);
        let sync_handle = Arc::new(file.try_clone()?);

        Ok(Self {
            id,
            base_offset,
            path,
            file,
            index,
            size_bytes: SEGMENT_HEADER_LEN,
            synced_bytes: SEGMENT_HEADER_LEN,
            next_offset: base_offset,
            record_count: 0,
            staging: Vec::new(),
            sync_handle,
            poisoned: false,
        })
    }

    /// Reopen an existing, already validated segment for further appends.
    ///
    /// Trusts `resume` and does not re-validate.
    pub fn reopen(
        dir: &Path,
        id: SegmentId,
        resume: ResumeState,
        index_spacing_bytes: u64,
    ) -> Result<Self> {
        let ResumeState {
            base_offset,
            valid_bytes,
            next_offset,
            record_count,
            index,
        } = resume;
        let path = dir.join(segment_file_name(id));
        let file = OpenOptions::new().write(true).open(&path)?;
        // Recovery has already decided where valid data ends; make the file
        // agree so an append cannot land after a hole, and position the write
        // cursor there — a freshly opened handle starts at zero and would
        // otherwise overwrite the segment header.
        let mut file = file;
        file.set_len(valid_bytes)?;
        file.seek(SeekFrom::Start(valid_bytes))?;
        sync_data(&file)?;

        let index = IndexWriter::open(&dir.join(index_file_name(id)), index)?
            .with_spacing(index_spacing_bytes);
        let sync_handle = Arc::new(file.try_clone()?);

        Ok(Self {
            id,
            base_offset,
            path,
            file,
            index,
            size_bytes: valid_bytes,
            synced_bytes: valid_bytes,
            next_offset,
            record_count,
            staging: Vec::new(),
            sync_handle,
            poisoned: false,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index(&self) -> &SparseIndex {
        self.index.index()
    }

    /// True when every byte written so far is on stable storage.
    pub fn is_synced(&self) -> bool {
        self.synced_bytes >= self.size_bytes
    }

    /// Bytes written but not yet synced — the exposure window of the current
    /// fsync policy.
    pub fn unsynced_bytes(&self) -> u64 {
        self.size_bytes.saturating_sub(self.synced_bytes)
    }

    pub fn descriptor(&self) -> SegmentDescriptor {
        SegmentDescriptor {
            id: self.id,
            base_offset: self.base_offset,
            // An empty segment has no last offset; report the base so the range
            // is empty rather than wrapping below zero.
            last_offset: self.next_offset.saturating_sub(1).max(self.base_offset),
            size_bytes: self.size_bytes,
        }
    }

    /// How large this segment would become if `records` were appended.
    ///
    /// Used by rollover to decide *before* writing, so a batch is never split
    /// across two segments.
    pub fn projected_size(&self, records: &[AppendRecord]) -> u64 {
        records.iter().fold(self.size_bytes, |acc, record| {
            acc + crate::segment::format::RECORD_HEADER_LEN + record.payload.len() as u64
        })
    }

    /// Append a batch, assigning consecutive offsets from `next_offset`.
    ///
    /// The bytes reach the page cache before this returns; they are *not*
    /// durable until [`SegmentWriter::sync`] succeeds. Callers that promise
    /// durability must sequence the two.
    pub fn append(&mut self, records: &[AppendRecord]) -> Result<(Offset, Offset)> {
        debug_assert!(!records.is_empty());

        if self.poisoned {
            return Err(StorageError::Unsupported(
                "segment writer is poisoned after a failed append could not be rolled back",
            ));
        }

        for record in records {
            if record.payload.len() > MAX_PAYLOAD_BYTES as usize {
                return Err(StorageError::Unsupported(
                    "record payload exceeds the maximum supported size",
                ));
            }
        }

        self.staging.clear();
        let first_offset = self.next_offset;
        // Record boundaries for the index, captured while encoding so the index
        // never needs a second pass over the batch.
        let mut boundaries = Vec::with_capacity(records.len());
        let mut position = self.size_bytes;
        let mut offset = self.next_offset;
        for record in records {
            let written = encode_record(
                &mut self.staging,
                offset,
                record.timestamp_micros,
                &record.payload,
            );
            boundaries.push((offset, position, written));
            position += written;
            offset += 1;
        }

        // One syscall for the whole batch.
        //
        // A failure here can still have written some of the buffer: `write_all`
        // loops over partial writes, so an error means "some prefix landed",
        // not "nothing happened". Left alone, those bytes sit past the last
        // record this writer knows about, the file cursor points past them, and
        // the next append lands after the debris - turning a failed write into
        // interior corruption that recovery must refuse to start on, or into a
        // duplicate if the caller retries.
        //
        // So a failed append rewinds the file to the last good byte. If the
        // rewind itself fails there is no way to restore the invariant, and the
        // writer refuses further appends rather than building on a file whose
        // shape it no longer knows.
        if let Err(err) = self.file.write_all(&self.staging) {
            self.rewind_after_failed_write()?;
            return Err(StorageError::Io(err));
        }

        self.size_bytes = position;
        self.next_offset = offset;
        self.record_count += records.len() as u64;
        for (offset, position, written) in boundaries {
            self.index.observe_record(offset, position, written)?;
        }

        metrics::counter!(metrics_names::APPEND_RECORDS_TOTAL).increment(records.len() as u64);
        metrics::counter!(metrics_names::APPEND_BYTES_TOTAL).increment(self.staging.len() as u64);
        metrics::histogram!(metrics_names::APPEND_BATCH_RECORDS).record(records.len() as f64);

        Ok((first_offset, self.next_offset - 1))
    }

    /// Flush every written byte to stable storage.
    ///
    /// Cheap and idempotent when nothing has changed since the last call, which
    /// matters because the periodic syncer polls on a timer regardless of load.
    pub fn sync(&mut self) -> Result<()> {
        if self.is_synced() {
            return Ok(());
        }
        let pending = self.size_bytes;
        let started = std::time::Instant::now();
        sync_data(&self.file).map_err(|err| StorageError::SyncFailed(err.to_string()))?;
        // The index is rebuildable, so it gets a flush but not a device sync.
        self.index.flush()?;
        self.synced_bytes = pending;

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Restore the file to the last byte this writer accounts for.
    ///
    /// Called only after a failed append, so that a partial write leaves no
    /// trace and the segment stays exactly as it was before the attempt.
    fn rewind_after_failed_write(&mut self) -> Result<()> {
        let valid = self.size_bytes;
        let restore = self
            .file
            .set_len(valid)
            .and_then(|()| self.file.seek(SeekFrom::Start(valid)).map(|_| ()));
        match restore {
            Ok(()) => Ok(()),
            Err(err) => {
                // The segment's on-disk shape no longer matches this writer's
                // idea of it, and nothing here can reconcile them.
                self.poisoned = true;
                Err(StorageError::Io(std::io::Error::other(format!(
                    "append failed and the segment could not be rewound to {valid}: {err}"
                ))))
            }
        }
    }

    /// Record that everything up to `bytes` is now durable.
    ///
    /// The log-level syncer flushes through a cloned descriptor so it can do so
    /// without holding the writer lock; this is how the result gets back.
    pub fn mark_synced(&mut self, bytes: u64) {
        self.synced_bytes = self.synced_bytes.max(bytes.min(self.size_bytes));
    }

    /// A second descriptor for the same file, for flushing off the write path.
    pub fn sync_handle(&self) -> Arc<File> {
        Arc::clone(&self.sync_handle)
    }

    /// Finish this segment: sync data and index, then release any preallocated
    /// blocks past the last record so the file on disk is exactly its contents.
    ///
    /// Takes `&mut self` rather than consuming, so the caller can keep the
    /// sealed writer around to serve reads until it swaps in a replacement.
    pub fn seal(&mut self) -> Result<SegmentDescriptor> {
        self.sync()?;
        self.index.sync()?;
        self.file.set_len(self.size_bytes)?;
        sync_data(&self.file).map_err(|err| StorageError::SyncFailed(err.to_string()))?;
        Ok(self.descriptor())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{ScanOutcome, ScanStart, scan_segment};
    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};

    fn record(payload: &str) -> AppendRecord {
        AppendRecord {
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            timestamp_micros: 42,
        }
    }

    fn new_writer(dir: &TempDir, base: Offset) -> SegmentWriter {
        SegmentWriter::create(dir.path(), 0, base, 1, 0, 4096).expect("create")
    }

    fn scan(dir: &TempDir) -> ScanOutcome {
        scan_segment(
            &dir.path().join(segment_file_name(0)),
            0,
            "t/ns/s/0",
            4096,
            ScanStart::Full,
            true,
        )
        .expect("scan")
    }

    #[test]
    fn a_fresh_segment_holds_only_its_header() {
        let dir = tempdir().expect("dir");
        let writer = new_writer(&dir, 0);
        assert_eq!(writer.size_bytes(), SEGMENT_HEADER_LEN);
        assert_eq!(writer.next_offset(), 0);
        assert_eq!(writer.record_count(), 0);
        assert!(writer.is_synced());
        assert_eq!(scan(&dir).record_count, 0);
    }

    #[test]
    fn appends_assign_consecutive_offsets() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 100);

        let (first, last) = writer
            .append(&[record("a"), record("b"), record("c")])
            .expect("append");
        assert_eq!((first, last), (100, 102));

        let (first, last) = writer.append(&[record("d")]).expect("append");
        assert_eq!((first, last), (103, 103));
        assert_eq!(writer.next_offset(), 104);
        assert_eq!(writer.record_count(), 4);
    }

    #[test]
    fn appended_records_read_back_in_order() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer
            .append(&[record("first"), record("second")])
            .expect("append");
        writer.sync().expect("sync");

        let outcome = scan(&dir);
        assert_eq!(outcome.record_count, 2);
        assert_eq!(outcome.next_offset, 2);
        assert!(outcome.torn_tail.is_none());
    }

    #[test]
    fn sync_state_tracks_written_versus_durable_bytes() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        assert_eq!(writer.unsynced_bytes(), 0);

        writer.append(&[record("payload")]).expect("append");
        assert!(!writer.is_synced());
        assert_eq!(
            writer.unsynced_bytes(),
            crate::segment::format::RECORD_HEADER_LEN + 7
        );

        writer.sync().expect("sync");
        assert!(writer.is_synced());
        assert_eq!(writer.unsynced_bytes(), 0);

        // Syncing again with nothing pending is a no-op, not an error.
        writer.sync().expect("resync");
    }

    #[test]
    fn projected_size_predicts_the_post_append_size() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        let batch = [record("aaaa"), record("bb")];
        let projected = writer.projected_size(&batch);
        writer.append(&batch).expect("append");
        assert_eq!(writer.size_bytes(), projected);
    }

    #[test]
    fn empty_payloads_are_valid_records() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("")]).expect("append");
        writer.sync().expect("sync");
        assert_eq!(scan(&dir).record_count, 1);
    }

    #[test]
    fn an_oversized_payload_is_rejected_without_writing() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        let before = writer.size_bytes();
        let huge = AppendRecord {
            payload: Bytes::from(vec![0u8; MAX_PAYLOAD_BYTES as usize + 1]),
            timestamp_micros: 0,
        };
        assert!(matches!(
            writer.append(&[huge]).expect_err("oversized"),
            StorageError::Unsupported(_)
        ));
        assert_eq!(writer.size_bytes(), before);
        assert_eq!(writer.next_offset(), 0);
    }

    #[test]
    fn a_rejected_batch_leaves_no_partial_records() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        let batch = vec![
            record("fine"),
            AppendRecord {
                payload: Bytes::from(vec![0u8; MAX_PAYLOAD_BYTES as usize + 1]),
                timestamp_micros: 0,
            },
        ];
        assert!(writer.append(&batch).is_err());
        writer.sync().expect("sync");
        // The valid first record must not have been written either: the batch
        // is validated in full before any byte reaches the file.
        assert_eq!(scan(&dir).record_count, 0);
    }

    #[test]
    fn descriptor_reports_the_offset_and_byte_range() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 50);
        assert_eq!(writer.descriptor().last_offset, 50);

        writer.append(&[record("a"), record("b")]).expect("append");
        let descriptor = writer.descriptor();
        assert_eq!(descriptor.id, 0);
        assert_eq!(descriptor.base_offset, 50);
        assert_eq!(descriptor.last_offset, 51);
        assert_eq!(descriptor.size_bytes, writer.size_bytes());
    }

    #[test]
    fn reopen_resumes_appending_where_recovery_left_off() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("a"), record("b")]).expect("append");
        writer.sync().expect("sync");
        let outcome = scan(&dir);
        drop(writer);

        let mut writer = SegmentWriter::reopen(
            dir.path(),
            0,
            ResumeState {
                base_offset: 0,
                valid_bytes: outcome.valid_bytes,
                next_offset: outcome.next_offset,
                record_count: outcome.record_count,
                index: outcome.index,
            },
            4096,
        )
        .expect("reopen");
        assert_eq!(writer.next_offset(), 2);
        writer.append(&[record("c")]).expect("append");
        writer.sync().expect("sync");

        let outcome = scan(&dir);
        assert_eq!(outcome.record_count, 3);
        assert_eq!(outcome.next_offset, 3);
        assert!(outcome.torn_tail.is_none());
    }

    #[test]
    fn reopen_truncates_a_torn_tail_off_the_file() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("a"), record("b")]).expect("append");
        writer.sync().expect("sync");
        let valid_bytes = writer.size_bytes();
        drop(writer);

        // Simulate an interrupted third append.
        let path = dir.path().join(segment_file_name(0));
        let mut bytes = std::fs::read(&path).expect("read");
        bytes.extend_from_slice(&[0xAB; 9]);
        std::fs::write(&path, &bytes).expect("write");

        let outcome = scan(&dir);
        assert!(outcome.torn_tail.is_some());
        assert_eq!(outcome.valid_bytes, valid_bytes);

        SegmentWriter::reopen(
            dir.path(),
            0,
            ResumeState {
                base_offset: 0,
                valid_bytes: outcome.valid_bytes,
                next_offset: outcome.next_offset,
                record_count: outcome.record_count,
                index: outcome.index,
            },
            4096,
        )
        .expect("reopen");

        // The file is now exactly the valid prefix, and a rescan is clean —
        // recovery is idempotent.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), valid_bytes);
        assert!(scan(&dir).torn_tail.is_none());
    }

    #[test]
    fn mark_synced_is_monotonic_and_clamped() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("abc")]).expect("append");
        let size = writer.size_bytes();

        writer.mark_synced(size);
        assert!(writer.is_synced());
        // A stale, lower report cannot un-sync durable bytes.
        writer.mark_synced(0);
        assert!(writer.is_synced());
        // Nor can an over-report claim bytes that were never written.
        writer.mark_synced(u64::MAX);
        assert_eq!(writer.unsynced_bytes(), 0);

        writer.append(&[record("def")]).expect("append");
        assert!(!writer.is_synced());
    }

    #[test]
    fn a_partially_written_batch_is_rewound_and_leaves_no_debris() {
        use std::io::{Seek as _, SeekFrom as _SeekFrom, Write as _};

        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("first")]).expect("append");
        let good_len = writer.size_bytes();

        // Stand in for a `write_all` that failed part-way: bytes on disk past
        // the last byte the writer accounts for. This is what the OS can leave
        // behind on ENOSPC or EIO mid-batch.
        {
            let path = dir.path().join(segment_file_name(0));
            let mut handle = OpenOptions::new().write(true).open(&path).expect("open");
            handle.seek(_SeekFrom::End(0)).expect("seek");
            handle.write_all(&[0xAB; 11]).expect("write debris");
            handle.sync_all().expect("sync");
        }
        assert_eq!(
            std::fs::metadata(dir.path().join(segment_file_name(0)))
                .expect("meta")
                .len(),
            good_len + 11
        );

        writer.rewind_after_failed_write().expect("rewind");

        // The debris is gone and the file matches the writer's own accounting.
        assert_eq!(
            std::fs::metadata(dir.path().join(segment_file_name(0)))
                .expect("meta")
                .len(),
            good_len
        );

        // And the segment keeps working: the next append lands contiguously
        // rather than after a hole, so a scan stays clean.
        writer
            .append(&[record("second")])
            .expect("append after rewind");
        writer.sync().expect("sync");
        let outcome = scan(&dir);
        assert_eq!(outcome.record_count, 2);
        assert!(
            outcome.torn_tail.is_none(),
            "rewound segment should scan clean, got {:?}",
            outcome.torn_tail
        );
    }

    #[test]
    fn a_poisoned_writer_refuses_further_appends() {
        let dir = tempdir().expect("dir");
        let mut writer = new_writer(&dir, 0);
        writer.append(&[record("a")]).expect("append");
        // A rewind that cannot be performed leaves the segment's real length
        // unknown; building on it would compound the damage.
        writer.poisoned = true;
        assert!(matches!(
            writer.append(&[record("b")]).expect_err("poisoned"),
            StorageError::Unsupported(_)
        ));
    }

    #[test]
    fn seal_trims_preallocated_space() {
        let dir = tempdir().expect("dir");
        // Reserve far more than the records need.
        let mut writer =
            SegmentWriter::create(dir.path(), 0, 0, 1, 1024 * 1024, 4096).expect("create");
        writer.append(&[record("small")]).expect("append");
        let expected = writer.size_bytes();

        let descriptor = writer.seal().expect("seal");
        drop(writer);
        assert_eq!(descriptor.size_bytes, expected);
        assert_eq!(
            std::fs::metadata(dir.path().join(segment_file_name(0)))
                .expect("meta")
                .len(),
            expected
        );
        assert!(scan(&dir).torn_tail.is_none());
    }

    #[test]
    fn the_index_is_populated_and_reloadable() {
        let dir = tempdir().expect("dir");
        let mut writer = SegmentWriter::create(dir.path(), 0, 0, 1, 0, 64).expect("create");
        for i in 0..20 {
            writer
                .append(&[record(&format!("record-{i}"))])
                .expect("append");
        }
        writer.sync().expect("sync");
        let in_memory = writer.index().clone();
        assert!(in_memory.len() > 1);

        let reloaded = SparseIndex::load(&dir.path().join(index_file_name(0)), 0).expect("load");
        assert_eq!(reloaded, in_memory);

        // And a rebuild from the segment must agree with both.
        let rebuilt = scan_segment(
            &dir.path().join(segment_file_name(0)),
            0,
            "t/ns/s/0",
            64,
            ScanStart::Full,
            true,
        )
        .expect("scan")
        .index;
        assert_eq!(rebuilt, in_memory);
    }

    #[test]
    fn creating_over_an_existing_segment_fails() {
        let dir = tempdir().expect("dir");
        let _writer = new_writer(&dir, 0);
        assert!(SegmentWriter::create(dir.path(), 0, 0, 1, 0, 4096).is_err());
    }
}
