// Reading and validating a single segment file.
//
// Two entry points with different jobs:
//
// * [`scan_segment`] walks a whole segment at startup, verifying every record
//   and rebuilding the sparse index. It is the only place that decides whether
//   damage is a repairable torn tail or committed data that has rotted.
// * [`SegmentReader`] serves range reads on a healthy segment, seeking via the
//   index and decoding forward under a byte budget.
//
// Both go through [`SegmentCursor`], which read-aheads in fixed chunks using
// positioned reads. That keeps peak memory at one chunk plus one record — never
// the segment size — and lets a single descriptor serve concurrent readers,
// because `pread` does not touch the file cursor.

use bytes::Bytes;
use std::fs::File;
use std::io;
use std::path::Path;

use crate::log::{LogRecord, Offset, SegmentId};
use crate::segment::format::{
    Corruption, CorruptionKind, IndexEntry, RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN,
    SegmentHeader, check_offset_continuity, decode_record,
};
use crate::segment::index::SparseIndex;
use crate::segment::io::read_at;
use crate::{Result, StorageError};

/// Read-ahead window. Large enough that a scan of small records is dominated by
/// memcpy rather than syscalls, small enough to stay comfortably in L2.
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// A sliding read-ahead window over a segment file.
///
/// Callers ask for "at least `want` bytes at file position `pos`" and get back
/// however many are available. The window only ever moves forward in practice,
/// so a sequential walk refills once per chunk.
struct SegmentCursor<'a> {
    file: &'a File,
    buf: Vec<u8>,
    /// File position of `buf[0]`.
    buf_start: u64,
    /// Valid bytes in `buf`.
    buf_len: usize,
}

impl<'a> SegmentCursor<'a> {
    fn new(file: &'a File) -> Self {
        Self {
            file,
            buf: vec![0u8; READ_CHUNK_BYTES],
            buf_start: 0,
            buf_len: 0,
        }
    }

    /// Bytes available at `pos`, up to at least `want` where the file allows.
    ///
    /// A returned slice shorter than `want` means end of file, which callers
    /// interpret as truncation.
    fn slice_at(&mut self, pos: u64, want: usize) -> io::Result<&[u8]> {
        let cached = pos >= self.buf_start
            && pos
                .checked_sub(self.buf_start)
                .is_some_and(|delta| delta as usize + want <= self.buf_len);
        if !cached {
            // A record bigger than the window gets a one-off larger read rather
            // than a permanently inflated buffer.
            let capacity = want.max(READ_CHUNK_BYTES);
            if self.buf.len() < capacity {
                self.buf.resize(capacity, 0);
            }
            self.buf_len = read_at(self.file, &mut self.buf[..capacity], pos)?;
            self.buf_start = pos;
        }
        let from = (pos - self.buf_start) as usize;
        let to = self.buf_len.min(from + want);
        Ok(&self.buf[from..to])
    }
}

/// Damage found at the end of a segment that recovery is allowed to discard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTail {
    /// Byte position of the first unreadable record.
    pub position: u64,
    /// Bytes from `position` to the end of the file, all of which are dropped.
    pub discarded_bytes: u64,
    /// Why the record failed to decode.
    pub cause: CorruptionKind,
}

/// What a full validating scan of a segment found.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub header: SegmentHeader,
    /// Offset the next appended record will take.
    pub next_offset: Offset,
    /// File length after the last intact record: where appends resume, and
    /// where recovery truncates to if `torn_tail` is set.
    pub valid_bytes: u64,
    pub record_count: u64,
    /// Index rebuilt from the records actually present.
    pub index: SparseIndex,
    /// `Some` when the tail was damaged and must be truncated to `valid_bytes`.
    pub torn_tail: Option<TornTail>,
}

impl ScanOutcome {
    /// Offset of the last intact record, or `None` for an empty segment.
    pub fn last_offset(&self) -> Option<Offset> {
        (self.record_count > 0).then(|| self.next_offset - 1)
    }
}

/// Decide whether a decode failure at `position` may be truncated away.
///
/// Repairable means "this damage is confined to the end of the file, so no
/// record beyond it could have been acknowledged". Anything else is committed
/// data that has changed underneath us, and callers must fail loudly rather than
/// silently shorten the log.
fn is_repairable_tail(
    kind: &CorruptionKind,
    position: u64,
    claimed_len: Option<u64>,
    file_len: u64,
    repair_checksum_tail: bool,
) -> bool {
    match kind {
        // The header verified, so `payload_len` is the length the writer
        // actually intended — and the file ends before it. Nothing but an
        // unfinished write produces that, and nothing can have acknowledged a
        // record that was never finished. Provably repairable.
        //
        // Before v2 this was only *probably* true: a rotted length field on a
        // complete, acknowledged record produced the same error, and truncating
        // deleted data the caller had been told was safe. The header checksum
        // is what turned the guess into a decision.
        CorruptionKind::Truncated { .. } => true,
        // The header itself did not verify, so nothing it says can be trusted —
        // including its length. This may be an unfinished write, or a complete
        // record whose header rotted after being acknowledged. The two are
        // indistinguishable from the bytes, so recovery refuses to choose
        // unless an operator has said which risk they prefer.
        CorruptionKind::RecordHeaderChecksum { .. } => {
            repair_checksum_tail && position.saturating_add(RECORD_HEADER_LEN) >= file_len
        }
        // The header verified but the payload did not. The record is complete
        // on disk, so this is rot rather than a torn write, and under
        // `OnCommit` it may already have been acknowledged.
        CorruptionKind::RecordChecksum { .. } | CorruptionKind::OffsetOutOfOrder { .. } => {
            repair_checksum_tail
                && claimed_len.is_some_and(|len| position.saturating_add(len) >= file_len)
        }
        // A verified header carrying an impossible length. The writer rejects
        // oversized records, so this is damage the checksum did not catch;
        // treat it as ambiguous rather than assume a torn write.
        CorruptionKind::RecordTooLarge { payload_len, .. } => {
            repair_checksum_tail
                && position.saturating_add(RECORD_HEADER_LEN + u64::from(*payload_len)) > file_len
        }
        _ => false,
    }
}

/// Running state of a scan, so the several exit paths agree on what to report.
struct ScanState {
    header: SegmentHeader,
    next_offset: Offset,
    position: u64,
    record_count: u64,
    index: SparseIndex,
    bytes_since_entry: u64,
    spacing: u64,
    rebuild_index: bool,
    repair_checksum_tail: bool,
}

impl ScanState {
    fn into_outcome(self, torn_tail: Option<TornTail>) -> ScanOutcome {
        ScanOutcome {
            header: self.header,
            next_offset: self.next_offset,
            valid_bytes: self.position,
            record_count: self.record_count,
            index: self.index,
            torn_tail,
        }
    }

    fn torn(self, file_len: u64, cause: CorruptionKind) -> ScanOutcome {
        let position = self.position;
        let discarded_bytes = file_len.saturating_sub(position);
        self.into_outcome(Some(TornTail {
            position,
            discarded_bytes,
            cause,
        }))
    }

    /// Turn a mid-scan decode failure into either a repaired tail or a hard
    /// error.
    fn finish_or_fail(
        self,
        err: Corruption,
        claimed_len: Option<u64>,
        file_len: u64,
        shard_label: &str,
        segment_id: SegmentId,
    ) -> Result<ScanOutcome> {
        if is_repairable_tail(
            &err.kind,
            self.position,
            claimed_len,
            file_len,
            self.repair_checksum_tail,
        ) {
            return Ok(self.torn(file_len, err.kind));
        }
        let position = self.position;
        Err(StorageError::Corruption(
            err.in_segment(shard_label, segment_id)
                .at_position(position),
        ))
    }

    /// Emit an index entry on exactly the same rule `IndexWriter` uses, so a
    /// rebuilt index is byte-identical to one written during append.
    fn observe(&mut self, offset: Offset, position: u64, total_len: u64) {
        if !self.rebuild_index {
            return;
        }
        if self.index.is_empty() || self.bytes_since_entry >= self.spacing {
            self.index.push(IndexEntry { offset, position });
            self.bytes_since_entry = 0;
        }
        self.bytes_since_entry = self.bytes_since_entry.saturating_add(total_len);
    }
}

/// Read and validate only a segment's header.
///
/// Cheap enough to call on every segment at startup: it proves the file is ours
/// and yields the base offset that everything else is relative to.
pub fn read_segment_header(
    path: &Path,
    segment_id: SegmentId,
    shard_label: &str,
) -> Result<SegmentHeader> {
    let file = File::open(path)?;
    let mut cursor = SegmentCursor::new(&file);
    let bytes = cursor.slice_at(0, SEGMENT_HEADER_LEN as usize)?;
    SegmentHeader::decode(bytes).map_err(|err| {
        StorageError::Corruption(err.in_segment(shard_label, segment_id).at_position(0))
    })
}

/// Where a scan begins, and therefore how much work it does.
#[derive(Debug, Clone, Copy)]
pub enum ScanStart {
    /// Validate every record from the head of the segment and rebuild the
    /// sparse index. Cost is proportional to the segment's size.
    Full,
    /// Resume from a record boundary already known to be good — typically the
    /// last sparse index entry. Only the records after it are validated and the
    /// index is *not* rebuilt, so cost is proportional to one index interval.
    Resume { position: u64, next_offset: Offset },
}

/// Validate a segment and, for a [`ScanStart::Full`] scan, rebuild its index.
///
/// This is the startup path. It never mutates the file; the caller applies the
/// truncation implied by [`ScanOutcome::torn_tail`].
///
/// `record_count` counts the records this scan validated, which for a
/// [`ScanStart::Resume`] scan is only those after the resume point.
///
/// `repair_checksum_tail` mirrors [`crate::log::LogConfig::repair_checksum_tail`]:
/// when false, only a provably incomplete trailing record is truncated.
pub fn scan_segment(
    path: &Path,
    segment_id: SegmentId,
    shard_label: &str,
    index_spacing_bytes: u64,
    start: ScanStart,
    repair_checksum_tail: bool,
) -> Result<ScanOutcome> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut cursor = SegmentCursor::new(&file);

    let annotate = |err: Corruption, position: u64| -> StorageError {
        StorageError::Corruption(
            err.in_segment(shard_label, segment_id)
                .at_position(position),
        )
    };

    // A segment header is written once, before any record, and is synced with
    // the first append. Damage here is never a repairable tail.
    let header_bytes = cursor.slice_at(0, SEGMENT_HEADER_LEN as usize)?;
    let header = SegmentHeader::decode(header_bytes).map_err(|err| annotate(err, 0))?;

    let (position, next_offset, rebuild_index) = match start {
        ScanStart::Full => (SEGMENT_HEADER_LEN, header.base_offset, true),
        ScanStart::Resume {
            position,
            next_offset,
        } => (position.max(SEGMENT_HEADER_LEN), next_offset, false),
    };

    let mut state = ScanState {
        header,
        next_offset,
        position,
        record_count: 0,
        index: SparseIndex::new(header.base_offset),
        bytes_since_entry: 0,
        // A resume scan leaves the index alone; `u64::MAX` spacing is how it
        // says "emit nothing" without a second code path in `observe`.
        spacing: if rebuild_index {
            index_spacing_bytes.max(1)
        } else {
            u64::MAX
        },
        rebuild_index,
        repair_checksum_tail,
    };

    while state.position < file_len {
        let position = state.position;

        // Read the header first so the payload read is sized before it is
        // attempted; a corrupt length is caught here, not by a huge allocation.
        let header_slice = cursor.slice_at(position, RECORD_HEADER_LEN as usize)?;
        if (header_slice.len() as u64) < RECORD_HEADER_LEN {
            let available = header_slice.len() as u64;
            return Ok(state.torn(
                file_len,
                CorruptionKind::Truncated {
                    needed: RECORD_HEADER_LEN,
                    available,
                },
            ));
        }
        let total_len = match RecordHeader::decode(header_slice) {
            Ok(record_header) => record_header.encoded_len(),
            Err(err) => return state.finish_or_fail(err, None, file_len, shard_label, segment_id),
        };

        let record_slice = cursor.slice_at(position, total_len as usize)?;
        if (record_slice.len() as u64) < total_len {
            let available = record_slice.len() as u64;
            return Ok(state.torn(
                file_len,
                CorruptionKind::Truncated {
                    needed: total_len,
                    available,
                },
            ));
        }

        let decoded = match decode_record(record_slice) {
            Ok((decoded, _)) => decoded,
            Err(err) => {
                return state.finish_or_fail(
                    err,
                    Some(total_len),
                    file_len,
                    shard_label,
                    segment_id,
                );
            }
        };

        if let Err(err) = check_offset_continuity(state.next_offset, decoded.header.offset) {
            return state.finish_or_fail(err, Some(total_len), file_len, shard_label, segment_id);
        }

        state.observe(decoded.header.offset, position, total_len);
        state.position += total_len;
        state.next_offset = decoded.header.offset + 1;
        state.record_count += 1;
    }

    Ok(state.into_outcome(None))
}

/// How much a read may produce, carried across every segment it visits.
///
/// One budget spans a whole `read_range`, which is what makes the guarantees
/// global rather than per file: `max_bytes` bounds the response overall, and the
/// "always return at least one record" rule applies once, not once per segment.
#[derive(Debug, Clone, Copy)]
pub struct ReadBudget {
    pub max_bytes: usize,
    pub max_records: usize,
    /// Records produced so far under this budget.
    produced: usize,
}

impl ReadBudget {
    pub fn new(max_bytes: usize, max_records: usize) -> Self {
        Self {
            max_bytes,
            max_records,
            produced: 0,
        }
    }

    pub fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// True when no further record may be produced.
    pub fn is_spent(&self) -> bool {
        self.max_records == 0
    }

    pub fn produced(&self) -> usize {
        self.produced
    }

    /// Whether `payload_len` fits, given that an empty response is never a
    /// useful answer for a range that has data.
    fn admits(&self, payload_len: usize) -> bool {
        self.produced == 0 || payload_len <= self.max_bytes
    }

    /// Charge one record against the budget.
    fn consume(&mut self, payload_len: usize) {
        self.max_bytes = self.max_bytes.saturating_sub(payload_len);
        self.max_records = self.max_records.saturating_sub(1);
        self.produced += 1;
    }
}

/// Read-side handle on one segment.
///
/// Holds a single descriptor and reads through it positionally, so `&self` is
/// enough to serve a read and several can run at once.
#[derive(Debug)]
pub struct SegmentReader {
    id: SegmentId,
    base_offset: Offset,
    file: File,
}

impl SegmentReader {
    pub fn open(path: &Path, id: SegmentId, base_offset: Offset) -> Result<Self> {
        Ok(Self {
            id,
            base_offset,
            file: File::open(path)?,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Append records from `start_offset` onward into `out`.
    ///
    /// Stops at `valid_bytes` or once `budget` is spent, whichever comes first,
    /// and decrements `budget` by what it produced so a caller reading across
    /// several segments keeps one running allowance.
    ///
    /// At least one record is always returned when one exists at or after
    /// `start_offset`, even if it alone exceeds the byte budget; otherwise a
    /// record larger than the caller's budget could never be read at all.
    pub fn read_from(
        &self,
        index: &SparseIndex,
        start_offset: Offset,
        valid_bytes: u64,
        budget: &mut ReadBudget,
        shard_label: &str,
        out: &mut Vec<LogRecord>,
    ) -> Result<()> {
        if budget.is_spent() {
            return Ok(());
        }

        let mut position = index.seek_position(start_offset);
        let mut cursor = SegmentCursor::new(&self.file);

        while position < valid_bytes && !budget.is_spent() {
            let header_slice = cursor.slice_at(position, RECORD_HEADER_LEN as usize)?;
            let record_header = RecordHeader::decode(header_slice).map_err(|err| {
                StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(position))
            })?;
            let total_len = record_header.encoded_len();

            // Records before the requested start are skipped by arithmetic
            // alone — their payloads are never even read.
            if record_header.offset < start_offset {
                position += total_len;
                continue;
            }

            // Honour the budget, but never return an empty result for a range
            // that does have data.
            if !budget.admits(record_header.payload_len as usize) {
                break;
            }

            let record_slice = cursor.slice_at(position, total_len as usize)?;
            let (decoded, _) = decode_record(record_slice).map_err(|err| {
                StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(position))
            })?;

            budget.consume(decoded.payload.len());
            out.push(LogRecord {
                offset: decoded.header.offset,
                timestamp_micros: decoded.header.timestamp_micros,
                checksum: decoded.header.checksum,
                payload: decoded.payload,
            });
            position += total_len;
        }

        Ok(())
    }
}

/// Re-exported so callers can name the payload type without depending on
/// `bytes` directly in signatures they only forward.
pub type ReadPayload = Bytes;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::format::encode_record;
    use tempfile::tempdir;

    /// Build a segment file containing `count` records starting at `base`.
    fn write_segment(path: &Path, base: Offset, count: u64) -> Vec<u8> {
        let mut bytes = SegmentHeader::new(base, 1).encode().to_vec();
        for i in 0..count {
            encode_record(
                &mut bytes,
                base + i,
                100 + i,
                format!("payload-{i}").as_bytes(),
            );
        }
        std::fs::write(path, &bytes).expect("write");
        bytes
    }

    fn scan(path: &Path) -> Result<ScanOutcome> {
        scan_segment(path, 0, "t/ns/s/0", 4096, ScanStart::Full, true)
    }

    fn read_all(path: &Path, outcome: &ScanOutcome, start: Offset) -> Vec<LogRecord> {
        let reader = SegmentReader::open(path, 0, outcome.header.base_offset).expect("open");
        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                start,
                outcome.valid_bytes,
                &mut ReadBudget::unbounded(),
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        out
    }

    #[test]
    fn scan_of_a_healthy_segment_reports_every_record() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 10, 5);

        let outcome = scan(&path).expect("scan");
        assert_eq!(outcome.record_count, 5);
        assert_eq!(outcome.next_offset, 15);
        assert_eq!(outcome.last_offset(), Some(14));
        assert!(outcome.torn_tail.is_none());
        assert_eq!(
            outcome.valid_bytes,
            std::fs::metadata(&path).expect("meta").len()
        );
    }

    #[test]
    fn scan_of_an_empty_segment_is_valid() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 3, 0);

        let outcome = scan(&path).expect("scan");
        assert_eq!(outcome.record_count, 0);
        assert_eq!(outcome.next_offset, 3);
        assert_eq!(outcome.last_offset(), None);
        assert_eq!(outcome.valid_bytes, SEGMENT_HEADER_LEN);
    }

    #[test]
    fn truncation_at_every_byte_of_the_tail_record_is_repairable() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let full = write_segment(&path, 0, 3);

        let mut prefix = SegmentHeader::new(0, 1).encode().to_vec();
        encode_record(&mut prefix, 0, 100, b"payload-0");
        encode_record(&mut prefix, 1, 101, b"payload-1");
        let third_start = prefix.len() as u64;

        // Start one byte in: cutting exactly at the boundary leaves a healthy
        // two-record segment, not a torn one.
        for cut in (third_start as usize + 1)..full.len() {
            std::fs::write(&path, &full[..cut]).expect("write");
            let outcome = scan(&path).expect("scan should repair");
            assert_eq!(outcome.record_count, 2, "cut at {cut}");
            assert_eq!(outcome.next_offset, 2);
            assert_eq!(outcome.valid_bytes, third_start);
            let tail = outcome.torn_tail.expect("torn tail");
            assert_eq!(tail.position, third_start);
            assert_eq!(tail.discarded_bytes, cut as u64 - third_start);
        }
    }

    #[test]
    fn a_corrupt_final_record_is_treated_as_a_torn_tail() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = write_segment(&path, 0, 3);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");

        let outcome = scan(&path).expect("scan should repair");
        assert_eq!(outcome.record_count, 2);
        let tail = outcome.torn_tail.expect("torn tail");
        assert!(matches!(tail.cause, CorruptionKind::RecordChecksum { .. }));
    }

    #[test]
    fn interior_payload_corruption_is_a_hard_error() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = write_segment(&path, 0, 4);
        let inside = SEGMENT_HEADER_LEN as usize + RECORD_HEADER_LEN as usize + 1;
        bytes[inside] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");

        let err = scan(&path).expect_err("interior corruption");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert!(matches!(detail.kind, CorruptionKind::RecordChecksum { .. }));
        assert_eq!(detail.site.shard.as_deref(), Some("t/ns/s/0"));
        assert_eq!(detail.site.segment, Some(0));
        assert_eq!(detail.site.position, Some(SEGMENT_HEADER_LEN));
    }

    #[test]
    fn an_offset_gap_in_committed_data_is_a_hard_error() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = SegmentHeader::new(0, 1).encode().to_vec();
        encode_record(&mut bytes, 0, 1, b"a");
        encode_record(&mut bytes, 5, 2, b"b");
        encode_record(&mut bytes, 6, 3, b"c");
        std::fs::write(&path, &bytes).expect("write");

        let err = scan(&path).expect_err("offset gap");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert!(matches!(
            detail.kind,
            CorruptionKind::OffsetOutOfOrder {
                expected: 1,
                found: 5
            }
        ));
    }

    #[test]
    fn a_corrupt_segment_header_is_never_repaired() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = write_segment(&path, 0, 2);
        bytes[8] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");

        assert!(matches!(
            scan(&path).expect_err("bad header"),
            StorageError::Corruption(_)
        ));
    }

    #[test]
    fn a_truncated_segment_header_is_reported_with_context() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        std::fs::write(&path, b"FLS").expect("write");

        let err = scan(&path).expect_err("short header");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert!(detail.is_truncation());
        assert_eq!(detail.site.segment, Some(0));
    }

    #[test]
    fn a_garbage_header_at_the_tail_is_not_repaired_by_default() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = write_segment(&path, 0, 1);
        // Debris that happens to encode a huge length. Its header checksum does
        // not verify, so the length means nothing — and a record whose header
        // cannot be trusted might equally be a complete, acknowledged record
        // that rotted. Recovery refuses to decide.
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.extend_from_slice(&[0u8; (RECORD_HEADER_LEN - 4) as usize]);
        std::fs::write(&path, &bytes).expect("write");

        // Explicitly the default policy, not the permissive test helper.
        let err = scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, false)
            .expect_err("ambiguous tail must not be truncated silently");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert!(matches!(
            detail.kind,
            CorruptionKind::RecordHeaderChecksum { .. }
        ));
    }

    #[test]
    fn an_operator_can_opt_in_to_repairing_an_ambiguous_tail() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let mut bytes = write_segment(&path, 0, 1);
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.extend_from_slice(&[0u8; (RECORD_HEADER_LEN - 4) as usize]);
        std::fs::write(&path, &bytes).expect("write");

        let outcome =
            scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, true).expect("opt-in repair");
        assert_eq!(outcome.record_count, 1);
        assert!(matches!(
            outcome.torn_tail.expect("tail").cause,
            CorruptionKind::RecordHeaderChecksum { .. }
        ));
    }

    #[test]
    fn a_payload_cut_short_by_a_crash_is_still_repaired_automatically() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let full = write_segment(&path, 0, 3);
        // Cut inside the last record's payload: the header is intact and
        // verifies, so its length is trustworthy and the shortfall is provably
        // an unfinished write. This is the ordinary crash case and must not
        // require operator intervention.
        std::fs::write(&path, &full[..full.len() - 3]).expect("write");

        let outcome = scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, false)
            .expect("a torn payload is provably incomplete");
        assert_eq!(outcome.record_count, 2);
        assert!(matches!(
            outcome.torn_tail.expect("tail").cause,
            CorruptionKind::Truncated { .. }
        ));
    }

    #[test]
    fn scan_rebuilds_an_index_that_matches_the_records() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 20);

        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
        assert!(outcome.index.len() > 1);
        for entry in outcome.index.entries() {
            assert!(entry.position >= SEGMENT_HEADER_LEN);
            assert!(entry.position < outcome.valid_bytes);
        }
        // Every indexed position must actually start the record it claims.
        for entry in outcome.index.entries() {
            let found = read_all(&path, &outcome, entry.offset);
            assert_eq!(found[0].offset, entry.offset);
        }
    }

    #[test]
    fn scan_reads_records_larger_than_the_read_ahead_window() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        let big = vec![7u8; READ_CHUNK_BYTES * 2 + 11];
        let mut bytes = SegmentHeader::new(0, 1).encode().to_vec();
        encode_record(&mut bytes, 0, 1, &big);
        encode_record(&mut bytes, 1, 2, b"small");
        std::fs::write(&path, &bytes).expect("write");

        let outcome = scan(&path).expect("scan");
        assert_eq!(outcome.record_count, 2);
        let records = read_all(&path, &outcome, 0);
        assert_eq!(records[0].payload.len(), big.len());
        assert_eq!(records[1].payload, Bytes::from_static(b"small"));
    }

    #[test]
    fn reader_returns_records_from_the_requested_offset() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 10);
        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");

        let out = read_all(&path, &outcome, 4);
        let offsets: Vec<Offset> = out.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, (4..10).collect::<Vec<_>>());
        assert_eq!(out[0].payload, Bytes::from_static(b"payload-4"));
    }

    #[test]
    fn reader_respects_the_byte_budget_but_always_makes_progress() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 10);
        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
        let reader = SegmentReader::open(&path, 0, 0).expect("open");

        let mut budget = ReadBudget::new(1, usize::MAX);
        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                0,
                outcome.valid_bytes,
                &mut budget,
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        assert_eq!(out.len(), 1);

        let mut budget = ReadBudget::new(b"payload-0".len() * 2, usize::MAX);
        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                0,
                outcome.valid_bytes,
                &mut budget,
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn reader_respects_the_record_budget_and_reports_what_it_spent() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 10);
        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
        let reader = SegmentReader::open(&path, 0, 0).expect("open");

        let mut budget = ReadBudget::new(usize::MAX, 3);
        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                0,
                outcome.valid_bytes,
                &mut budget,
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        assert_eq!(out.len(), 3);
        assert_eq!(budget.max_records, 0);

        // An exhausted budget produces nothing rather than looping.
        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                0,
                outcome.valid_bytes,
                &mut budget,
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        assert!(out.is_empty());
    }

    #[test]
    fn reader_stops_at_the_valid_boundary() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 10);
        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
        let reader = SegmentReader::open(&path, 0, 0).expect("open");

        // Pretend only the first three records were committed.
        let committed_bytes = SEGMENT_HEADER_LEN
            + read_all(&path, &outcome, 0)
                .iter()
                .take(3)
                .map(|r| RECORD_HEADER_LEN + r.payload.len() as u64)
                .sum::<u64>();

        let mut out = Vec::new();
        reader
            .read_from(
                &outcome.index,
                0,
                committed_bytes,
                &mut ReadBudget::unbounded(),
                "t/ns/s/0",
                &mut out,
            )
            .expect("read");
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn reader_past_the_tail_returns_nothing() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("a.log");
        write_segment(&path, 0, 4);
        let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
        assert!(read_all(&path, &outcome, 99).is_empty());
    }

    #[test]
    fn cursor_serves_overlapping_windows_without_moving_backwards_wrongly() {
        let dir = tempdir().expect("dir");
        let path = dir.path().join("f");
        std::fs::write(&path, b"0123456789").expect("write");
        let file = File::open(&path).expect("open");
        let mut cursor = SegmentCursor::new(&file);

        assert_eq!(cursor.slice_at(0, 4).expect("read"), b"0123");
        assert_eq!(cursor.slice_at(4, 4).expect("read"), b"4567");
        // Re-reading an earlier position must still be correct.
        assert_eq!(cursor.slice_at(1, 3).expect("read"), b"123");
        // Past the end returns fewer bytes than asked for.
        assert_eq!(cursor.slice_at(8, 8).expect("read"), b"89");
        assert!(cursor.slice_at(50, 4).expect("read").is_empty());
    }

    #[test]
    fn repairable_tail_rules() {
        // Provably incomplete: repaired regardless of policy.
        assert!(is_repairable_tail(
            &CorruptionKind::Truncated {
                needed: 10,
                available: 2
            },
            100,
            None,
            102,
            false
        ));
        // A complete-but-unverifiable tail: repaired only when opted in,
        // because it is indistinguishable from rot on acknowledged data.
        assert!(is_repairable_tail(
            &CorruptionKind::RecordChecksum {
                expected: 1,
                found: 2
            },
            100,
            Some(50),
            150,
            true
        ));
        assert!(!is_repairable_tail(
            &CorruptionKind::RecordChecksum {
                expected: 1,
                found: 2
            },
            100,
            Some(50),
            150,
            false
        ));
        // Committed records follow it, so it is interior damage either way.
        assert!(!is_repairable_tail(
            &CorruptionKind::RecordChecksum {
                expected: 1,
                found: 2
            },
            100,
            Some(50),
            300,
            true
        ));
        assert!(!is_repairable_tail(
            &CorruptionKind::SegmentMagic { found: 0 },
            0,
            None,
            100,
            true
        ));
    }
}
