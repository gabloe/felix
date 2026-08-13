// Bringing a shard's segments back after an unclean shutdown.
//
// The contract recovery upholds:
//
// * **A torn tail is repaired.** A crash mid-append leaves a partial record at
//   the end of the last segment. That record was never acknowledged under any
//   fsync policy, so it is truncated away and the log resumes at the last intact
//   record.
// * **Committed data is never silently discarded.** Corruption anywhere that is
//   not the very end of the newest segment is an error at startup, naming the
//   shard, segment and byte position. Losing acknowledged records quietly is
//   worse than refusing to start.
// * **Recovery is idempotent.** Opening an already recovered log changes
//   nothing, so a crash *during* recovery is safe.
// * **Indexes are derived, never trusted.** A missing, short or mismatched index
//   is rebuilt from the segment it describes.
//
// ## What is validated, and what it costs
//
// Fully checksumming every segment at startup is O(bytes on disk) — minutes for
// a large shard, which is the difference between a rolling restart and an
// outage. So by default:
//
// * The **active** segment is always scanned in full. It is the only one that
//   can have a torn tail, and it is bounded by `segment_size_bytes`.
// * **Sealed** segments get their header validated, their index loaded, and the
//   records after the last index entry checked — bounded by one index interval.
//   Everything else is verified lazily, because every read verifies the checksum
//   of every record it returns.
//
// Set `LogConfig::verify_all_on_open` to trade startup time for eager detection
// of bit rot in cold data.

use std::path::{Path, PathBuf};

use crate::log::{LogConfig, Offset, SegmentDescriptor, SegmentId};
use crate::segment::io::sync_dir;
use crate::segment::writer::ResumeState;
use crate::segment::{
    ScanOutcome, ScanStart, SegmentReader, SegmentWriter, SparseIndex, index_file_name,
    parse_segment_file_name, read_segment_header, scan_segment, segment_file_name,
};
use crate::{Result, StorageError, metrics_names};

use super::segments::{SealedEntry, now_micros};

/// The outcome of recovering one shard directory.
#[derive(Debug)]
pub struct Recovered {
    pub sealed: Vec<SealedEntry>,
    pub active: SegmentWriter,
    /// Bytes discarded from a torn tail, for logging and metrics.
    pub truncated_bytes: u64,
    /// Indexes that had to be rebuilt.
    pub index_rebuilds: usize,
}

/// Segment ids present in `dir`, in ascending numeric order.
///
/// Directory iteration order is filesystem-defined and must never be relied on:
/// on some filesystems it is hash order, which would interleave segments and
/// make the log look shuffled.
pub fn discover_segment_ids(dir: &Path) -> Result<Vec<SegmentId>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(id) = parse_segment_file_name(name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    // Two files cannot share an id, but a corrupt listing should not produce a
    // duplicate that later code treats as two segments.
    ids.dedup();
    Ok(ids)
}

/// Open, validate and repair every segment for one shard.
pub fn recover_shard(dir: &Path, label: &str, config: &LogConfig) -> Result<Recovered> {
    let started = std::time::Instant::now();
    std::fs::create_dir_all(dir)?;
    // The directory entry itself must be durable, or a crash could lose a shard
    // that already reported successful writes.
    if let Some(parent) = dir.parent() {
        sync_dir(parent)?;
    }

    let ids = discover_segment_ids(dir)?;
    let recovered = match ids.split_last() {
        None => Recovered {
            sealed: Vec::new(),
            active: SegmentWriter::create(
                dir,
                0,
                0,
                now_micros(),
                preallocate_bytes(config),
                config.index_spacing_bytes,
            )?,
            truncated_bytes: 0,
            index_rebuilds: 0,
        },
        Some((active_id, sealed_ids)) => {
            recover_existing(dir, label, config, sealed_ids, *active_id)?
        }
    };

    metrics::histogram!(metrics_names::RECOVERY_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    if recovered.truncated_bytes > 0 {
        metrics::counter!(metrics_names::RECOVERY_TRUNCATED_BYTES)
            .increment(recovered.truncated_bytes);
    }
    if recovered.index_rebuilds > 0 {
        metrics::counter!(metrics_names::RECOVERY_INDEX_REBUILDS_TOTAL)
            .increment(recovered.index_rebuilds as u64);
    }
    Ok(recovered)
}

fn preallocate_bytes(config: &LogConfig) -> u64 {
    if config.preallocate_segments {
        config.segment_size_bytes
    } else {
        0
    }
}

fn recover_existing(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    sealed_ids: &[SegmentId],
    active_id: SegmentId,
) -> Result<Recovered> {
    let mut sealed = Vec::with_capacity(sealed_ids.len());
    let mut index_rebuilds = 0usize;
    let mut expected_base: Option<Offset> = None;

    for id in sealed_ids {
        let opened = open_sealed(dir, label, config, *id)?;
        if opened.rebuilt_index {
            index_rebuilds += 1;
        }
        // Offsets must be contiguous across segment boundaries. A gap means a
        // segment file was deleted or replaced out from under us.
        if let Some(expected) = expected_base
            && expected != opened.entry.descriptor.base_offset
        {
            return Err(gap_error(
                label,
                *id,
                expected,
                opened.entry.descriptor.base_offset,
            ));
        }
        expected_base = Some(opened.entry.descriptor.last_offset + 1);
        sealed.push(opened.entry);
    }

    // The newest segment is the only one that can have been mid-write when the
    // process died, so it always gets a full scan.
    let active_path = dir.join(segment_file_name(active_id));
    let outcome = scan_segment(
        &active_path,
        active_id,
        label,
        config.index_spacing_bytes,
        ScanStart::Full,
    )?;
    if let Some(expected) = expected_base
        && expected != outcome.header.base_offset
    {
        return Err(gap_error(
            label,
            active_id,
            expected,
            outcome.header.base_offset,
        ));
    }

    let truncated_bytes = outcome
        .torn_tail
        .as_ref()
        .map(|tail| tail.discarded_bytes)
        .unwrap_or(0);
    if let Some(tail) = &outcome.torn_tail {
        tracing::warn!(
            shard = label,
            segment = active_id,
            position = tail.position,
            discarded_bytes = tail.discarded_bytes,
            cause = %tail.cause,
            "repaired a torn tail in the active segment"
        );
    }

    // `reopen` applies the truncation and positions the write cursor, which is
    // what makes recovery idempotent: a second open finds nothing to repair.
    let active = SegmentWriter::reopen(
        dir,
        active_id,
        ResumeState {
            base_offset: outcome.header.base_offset,
            valid_bytes: outcome.valid_bytes,
            next_offset: outcome.next_offset,
            record_count: outcome.record_count,
            index: outcome.index,
        },
        config.index_spacing_bytes,
    )?;
    // The index was just rewritten from the scan, so it always counts as a
    // rebuild for the active segment.
    index_rebuilds += 1;

    Ok(Recovered {
        sealed,
        active,
        truncated_bytes,
        index_rebuilds,
    })
}

struct OpenedSealed {
    entry: SealedEntry,
    rebuilt_index: bool,
}

/// Validate one sealed segment and prepare it for reads.
fn open_sealed(dir: &Path, label: &str, config: &LogConfig, id: SegmentId) -> Result<OpenedSealed> {
    let path = dir.join(segment_file_name(id));
    let file_len = std::fs::metadata(&path)?.len();

    // The header alone establishes the base offset every other check is
    // relative to, and proves the file is ours before anything else is trusted.
    let base_offset = read_segment_header(&path, id, label)?.base_offset;

    let loaded = SparseIndex::load(&dir.join(index_file_name(id)), base_offset);
    let mut rebuilt_index = false;

    let (index, outcome) = match (loaded, config.verify_all_on_open) {
        (Some(index), false) if !index.is_empty() => {
            // Resume from the last index entry: only the records it does not
            // cover need checking, which is bounded by one index interval.
            let last = index.entries().last().copied().expect("non-empty");
            let outcome = scan_segment(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Resume {
                    position: last.position,
                    next_offset: last.offset,
                },
            )?;
            (index, outcome)
        }
        _ => {
            // No usable index, or a full verification was requested: walk the
            // whole segment and rebuild.
            rebuilt_index = true;
            let outcome = scan_segment(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
            )?;
            outcome.index.persist(&dir.join(index_file_name(id)))?;
            (outcome.index.clone(), outcome)
        }
    };

    // A sealed segment was synced and trimmed when it was sealed, so damage at
    // its tail is not a torn write — it is data loss in committed bytes.
    if let Some(tail) = outcome.torn_tail {
        return Err(StorageError::Corruption(
            crate::segment::Corruption::new(tail.cause)
                .in_segment(label, id)
                .at_position(tail.position),
        ));
    }
    if outcome.valid_bytes != file_len {
        return Err(StorageError::Corruption(
            crate::segment::Corruption::new(crate::segment::CorruptionKind::Truncated {
                needed: file_len,
                available: outcome.valid_bytes,
            })
            .in_segment(label, id)
            .at_position(outcome.valid_bytes),
        ));
    }

    let descriptor = SegmentDescriptor {
        id,
        base_offset,
        last_offset: outcome.next_offset.saturating_sub(1).max(base_offset),
        size_bytes: file_len,
    };
    Ok(OpenedSealed {
        entry: SealedEntry {
            descriptor,
            index,
            reader: SegmentReader::open(&path, id, base_offset)?,
        },
        rebuilt_index,
    })
}

fn gap_error(label: &str, id: SegmentId, expected: Offset, found: Offset) -> StorageError {
    StorageError::Corruption(
        crate::segment::Corruption::new(crate::segment::CorruptionKind::OffsetOutOfOrder {
            expected,
            found,
        })
        .in_segment(label, id)
        .at_position(0),
    )
}

/// Path to a shard directory's data file for `id`. Exposed for tests and tools.
pub fn segment_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(segment_file_name(id))
}

/// Report of what a scan found, re-exported so callers can log it.
pub type SegmentScan = ScanOutcome;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{AppendRecord, FsyncMode};
    use crate::segment::format::SEGMENT_HEADER_LEN;
    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};

    fn config() -> LogConfig {
        LogConfig {
            segment_size_bytes: SEGMENT_HEADER_LEN + 80,
            index_spacing_bytes: 32,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        }
    }

    fn record(payload: &str) -> AppendRecord {
        AppendRecord {
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            timestamp_micros: 1,
        }
    }

    /// Write `count` records through a real `SegmentSet`, rolling as configured.
    fn populate(dir: &TempDir, count: usize) -> u64 {
        let recovered = recover_shard(dir.path(), "t/ns/s/0", &config()).expect("recover");
        let mut set = super::super::segments::SegmentSet::new(
            dir.path().to_path_buf(),
            "t/ns/s/0".into(),
            config(),
            recovered.sealed,
            recovered.active,
        )
        .expect("set");
        for i in 0..count {
            set.append(&[record(&format!("value-{i:03}"))])
                .expect("append");
        }
        set.active_mut().sync().expect("sync");
        set.tail_offset()
    }

    fn reopen(dir: &TempDir) -> Result<Recovered> {
        recover_shard(dir.path(), "t/ns/s/0", &config())
    }

    #[test]
    fn an_empty_directory_starts_a_fresh_log() {
        let dir = tempdir().expect("dir");
        let recovered = reopen(&dir).expect("recover");
        assert!(recovered.sealed.is_empty());
        assert_eq!(recovered.active.next_offset(), 0);
        assert_eq!(recovered.truncated_bytes, 0);
    }

    #[test]
    fn discovery_orders_segments_numerically_not_lexically() {
        let dir = tempdir().expect("dir");
        for id in [0u64, 2, 10, 3] {
            std::fs::write(dir.path().join(segment_file_name(id)), b"").expect("write");
        }
        std::fs::write(dir.path().join("notes.txt"), b"ignored").expect("write");
        std::fs::write(dir.path().join(index_file_name(0)), b"ignored").expect("write");

        assert_eq!(
            discover_segment_ids(dir.path()).expect("discover"),
            vec![0, 2, 3, 10]
        );
    }

    #[test]
    fn a_clean_log_reopens_with_every_record() {
        let dir = tempdir().expect("dir");
        let tail = populate(&dir, 12);

        let recovered = reopen(&dir).expect("recover");
        assert_eq!(recovered.active.next_offset(), tail);
        assert_eq!(recovered.truncated_bytes, 0);
        assert!(!recovered.sealed.is_empty(), "expected rollovers");
    }

    #[test]
    fn recovery_is_idempotent() {
        let dir = tempdir().expect("dir");
        populate(&dir, 12);

        let first = reopen(&dir).expect("first");
        let tail = first.active.next_offset();
        let sealed_count = first.sealed.len();
        drop(first);

        let bytes_before: Vec<u64> = discover_segment_ids(dir.path())
            .expect("ids")
            .iter()
            .map(|id| {
                std::fs::metadata(segment_path(dir.path(), *id))
                    .expect("meta")
                    .len()
            })
            .collect();

        let second = reopen(&dir).expect("second");
        assert_eq!(second.active.next_offset(), tail);
        assert_eq!(second.sealed.len(), sealed_count);
        assert_eq!(second.truncated_bytes, 0);
        drop(second);

        let bytes_after: Vec<u64> = discover_segment_ids(dir.path())
            .expect("ids")
            .iter()
            .map(|id| {
                std::fs::metadata(segment_path(dir.path(), *id))
                    .expect("meta")
                    .len()
            })
            .collect();
        assert_eq!(bytes_before, bytes_after);
    }

    #[test]
    fn a_torn_tail_is_truncated_back_to_the_last_valid_record() {
        let dir = tempdir().expect("dir");
        let tail = populate(&dir, 5);
        let active_id = *discover_segment_ids(dir.path())
            .expect("ids")
            .last()
            .expect("id");
        let path = segment_path(dir.path(), active_id);

        // Simulate a crash part-way through writing another record.
        let mut bytes = std::fs::read(&path).expect("read");
        let good_len = bytes.len() as u64;
        bytes.extend_from_slice(&[0x11; 13]);
        std::fs::write(&path, &bytes).expect("write");

        let recovered = reopen(&dir).expect("recover");
        assert_eq!(recovered.active.next_offset(), tail);
        assert_eq!(recovered.truncated_bytes, 13);
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), good_len);
    }

    #[test]
    fn truncation_at_every_byte_of_a_trailing_record_recovers() {
        let dir = tempdir().expect("dir");
        populate(&dir, 5);
        let active_id = *discover_segment_ids(dir.path())
            .expect("ids")
            .last()
            .expect("id");
        let path = segment_path(dir.path(), active_id);
        let full = std::fs::read(&path).expect("read");

        // Cut the file at every byte inside the last record and confirm the log
        // always comes back to a consistent prefix.
        for cut in (SEGMENT_HEADER_LEN as usize)..full.len() {
            std::fs::write(&path, &full[..cut]).expect("write");
            let recovered = reopen(&dir).expect("recover");
            let tail = recovered.active.next_offset();
            drop(recovered);
            // A second open must find nothing left to repair.
            let again = reopen(&dir).expect("recover again");
            assert_eq!(again.truncated_bytes, 0, "cut at {cut}");
            assert_eq!(again.active.next_offset(), tail, "cut at {cut}");
        }
    }

    #[test]
    fn interior_corruption_in_the_active_segment_fails_loudly() {
        let dir = tempdir().expect("dir");
        // Six records fill the active segment with two, so the first of them has
        // a committed record after it and cannot be mistaken for a torn tail.
        populate(&dir, 6);
        let active_id = *discover_segment_ids(dir.path())
            .expect("ids")
            .last()
            .expect("id");
        let path = segment_path(dir.path(), active_id);

        let mut bytes = std::fs::read(&path).expect("read");
        let first_record = SEGMENT_HEADER_LEN as usize + crate::segment::RECORD_HEADER_LEN as usize;
        assert!(
            bytes.len() as u64 > SEGMENT_HEADER_LEN + 2 * crate::segment::RECORD_HEADER_LEN,
            "the active segment needs more than one record"
        );
        bytes[first_record] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");

        let err = reopen(&dir).expect_err("interior corruption");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert_eq!(detail.site.shard.as_deref(), Some("t/ns/s/0"));
        assert_eq!(detail.site.segment, Some(active_id));
        assert!(detail.site.position.is_some());
    }

    #[test]
    fn corruption_in_a_sealed_segment_fails_loudly() {
        let dir = tempdir().expect("dir");
        populate(&dir, 12);
        let ids = discover_segment_ids(dir.path()).expect("ids");
        assert!(ids.len() > 1, "expected a sealed segment");
        let sealed_id = ids[0];
        let path = segment_path(dir.path(), sealed_id);

        // Truncate a sealed segment: its bytes were already committed.
        let bytes = std::fs::read(&path).expect("read");
        std::fs::write(&path, &bytes[..bytes.len() - 3]).expect("write");

        let err = recover_shard(
            dir.path(),
            "t/ns/s/0",
            &LogConfig {
                verify_all_on_open: true,
                ..config()
            },
        )
        .expect_err("sealed corruption");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption");
        };
        assert_eq!(detail.site.segment, Some(sealed_id));
    }

    #[test]
    fn a_missing_index_is_rebuilt() {
        let dir = tempdir().expect("dir");
        populate(&dir, 12);
        let ids = discover_segment_ids(dir.path()).expect("ids");
        let sealed_id = ids[0];
        std::fs::remove_file(dir.path().join(index_file_name(sealed_id))).expect("remove");

        let recovered = reopen(&dir).expect("recover");
        assert!(recovered.index_rebuilds >= 2, "sealed plus active");
        assert!(dir.path().join(index_file_name(sealed_id)).exists());

        // The rebuilt index seeks to the same places as the original.
        let entry = &recovered.sealed[0];
        for indexed in entry.index.entries() {
            assert_eq!(entry.index.seek_position(indexed.offset), indexed.position);
        }
    }

    #[test]
    fn a_stale_index_is_replaced() {
        let dir = tempdir().expect("dir");
        populate(&dir, 12);
        let ids = discover_segment_ids(dir.path()).expect("ids");
        let sealed_id = ids[0];
        // An index that claims a different segment generation is unusable.
        SparseIndex::new(9_999)
            .persist(&dir.path().join(index_file_name(sealed_id)))
            .expect("persist");

        let recovered = reopen(&dir).expect("recover");
        let reloaded = SparseIndex::load(&dir.path().join(index_file_name(sealed_id)), 0);
        assert!(reloaded.is_some());
        assert!(!recovered.sealed[0].index.is_empty());
    }

    #[test]
    fn a_missing_segment_in_the_middle_is_an_error() {
        let dir = tempdir().expect("dir");
        populate(&dir, 20);
        let ids = discover_segment_ids(dir.path()).expect("ids");
        assert!(ids.len() > 2, "need an interior segment");

        // Delete an interior segment, leaving an offset gap.
        std::fs::remove_file(segment_path(dir.path(), ids[1])).expect("remove");
        std::fs::remove_file(dir.path().join(index_file_name(ids[1]))).expect("remove");

        let err = reopen(&dir).expect_err("gap");
        let StorageError::Corruption(detail) = err else {
            panic!("expected corruption, got {err}");
        };
        assert!(matches!(
            detail.kind,
            crate::segment::CorruptionKind::OffsetOutOfOrder { .. }
        ));
    }

    #[test]
    fn a_log_recovered_from_a_torn_tail_accepts_new_appends() {
        let dir = tempdir().expect("dir");
        populate(&dir, 5);
        let active_id = *discover_segment_ids(dir.path())
            .expect("ids")
            .last()
            .expect("id");
        let path = segment_path(dir.path(), active_id);
        let mut bytes = std::fs::read(&path).expect("read");
        bytes.extend_from_slice(&[0x77; 7]);
        std::fs::write(&path, &bytes).expect("write");

        let recovered = reopen(&dir).expect("recover");
        let mut set = super::super::segments::SegmentSet::new(
            dir.path().to_path_buf(),
            "t/ns/s/0".into(),
            config(),
            recovered.sealed,
            recovered.active,
        )
        .expect("set");
        let tail = set.tail_offset();
        set.append(&[record("after-recovery")]).expect("append");
        set.active_mut().sync().expect("sync");

        let reread = reopen(&dir).expect("recover again");
        assert_eq!(reread.active.next_offset(), tail + 1);
        assert_eq!(reread.truncated_bytes, 0);
    }
}
