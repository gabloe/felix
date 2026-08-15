// The set of segments that make up one shard's log: many sealed, exactly one
// active.
//
// This is where rollover, offset-to-segment routing and truncation live. It is
// entirely synchronous and holds no locks of its own — `disk_log::mod` owns the
// lock and calls in.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::log::{AppendRecord, LogConfig, LogRecord, Offset, SegmentDescriptor, SegmentId};
use crate::segment::format::SEGMENT_HEADER_LEN;
use crate::segment::io::read_at;
use crate::segment::writer::{BlankSegment, ResumeState};
use crate::segment::{
    ReadBudget, ScanStart, SegmentReader, SegmentWriter, SparseIndex, index_file_name,
    scan_segment, segment_file_name,
};
use crate::{Result, StorageError, metrics_names};

/// Everything needed to build a replacement segment, captured under the lock so
/// that the building itself can happen without one.
#[derive(Debug, Clone)]
pub struct RollPlan {
    dir: PathBuf,
    id: SegmentId,
    previous_active_id: SegmentId,
    preallocate_bytes: u64,
    index_spacing_bytes: u64,
}

impl RollPlan {
    /// Create the replacement segment. Blocking, and safe to call with no lock.
    ///
    /// The segment is left *headerless*: it claims no base offset yet, because
    /// the correct one is not known until the swap. See [`BlankSegment`].
    pub fn build(self) -> Result<PreparedSegment> {
        let blank = BlankSegment::create(
            &self.dir,
            self.id,
            self.preallocate_bytes,
            self.index_spacing_bytes,
        )?;
        Ok(PreparedSegment {
            blank,
            previous_active_id: self.previous_active_id,
        })
    }
}

/// A replacement segment built ahead of the swap that installs it.
#[derive(Debug)]
pub struct PreparedSegment {
    blank: BlankSegment,
    /// The segment this replacement was built to retire.
    ///
    /// The only thing that can invalidate a prepared segment now that it takes
    /// its base offset at the swap: another roll got there first, so the
    /// segment this one was built to retire is no longer the active one.
    /// Installing anyway would abandon that segment's file with no owner.
    previous_active_id: SegmentId,
}

impl PreparedSegment {
    /// Remove the files backing a replacement that was never installed.
    pub fn discard(self) -> Result<()> {
        self.blank.discard()
    }
}

/// What happened to a prepared segment offered to [`SegmentSet::commit_roll`].
#[derive(Debug)]
pub enum RollOutcome {
    /// Installed as the active segment. Carries the retired writer to be sealed.
    Installed(SegmentWriter),
    /// Rejected: the tail moved past the offset it was built for. Handed back
    /// so the caller can delete its files — dropping it would leave them on
    /// disk for recovery to clean up instead.
    Stale(PreparedSegment),
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

    /// Whether appending `records` would first require a rollover.
    ///
    /// Exposed so the async layer can perform the roll — which seals a segment,
    /// creates another, and fsyncs both plus the directory — on a blocking
    /// thread instead of inline on a reactor worker.
    pub fn would_roll(&self, records: &[AppendRecord]) -> bool {
        self.would_roll_within(records, false)
    }

    /// Whether appending `records` requires an *inline* rollover — the blocking
    /// kind, which seals and fsyncs while holding the caller's lock.
    ///
    /// `roll_pending` says a background rollover is already building the
    /// replacement. When it is, the ceiling rises to `max_overshoot_percent`
    /// above the configured size, and this is the crux of why the background
    /// roll works at all:
    ///
    /// The gap between the soft threshold and the hard limit is spent in
    /// *appends*, which cost microseconds. Building a replacement segment costs
    /// two *fsyncs*, which cost milliseconds. Hold the configured size as a hard
    /// ceiling and the inline path always wins that race, so every roll blocks
    /// exactly as it did before and the preparation is discarded unused —
    /// measurably worse than not trying. Letting the segment overshoot while its
    /// replacement is built is what buys the preparation enough time to finish.
    ///
    /// The overshoot is bounded rather than open-ended, because
    /// `segment_size_bytes` is also what bounds recovery time: the active
    /// segment is the one scanned in full at startup. Past the bound, appends
    /// roll inline and take the latency hit, which is the correct trade when the
    /// alternative is an unboundedly large segment to re-scan after a crash.
    pub fn would_roll_within(&self, records: &[AppendRecord], roll_pending: bool) -> bool {
        self.active.projected_size(records) > self.size_ceiling(roll_pending)
            && self.active.record_count() > 0
    }

    /// Largest the active segment may grow before an inline roll is forced.
    fn size_ceiling(&self, roll_pending: bool) -> u64 {
        if !roll_pending {
            return self.config.segment_size_bytes;
        }
        let overshoot = 100u64.saturating_add(self.config.max_overshoot_percent as u64);
        self.config
            .segment_size_bytes
            .saturating_mul(overshoot)
            .max(self.config.segment_size_bytes)
            / 100
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

    /// Build a replacement segment for the current tail, touching no shared
    /// state.
    ///
    /// This is the expensive half of a rollover — creating the file, writing
    /// and syncing its header, syncing the directory entry — and it runs with
    /// no lock held. `commit_roll` then swaps it in under a lock held only long
    /// enough to move a pointer.
    ///
    /// This half is deliberately trivial: it copies a path and bumps a counter.
    /// Everything expensive belongs to [`RollPlan::build`], which must run with
    /// no lock held — creating a segment fsyncs its header *and* its parent
    /// directory, and holding even a read lock across that blocks every append
    /// waiting for the write lock, which is the stall this whole design exists
    /// to remove.
    pub fn roll_plan(&self) -> RollPlan {
        RollPlan {
            dir: self.dir.clone(),
            id: self.next_segment_id.fetch_add(1, Ordering::AcqRel),
            previous_active_id: self.active.id(),
            preallocate_bytes: self.preallocate_bytes(),
            index_spacing_bytes: self.config.index_spacing_bytes,
        }
    }

    /// Swap a prepared segment in, returning the retired writer to be sealed.
    ///
    /// Fast by construction: a pointer swap plus a descriptor built from
    /// in-memory state. The retired segment joins the sealed list immediately,
    /// so reads never see a gap while it is still being flushed — its
    /// descriptor already describes every record it holds, because
    /// preallocation reserves blocks without extending the file, so a segment's
    /// length always equals the bytes actually written to it.
    ///
    /// The replacement takes its base offset *here*, from the tail as it stands
    /// at the swap. That is what lets appends keep landing in the old segment
    /// for as long as the preparation takes: however far the tail has moved,
    /// the replacement still continues it exactly, so no gap is possible and no
    /// preparation is wasted for having been built a moment too early.
    ///
    /// Returns [`RollOutcome::Stale`] only when another roll already replaced
    /// the segment this one was built to retire.
    pub fn commit_roll(&mut self, prepared: PreparedSegment) -> Result<RollOutcome> {
        // Rolling an empty segment would push a `SealedEntry` with no
        // `last_offset` to describe, and gains nothing.
        if prepared.previous_active_id != self.active.id() || self.active.record_count() == 0 {
            return Ok(RollOutcome::Stale(prepared));
        }
        let descriptor = self.active.descriptor();
        // One page-cache write, no flush: see `BlankSegment::activate`.
        let replacement = prepared
            .blank
            .activate(self.active.next_offset(), now_micros())?;
        self.bump_next_segment_id(replacement.id() + 1);
        let retired = std::mem::replace(&mut self.active, replacement);
        self.active_reader = SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?;

        // Guaranteed non-empty by the check above, so `last_offset` is real.
        self.sealed.push(SealedEntry {
            descriptor,
            index: retired.index().clone(),
            reader: SegmentReader::open(retired.path(), retired.id(), retired.base_offset())?,
        });

        metrics::counter!(metrics_names::SEGMENT_ROLL_TOTAL).increment(1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(RollOutcome::Installed(retired))
    }

    /// Whether the active segment has crossed the point where a rollover should
    /// be started in the background.
    ///
    /// Deliberately below `segment_size_bytes`: the roll is prepared while
    /// there is still room, so appends keep landing in the current segment
    /// instead of waiting for one to be built. That makes the configured size a
    /// target rather than a hard ceiling — a segment can overshoot slightly
    /// while its replacement is being created.
    pub fn should_prepare_roll(&self) -> bool {
        if self.active.record_count() == 0 {
            return false;
        }
        let threshold = self
            .config
            .segment_size_bytes
            .saturating_mul(self.config.rollover_threshold_percent.min(100) as u64)
            / 100;
        self.active.size_bytes() >= threshold.max(1)
    }

    /// Seal the active segment and start a new one at the current tail.
    ///
    /// The all-in-one path, kept for the hard-limit fallback and for callers
    /// that drive `SegmentSet` directly. It holds the caller's lock across
    /// several fsyncs; the split `roll_plan`/`commit_roll` pair is what the
    /// async log uses to keep that off the append path.
    pub fn roll(&mut self) -> Result<()> {
        let descriptor = self.active.seal()?;
        let base_offset = self.active.next_offset();
        let id = self.next_segment_id.fetch_add(1, Ordering::AcqRel);

        let replacement = SegmentWriter::create(
            &self.dir,
            id,
            base_offset,
            now_micros(),
            self.preallocate_bytes(),
            self.config.index_spacing_bytes,
        )?;
        let retired = std::mem::replace(&mut self.active, replacement);
        self.active_reader =
            SegmentReader::open(self.active.path(), self.active.id(), base_offset)?;
        self.bump_next_segment_id(id + 1);

        // A sealed segment holding no records would break the `last_offset`
        // invariant `SealedEntry` relies on. Rather than list it, delete it:
        // leaving the file behind would put an empty segment in the middle of
        // the chain, where recovery reads its base offset as a break.
        if retired.record_count() > 0 {
            self.sealed.push(SealedEntry {
                descriptor,
                index: retired.index().clone(),
                reader: SegmentReader::open(retired.path(), retired.id(), retired.base_offset())?,
            });
        } else {
            let path = retired.path().to_path_buf();
            let index = self.dir.join(index_file_name(retired.id()));
            drop(retired);
            for path in [path, index] {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(StorageError::Io(err)),
                }
            }
        }

        metrics::counter!(metrics_names::SEGMENT_ROLL_TOTAL).increment(1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(())
    }

    fn bump_next_segment_id(&self, at_least: SegmentId) {
        self.next_segment_id.fetch_max(at_least, Ordering::AcqRel);
    }

    fn preallocate_bytes(&self) -> u64 {
        if self.config.preallocate_segments {
            self.config.segment_size_bytes
        } else {
            0
        }
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
                .map(|record| {
                    crate::segment::format::RECORD_HEADER_LEN + record.payload.len() as u64
                })
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
                self.preallocate_bytes(),
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

    /// Seal the active segment and report a verifiable summary of it.
    pub fn seal_active(&mut self) -> Result<(SegmentDescriptor, u64)> {
        let descriptor = self.active.seal()?;
        let checksum = checksum_file(self.active.path())?;
        Ok((descriptor, checksum))
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

pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::FsyncMode;
    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};

    fn record(payload: &str) -> AppendRecord {
        AppendRecord {
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            timestamp_micros: 7,
        }
    }

    fn config(segment_size_bytes: u64) -> LogConfig {
        LogConfig {
            segment_size_bytes,
            index_spacing_bytes: 64,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        }
    }

    fn new_set(dir: &TempDir, segment_size_bytes: u64) -> SegmentSet {
        let config = config(segment_size_bytes);
        let active = SegmentWriter::create(dir.path(), 0, 0, 1, 0, config.index_spacing_bytes)
            .expect("create");
        SegmentSet::new(
            dir.path().to_path_buf(),
            "t/ns/s/0".to_string(),
            config,
            Vec::new(),
            active,
        )
        .expect("set")
    }

    #[test]
    fn a_preparation_still_installs_after_the_tail_has_moved() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        set.append(&[record("a")]).expect("append");

        let prepared = set.roll_plan().build().expect("prepare");
        // The whole point of preparing off the lock: appends keep landing while
        // the replacement is built, and they must not invalidate it. The
        // replacement takes its base offset at the swap, so it continues from
        // wherever the tail ended up.
        set.append(&[record("b")]).expect("append");
        set.append(&[record("c")]).expect("append");
        let tail = set.tail_offset();

        match set.commit_roll(prepared).expect("commit") {
            RollOutcome::Installed(mut retired) => retired.seal().expect("seal"),
            RollOutcome::Stale(_) => panic!("a moving tail must not waste a preparation"),
        };
        assert_eq!(set.active().base_offset(), tail);
        assert_eq!(set.tail_offset(), tail, "no gap and no rewind");

        set.append(&[record("d")]).expect("append");
        assert_eq!(read_all(&set, 0), ["a", "b", "c", "d"]);
    }

    #[test]
    fn a_preparation_is_rejected_once_another_roll_has_replaced_the_active_segment() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        set.append(&[record("a")]).expect("append");

        let prepared = set.roll_plan().build().expect("prepare");
        let path = prepared.blank.path().to_path_buf();
        // The inline hard-limit path rolls while the preparation is in flight.
        // The segment this preparation was built to retire is gone, so
        // installing it now would strand the segment that replaced it.
        set.roll().expect("roll");

        match set.commit_roll(prepared).expect("commit") {
            RollOutcome::Stale(prepared) => prepared.discard().expect("discard"),
            RollOutcome::Installed(_) => panic!("the active segment is not the one it retires"),
        }
        assert!(!path.exists());
    }

    #[test]
    fn a_completed_background_roll_keeps_every_record_readable() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        for i in 0..4 {
            set.append(&[record(&format!("v{i}"))]).expect("append");
        }

        let prepared = set.roll_plan().build().expect("prepare");
        let mut retired = match set.commit_roll(prepared).expect("commit") {
            RollOutcome::Installed(retired) => retired,
            RollOutcome::Stale(_) => panic!("nothing moved the tail"),
        };
        // Reads route across the retired segment before it is sealed: it joins
        // the sealed list at swap time, not at flush time.
        assert_eq!(read_all(&set, 0).len(), 4);
        retired.seal().expect("seal");

        set.append(&[record("v4")]).expect("append");
        assert_eq!(read_all(&set, 0).len(), 5);
        assert_eq!(set.descriptors().len(), 2);
    }

    fn read_all(set: &SegmentSet, start: Offset) -> Vec<String> {
        set.read(start, ReadBudget::unbounded())
            .expect("read")
            .into_iter()
            .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
            .collect()
    }

    #[test]
    fn a_new_set_is_empty_at_offset_zero() {
        let dir = tempdir().expect("dir");
        let set = new_set(&dir, 1024);
        assert_eq!(set.tail_offset(), 0);
        assert_eq!(set.base_offset(), 0);
        assert_eq!(set.descriptors().len(), 1);
        assert!(read_all(&set, 0).is_empty());
    }

    #[test]
    fn appends_stay_in_one_segment_until_it_fills() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        for i in 0..10 {
            set.append(&[record(&format!("v{i}"))]).expect("append");
        }
        assert_eq!(set.descriptors().len(), 1);
        assert_eq!(set.tail_offset(), 10);
    }

    #[test]
    fn rollover_preserves_monotonic_offsets_and_bounds_segment_size() {
        let dir = tempdir().expect("dir");
        // Room for roughly three 26-byte records per segment.
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 3 * 26);
        for i in 0..12 {
            set.append(&[record(&format!("value-{i:02}"))])
                .expect("append");
        }

        let descriptors = set.descriptors();
        assert!(descriptors.len() > 1, "expected a rollover");
        // Offsets are contiguous across the segment boundary.
        for pair in descriptors.windows(2) {
            assert_eq!(pair[0].last_offset + 1, pair[1].base_offset);
        }
        for descriptor in descriptors.iter().take(descriptors.len() - 1) {
            assert!(
                descriptor.size_bytes <= SEGMENT_HEADER_LEN + 3 * 26,
                "{descriptor:?}"
            );
        }

        let values = read_all(&set, 0);
        assert_eq!(values.len(), 12);
        assert_eq!(values[0], "value-00");
        assert_eq!(values[11], "value-11");
    }

    #[test]
    fn a_record_larger_than_a_segment_gets_its_own_segment() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 16);
        set.append(&[record("small")]).expect("small");
        let big = "x".repeat(500);
        set.append(&[record(&big)]).expect("big");
        set.append(&[record("after")]).expect("after");

        let values = read_all(&set, 0);
        assert_eq!(values, vec!["small".to_string(), big, "after".to_string()]);
        // The oversized record was not split, and the log kept going.
        assert_eq!(set.tail_offset(), 3);
    }

    #[test]
    fn a_batch_is_never_split_across_segments() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 60);
        set.append(&[record("aaaa")]).expect("first");
        let before = set.descriptors().len();
        set.append(&[record("bbbb"), record("cccc"), record("dddd")])
            .expect("batch");
        let descriptors = set.descriptors();
        assert!(descriptors.len() > before, "the batch should have rolled");
        // All three landed together in the new segment.
        assert_eq!(descriptors.last().expect("active").base_offset, 1);
    }

    #[test]
    fn reads_cross_segment_boundaries_in_order() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 50);
        for i in 0..20 {
            set.append(&[record(&format!("v{i:03}"))]).expect("append");
        }
        assert!(set.descriptors().len() > 2);

        let all = read_all(&set, 0);
        assert_eq!(all.len(), 20);
        // Starting mid-way through an interior segment still yields a
        // contiguous run to the tail.
        let tail = read_all(&set, 7);
        assert_eq!(tail.len(), 13);
        assert_eq!(tail[0], "v007");
    }

    #[test]
    fn reads_past_the_tail_are_empty_not_an_error() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        set.append(&[record("only")]).expect("append");
        assert!(read_all(&set, 1).is_empty());
        assert!(read_all(&set, 99).is_empty());
    }

    #[test]
    fn a_byte_budget_bounds_a_multi_segment_read() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 50);
        for i in 0..20 {
            set.append(&[record(&format!("v{i:03}"))]).expect("append");
        }

        let budget = ReadBudget::new(12, usize::MAX);
        let records = set.read(0, budget).expect("read");
        // Four-byte payloads: three fit in the budget.
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[2].offset, 2);
    }

    #[test]
    fn truncate_at_or_past_the_tail_is_a_no_op() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        set.append(&[record("a"), record("b")]).expect("append");
        set.truncate(2).expect("truncate");
        set.truncate(99).expect("truncate");
        assert_eq!(set.tail_offset(), 2);
        assert_eq!(read_all(&set, 0), vec!["a", "b"]);
    }

    #[test]
    fn truncate_inside_the_active_segment_drops_the_suffix() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        for i in 0..6 {
            set.append(&[record(&format!("v{i}"))]).expect("append");
        }
        set.truncate(4).expect("truncate");

        assert_eq!(set.tail_offset(), 4);
        assert_eq!(read_all(&set, 0), vec!["v0", "v1", "v2", "v3"]);

        // The log keeps working, and the offsets resume where the cut left off.
        set.append(&[record("new")]).expect("append");
        assert_eq!(set.tail_offset(), 5);
        assert_eq!(read_all(&set, 4), vec!["new"]);
    }

    #[test]
    fn truncate_across_segments_deletes_whole_segments() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 50);
        for i in 0..20 {
            set.append(&[record(&format!("v{i:03}"))]).expect("append");
        }
        let before = set.descriptors().len();
        assert!(before > 2);

        set.truncate(5).expect("truncate");
        assert_eq!(set.tail_offset(), 5);
        assert_eq!(read_all(&set, 0).len(), 5);
        assert!(set.descriptors().len() < before);

        set.append(&[record("resumed")]).expect("append");
        assert_eq!(read_all(&set, 5), vec!["resumed"]);
    }

    #[test]
    fn truncate_to_zero_empties_the_log() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, SEGMENT_HEADER_LEN + 50);
        for i in 0..12 {
            set.append(&[record(&format!("v{i:03}"))]).expect("append");
        }
        set.truncate(0).expect("truncate");

        assert_eq!(set.tail_offset(), 0);
        assert!(read_all(&set, 0).is_empty());

        set.append(&[record("fresh")]).expect("append");
        assert_eq!(read_all(&set, 0), vec!["fresh"]);
    }

    #[test]
    fn sealing_reports_a_stable_checksum() {
        let dir = tempdir().expect("dir");
        let mut set = new_set(&dir, 4096);
        set.append(&[record("a")]).expect("append");

        let (descriptor, checksum) = set.seal_active().expect("seal");
        assert_eq!(descriptor.base_offset, 0);
        assert_eq!(descriptor.last_offset, 0);
        let (_, again) = set.seal_active().expect("seal again");
        assert_eq!(checksum, again);
    }

    #[test]
    fn an_index_prefix_drops_entries_past_the_cut() {
        let mut index = SparseIndex::new(0);
        for (offset, position) in [(0u64, 32u64), (5, 200), (9, 400)] {
            index.push(crate::segment::IndexEntry { offset, position });
        }
        let trimmed = rebuild_index_prefix(index, 300);
        assert_eq!(trimmed.len(), 2);
        assert_eq!(trimmed.seek_position(9), 200);
    }
}
