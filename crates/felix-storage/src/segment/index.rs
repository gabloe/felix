// The sparse offset index that sits beside each segment.
//
// One entry every `index_spacing_bytes` of segment data, mapping a logical
// offset to the byte position where that record starts. A read binary-searches
// the entries to find a floor, then scans forward a bounded distance — so
// locating an offset costs O(log n) plus at most one spacing interval of I/O,
// rather than a scan from the head of the log.
//
// Index files are an accelerator and are never trusted on their own: entries are
// only ever used as a *starting position* for a scan that re-validates real
// records, and any index that fails to load is rebuilt from its segment.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use crate::Result;
use crate::log::Offset;
use crate::segment::format::{
    INDEX_ENTRY_LEN, INDEX_HEADER_LEN, IndexEntry, IndexHeader, SEGMENT_HEADER_LEN,
};

/// The in-memory form of a segment's sparse index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseIndex {
    base_offset: Offset,
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    pub fn new(base_offset: Offset) -> Self {
        Self {
            base_offset,
            entries: Vec::new(),
        }
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Record that `offset` begins at byte `position`.
    ///
    /// Entries must be pushed in ascending offset order; out-of-order pushes are
    /// dropped rather than corrupting the search invariant.
    pub fn push(&mut self, entry: IndexEntry) {
        if let Some(last) = self.entries.last()
            && entry.offset <= last.offset
        {
            return;
        }
        self.entries.push(entry);
    }

    /// Byte position to start scanning from when looking for `offset`.
    ///
    /// Returns the position of the greatest indexed offset that is `<= offset`,
    /// falling back to the first record in the segment when the index has no
    /// entry that early. The result is always a valid record boundary, so the
    /// caller can decode forward from it.
    pub fn seek_position(&self, offset: Offset) -> u64 {
        match self.entries.binary_search_by(|e| e.offset.cmp(&offset)) {
            Ok(idx) => self.entries[idx].position,
            // `Err(idx)` is the insertion point, so `idx - 1` is the floor.
            Err(0) => SEGMENT_HEADER_LEN,
            Err(idx) => self.entries[idx - 1].position,
        }
    }

    /// Load an index file, returning `None` when it is absent or unusable.
    ///
    /// A `None` here is not an error: the caller rebuilds from the segment,
    /// which is the same work a first-ever open would do.
    pub fn load(path: &Path, base_offset: Offset) -> Option<Self> {
        let mut buf = Vec::new();
        File::open(path).ok()?.read_to_end(&mut buf).ok()?;
        let header = IndexHeader::decode(&buf).ok()?;
        if header.base_offset != base_offset {
            // Index belongs to a different segment generation; rebuild.
            return None;
        }

        let mut index = Self::new(base_offset);
        let mut at = INDEX_HEADER_LEN as usize;
        // A torn final entry is expected after a crash — stop at the last whole
        // one instead of discarding the file.
        while at + (INDEX_ENTRY_LEN as usize) <= buf.len() {
            let entry = IndexEntry::decode(&buf[at..]).ok()?;
            index.push(entry);
            at += INDEX_ENTRY_LEN as usize;
        }
        Some(index)
    }

    /// Write the whole index out, replacing whatever was there.
    ///
    /// Used after a rebuild. Incremental appends during normal operation go
    /// through [`IndexWriter`].
    pub fn persist(&self, path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(
            &IndexHeader {
                base_offset: self.base_offset,
            }
            .encode(),
        )?;
        for entry in &self.entries {
            writer.write_all(&entry.encode())?;
        }
        writer.flush()?;
        writer
            .into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()?;
        Ok(())
    }
}

/// Appends index entries to an index file as its segment grows.
///
/// The writer decides *when* to emit an entry (every `spacing_bytes` of segment
/// data, plus one for the segment's first record); [`SparseIndex`] decides how
/// entries are searched.
#[derive(Debug)]
pub struct IndexWriter {
    file: File,
    index: SparseIndex,
    spacing_bytes: u64,
    /// Segment bytes written since the last entry was emitted.
    bytes_since_entry: u64,
}

impl IndexWriter {
    /// Open `path` for appending, seeding in-memory state from `index`.
    /// Open an index for a segment that has just been created.
    ///
    /// Skips the fsync that [`Self::open`] performs, because there is nothing
    /// yet to make durable: the file holds a header and no entries. That
    /// matters because this runs while installing a rollover, under the lock
    /// appends contend on, where an fsync costs milliseconds and would put back
    /// exactly the stall that preparing the segment ahead of time removes.
    ///
    /// Safe for the same reason a stale index is safe anywhere: indexes are
    /// derived data, rebuilt from the segment whenever they are missing, short
    /// or inconsistent. Losing this write costs a rebuild, never a record.
    pub fn create(path: &Path, index: SparseIndex, spacing_bytes: u64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(
            &IndexHeader {
                base_offset: index.base_offset(),
            }
            .encode(),
        )?;
        drop(file);
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file,
            index,
            spacing_bytes: spacing_bytes.max(1),
            bytes_since_entry: 0,
        })
    }

    pub fn open(path: &Path, index: SparseIndex) -> Result<Self> {
        // The index was just rebuilt or loaded, so rewrite it whole and append
        // from there. This is what makes a stale index self-correcting.
        index.persist(path)?;
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file,
            index,
            // Replaced by `with_spacing`; a zero here would emit an entry per
            // record, so start from the documented default instead.
            spacing_bytes: 4 * 1024,
            bytes_since_entry: 0,
        })
    }

    pub fn with_spacing(mut self, spacing_bytes: u64) -> Self {
        // Zero would mean "index every record", which defeats the point of a
        // sparse index and unbounds the file's size.
        self.spacing_bytes = spacing_bytes.max(1);
        self
    }

    pub fn index(&self) -> &SparseIndex {
        &self.index
    }

    /// Offer a record boundary to the index.
    ///
    /// Emits an entry when the spacing threshold has been crossed, otherwise
    /// just accumulates. `record_len` is the record's full on-disk size.
    pub fn observe_record(&mut self, offset: Offset, position: u64, record_len: u64) -> Result<()> {
        let first_entry = self.index.is_empty();
        if first_entry || self.bytes_since_entry >= self.spacing_bytes {
            let entry = IndexEntry { offset, position };
            self.index.push(entry);
            self.file.write_all(&entry.encode())?;
            self.bytes_since_entry = 0;
        }
        self.bytes_since_entry = self.bytes_since_entry.saturating_add(record_len);
        Ok(())
    }

    /// Flush buffered entries to the OS. Not an fsync: the index is rebuildable,
    /// so paying for a second device sync per append would buy nothing.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Durably persist the index. Used when sealing a segment, where the extra
    /// sync is amortised over the whole segment.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn into_index(self) -> SparseIndex {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn index_with(base: Offset, entries: &[(Offset, u64)]) -> SparseIndex {
        let mut index = SparseIndex::new(base);
        for (offset, position) in entries {
            index.push(IndexEntry {
                offset: *offset,
                position: *position,
            });
        }
        index
    }

    #[test]
    fn empty_index_seeks_to_the_first_record() {
        let index = SparseIndex::new(0);
        assert_eq!(index.seek_position(0), SEGMENT_HEADER_LEN);
        assert_eq!(index.seek_position(u64::MAX), SEGMENT_HEADER_LEN);
    }

    #[test]
    fn seek_finds_the_floor_entry() {
        let index = index_with(0, &[(0, 32), (10, 500), (20, 900)]);
        assert_eq!(index.seek_position(0), 32);
        assert_eq!(index.seek_position(9), 32);
        assert_eq!(index.seek_position(10), 500);
        assert_eq!(index.seek_position(19), 500);
        assert_eq!(index.seek_position(20), 900);
        // Past the last entry: start at the last known boundary and scan.
        assert_eq!(index.seek_position(1_000), 900);
    }

    #[test]
    fn seek_before_the_first_entry_starts_at_the_segment_header() {
        let index = index_with(100, &[(105, 500)]);
        assert_eq!(index.seek_position(100), SEGMENT_HEADER_LEN);
    }

    #[test]
    fn out_of_order_pushes_are_ignored() {
        let mut index = SparseIndex::new(0);
        index.push(IndexEntry {
            offset: 10,
            position: 100,
        });
        index.push(IndexEntry {
            offset: 5,
            position: 999,
        });
        index.push(IndexEntry {
            offset: 10,
            position: 999,
        });
        assert_eq!(index.len(), 1);
        assert_eq!(index.seek_position(10), 100);
    }

    #[test]
    fn persist_then_load_round_trips() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        let index = index_with(7, &[(7, 32), (19, 640)]);
        index.persist(&path).expect("persist");
        let loaded = SparseIndex::load(&path, 7).expect("load");
        assert_eq!(loaded, index);
    }

    #[test]
    fn load_rejects_a_mismatched_base_offset() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        index_with(7, &[(7, 32)]).persist(&path).expect("persist");
        assert!(SparseIndex::load(&path, 8).is_none());
    }

    #[test]
    fn load_of_a_missing_file_is_none() {
        let dir = tempdir().expect("tempdir");
        assert!(SparseIndex::load(&dir.path().join("nope.index"), 0).is_none());
    }

    #[test]
    fn load_tolerates_a_torn_trailing_entry() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        index_with(0, &[(0, 32), (5, 400)])
            .persist(&path)
            .expect("persist");

        // Chop the last entry in half, as an interrupted append would.
        let file = OpenOptions::new().write(true).open(&path).expect("open");
        let len = INDEX_HEADER_LEN + INDEX_ENTRY_LEN + INDEX_ENTRY_LEN / 2;
        file.set_len(len).expect("truncate");

        let loaded = SparseIndex::load(&path, 0).expect("load");
        assert_eq!(
            loaded.entries(),
            &[IndexEntry {
                offset: 0,
                position: 32
            }]
        );
    }

    #[test]
    fn load_rejects_a_corrupt_header() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        index_with(0, &[(0, 32)]).persist(&path).expect("persist");
        std::fs::write(&path, b"garbage!").expect("clobber");
        assert!(SparseIndex::load(&path, 0).is_none());
    }

    #[test]
    fn writer_emits_entries_at_the_spacing_interval() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        let mut writer = IndexWriter::open(&path, SparseIndex::new(0))
            .expect("open")
            .with_spacing(100);

        let mut position = SEGMENT_HEADER_LEN;
        for offset in 0..10u64 {
            writer
                .observe_record(offset, position, 40)
                .expect("observe");
            position += 40;
        }
        writer.sync().expect("sync");

        // First record always indexed, then one per 100 bytes of segment data:
        // entries land at offsets 0, 3, 6 and 9.
        let offsets: Vec<Offset> = writer.index().entries().iter().map(|e| e.offset).collect();
        assert_eq!(offsets, vec![0, 3, 6, 9]);

        let reloaded = SparseIndex::load(&path, 0).expect("load");
        assert_eq!(reloaded.entries(), writer.index().entries());
    }

    #[test]
    fn writer_spacing_never_degenerates_to_every_record() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        let mut writer = IndexWriter::open(&path, SparseIndex::new(0))
            .expect("open")
            .with_spacing(0);
        for offset in 0..4u64 {
            writer
                .observe_record(offset, SEGMENT_HEADER_LEN + offset, 1)
                .expect("observe");
        }
        // Spacing 1 still indexes every record, but the file stays bounded and
        // no divide-by-zero or unbounded growth is possible.
        assert_eq!(writer.index().len(), 4);
    }

    #[test]
    fn writer_rewrites_a_stale_index_on_open() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("a.index");
        index_with(0, &[(0, 32), (5, 400), (9, 800)])
            .persist(&path)
            .expect("persist");

        // Reopen with a shorter, rebuilt index: the file must shrink to match.
        let writer = IndexWriter::open(&path, index_with(0, &[(0, 32)])).expect("open");
        drop(writer);
        let reloaded = SparseIndex::load(&path, 0).expect("load");
        assert_eq!(
            reloaded.entries(),
            &[IndexEntry {
                offset: 0,
                position: 32
            }]
        );
    }
}
