// Durable segment files: the byte format, the sparse index beside each segment,
// and the writer/reader pair that own a single segment.
//
// Nothing in here knows about shards, retention, or rollover policy — a
// `SegmentWriter` writes one file until someone tells it to stop. Composing
// segments into a log is `crate::disk_log`'s job.

pub mod format;
pub mod index;
pub mod io;
pub mod reader;
pub mod writer;

pub use format::{
    Corruption, CorruptionKind, CorruptionSite, DecodedRecord, FORMAT_VERSION, IndexEntry,
    IndexHeader, MAX_PAYLOAD_BYTES, RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN,
    SegmentHeader,
};
pub use index::{IndexWriter, SparseIndex};
pub use reader::{
    ReadBudget, ScanOutcome, ScanStart, SegmentReader, TornTail, read_segment_header, scan_segment,
};
pub use writer::{ResumeState, SegmentWriter};

use crate::log::SegmentId;

/// File name of the data file for `id`, e.g. `00000000000000000007.log`.
///
/// Zero padding keeps lexicographic and numeric order identical, which makes a
/// directory listing readable — but recovery still parses the number and sorts
/// on it rather than trusting the listing.
pub fn segment_file_name(id: SegmentId) -> String {
    format!("{id:020}.log")
}

/// File name of the sparse index that accompanies segment `id`.
pub fn index_file_name(id: SegmentId) -> String {
    format!("{id:020}.index")
}

/// Parse a segment id back out of a data file name, or `None` if the name is not
/// one of ours.
pub fn parse_segment_file_name(name: &str) -> Option<SegmentId> {
    name.strip_suffix(".log")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_are_zero_padded() {
        assert_eq!(segment_file_name(7), "00000000000000000007.log");
        assert_eq!(index_file_name(7), "00000000000000000007.index");
    }

    #[test]
    fn segment_file_names_round_trip() {
        for id in [0, 1, 42, u64::MAX] {
            assert_eq!(parse_segment_file_name(&segment_file_name(id)), Some(id));
        }
    }

    #[test]
    fn unrelated_names_are_not_segments() {
        assert_eq!(parse_segment_file_name("00000000000000000007.index"), None);
        assert_eq!(parse_segment_file_name("notanumber.log"), None);
        assert_eq!(parse_segment_file_name("7.log.tmp"), None);
        assert_eq!(parse_segment_file_name(""), None);
        // Negative numbers are not offsets.
        assert_eq!(parse_segment_file_name("-1.log"), None);
    }

    #[test]
    fn lexicographic_order_matches_numeric_order() {
        let mut names: Vec<String> = [10u64, 2, 33, 1]
            .iter()
            .map(|id| segment_file_name(*id))
            .collect();
        names.sort();
        let ids: Vec<SegmentId> = names
            .iter()
            .map(|n| parse_segment_file_name(n).expect("id"))
            .collect();
        assert_eq!(ids, vec![1, 2, 10, 33]);
    }
}
