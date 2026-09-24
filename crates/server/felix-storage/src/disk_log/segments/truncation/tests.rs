use tempfile::tempdir;

use super::*;
use crate::disk_log::segments::test_support::{new_set, read_all, record};

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
fn an_index_prefix_drops_entries_past_the_cut() {
    let mut index = SparseIndex::new(0);
    for (offset, position) in [(0u64, 32u64), (5, 200), (9, 400)] {
        index.push(crate::segment::IndexEntry { offset, position });
    }
    let trimmed = rebuild_index_prefix(index, 300);
    assert_eq!(trimmed.len(), 2);
    assert_eq!(trimmed.seek_position(9), 200);
}
