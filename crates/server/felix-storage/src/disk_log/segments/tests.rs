use tempfile::tempdir;

use super::test_support::{new_set, read_all, record};
use super::*;
use crate::segment::SEGMENT_HEADER_LEN;

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
