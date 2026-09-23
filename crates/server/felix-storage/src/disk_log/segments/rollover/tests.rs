use tempfile::tempdir;

use super::*;
use crate::disk_log::segments::test_support::{new_set, read_all, record};
use crate::segment::SEGMENT_HEADER_LEN;

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

#[test]
fn the_default_threshold_never_starts_a_background_roll() {
    let dir = tempdir().expect("dir");
    // 100 is the default and is documented as disabling the background
    // roll. An exactly-full segment is the case that used to slip through.
    let mut set = new_set(&dir, 4096);
    assert_eq!(set.config.rollover_threshold_percent, 100);
    // Enough appends to fill the segment several times over, so the check
    // covers a segment that is nearly full, exactly full, and freshly
    // rolled. `append` rolls on its own, so this cannot run forever.
    for _ in 0..400 {
        set.append(&[record("padding")]).expect("append");
        assert!(
            !set.should_prepare_roll(),
            "a threshold of 100 must never ask for an early roll",
        );
    }
    assert!(set.descriptors().len() > 1, "expected inline rollovers");
}

#[test]
fn an_oversized_first_record_does_not_trigger_a_disabled_roll() {
    let dir = tempdir().expect("dir");
    let mut set = new_set(&dir, 128);
    // One record larger than the whole segment: `size_bytes` lands far past
    // the threshold in a single step.
    set.append(&[record(&"x".repeat(512))]).expect("append");
    assert!(set.active().size_bytes() > 128);
    assert!(!set.should_prepare_roll());
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
