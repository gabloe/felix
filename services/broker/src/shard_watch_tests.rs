//! Continuity and apply rules: every branch here is a way to silently lose an
//! ownership change.
use super::*;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
    }
}

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
    }
}

#[test]
fn a_contiguous_page_needs_no_resync() {
    // Asked from 5, got 5 onwards.
    assert_eq!(check_continuity(5, Some(5), 9), None);
    // Asked from 5, nothing new yet.
    assert_eq!(check_continuity(5, None, 5), None);
    // First poll of an empty cluster.
    assert_eq!(check_continuity(0, None, 0), None);
}

/// The change we asked for is gone from the window, so everything between our
/// checkpoint and the first returned change was evicted.
#[test]
fn a_first_seq_above_the_checkpoint_is_a_gap() {
    assert_eq!(check_continuity(5, Some(9), 20), Some(Resync::GapInHistory),);
}

/// The subtle one: an empty page is only safe when the log has not moved. If it
/// has, the whole span was evicted rather than being empty, and resuming would
/// skip it in silence.
#[test]
fn an_empty_page_with_a_moved_log_is_a_gap() {
    assert_eq!(check_continuity(5, None, 12), Some(Resync::GapInHistory));
    assert_eq!(
        check_continuity(5, None, 5),
        None,
        "not moved: genuinely empty"
    );
}

/// A control plane restarted onto a store that does not persist its sequence,
/// so the log is now behind our checkpoint. Continuing would apply old changes
/// as if they were new.
#[test]
fn a_log_behind_the_checkpoint_is_a_reset() {
    assert_eq!(check_continuity(40, None, 0), Some(Resync::SequenceReset));
    assert_eq!(
        check_continuity(40, Some(0), 3),
        Some(Resync::SequenceReset),
        "a reset is checked before a gap, since the gap test is meaningless here",
    );
}

#[test]
fn applying_a_newer_generation_takes_effect() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 0))));
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-b", 1))));
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-b");
}

/// Duplicate delivery has to be harmless, or an at-least-once poll would need
/// exactly-once plumbing it does not have.
#[test]
fn a_duplicate_change_is_ignored() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert_eq!(owned.len(), 1);
}

/// The property that makes reordering safe: a change that would move ownership
/// backwards is dropped, not applied.
#[test]
fn a_stale_generation_cannot_roll_ownership_back() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-b", 5)));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 4))));
    assert_eq!(
        owned.get(&key(0)).expect("present").leader,
        "broker-b",
        "an older generation must not take the shard back",
    );
}

#[test]
fn an_unassignment_removes_the_shard() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.apply(&key(0), None));
    assert!(owned.get(&key(0)).is_none());
    // Removing it twice is harmless, and reports that nothing changed.
    assert!(!owned.apply(&key(0), None));
}

#[test]
fn a_snapshot_replaces_everything() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    owned.apply(&key(1), Some(assignment(1, "broker-a", 0)));

    owned.reset(vec![assignment(2, "broker-c", 7)]);
    assert_eq!(owned.len(), 1);
    assert!(
        owned.get(&key(0)).is_none(),
        "stale ownership must not survive"
    );
    assert_eq!(owned.get(&key(2)).expect("present").leader, "broker-c");
}

#[test]
fn leadership_is_reported_for_this_node_only() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.is_leader(&key(0), "broker-a"));
    assert!(!owned.is_leader(&key(0), "broker-b"));
    assert!(!owned.is_leader(&key(9), "broker-a"), "unknown shard");
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    let interval = Duration::from_millis(100);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), interval * 2);
    assert_eq!(backoff(interval, 3), interval * 4);
    assert_eq!(backoff(interval, 40), MAX_BACKOFF);
}
