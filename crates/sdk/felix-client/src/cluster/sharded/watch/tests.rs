use super::ShardProgress;

#[test]
fn a_live_watch_resumes_after_the_last_change_handed_out() {
    let mut shard = ShardProgress::new(10, None, None, false);
    assert_eq!(shard.resume_from, 10);
    assert!(!shard.observe_change(12));
    assert_eq!(shard.resume_from, 13);
}

/// Replay arrives in log order, so each replayed change moves the position,
/// and nothing before the requested offset is skipped.
#[test]
fn a_replay_resumes_after_what_was_replayed_not_where_live_began() {
    let mut shard = ShardProgress::new(50, Some(20), None, false);
    assert_eq!(shard.resume_from, 20);
    shard.observe_change(25);
    assert_eq!(shard.resume_from, 26);
}

/// Retained values arrive in key order with any offset below where live
/// began, so the position stays at 0 until the last one is handed out.
#[test]
fn retained_values_hold_the_position_until_the_state_phase_ends() {
    let mut shard = ShardProgress::new(100, None, Some(2), false);
    assert!(!shard.observe_change(90));
    assert_eq!(shard.resume_from, 0);
    assert!(
        shard.observe_change(40),
        "the last retained value ends the phase"
    );
    assert_eq!(shard.resume_from, 100);
    assert!(!shard.observe_change(105));
    assert_eq!(shard.resume_from, 106);
}

#[test]
fn a_resnapshot_only_moves_the_position_once_live() {
    let mut shard = ShardProgress::new(100, Some(5), None, true);
    shard.observe_change(90);
    shard.observe_change(40);
    assert_eq!(shard.resume_from, 5);
    shard.observe_change(101);
    assert_eq!(shard.resume_from, 102);
}

#[test]
fn a_lag_resumes_where_the_broker_says() {
    let mut shard = ShardProgress::new(10, None, None, false);
    shard.observe_change(11);
    shard.observe_lag(30);
    assert_eq!(shard.resume_from, 30);
}

#[test]
fn a_lag_mid_state_resumes_from_the_start() {
    let mut shard = ShardProgress::new(100, None, Some(3), false);
    shard.observe_change(70);
    shard.observe_lag(100);
    assert_eq!(shard.resume_from, 0);
}
