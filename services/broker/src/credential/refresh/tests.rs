//! Persistence and backoff — the parts that do not need a control plane.
//!
//! The loop itself is exercised end to end in
//! `tests/credential_refresh.rs`, against a stub control plane that mints
//! short-lived tokens.
use super::*;

#[test]
fn the_replacement_is_written_whole_or_not_at_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("refresh.token");
    std::fs::write(&path, "original\n").expect("seed");

    persist(&path, "replacement").expect("persist");
    assert_eq!(
        std::fs::read_to_string(&path).expect("read").trim(),
        "replacement",
    );
    // The temporary is gone, not left beside the real file for an operator to
    // wonder about.
    assert!(!path.with_extension("tmp").exists());
}

#[test]
fn persisting_leaves_nothing_behind_on_a_bad_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("no-such-dir").join("refresh.token");
    assert!(persist(&path, "replacement").is_err());
    assert!(!path.exists());
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    // Bounded, because the control plane being unreachable for an hour must
    // not mean the next attempt is an hour away — the credential expires long
    // before that.
    let delays: Vec<_> = (0..10).map(backoff).collect();
    assert_eq!(delays[0], Duration::from_secs(1));
    assert!(delays.windows(2).all(|pair| pair[1] >= pair[0]));
    assert!(
        delays.iter().all(|delay| *delay <= Duration::from_secs(60)),
        "backoff exceeded its ceiling: {delays:?}",
    );
}
