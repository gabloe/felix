//! What the control plane believes about replicas, and for how long.
use super::*;
use crate::config::NodeLivenessConfig;

const HEARTBEAT_MS: u64 = 500;
const EXPIRY_MS: u64 = 1_500;
/// Reports are believed for twice the expiry plus one heartbeat.
const TTL_MS: u64 = EXPIRY_MS * 2 + HEARTBEAT_MS;

fn positions() -> ReplicaPositions {
    ReplicaPositions::new(&NodeLivenessConfig {
        heartbeat_interval_ms: HEARTBEAT_MS,
        expiry_timeout_ms: EXPIRY_MS,
        ..Default::default()
    })
}

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
    }
}

fn caught_up(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

/// Every caught-up node at the same offset, for tests that are about freshness
/// and generations rather than about which replica is furthest ahead.
fn offsets_for(nodes: &BTreeSet<String>) -> std::collections::HashMap<String, u64> {
    nodes.iter().map(|n| (n.clone(), 10)).collect()
}

fn at(positions: &ReplicaPositions, now_millis: u64) -> CaughtUpAt<'_> {
    CaughtUpAt {
        positions,
        now_millis,
    }
}

/// A reported follower can take over; one that was not reported cannot.
#[test]
fn a_reported_follower_is_caught_up_and_others_are_not() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );

    let view = at(&positions, 1_000);
    assert!(view.is_caught_up(&key("orders"), "broker-b"));
    assert!(!view.is_caught_up(&key("orders"), "broker-c"));
}

/// **Nothing is caught up until a leader says so.** The absence of a report is
/// not permission to promote.
#[test]
fn an_unreported_shard_has_nothing_caught_up() {
    let positions = positions();

    assert!(!at(&positions, 1_000).is_caught_up(&key("orders"), "broker-b"));
}

/// **A report expires.** It says a follower *was* caught up; the leader kept
/// writing afterwards, and promoting on a stale report silently loses whatever
/// was written since.
#[test]
fn a_report_is_not_believed_forever() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );

    assert!(
        at(&positions, 1_000 + TTL_MS).is_caught_up(&key("orders"), "broker-b"),
        "a report expired before the window it has to survive",
    );
    assert!(
        !at(&positions, 1_000 + TTL_MS + 1).is_caught_up(&key("orders"), "broker-b"),
        "a stale report was still believed",
    );
}

/// **The window outlives the detection of a dead leader.** The last report a
/// leader makes is from just before it dies, and promotion happens only after
/// the cluster has noticed — which takes the expiry timeout. A report that
/// expired sooner could never be used for the failover it exists for.
#[test]
fn a_report_outlives_the_window_a_dead_leader_is_noticed_in() {
    let positions = positions();
    let last_report = 1_000;
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        last_report,
    );

    // The leader dies just after reporting; the cluster notices an expiry
    // timeout later and plans then.
    let planning_at = last_report + EXPIRY_MS;

    assert!(
        at(&positions, planning_at).is_caught_up(&key("orders"), "broker-b"),
        "the report expired before failover could use it",
    );
}

/// A later report replaces an earlier one, so a follower that falls behind
/// stops being promotable.
#[test]
fn a_later_report_replaces_an_earlier_one() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );
    positions.record(
        key("orders"),
        4,
        caught_up(&[]),
        offsets_for(&caught_up(&[])),
        1_100,
    );

    assert!(!at(&positions, 1_100).is_caught_up(&key("orders"), "broker-b"));
}

/// **An older generation's report is dropped.** Leadership has moved on, and
/// the old leader's view is about a replica set that may no longer exist.
#[test]
fn a_report_from_a_superseded_leader_is_dropped() {
    let positions = positions();
    positions.record(
        key("orders"),
        5,
        caught_up(&[]),
        offsets_for(&caught_up(&[])),
        1_000,
    );

    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_100,
    );

    assert!(
        !at(&positions, 1_100).is_caught_up(&key("orders"), "broker-b"),
        "a superseded leader's report overwrote the current one",
    );
}

/// A report at the same generation is an update, not a stale duplicate: the
/// same leader reporting again is exactly the normal case.
#[test]
fn a_report_at_the_same_generation_is_an_update() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&[]),
        offsets_for(&caught_up(&[])),
        1_000,
    );
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_100,
    );

    assert!(at(&positions, 1_100).is_caught_up(&key("orders"), "broker-b"));
}

/// Shards are independent: one shard's replicas say nothing about another's.
#[test]
fn shards_do_not_share_reports() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );

    assert!(!at(&positions, 1_000).is_caught_up(&key("payments"), "broker-b"));
}

/// A planning pass judges every shard against one instant, so a report cannot
/// be fresh for one shard and stale for the next within the same pass.
#[test]
fn one_pass_judges_every_shard_at_the_same_instant() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );
    positions.record(
        key("payments"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );

    let view = at(&positions, 1_000 + TTL_MS);

    assert!(view.is_caught_up(&key("orders"), "broker-b"));
    assert!(view.is_caught_up(&key("payments"), "broker-b"));
}

/// A forgotten shard reports nothing, so a shard that is deleted and recreated
/// does not inherit the old one's promotability.
#[test]
fn a_forgotten_shard_reports_nothing() {
    let positions = positions();
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        1_000,
    );

    positions.forget(&key("orders"));

    assert!(!at(&positions, 1_000).is_caught_up(&key("orders"), "broker-b"));
}

/// **A report has to outlive the detection of the leader that made it.**
///
/// A leader is declared down an expiry timeout after its last *heartbeat*, and
/// its last *report* is older still. If the report expires first the shard
/// becomes unpromotable forever — nothing else will ever report on it, because
/// the only broker that could is the one that died.
#[test]
fn a_report_outlives_the_detection_of_the_leader_that_made_it() {
    let positions = positions();
    // The worst realistic case: the leader reports, then survives a further
    // heartbeat interval before dying, and is declared down an expiry timeout
    // after that.
    let reported_at = 1_000;
    positions.record(
        key("orders"),
        4,
        caught_up(&["broker-b"]),
        offsets_for(&caught_up(&["broker-b"])),
        reported_at,
    );
    let declared_down_at = reported_at + HEARTBEAT_MS + EXPIRY_MS;

    assert!(
        at(&positions, declared_down_at).is_caught_up(&key("orders"), "broker-b"),
        "the report expired before the leader was even known to be gone, which \
         leaves the shard unpromotable for good",
    );
}
