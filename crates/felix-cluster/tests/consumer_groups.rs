//! A consumer group across brokers.
//!
//! The property under test is the one a queue exists for: a record is held by
//! one consumer at a time. Across a cluster that means only the broker leading
//! a shard may serve its groups — two brokers each keeping their own in-flight
//! state would each hand out the same records, and neither would know.
//!
//! Run with `cargo test -p felix-cluster --test consumer_groups`.
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "jobs";
const GROUP: &str = "workers";

fn cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// **The cross-broker gate.** Only the shard's leader serves its groups; every
/// other broker refuses rather than starting a second, independent view of what
/// the group has been handed.
#[tokio::test]
#[serial]
async fn only_the_shard_owner_serves_a_group() {
    let cluster = Cluster::start(cluster()).await.expect("start cluster");
    let (owner, other) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("owner and non-owner");

    cluster
        .publish_via(&owner, STREAM, b"work".to_vec())
        .await
        .expect("publish");

    let refused = cluster.group_poll_via(&other, STREAM, 0, GROUP, 10).await;
    assert!(
        refused.is_err(),
        "{other} served a group for a shard it does not lead",
    );

    let served = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("the owner should serve the group");
    assert!(!served.is_empty(), "the owner returned nothing to consume");
}

/// A record claimed through the owner is not handed out again while the claim
/// stands, no matter which broker is asked.
#[tokio::test]
#[serial]
async fn a_claimed_record_is_not_reissued_by_the_cluster() {
    let cluster = Cluster::start(cluster()).await.expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");

    cluster
        .publish_via(&owner, STREAM, b"once".to_vec())
        .await
        .expect("publish");

    let first = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(!first.is_empty());

    let second = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(
        second.is_empty(),
        "a record held by one consumer was handed to another",
    );
}

/// **#314.** A group's position survives losing the broker that led its shard.
///
/// Before the cursors were replicated, a promoted replica had no record of the
/// group and started it at zero — redelivering everything already finished.
/// Within at-least-once, and severe enough to be a bug: a group that had
/// consumed a million records would be handed all million again.
#[tokio::test]
#[serial]
async fn a_group_position_survives_a_leader_failover() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        // Replicated, so there is a caught-up replica to promote.
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    for job in ["one", "two", "three"] {
        cluster
            .publish_via(&owner, STREAM, job.as_bytes().to_vec())
            .await
            .expect("publish");
    }

    // Consume and finish everything.
    let claimed = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(!claimed.is_empty(), "nothing to consume");
    // However many the harness's own startup probes add, this group finished
    // all of them. That count is what a fresh group should see afterwards, and
    // what this one should not.
    let finished = claimed.len();
    for offset in 0..finished as u64 {
        cluster
            .group_ack_via(&owner, STREAM, 0, GROUP, offset)
            .await
            .expect("ack");
    }
    assert!(
        cluster
            .group_poll_via(&owner, STREAM, 0, GROUP, 10)
            .await
            .expect("poll")
            .is_empty(),
        "the group should have finished its work before the kill",
    );

    // Give the cursors a pass to reach the replicas, then take the leader out.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    cluster.kill_node(&owner).expect("kill the owner");

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &owner)
        .collect();
    let promoted = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "a new leader for the stream",
        || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(next) if next != owner)
        },
    )
    .await;
    assert!(promoted.is_ok(), "no replica was promoted");
    let leader = cluster.owner(STREAM).await.expect("new owner");
    assert!(survivors.contains(&leader));

    // Waiting on a *different* group, deliberately. Polling the group under
    // test here would claim the very redelivery the assertion is about, and the
    // check below would then see an empty second poll whether or not the
    // position survived. It did exactly that in an earlier draft.
    let ready = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "the promoted leader to serve consumer groups",
        || async {
            cluster
                .group_poll_via(&leader, STREAM, 0, "control-group", 10)
                .await
                .is_ok_and(|records| !records.is_empty())
        },
    )
    .await;
    assert!(ready.is_ok(), "the promoted leader never served a group");

    // And that control group proves the records themselves survived, so an
    // empty answer below means the position was kept rather than that there was
    // nothing to hand out.
    let fresh = cluster
        .group_poll_via(&leader, STREAM, 0, "control-group-2", 10)
        .await
        .expect("poll as a new group");
    assert_eq!(
        fresh.len(),
        finished,
        "the promoted leader is missing the records themselves, so this test \
         cannot say anything about the group's position",
    );

    let redelivered = cluster
        .group_poll_via(&leader, STREAM, 0, GROUP, 10)
        .await
        .expect("poll the promoted leader");
    assert!(
        redelivered.is_empty(),
        "the group lost its position and was handed {} finished record(s) again",
        redelivered.len(),
    );
}
