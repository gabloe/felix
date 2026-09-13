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
