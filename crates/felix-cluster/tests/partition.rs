//! A leader that is healthy and alone.
//!
//! #115's remaining fault. Killing, stopping and freezing a broker all make the
//! cluster notice by the same means: the heartbeat stops. A **partition** does
//! not. The broker keeps running and keeps heartbeating, so the control plane
//! goes on believing it is fine, while it cannot reach a single follower.
//!
//! That is where a replication design is most likely to be wrong, because it is
//! the one fault where the cluster's account of itself and the truth disagree
//! without anything looking broken.
//!
//! Run with `cargo test -p felix-cluster --test partition`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn quorum_cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// **A partitioned leader cannot acknowledge a `Quorum` publish.** It is alive,
/// it still holds its lease, and the control plane still believes it leads —
/// and none of that is a majority. Only the followers it can no longer reach
/// could have made one.
#[serial]
#[tokio::test]
async fn a_partitioned_leader_cannot_reach_a_quorum() {
    let cluster = Cluster::start(quorum_cluster()).await.expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");

    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while the cluster is whole");

    cluster
        .partition_node(&leader)
        .expect("partition the leader");

    let outcome = cluster
        .publish_via(&leader, STREAM, b"while-partitioned".to_vec())
        .await;
    assert!(
        outcome.is_err(),
        "a partitioned leader acknowledged a Quorum publish no majority could hold",
    );

    cluster.heal_partitions().expect("heal");
    cluster.shutdown().await;
}

/// **A partition heals.** The counterpart: if the fault could not be lifted the
/// test above would prove only that something was broken, not that the
/// partition was what broke it.
#[serial]
#[tokio::test]
async fn a_healed_partition_restores_the_quorum() {
    let cluster = Cluster::start(quorum_cluster()).await.expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");

    cluster.partition_node(&leader).expect("partition");
    assert!(
        cluster
            .publish_via(&leader, STREAM, b"refused".to_vec())
            .await
            .is_err(),
        "the partition did not take effect, so healing it proves nothing",
    );

    cluster.heal_partitions().expect("heal");

    // The injector caches its reading briefly, and the pool has to redial peers
    // it gave up on.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match cluster
            .publish_via(&leader, STREAM, b"after-healing".to_vec())
            .await
        {
            Ok(()) => break,
            Err(err) => assert!(
                std::time::Instant::now() < deadline,
                "the quorum never came back after the partition healed; last: {err:#}",
            ),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    cluster.shutdown().await;
}

/// **A `Leader` stream is unaffected by a partition.** It never needed the
/// followers, so severing them must not stop it — and if it did, the test above
/// would be measuring the wrong thing.
#[serial]
#[tokio::test]
async fn a_partitioned_leader_still_serves_a_leader_stream() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");

    cluster.partition_node(&leader).expect("partition");

    cluster
        .publish_via(&leader, STREAM, b"leader-only".to_vec())
        .await
        .expect("a Leader publish needs no follower and must survive a partition");

    cluster.heal_partitions().expect("heal");
    cluster.shutdown().await;
}
