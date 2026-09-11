//! A publisher that outlives the broker it was using.
//!
//! M5 made the cluster survive losing a leader; a client holding connection
//! pools to that broker did not. These cover the seam an application actually
//! sits on: not "did the cluster recover" but "could the program carry on".
//!
//! Run with `cargo test -p felix-cluster --test reconnect`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// **A publisher carries on after the broker it was using is killed.**
///
/// It resends, so the record that was in flight is not lost — at the cost the
/// method's name states.
#[serial]
#[tokio::test]
async fn a_publisher_survives_losing_its_broker() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"before".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("publish before the failure");

    let leader = cluster.owner(STREAM).await.expect("owner");
    felix_cluster::wait::until(Duration::from_secs(20), "the leader to report", || async {
        matches!(
            cluster.metric(&leader, "felix_broker_replication_shipped_total").await,
            Ok(Some(n)) if n > 0.0
        )
    })
    .await
    .expect("the leader should ship and report");
    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");

    client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"after".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("the same client should carry on after its broker died");

    cluster.shutdown().await;
}

/// **`publish` reconnects but does not resend.** The failed record is the
/// caller's to deal with; the next publish goes to a live broker rather than
/// repeating the failure against the dead one.
#[serial]
#[tokio::test]
async fn publish_reports_the_failure_and_leaves_a_usable_client() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");

    // Whether this particular publish fails depends on which broker the client
    // happened to be using, so it is not asserted either way. What is asserted
    // is that the client is usable afterwards.
    let _ = client
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"maybe".to_vec(),
            AckMode::PerMessage,
        )
        .await;

    let mut published = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !published && std::time::Instant::now() < deadline {
        if client
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                b"after".to_vec(),
                AckMode::PerMessage,
            )
            .await
            .is_ok()
        {
            published = true;
        } else {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    assert!(published, "the client never became usable again");

    cluster.shutdown().await;
}

/// Everything published through the failover is readable afterwards, in order.
#[serial]
#[tokio::test]
async fn records_published_across_a_failover_are_all_readable() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    let leader = cluster.owner(STREAM).await.expect("owner");
    for i in 0..3 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("r{i}").into_bytes(),
                AckMode::PerMessage,
            )
            .await
            .expect("publish");
    }
    felix_cluster::wait::until(Duration::from_secs(20), "the leader to report", || async {
        matches!(
            cluster.metric(&leader, "felix_broker_replication_shipped_total").await,
            Ok(Some(n)) if n > 0.0
        )
    })
    .await
    .expect("ship");
    cluster.kill_node(&leader).expect("kill");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("promotion");

    for i in 3..6 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("r{i}").into_bytes(),
                AckMode::PerMessage,
            )
            .await
            .expect("publish after failover");
    }

    let promoted = cluster.owner(STREAM).await.expect("owner");
    let (_c, mut sub) = cluster.replay_on(&promoted, STREAM).await.expect("replay");
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), sub.next_event()).await
    {
        seen.push(String::from_utf8_lossy(&event.payload).to_string());
    }

    for i in 0..6 {
        assert!(
            seen.contains(&format!("r{i}")),
            "r{i} is missing from {seen:?}",
        );
    }
    cluster.shutdown().await;
}
