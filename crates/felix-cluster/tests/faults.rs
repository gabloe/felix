//! The faults the harness can inject, and proof that each is the fault it claims.
//!
//! A failure-injection test is only as good as the failure it injects. A
//! "paused" broker that is really just slow, or a "killed" one that is still
//! answering, would make every scenario built on it pass for the wrong reason —
//! so these assert the fault itself rather than anything about replication.
//!
//! Run with `cargo test -p felix-cluster` (or `task cluster:test`).
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// Whether a broker *successfully answers* its metrics endpoint within `budget`.
///
/// Success, not merely completion. A suspended process may leave its listening
/// socket to hang the request or to refuse it outright depending on the
/// platform, and both mean "not answering" — an earlier version of this asked
/// only whether the call returned, so a fast refusal on Linux read as a healthy
/// broker and the fault went untested.
async fn answers_within(cluster: &Cluster, node_id: &str, budget: Duration) -> bool {
    matches!(
        tokio::time::timeout(budget, cluster.metric(node_id, "felix_broker_up")).await,
        Ok(Ok(_))
    )
}

/// **A paused broker stops answering and stays alive.** Both halves matter: if
/// it died the test would be a kill by another name, and if it kept answering
/// there would be no fault at all.
#[serial]
#[tokio::test]
async fn a_paused_broker_stops_answering_without_dying() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();
    assert!(
        answers_within(&cluster, &node_id, Duration::from_secs(5)).await,
        "the broker was not answering before it was paused",
    );

    cluster.pause_node(&node_id).expect("pause");

    assert!(
        !answers_within(&cluster, &node_id, Duration::from_millis(750)).await,
        "a paused broker was still answering",
    );
    assert!(
        cluster
            .node(&node_id)
            .expect("the node should still be known")
            .is_running(),
        "pausing killed the broker instead of suspending it",
    );

    cluster.resume_node(&node_id).expect("resume");
    cluster.shutdown().await;
}

/// And it answers again once resumed, which is what makes the fault a pause
/// rather than a slower kill.
#[serial]
#[tokio::test]
async fn a_resumed_broker_answers_again() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();

    cluster.pause_node(&node_id).expect("pause");
    assert!(!answers_within(&cluster, &node_id, Duration::from_millis(750)).await);

    cluster.resume_node(&node_id).expect("resume");

    assert!(
        answers_within(&cluster, &node_id, Duration::from_secs(10)).await,
        "a resumed broker never came back",
    );
    cluster.shutdown().await;
}

/// An immediate kill returns without waiting for the cluster to react, so a
/// test can start its own clock at the moment of the kill.
#[serial]
#[tokio::test]
async fn an_immediate_kill_does_not_wait_for_the_cluster_to_notice() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();

    let started = std::time::Instant::now();
    cluster.kill_node(&node_id).expect("kill");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "kill_node waited for something: {elapsed:?}",
    );
    assert!(
        !cluster
            .node(&node_id)
            .expect("the node should still be known")
            .is_running(),
    );
    cluster.shutdown().await;
}

/// Both faults refuse a node that is not there, rather than reporting success
/// for something they did not do.
#[serial]
#[tokio::test]
async fn a_fault_aimed_at_an_unknown_node_is_an_error() {
    let cluster = Cluster::start(config()).await.expect("start cluster");

    assert!(cluster.pause_node("broker-does-not-exist").is_err());
    assert!(cluster.resume_node("broker-does-not-exist").is_err());
    cluster.shutdown().await;
}

/// Pausing a broker that has already been killed is an error too: there is no
/// process to suspend, and silently succeeding would let a scenario believe it
/// had injected a fault it had not.
#[serial]
#[tokio::test]
async fn pausing_a_stopped_broker_is_an_error() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();
    cluster.kill_node(&node_id).expect("kill");

    assert!(cluster.pause_node(&node_id).is_err());
    cluster.shutdown().await;
}

/// **Teardown reclaims a suspended broker.** A test that panics mid-fault
/// leaves exactly this, and a teardown that hung there would wedge the whole
/// suite rather than failing one test — so the property is worth pinning even
/// though a signal-stopped process does die to `SIGKILL`.
#[serial]
#[tokio::test]
async fn teardown_reclaims_a_suspended_broker() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();
    cluster.pause_node(&node_id).expect("pause");

    // Deliberately not resumed.
    tokio::time::timeout(Duration::from_secs(20), cluster.shutdown())
        .await
        .expect("shutdown hung on a suspended broker");
}
