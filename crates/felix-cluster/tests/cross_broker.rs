//! M4's completion signal: a three-node cluster delivers across node boundaries.
//!
//! These start real broker processes, so they are slower than a unit test and
//! deliberately few. What they cover is the seam nothing else can: the wiring
//! between a broker's configuration, its cluster view, and the publish path a
//! real client actually takes.
//!
//! Run with `cargo test -p felix-cluster` (or `task cluster:test`), which builds
//! the `felix-broker` binary these need.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig};

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![(STREAM.to_string(), 1)],
        ..Default::default()
    }
}

/// The milestone signal. A publish that arrives at a broker which does not own
/// the shard reaches a subscriber on the one that does.
#[tokio::test]
async fn a_publish_through_a_non_owner_is_delivered_by_the_owner() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve owner");

    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("subscribe on the owner");

    let payload = b"across-the-boundary".to_vec();
    cluster
        .publish_via(&non_owner, STREAM, payload.clone())
        .await
        .expect("publish through a non-owner");

    // Asserted before the delivery wait: a broker that served the publish
    // locally would look exactly like one whose delivery is merely slow, and
    // this is the difference the whole milestone is about.
    let forwarded = cluster
        .metric(&non_owner, "felix_broker_forwards_total")
        .await
        .expect("read metrics");
    assert!(
        forwarded.is_some_and(|count| count > 0.0),
        "{non_owner} did not forward: it served a shard owned by {owner} locally",
    );

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .expect("timed out waiting for the record")
        .expect("subscription failed")
        .expect("subscription closed");
    assert_eq!(event.payload, payload);

    cluster.shutdown().await;
}

/// The owner serves its own shard without forwarding, which is what makes the
/// test above about routing rather than about publishing at all.
#[tokio::test]
async fn a_publish_through_the_owner_is_not_forwarded() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (owner, _) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve owner");

    let before = cluster
        .metric(&owner, "felix_broker_forwards_total")
        .await
        .expect("read metrics")
        .unwrap_or(0.0);

    cluster
        .publish_via(&owner, STREAM, b"local".to_vec())
        .await
        .expect("publish through the owner");

    let after = cluster
        .metric(&owner, "felix_broker_forwards_total")
        .await
        .expect("read metrics")
        .unwrap_or(0.0);
    assert_eq!(
        before, after,
        "the owner forwarded a shard it owns; ownership resolution is wrong",
    );

    cluster.shutdown().await;
}

/// A stopped broker leaves the cluster, and the harness can tell when.
///
/// This is the primitive the M5-M10 failure tests need: without it every such
/// test races the expiry sweep.
#[tokio::test]
async fn a_stopped_broker_leaves_the_cluster() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let victim = cluster.nodes[1].node_id.clone();
    assert!(
        cluster
            .placeable_nodes()
            .await
            .expect("list nodes")
            .contains(&victim),
    );

    cluster.stop_node(&victim).await.expect("stop the broker");

    let live = cluster.placeable_nodes().await.expect("list nodes");
    assert!(!live.contains(&victim), "{victim} is still placeable");
    assert_eq!(live.len(), 2, "the survivors must stay placeable: {live:?}");

    cluster.shutdown().await;
}
