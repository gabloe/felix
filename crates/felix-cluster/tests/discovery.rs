//! A client that learns the cluster instead of being told all of it.
//!
//! #117. An application configured with one broker address is configured with
//! a single point of failure, however many brokers the cluster has. These cover
//! the seam: what the client is told, what it does with it, and whether the one
//! address it started from can then be taken away.
//!
//! Run with `cargo test -p felix-cluster --test discovery`.
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

/// **One seed is enough to learn the whole cluster.** This is the point of
/// discovery: the address an application was configured with stops being the
/// only one it can use.
#[serial]
#[tokio::test]
async fn a_client_given_one_seed_learns_the_other_brokers() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let every_broker = cluster.broker_addrs();
    let one_seed = [every_broker[0]];

    let client = felix_cluster::client::connect_cluster(
        &one_seed,
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    let known = client.endpoints().await;
    for addr in &every_broker {
        assert!(
            known.contains(addr),
            "discovery did not report {addr}; knows {known:?}",
        );
    }

    cluster.shutdown().await;
}

/// **The configured seed survives discovery.** The cluster's account of itself
/// can be wrong or stale; the address someone chose deliberately is the one
/// thing a client must never be talked out of.
#[serial]
#[tokio::test]
async fn the_configured_seed_is_never_dropped() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let seed = cluster.broker_addrs()[0];

    let client =
        felix_cluster::client::connect_cluster(&[seed], &cluster.tenant_id, &cluster.client_token)
            .await
            .expect("connect");

    client.refresh_topology().await.expect("refresh");
    assert!(
        client.endpoints().await.contains(&seed),
        "a refresh removed the address the client was configured with",
    );

    cluster.shutdown().await;
}

/// **A client given one seed survives that seed being killed.** The acceptance
/// criterion for #117: it can connect without being handed the final owner's
/// address, and keeps working when the address it *was* handed stops answering.
#[serial]
#[tokio::test]
async fn a_client_given_one_seed_survives_losing_it() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    // Deliberately not the shard's owner. Killing the owner tests failover as
    // well; what is under test here is that a client whose *only* configured
    // address is gone still has somewhere to go.
    let (_owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve a non-owner");
    let seed = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == non_owner)
        .map(|node| node.client_addr)
        .expect("the non-owner's client address");

    let client =
        felix_cluster::client::connect_cluster(&[seed], &cluster.tenant_id, &cluster.client_token)
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
        .expect("publish before the seed is killed");

    cluster.kill_node(&non_owner).expect("kill the seed broker");
    // Long enough for the control plane to notice, so the publish below is not
    // racing the cluster's own detection.
    tokio::time::sleep(Duration::from_secs(2)).await;

    client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"after".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("the only configured broker is gone, but discovery found others");

    cluster.shutdown().await;
}
