//! A cache whose keys are owned by different brokers.
//!
//! #278. Every broker used to serve its own copy of every cache, so a `put` on
//! one was invisible to a `get` on another — and worse, two brokers could hold
//! *different* values for one key with nothing to reconcile them. A miss is
//! detectable; divergence is not.
//!
//! These tests are written from the client's side on purpose. The unit tests
//! prove the routing decision; these prove the thing a user would notice.
//!
//! Run with `cargo test -p felix-cluster --test cache_routing`.
use std::collections::HashSet;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const CACHE: &str = "sessions";
const SHARDS: u32 = 4;

fn with_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        // A stream is still needed: the harness gates readiness on a publish
        // being accepted, which is the only honest proof the cluster can serve.
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::new(CACHE, SHARDS)],
        ..Default::default()
    }
}

/// The headline property. A value written through any broker is readable
/// through every other, because exactly one of them owns the key and the rest
/// forward to it.
#[tokio::test]
#[serial]
async fn a_value_written_through_one_broker_is_readable_through_every_other() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let nodes: Vec<String> = cluster.node_ids();

    for (i, writer) in nodes.iter().enumerate() {
        let key = format!("key-from-{writer}");
        let value = format!("value-{i}");
        cluster
            .cache_put_via(writer, CACHE, &key, value.as_bytes())
            .await
            .expect("cache put");

        for reader in &nodes {
            let read = cluster
                .cache_get_via(reader, CACHE, &key)
                .await
                .expect("cache get");
            assert_eq!(
                read.as_deref(),
                Some(value.as_bytes()),
                "{key} written through {writer} was not readable through {reader}",
            );
        }
    }
}

/// The divergence itself. Two brokers writing the same key must not end up with
/// two answers: the second write goes to the same owner as the first, so every
/// reader sees the second value and nothing is left holding the first.
#[tokio::test]
#[serial]
async fn two_brokers_writing_one_key_do_not_diverge() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    assert!(nodes.len() >= 2);

    let key = "contended";
    cluster
        .cache_put_via(&nodes[0], CACHE, key, b"first")
        .await
        .expect("first write");
    cluster
        .cache_put_via(&nodes[1], CACHE, key, b"second")
        .await
        .expect("second write");

    for reader in &nodes {
        let read = cluster
            .cache_get_via(reader, CACHE, key)
            .await
            .expect("cache get");
        assert_eq!(
            read.as_deref(),
            Some(&b"second"[..]),
            "{reader} still answers with a value the last write replaced",
        );
    }
}

/// Ownership is actually spread. If every shard landed on one broker the tests
/// above would pass without a single operation ever being forwarded, and the
/// routing they exist to check would be untested.
#[tokio::test]
#[serial]
async fn cache_shards_are_spread_across_brokers() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");

    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");

    assert_eq!(
        owners.len(),
        SHARDS as usize,
        "every cache shard should have a leader, got {owners:?}",
    );
    let distinct: HashSet<&String> = owners.values().collect();
    assert!(
        distinct.len() > 1,
        "all {SHARDS} cache shards landed on one broker: {owners:?}",
    );
}

/// Keys must actually land in different shards. One shard holding everything
/// would make the cluster tests above pass while exercising a single owner.
#[tokio::test]
#[serial]
async fn keys_are_spread_across_cache_shards() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");
    let nodes = cluster.node_ids();

    // Written through one broker and read through a *different* one, on
    // purpose. A same-broker round trip succeeds whether or not the key was
    // routed — it would just find the local copy — so it could not tell a
    // working keyspace split from the divergence this replaced.
    for n in 0..40 {
        let key = format!("spread-{n}");
        cluster
            .cache_put_via(&nodes[0], CACHE, &key, key.as_bytes())
            .await
            .expect("cache put");
    }
    for n in 0..40 {
        let key = format!("spread-{n}");
        let read = cluster
            .cache_get_via(&nodes[n % nodes.len()], CACHE, &key)
            .await
            .expect("cache get");
        assert_eq!(read.as_deref(), Some(key.as_bytes()), "{key} was lost");
    }

    assert!(
        owners.values().collect::<HashSet<_>>().len() > 1,
        "the keyspace was not actually split: {owners:?}",
    );
}

/// A cache and a stream may share a name, and when they do they are unrelated.
/// A broker that confused them would route cache keys to the stream's owner.
#[tokio::test]
#[serial]
async fn a_cache_named_after_a_stream_is_a_different_thing() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::new("orders", SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let nodes = cluster.node_ids();
    cluster
        .cache_put_via(&nodes[0], "orders", "k", b"cached")
        .await
        .expect("cache put");

    for reader in &nodes {
        let read = cluster
            .cache_get_via(reader, "orders", "k")
            .await
            .expect("cache get");
        assert_eq!(read.as_deref(), Some(&b"cached"[..]), "via {reader}");
    }

    // And the stream of the same name still works, on its own owner.
    let owner = cluster.owner("orders").await.expect("stream owner");
    cluster
        .publish_via(&owner, "orders", b"streamed".to_vec())
        .await
        .expect("publish to the stream of the same name");
}
