//! #278's last acceptance criterion: a cache survives losing the broker that
//! owned its shard.
//!
//! A cache is a log, so replicating one is the same machinery that replicates a
//! stream — the leader ships records at their offsets and a promoted follower
//! serves what it was shipped. What makes it worth its own test is that the
//! cache reaches those records through an index rather than by replaying, and
//! a follower's index is built from a log that grew underneath it.
//!
//! Run with `cargo test -p felix-cluster --test cache_failover`.
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const CACHE: &str = "sessions";

/// One cache shard held by all three brokers, so killing any one leaves two
/// that still have it.
fn replicated_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::replicated(CACHE, 1, 3)],
        ..Default::default()
    }
}

/// Wait until the leader has shipped past `before` and reports no lag.
///
/// Killing a leader that has shipped nothing tests startup, not failover. The
/// baseline has to be sampled before the write, because the counter only moves
/// on a real exchange and an idle pass leaves it alone.
async fn replication_settled(cluster: &Cluster, leader: &str, before: f64) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the cache write to reach every follower",
        || async {
            let shipped = cluster
                .metric(leader, "felix_broker_replication_shipped_total")
                .await
                .ok()
                .flatten()
                .unwrap_or(0.0);
            let lag = cluster
                .metric(leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten();
            shipped > before && matches!(lag, Some(lag) if lag == 0.0)
        },
    )
    .await
    .expect("a healthy replicated cache shard should reach its followers");
}

/// Wait until the control plane has named an owner other than `gone`.
///
/// Kept free of anything slow. Placement only promotes a replica the leader
/// reported caught up, and that report expires — so the window opens when the
/// dead leader is finally declared not-live and closes a couple of seconds
/// later. A loop that also tried to *read* between attempts spends the window
/// waiting on a broker that cannot answer yet, and misses it.
///
/// The harness's control plane does not run the reconciler on a timer, so a
/// failure test has to step placement itself.
async fn failover_from(cluster: &Cluster, gone: &str, budget: Duration) -> bool {
    felix_cluster::wait::until(budget, "a new cache shard owner", || async {
        cluster.place_shards().await;
        match cluster.shard_owner_of("cache", CACHE, 0).await {
            Ok(owner) => owner != gone,
            Err(_) => false,
        }
    })
    .await
    .is_ok()
}

/// Read a key until some broker answers, or the budget runs out.
///
/// Being named owner and being ready to serve are different moments: a promoted
/// broker learns of its own promotion through the same watch as everything
/// else, and has to open the shard before it can answer for it.
async fn read_until(
    cluster: &Cluster,
    readers: &[String],
    key: &str,
    budget: Duration,
) -> Option<Vec<u8>> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        for reader in readers {
            if let Ok(Some(value)) = cluster.cache_get_via(reader, CACHE, key).await {
                return Some(value);
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **The acceptance criterion.** A value written before the owning broker dies
/// is still readable afterwards.
#[serial]
#[tokio::test]
async fn a_cache_value_survives_the_loss_of_its_owner() {
    let mut cluster = Cluster::start(replicated_cache())
        .await
        .expect("start cluster");

    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let shipped_before = cluster
        .metric(&leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);

    cluster
        .cache_put_via(&leader, CACHE, "k", b"survives")
        .await
        .expect("cache put");
    // Readable before the kill, so a failure afterwards is about the failover
    // rather than about the write never landing.
    assert_eq!(
        cluster
            .cache_get_via(&leader, CACHE, "k")
            .await
            .expect("cache get"),
        Some(b"survives".to_vec()),
    );

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted after {leader} died",
    );

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &leader)
        .collect();

    let read = read_until(&cluster, &survivors, "k", Duration::from_secs(45)).await;
    assert_eq!(
        read.as_deref(),
        Some(&b"survives"[..]),
        "the cache lost its contents when {leader} died",
    );
}

/// Retained state until some survivor serves it, or the budget runs out.
///
/// A watch is redirected rather than forwarded, so most survivors answer
/// `not_leader` — the loop simply tries each until it lands on the promoted
/// owner, exactly as a redirect-following client would.
async fn retained_watch_until(
    cluster: &Cluster,
    survivors: &[String],
    key: &str,
    budget: Duration,
) -> Option<(
    (felix_client::Client, felix_client::CacheWatch),
    u64,
    Vec<u8>,
)> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        for node in survivors {
            let Ok((client, mut watch)) = cluster.cache_watch_retained_via(node, CACHE, key).await
            else {
                continue;
            };
            if watch.retained_count() != Some(1) {
                continue;
            }
            let item = tokio::time::timeout(Duration::from_secs(5), watch.recv()).await;
            if let Ok(Some(felix_client::CacheWatchItem::Change(change))) = item {
                let value = change.value.map(|value| value.to_vec()).unwrap_or_default();
                return Some(((client, watch), change.offset, value));
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **#349's failover criterion.** A promoted replica serves the retained value
/// from its rebuilt index — and the watch is *live* on it: a write after the
/// failover reaches the watcher that joined after it.
#[serial]
#[tokio::test]
async fn a_retained_watch_survives_the_loss_of_the_owner() {
    let mut cluster = Cluster::start(replicated_cache())
        .await
        .expect("start cluster");

    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let shipped_before = cluster
        .metric(&leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);

    cluster
        .cache_put_via(&leader, CACHE, "k", b"retained")
        .await
        .expect("cache put");
    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted after {leader} died",
    );

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &leader)
        .collect();

    let Some(((_client, mut watch), offset, value)) =
        retained_watch_until(&cluster, &survivors, "k", Duration::from_secs(45)).await
    else {
        panic!("no promoted replica served the retained value after {leader} died");
    };
    assert_eq!(
        value.as_slice(),
        b"retained",
        "the promoted replica served the wrong retained value",
    );

    // And the watch is live on the promoted owner: a write after failover
    // reaches the watcher that joined after it, at a later offset.
    cluster
        .cache_put_via(&survivors[0], CACHE, "k", b"after-failover")
        .await
        .expect("cache put after failover");
    let live = tokio::time::timeout(Duration::from_secs(15), watch.recv())
        .await
        .expect("the promoted watch went quiet")
        .expect("the promoted watch ended");
    match live {
        felix_client::CacheWatchItem::Change(change) => {
            assert_eq!(change.value.as_deref(), Some(&b"after-failover"[..]));
            assert!(
                change.offset > offset,
                "the promoted log rewound its offsets",
            );
        }
        other => panic!("expected a live change, got {other:?}"),
    }
}
