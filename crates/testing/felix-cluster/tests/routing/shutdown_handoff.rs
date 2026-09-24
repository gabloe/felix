//! A broker told to stop hands its shards to the others first, so a rolling
//! restart moves shards instead of failing them over.
//!
//! Run with `cargo test -p felix-cluster --test routing shutdown_handoff::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;
use tokio::sync::Mutex;

const STREAM: &str = "orders";

/// The broker's default `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` plus its default
/// drain deadline: an idle client connection holds the drain until the latter.
const EXIT_WITHIN: Duration = Duration::from_secs(30 + 25 + 10);

/// **SIGTERM under load moves the shard; nothing fails over.** A publisher
/// writes through another broker and a subscriber reads from the stopping
/// one. The shard is moved (placement writes move steps, never a new
/// placement), no publish is refused, every acknowledged record reaches the
/// subscriber once and in order, and the broker exits in bounded time.
#[serial]
#[tokio::test]
async fn a_stopping_broker_hands_its_shard_over_under_load() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster
        .wait_for_replication(STREAM, Duration::from_secs(30))
        .await
        .expect("the owner ships to its follower");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("another broker");
    let addr = |cluster: &Cluster, node: &str| cluster.node(node).expect("node").client_addr;

    let mut owed: Vec<Vec<u8>> = Vec::new();
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(
                &owner,
                STREAM,
                b"k",
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the stop");
        owed.push(payload);
    }

    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&cluster, &owner)],
            &cluster.tenant_id,
            &cluster.client_token,
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");

    let writer = felix_cluster::client::connect_cluster(
        &[addr(&cluster, &other)],
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let refused = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (acknowledged, refused, stop) = (
            Arc::clone(&acknowledged),
            Arc::clone(&refused),
            Arc::clone(&stop),
        );
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let payload = format!("during-{i}").into_bytes();
                i += 1;
                match writer
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        payload.clone(),
                        felix_wire::AckMode::PerMessage,
                    )
                    .await
                {
                    Ok(_) => acknowledged.lock().await.push(payload),
                    Err(_) => {
                        refused.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    let signalled = Instant::now();
    cluster.terminate_node(&owner).expect("SIGTERM");

    // Placement stepped by hand, so every pass's outcome is seen: a failover
    // is a placement, a handoff is move steps.
    let (mut placed, mut moved) = (0usize, 0usize);
    let mut exited = None;
    let mut moved_at = None;
    while exited.is_none() || moved_at.is_none() {
        let outcome = cluster.place_shards().await;
        placed += outcome.placed + outcome.failed;
        moved += outcome.moved;
        if moved_at.is_none() && cluster.owner(STREAM).await.is_ok_and(|now| now != owner) {
            moved_at = Some(signalled.elapsed());
        }
        if exited.is_none() {
            exited = cluster
                .exit_status(&owner)
                .expect("node")
                .map(|status| (status, signalled.elapsed()));
        }
        assert!(
            signalled.elapsed() < EXIT_WITHIN,
            "after {:?}: exited {exited:?}, shard moved at {moved_at:?}",
            signalled.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (status, exited_after) = exited.expect("exited");
    let moved_at = moved_at.expect("moved");

    // Traffic after the stop too, so the destination is shown taking writes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("publisher");
    let during = acknowledged.lock().await.clone();
    let refused = refused.load(Ordering::Relaxed);

    assert!(
        placed == 0 && moved > 0 && refused == 0,
        "the shard should be handed over, not failed over: {placed} placements, \
         {moved} move steps, {refused} publishes refused (of {}); moved {moved_at:?} \
         after SIGTERM, exited after {exited_after:?}",
        during.len() + refused
    );
    assert!(status.success(), "the broker should exit cleanly: {status}");
    assert!(
        moved_at < exited_after,
        "the shard should move before the broker exits ({moved_at:?} vs {exited_after:?})"
    );
    assert!(
        during.len() > 20,
        "too little was acknowledged to prove anything"
    );
    owed.extend(during);

    let mut offsets = Vec::new();
    let mut payloads = Vec::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout_at(deadline, subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                offsets.push(event.offset.expect("a durable stream carries offsets"));
                payloads.push(event.payload.to_vec());
            }
            Ok(Ok(None)) => panic!(
                "the subscription ended after {} records instead of following the shard",
                payloads.len()
            ),
            Ok(Err(err)) => panic!("the subscription failed: {err:#}"),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged records",
                owed.iter().filter(|p| payloads.contains(p)).count(),
                owed.len()
            ),
        }
    }
    let expected: Vec<u64> = (0..offsets.len() as u64).collect();
    assert_eq!(offsets, expected, "every offset once and in order");
    let mut unique = payloads.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), payloads.len(), "a record arrived twice");
    cluster.shutdown().await;
}

/// **A restarted broker takes shards again.** The drain its shutdown asked
/// for does not outlive the process: once it registers again it is placeable
/// and rebalancing gives it back a share.
#[serial]
#[tokio::test]
async fn a_restarted_broker_takes_shards_again() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 3, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leads = |owners: &std::collections::HashMap<u32, String>, node: &str| {
        owners
            .values()
            .filter(|owner| owner.as_str() == node)
            .count()
    };
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let stopping = owners.values().next().expect("a leader").clone();

    cluster.terminate_node(&stopping).expect("SIGTERM");
    let signalled = Instant::now();
    loop {
        cluster.place_shards().await;
        if cluster.exit_status(&stopping).expect("node").is_some() {
            break;
        }
        assert!(signalled.elapsed() < EXIT_WITHIN, "{stopping} did not exit");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    assert_eq!(leads(&owners, &stopping), 0, "it handed everything over");

    cluster.restart_node(&stopping).await.expect("restart");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the restarted broker to lead a shard again",
        || async {
            cluster.place_shards().await;
            cluster
                .shard_owners_for(STREAM)
                .await
                .is_ok_and(|owners| leads(&owners, &stopping) > 0)
        },
    )
    .await
    .expect("rebalanced onto the restarted broker");
    cluster.shutdown().await;
}

/// **A lone broker does not wait for a handoff that cannot happen.** With
/// nobody to take its shard it stops as promptly as before.
#[serial]
#[tokio::test]
async fn a_single_broker_stops_without_waiting_for_a_handoff() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster.terminate_node(&owner).expect("SIGTERM");
    let status = cluster
        .wait_for_exit(&owner, Duration::from_secs(10))
        .await
        .expect("a lone broker should not wait out the handoff timeout");
    assert!(status.success(), "clean exit: {status}");
    cluster.shutdown().await;
}
