//! A broker is added or drained and its shards move without losing anything
//! acknowledged.
//!
//! These start real broker processes and move shards between them, so they
//! are slow and deliberately few. Each drives placement by hand -- the
//! harness's control plane does not run the reconciler on a timer -- and
//! reads back through the new owner what was acknowledged through the old.
//!
//! Run with `cargo test -p felix-cluster --test rebalance`.
use std::collections::BTreeSet;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// Everything `node_id` replays from the start of the stream, retried until
/// it yields something: a promoted broker learns of its own promotion through
/// the same watch as everything else and has to open the shard first.
async fn replay_from(cluster: &Cluster, node_id: &str, budget: Duration) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(budget);
    let mut last;
    loop {
        match cluster.replay_on(node_id, STREAM).await {
            Ok((_client, mut subscription)) => {
                let mut payloads = Vec::new();
                loop {
                    match tokio::time::timeout(Duration::from_secs(2), subscription.next_event())
                        .await
                    {
                        Ok(Ok(Some(event))) => payloads.push(event.payload.to_vec()),
                        Ok(Ok(None)) => {
                            last = "the broker ended the subscription".into();
                            break;
                        }
                        Ok(Err(err)) => {
                            last = format!("delivery error: {err}");
                            break;
                        }
                        Err(_) => {
                            last = "no event within 2s".into();
                            break;
                        }
                    }
                }
                if !payloads.is_empty() {
                    return payloads;
                }
            }
            Err(err) => last = format!("subscribe refused: {err}"),
        }
        if std::time::Instant::now() >= deadline {
            panic!("nothing replayed from {node_id}; last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Everything the shard's leader holds for it, replayed from the start.
///
/// The leader is asked by the control plane's assignment and retried: the
/// broker learns of its own assignment through its watch, so for a moment it
/// redirects rather than serves.
async fn replay_shard_from(cluster: &Cluster, shard: u32) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    let mut last;
    loop {
        let leader = cluster
            .shard_owner_of("stream", STREAM, shard)
            .await
            .expect("owner");
        match cluster.replay_shard(&leader, STREAM, shard).await {
            Ok((_client, mut subscription)) => {
                let mut payloads = Vec::new();
                while let Ok(Ok(Some(event))) =
                    tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
                {
                    payloads.push(event.payload.to_vec());
                }
                if !payloads.is_empty() {
                    return payloads;
                }
                last = format!("{leader} replayed nothing for shard {shard}");
            }
            Err(err) => last = format!("replay shard {shard} on {leader}: {err}"),
        }
        if std::time::Instant::now() >= deadline {
            panic!("shard {shard} never replayed; last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Publish `count` records through `via`, retrying a refusal until the shard
/// is served again: the new owner opens the shard a watch interval after it
/// is named.
async fn publish_until_served(
    cluster: &Cluster,
    via: &str,
    prefix: &str,
    count: usize,
) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    let mut sent = Vec::with_capacity(count);
    let mut i = 0usize;
    while sent.len() < count {
        let payload = format!("{prefix}-{i}").into_bytes();
        i += 1;
        match cluster.publish_via(via, STREAM, payload.clone()).await {
            Ok(()) => sent.push(payload),
            Err(err) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the shard was never served again after the move: {err}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    sent
}

/// Publish `count` records through `via`, acknowledged, and return them.
async fn publish_batch(cluster: &Cluster, via: &str, prefix: &str, count: usize) -> Vec<Vec<u8>> {
    let mut sent = Vec::with_capacity(count);
    for i in 0..count {
        let payload = format!("{prefix}-{i}").into_bytes();
        cluster
            .publish_via(via, STREAM, payload.clone())
            .await
            .expect("publish");
        sent.push(payload);
    }
    sent
}

fn missing(sent: &[Vec<u8>], got: &[Vec<u8>]) -> Vec<String> {
    let got: BTreeSet<&[u8]> = got.iter().map(Vec::as_slice).collect();
    sent.iter()
        .filter(|payload| !got.contains(payload.as_slice()))
        .map(|payload| String::from_utf8_lossy(payload).into_owned())
        .collect()
}

/// **Drain.** An unreplicated durable shard --
/// the case where nothing else holds the log -- is handed to another broker
/// when its owner drains, and every record acknowledged before the drain is
/// readable from the new owner.
#[serial]
#[tokio::test]
async fn a_drained_broker_hands_its_shard_over_with_every_record() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let sent = publish_batch(&cluster, &owner, "before", 20).await;

    cluster.drain_node(&owner).await.expect("drain");
    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(60))
        .await
        .expect("the shard should move off the draining broker");

    let new_owner = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_owner, owner);
    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records acknowledged before the drain are missing from {new_owner}: {lost:?}",
    );

    // The drained broker leads nothing and follows nothing: it can leave.
    let owners = cluster.shard_owners().await.expect("owners");
    assert!(owners.values().all(|leader| leader != &owner));
    cluster.shutdown().await;
}

/// **Nothing acknowledged during a move is lost.** Publishes keep arriving
/// while the shard is staged, fenced, and cut over. Some are refused in the
/// window between the fence and the new owner opening -- that is the
/// handoff, and the client sees an error rather than a silent drop -- but
/// every publish that was acknowledged is on the new owner.
#[serial]
#[tokio::test]
async fn records_acknowledged_during_a_move_survive_it() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");

    cluster.drain_node(&owner).await.expect("drain");

    // Publish by turns through the draining owner and through the other
    // broker. The owner is the path the fence exists for: a client connected
    // to it keeps sending until the owner has been told to stop, and every
    // publish it acknowledges after the successor takes over would be lost.
    // The other broker forwards to whichever leader its routing view names,
    // which is the path a client with a seed list takes.
    let mut acknowledged = Vec::new();
    let mut refused = 0usize;
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    let mut i = 0usize;
    loop {
        cluster.place_shards().await;
        let payload = format!("during-{i}").into_bytes();
        let via = if i.is_multiple_of(2) { &owner } else { &other };
        i += 1;
        match cluster.publish_via(via, STREAM, payload.clone()).await {
            Ok(()) => acknowledged.push(payload),
            Err(_) => refused += 1,
        }
        let moved = cluster
            .owner(STREAM)
            .await
            .is_ok_and(|leader| leader != owner);
        if moved {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the shard never left {owner}; {} acknowledged, {refused} refused",
            acknowledged.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The fence itself: the old owner was told to stop and stopped before
    // the new owner was named, so nothing it is sent now is written locally.
    // It is refused until the owner's watch catches up, then forwarded.
    // Checked at once, while the owner's watch is still likely to be behind
    // the cut-over -- the window in which a leader that had not been fenced
    // would still be accepting.
    for _ in 0..10 {
        let forwarded_before = cluster
            .metric(&owner, "felix_broker_forwards_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
        let accepted = cluster
            .publish_via(&owner, STREAM, b"after-the-fence".to_vec())
            .await
            .is_ok();
        let forwarded_after = cluster
            .metric(&owner, "felix_broker_forwards_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
        assert!(
            !accepted || forwarded_after > forwarded_before,
            "{owner} wrote a publish locally after {} was named leader: the fence did not hold",
            cluster.owner(STREAM).await.unwrap_or_default(),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A few more after the cut-over, to show the new owner is serving.
    let after = publish_until_served(&cluster, &other, "after", 5).await;
    acknowledged.extend(after);
    println!(
        "{} acknowledged, {refused} refused across the move",
        acknowledged.len()
    );

    let new_owner = cluster.owner(STREAM).await.expect("owner");
    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&acknowledged, &got);
    assert!(
        lost.is_empty(),
        "records acknowledged during the move are missing from {new_owner}: {lost:?}",
    );
    cluster.shutdown().await;
}

/// **Join.** A broker added to a running cluster
/// takes shards from the one carrying more than its share, without losing
/// what was already on them -- the 49/0 case from the perf session, on a
/// smaller scale.
#[serial]
#[tokio::test]
async fn a_broker_that_joins_takes_shards_from_an_overloaded_one() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 4)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();
    // Keyed publishes so every shard holds something.
    let mut sent = Vec::new();
    for i in 0..40 {
        let payload = format!("keyed-{i}").into_bytes();
        cluster
            .publish_keyed_via(&first, STREAM, format!("k{i}").as_bytes(), payload.clone())
            .await
            .expect("publish");
        sent.push(payload);
    }

    let joined = cluster.add_node().await.expect("add a broker");

    // Two shards each is balanced; placement stops there.
    felix_cluster::wait::until(
        Duration::from_secs(90),
        "the joiner to lead half the shards",
        || async {
            cluster.place_shards_moving(2).await;
            match cluster.shard_owners().await {
                Ok(owners) => {
                    owners.values().filter(|leader| **leader == joined).count() == 2
                        && owners.values().filter(|leader| **leader == first).count() == 2
                }
                Err(_) => false,
            }
        },
    )
    .await
    .expect("rebalance");

    // And it stays there: no further moves once balanced.
    for _ in 0..5 {
        let outcome = cluster.place_shards_moving(2).await;
        assert_eq!(
            outcome.moved, 0,
            "placement kept moving after balance: {outcome:?}"
        );
    }

    // Every record is readable through the cluster, whichever broker leads
    // its shard now.
    let mut got = Vec::new();
    for shard in 0..4 {
        got.extend(replay_shard_from(&cluster, shard).await);
    }
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost across the rebalance: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **A destination that dies mid-transfer never takes the shard.** The move
/// is staged onto it, it is killed before the cut-over, and the shard ends up
/// on a broker that holds the log -- with every record still there.
#[serial]
#[tokio::test]
async fn a_destination_that_dies_mid_transfer_does_not_take_the_shard() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let sent = publish_batch(&cluster, &owner, "held", 10).await;

    cluster.drain_node(&owner).await.expect("drain");

    // Step once: the successor is staged as a replica.
    let key = format!(
        "stream/{}/{}/{STREAM}/0",
        cluster.tenant_id, cluster.namespace
    );
    let successor = felix_cluster::wait::until_some(
        Duration::from_secs(30),
        "a successor to be staged",
        || async {
            cluster.place_shards().await;
            cluster
                .shard_successors()
                .await
                .ok()
                .and_then(|successors| successors.get(&key).cloned().flatten())
        },
    )
    .await
    .expect("staged");
    assert_ne!(successor, owner);

    // The destination dies before it leads.
    cluster
        .stop_node(&successor)
        .await
        .expect("stop the successor");

    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(90))
        .await
        .expect("the shard should still leave the draining broker");
    let new_owner = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_owner, owner);
    assert_ne!(new_owner, successor, "a dead destination must not lead");

    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost when the destination died: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **Concurrent placement changes converge.** One broker drains while another
/// joins, with several moves in flight at once; everything ends up on live
/// brokers, nothing on the drained one, and every record is still readable.
#[serial]
#[tokio::test]
async fn a_drain_and_a_join_at_the_same_time_converge() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 4)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();
    let mut sent = Vec::new();
    for i in 0..40 {
        let payload = format!("keyed-{i}").into_bytes();
        cluster
            .publish_keyed_via(&first, STREAM, format!("k{i}").as_bytes(), payload.clone())
            .await
            .expect("publish");
        sent.push(payload);
    }

    let draining = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &first)
        .expect("two brokers");
    cluster.drain_node(&draining).await.expect("drain");
    let joined = cluster.add_node().await.expect("add a broker");

    cluster
        .drain_until_empty(&draining, 3, Duration::from_secs(120))
        .await
        .expect("the draining broker should end up leading nothing");
    let owners = cluster.shard_owners().await.expect("owners");
    assert_eq!(owners.len(), 4);
    assert!(owners.values().all(|leader| leader != &draining));
    assert!(
        owners.values().any(|leader| leader == &joined),
        "the joiner took nothing: {owners:?}"
    );

    let mut got = Vec::new();
    for shard in 0..4 {
        got.extend(replay_shard_from(&cluster, shard).await);
    }
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost across the concurrent changes: {lost:?}"
    );
    cluster.shutdown().await;
}
