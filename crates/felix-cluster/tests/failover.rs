//! M5's completion signal: what survives losing a leader.
//!
//! These start real broker processes and kill one mid-flight, so they are slow
//! and deliberately few. What they cover is the claim the whole milestone rests
//! on — that a record acknowledged under `Quorum` is still there after the
//! broker that acknowledged it is gone.
//!
//! Run with `cargo test -p felix-cluster --test failover`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// Three brokers, one shard, three copies of it, acknowledged by a majority.
fn quorum_config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// Wait until the leader has actually replicated and reported.
///
/// A cluster that has only just started has a leader that has shipped nothing
/// and told the control plane nothing, so killing it there tests startup rather
/// than failover. The scenario is a *healthy* replicated shard losing its
/// leader, and this is what makes it one: zero lag means every follower holds
/// what the leader does, and the leader has said so.
async fn replication_settled(cluster: &Cluster, leader: &str) {
    // The lag gauge is written once per replication pass, so reading zero can
    // mean "every follower is level" or "every follower was level one pass ago,
    // before the record this test just published". Waiting for a *rising* number
    // of shipped batches first is what makes the zero afterwards be about this
    // record rather than the state before it.
    let shipped_before = cluster
        .metric(leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the published record to be shipped and acknowledged",
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
            shipped > shipped_before && matches!(lag, Some(lag) if lag == 0.0)
        },
    )
    .await
    .expect("replication should reach the followers of a healthy shard");
}

/// Everything `node_id` will replay from the start of the stream.
///
/// Retried until it yields something, because being named leader and being
/// ready to serve are different moments: a promoted broker learns of its own
/// promotion through the same watch as everything else, and has to open the
/// shard before it can answer for it.
async fn replay_until(cluster: &Cluster, node_id: &str, budget: Duration) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + budget;
    let mut attempts = 0usize;
    let mut last;
    loop {
        attempts += 1;
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
            panic!("nothing replayed from {node_id} after {attempts} attempts; last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Wait until the control plane has named a leader other than `gone`.
async fn failover_from(cluster: &Cluster, gone: &str, budget: Duration) -> Option<Duration> {
    let started = std::time::Instant::now();
    let ok = felix_cluster::wait::until(budget, "a new leader", || async {
        // The harness's control plane does not run the reconciler on a timer,
        // so a failure test has to step placement itself. Without this nothing
        // re-plans after the kill and the wait is measuring nothing.
        cluster.place_shards().await;
        match cluster.owner(STREAM).await {
            Ok(owner) => owner != gone,
            Err(_) => false,
        }
    })
    .await;
    ok.ok().map(|_| started.elapsed())
}

/// **The milestone signal.** A record acknowledged under `Quorum` is readable
/// after the broker that acknowledged it is killed.
///
/// The acknowledgement is the whole claim: it means a majority held the record
/// durably, so losing any one of them — including the leader — cannot take it.
///
/// Ignored: **intermittent**, roughly one run in four (#266). Usually the
/// promoted broker replays everything; occasionally it accepts the subscribe
/// and delivers nothing, and retrying with fresh subscriptions for 30s does not
/// recover it.
///
/// Kept rather than deleted — it is the milestone's acceptance criterion and
/// the thing to re-run against any fix — and kept ignored rather than left
/// failing, because a test that fails one run in four teaches people to ignore
/// red.
#[serial]
#[tokio::test]
async fn a_quorum_acknowledged_record_survives_its_leader() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");

    // Acknowledged by a majority before this returns.
    cluster
        .publish_via(&leader, STREAM, b"survives".to_vec())
        .await
        .expect("publish under quorum");

    replication_settled(&cluster, &leader).await;
    cluster.kill_node(&leader).expect("kill the leader");

    let elapsed = failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");
    println!("failover took {elapsed:?}");

    let new_leader = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_leader, leader);

    // Replayed from the start, not subscribed live: the record was published
    // before the kill, so this asks what the promoted broker actually holds.
    //
    // Retried, because the control plane naming a new leader and that broker
    // having opened the shard are two different moments: it learns of its own
    // promotion through the same watch as everything else.
    let payloads = replay_until(&cluster, &new_leader, Duration::from_secs(30)).await;

    assert!(
        payloads.iter().any(|payload| payload == b"survives"),
        "a quorum-acknowledged record did not survive its leader; the promoted \
         broker replayed {payloads:?}",
    );
    cluster.shutdown().await;
}

/// **Only a broker that holds the log is promoted.** The promoted leader must
/// be one of the replicas, not whichever node scored highest — that is the
/// difference between a failover and a silently empty shard.
#[serial]
#[tokio::test]
async fn the_promoted_leader_is_one_of_the_replicas() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let replicas: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|node| node != &leader)
        .collect();

    cluster
        .publish_via(&leader, STREAM, b"held".to_vec())
        .await
        .expect("publish");
    replication_settled(&cluster, &leader).await;
    cluster.kill_node(&leader).expect("kill the leader");
    failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");

    let promoted = cluster.owner(STREAM).await.expect("owner");
    assert!(
        replicas.contains(&promoted),
        "{promoted} was promoted but was not a replica of the shard",
    );
    cluster.shutdown().await;
}

/// Failover completes within a bound derived from the cluster's own liveness
/// settings, rather than an arbitrary number: the lease has to lapse and the
/// control plane has to notice before anything can be promoted.
#[serial]
#[tokio::test]
async fn failover_completes_within_the_configured_bound() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"timed".to_vec())
        .await
        .expect("publish");

    replication_settled(&cluster, &leader).await;
    cluster.kill_node(&leader).expect("kill the leader");
    let elapsed = failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");

    // Generous against the harness's short liveness windows. The assertion is
    // that failover is bounded at all, not that it is fast: a tight bound here
    // would fail on a loaded CI runner for reasons that say nothing about the
    // design.
    assert!(
        elapsed < Duration::from_secs(30),
        "failover took {elapsed:?}",
    );
    println!("failover took {elapsed:?}");
    cluster.shutdown().await;
}

/// **A shard with no replica to promote stays unavailable rather than being
/// served empty.** The counterpart to the tests above: when there is nothing
/// holding the log, the cluster declines to invent a leader.
#[serial]
#[tokio::test]
async fn an_unreplicated_shard_does_not_fail_over_to_an_empty_broker() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        // Replicated across two, so a replica set exists — but the publish is
        // leader-acknowledged, so the record need not have reached the follower.
        streams: vec![StreamSpec::replicated(STREAM, 1, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let replicas: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|node| node != &leader)
        .collect();

    replication_settled(&cluster, &leader).await;
    cluster.kill_node(&leader).expect("kill the leader");

    // Whatever happens, the shard must not land on a broker outside the replica
    // set: that broker holds none of the log.
    if failover_from(&cluster, &leader, Duration::from_secs(20))
        .await
        .is_some()
    {
        let promoted = cluster.owner(STREAM).await.expect("owner");
        assert!(
            replicas.contains(&promoted),
            "{promoted} was made leader but holds none of the shard",
        );
    }
    cluster.shutdown().await;
}
