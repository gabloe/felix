//! A planned move carries the state that rides a shard: its consumer groups'
//! positions and dead letters, and its cache's counters.
//!
//! Those logs are shipped beside the shard's own log, and the control plane
//! cuts over on the leader's drained report. So the report must wait for them
//! too: a dead letter left behind is a record the new leader's group skips,
//! and a counter add left behind is an acknowledged add gone from the sum.
//!
//! Run with `cargo test -p felix-cluster --test queues handoff::`.
use std::collections::HashSet;
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec, wait};
use serial_test::serial;

const STREAM: &str = "jobs";
const CACHE: &str = "tallies";
const GROUP: &str = "workers";
const COUNTER: &str = "hits";

/// **A move off a live leader loses no group state and no counter add.** Group
/// acks and counter adds keep arriving while placement stages, fences and cuts
/// the shards over, so the last of them land right before the fence. On the
/// new owner no acknowledged record comes back, the dead-letter list is the
/// one the old leader had, and the counter is the sum of every acknowledged
/// add.
#[tokio::test]
#[serial]
async fn a_moved_shard_keeps_its_group_state_and_counters() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        caches: vec![CacheSpec::new(CACHE, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();

    wait::until(
        Duration::from_secs(30),
        "the first publish to land",
        || async {
            cluster
                .publish_via(&first, STREAM, b"poison".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("publish");
    for i in 0..40 {
        cluster
            .publish_via(&first, STREAM, format!("job-{i}").into_bytes())
            .await
            .expect("publish");
    }

    // Give the first record up. Everything else claimed on the way is
    // acknowledged, so the group's position moves too.
    let mut acked: HashSet<u64> = HashSet::new();
    let mut rounds = 0;
    let poison = loop {
        rounds += 1;
        assert!(rounds <= 20, "the record was never dead-lettered");
        let claimed = cluster
            .group_poll_records_via(&first, STREAM, 0, GROUP, 5)
            .await
            .expect("poll");
        for record in claimed {
            if record.payload.as_ref() == b"poison" {
                cluster
                    .group_nack_via(&first, STREAM, 0, GROUP, record.offset)
                    .await
                    .expect("nack");
            } else {
                cluster
                    .group_ack_via(&first, STREAM, 0, GROUP, record.offset)
                    .await
                    .expect("ack");
                acked.insert(record.offset);
            }
        }
        let dead = cluster
            .group_dead_letters_via(&first, STREAM, 0, GROUP)
            .await
            .expect("dead letters");
        if let [offset] = dead[..] {
            break offset;
        }
    };
    let dead_before = cluster
        .group_dead_letters_via(&first, STREAM, 0, GROUP)
        .await
        .expect("dead letters");
    assert_eq!(dead_before, vec![poison]);

    let mut added: i64 = 0;
    // An add that failed may still have landed (a timeout after the write),
    // so the sum is checked against both bounds.
    let mut unknown: i64 = 0;
    for _ in 0..10 {
        cluster
            .counter_add_via(&first, CACHE, COUNTER, 1)
            .await
            .expect("counter add");
        added += 1;
    }

    let joined = cluster.add_node().await.expect("add a broker");
    cluster.drain_node(&first).await.expect("drain");

    // Keep writing between placement steps, through the old leader, until it
    // leads nothing. Writes refused once the fence closes are fine; the ones
    // acknowledged before it are the ones the move has to carry.
    let deadline = tokio::time::Instant::now() + wait::budget(Duration::from_secs(120));
    loop {
        cluster.place_shards_moving(2).await;
        match cluster.counter_add_via(&first, CACHE, COUNTER, 1).await {
            Ok(_) => added += 1,
            Err(_) => unknown += 1,
        }
        if let Ok(claimed) = cluster
            .group_poll_records_via(&first, STREAM, 0, GROUP, 1)
            .await
        {
            for record in claimed {
                if cluster
                    .group_ack_via(&first, STREAM, 0, GROUP, record.offset)
                    .await
                    .is_ok()
                {
                    acked.insert(record.offset);
                }
            }
        }
        let owners = cluster.shard_owners().await.expect("owners");
        if owners.values().all(|leader| *leader == joined) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the shards never moved off {first}: {owners:?}"
        );
    }

    // Named owner and serving are different moments.
    let listed = wait::until(
        Duration::from_secs(30),
        "the new owner to serve the group",
        || async {
            cluster
                .group_dead_letters_via(&joined, STREAM, 0, GROUP)
                .await
                .is_ok()
        },
    )
    .await;
    assert!(listed.is_ok(), "{joined} never served the group");
    assert_eq!(
        cluster
            .group_dead_letters_via(&joined, STREAM, 0, GROUP)
            .await
            .expect("dead letters on the new owner"),
        dead_before,
        "the dead-letter list changed across the move",
    );

    let mut delivered = Vec::new();
    loop {
        let claimed = cluster
            .group_poll_records_via(&joined, STREAM, 0, GROUP, 100)
            .await
            .expect("poll the new owner");
        if claimed.is_empty() {
            break;
        }
        for record in claimed {
            cluster
                .group_ack_via(&joined, STREAM, 0, GROUP, record.offset)
                .await
                .expect("ack on the new owner");
            delivered.push(record.offset);
        }
    }
    let redelivered: Vec<u64> = delivered
        .iter()
        .copied()
        .filter(|offset| acked.contains(offset) || *offset == poison)
        .collect();
    assert!(
        redelivered.is_empty(),
        "the new owner handed out records the group had finished: {redelivered:?}",
    );

    let sum = cluster
        .counter_get_via(&joined, CACHE, COUNTER)
        .await
        .expect("counter on the new owner")
        .unwrap_or(0);
    assert!(
        (added..=added + unknown).contains(&sum),
        "the counter reads {sum}, but {added} adds were acknowledged ({unknown} unanswered)",
    );
    cluster.shutdown().await;
}
