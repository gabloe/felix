//! Waiting for a majority, and the ways a wait ends other than success.
use std::time::Duration;

use super::*;

const QUICK: Duration = Duration::from_millis(200);

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    }
}

/// A mark already past the batch returns at once rather than waiting for the
/// next change — a publish that arrives after its records replicated must not
/// wait for an unrelated one to move the mark.
#[tokio::test]
async fn a_mark_already_past_the_batch_returns_immediately() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
    );
}

/// The wait ends when the mark reaches the batch, not before.
#[tokio::test]
async fn a_wait_ends_when_the_majority_reaches_the_batch() {
    let marks = std::sync::Arc::new(QuorumMarks::new());
    marks.publish(&key("orders"), 4, 0);

    let waiting = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move { marks.wait_for(&key("orders"), 4, 5, QUICK).await })
    };

    // Short of the batch: not enough.
    marks.publish(&key("orders"), 4, 4);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "the wait ended one record short");

    marks.publish(&key("orders"), 4, 5);
    assert_eq!(waiting.await.expect("join"), QuorumWait::Reached);
}

/// **A majority that never arrives times out rather than hanging.** A client
/// waiting forever is worse than one told the broker cannot vouch for the write.
#[tokio::test]
async fn a_majority_that_never_arrives_times_out() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 0);

    assert_eq!(
        marks
            .wait_for(&key("orders"), 4, 5, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
    );
}

/// **An untracked shard is not a quorum.** If this broker is not tracking the
/// shard it is not leading it, and it cannot promise a majority for a write.
#[tokio::test]
async fn a_shard_this_broker_does_not_lead_is_not_a_quorum() {
    let marks = QuorumMarks::new();

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 1, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// **An older generation's mark does not satisfy a newer generation's wait.**
/// The replica set may be different, so what a majority held under the previous
/// leadership says nothing about this one.
#[tokio::test]
async fn a_wait_at_a_newer_generation_ignores_the_old_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);

    assert_eq!(
        marks.wait_for(&key("orders"), 5, 10, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// A generation change restarts the mark rather than carrying it forward.
#[tokio::test]
async fn a_new_generation_restarts_the_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);
    marks.publish(&key("orders"), 5, 0);

    assert_eq!(
        marks
            .wait_for(&key("orders"), 5, 10, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
        "the old generation's mark satisfied the new generation",
    );
}

/// **Losing the shard ends the wait rather than running it out.** A publish
/// held for its full timeout after leadership moved is a client kept waiting
/// for an answer that can no longer come.
#[tokio::test]
async fn losing_the_shard_ends_a_wait_in_progress() {
    let marks = std::sync::Arc::new(QuorumMarks::new());
    marks.publish(&key("orders"), 4, 0);

    let waiting = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move {
            marks
                .wait_for(&key("orders"), 4, 5, Duration::from_secs(30))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    marks.forget(&key("orders"));

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the wait should end when the shard is released")
            .expect("join"),
        QuorumWait::NotLeading,
    );
}

/// The mark only goes forward within a generation. A pass that saw less than
/// the last one saw a follower mid-answer, not a record becoming un-stored.
#[tokio::test]
async fn the_mark_does_not_go_backwards_within_a_generation() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);
    marks.publish(&key("orders"), 4, 3);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
        "the mark went backwards and un-acknowledged a batch",
    );
}

/// Shards are tracked independently: one shard's majority says nothing about
/// another's.
#[tokio::test]
async fn shards_do_not_share_a_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);
    marks.publish(&key("payments"), 4, 0);

    assert_eq!(
        marks
            .wait_for(&key("payments"), 4, 10, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
    );
}

/// Shards this broker no longer leads are forgotten in one pass.
#[tokio::test]
async fn retaining_the_live_shards_forgets_the_rest() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);
    marks.publish(&key("payments"), 4, 10);

    marks.retain(&[key("orders")]);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
    );
    assert_eq!(
        marks.wait_for(&key("payments"), 4, 10, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// A `Quorum` stream placed with one replica: the leader alone is the
/// majority. Nothing ships for it, so no mark is ever published, and the wait
/// must not read that silence as a lost leadership.
#[tokio::test]
async fn an_unreplicated_quorum_shard_is_acknowledged_by_its_leader() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use felix_router::{NodeRef, RegionRouter, ShardRouter};

    use crate::shards::routing::{IngressRouter, routing_table_from};
    use crate::shards::watch::ShardAssignment;

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let assignments: HashMap<ShardKey, ShardAssignment> = [(
        key("orders"),
        ShardAssignment {
            key: key("orders"),
            leader: "broker-a".to_string(),
            replicas: Vec::new(),
            generation: 4,
            state: "active".to_string(),
            successor: None,
        },
    )]
    .into_iter()
    .collect();
    let nodes: HashMap<String, NodeRef> = [(
        "broker-a".to_string(),
        NodeRef {
            node_id: "broker-a".to_string(),
            advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 7001)),
            region: "us-west-2".to_string(),
            live: true,
        },
    )]
    .into_iter()
    .collect();
    let ingress = IngressRouter::new(Arc::clone(&router), Arc::default());
    ingress.publish(
        routing_table_from(&assignments, &nodes),
        &nodes,
        [(key("orders"), 4)].into_iter().collect(),
    );

    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_with_outcome(&handle, &[bytes::Bytes::from_static(b"one")])
        .await
        .expect("publish");
    let marks = QuorumMarks::new();

    await_quorum(
        &handle,
        Some(&key("orders")),
        &outcome,
        Some(&marks),
        Some(&ingress),
        QUICK,
    )
    .await
    .expect("the leader is the whole replica set, and it has the record");
}
