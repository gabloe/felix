//! The checks standing between a shipped batch and this broker's disk.
//!
//! These are about *authority* rather than position: whether the sender is
//! still the leader, and whether this broker is a replica at all. Position is
//! `felix_broker::replication`'s subject and is tested there.
use bytes::Bytes;
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{ShardRef, batch_checksum};
use std::collections::HashMap;
use tempfile::TempDir;

use super::*;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LOCAL: &str = "broker-b";

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
    }
}

/// A router in which `LOCAL` is a follower of the shard at `generation`, unless
/// `replicas` says otherwise.
fn router_with(replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        (LOCAL.to_string(), node(LOCAL, 7002)),
        ("broker-c".to_string(), node("broker-c", 7003)),
    ]
    .into_iter()
    .collect();
    let table = RoutingTable::build(
        [(
            key(),
            "broker-a".to_string(),
            replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

async fn broker_with_storage() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    (
        Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage)),
        dir,
    )
}

fn batch(generation: u64, first_offset: u64, values: &[&str]) -> ReplicateRecords {
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect();
    ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation,
        },
        first_offset,
        checksum: batch_checksum(&payloads),
        payloads,
    }
}

fn refusal(answer: &InternalMessage) -> &ReplicateError {
    match answer {
        InternalMessage::ReplicateError(err) => err,
        other => panic!("expected a refusal, got {:?}", other.kind()),
    }
}

/// A follower at the leader's epoch stores the batch and reports its durable
/// mark.
#[tokio::test]
async fn a_follower_at_the_leaders_epoch_stores_the_batch() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    let answer = handler.apply(batch(4, 0, &["a", "b"])).await;

    match answer {
        InternalMessage::ReplicateOk(ok) => {
            assert_eq!(ok.durable_offset, 2);
            assert_eq!(ok.correlation_id, 1);
        }
        other => panic!("expected an acknowledgement, got {:?}", other.kind()),
    }
}

/// **A superseded leader is refused.** Its records may have been written after
/// it lost the shard, and a follower that stored them would hold bytes no
/// current leader ever ordered. The fence does not lift, so this is not
/// retryable.
#[tokio::test]
async fn a_leader_at_an_older_epoch_is_fenced() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 5));

    let answer = handler.apply(batch(4, 0, &["a"])).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::FencedEpoch);
    assert!(
        !refused.code.is_retryable(),
        "a fenced leader was told to retry"
    );
}

/// A follower whose watch is behind refuses *for now* — retryable, because the
/// assignment is on its way and the leader is not at fault.
#[tokio::test]
async fn a_follower_behind_the_epoch_refuses_retryably() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 3));

    let answer = handler.apply(batch(4, 0, &["a"])).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleRoute);
    assert!(refused.code.is_retryable());
}

/// **A broker outside the replica set stores nothing.** Otherwise any peer
/// could place bytes on any broker's disk by naming a shard.
#[tokio::test]
async fn a_broker_outside_the_replica_set_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&["broker-c"], 4));

    let answer = handler.apply(batch(4, 0, &["a"])).await;

    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// A shard this broker has never heard of reads as "behind", not as a refusal:
/// the assignment may simply not have arrived, and refusing permanently would
/// strand a follower whose watch is a moment late.
#[tokio::test]
async fn an_unknown_shard_is_treated_as_a_late_watch() {
    let (broker, _dir) = broker_with_storage().await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let handler = ReplicaHandler::new(broker, router);

    let answer = handler.apply(batch(4, 0, &["a"])).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleRoute);
    assert!(refused.code.is_retryable());
}

/// A gap is reported with the offset the leader should resume from, so the
/// repair needs no separate negotiation.
#[tokio::test]
async fn a_gap_names_the_offset_to_resume_from() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));
    handler.apply(batch(4, 0, &["a", "b"])).await;

    let answer = handler.apply(batch(4, 7, &["h"])).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogGap);
    assert_eq!(refused.expected_offset, 2);
}

/// A conflict is reported as its own code, and is not retryable: two logs that
/// disagree do not converge by resending.
#[tokio::test]
async fn a_conflict_is_reported_as_divergence() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));
    handler.apply(batch(4, 0, &["a", "b"])).await;

    let answer = handler.apply(batch(4, 0, &["a", "DIFFERENT"])).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogConflict);
    assert!(!refused.code.is_retryable());
}

/// **A replica with nowhere to put records says so.** Accepting and keeping
/// nothing would let the leader count this broker toward a quorum that does not
/// exist.
#[tokio::test]
async fn a_replica_without_durable_storage_refuses() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    let answer = handler.apply(batch(4, 0, &["a"])).await;

    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// The answer always carries the request's correlation id, whatever the
/// outcome. A requester that could not match a refusal would wait out its
/// timeout instead of acting on it.
#[tokio::test]
async fn every_answer_carries_the_requests_correlation_id() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    for generation in [3, 4, 5] {
        let mut request = batch(generation, 0, &["a"]);
        request.correlation_id = 99;
        assert_eq!(handler.apply(request).await.correlation_id(), 99);
    }
}
