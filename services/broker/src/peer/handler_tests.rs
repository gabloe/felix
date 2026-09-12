//! What a forwarded publish owes the client on the other end of it.
//!
//! A forwarded publish is still a publish to the stream, so it owes the
//! guarantee the *stream* asks for — not whichever one this path happens to
//! provide. Acknowledging on local durability alone made `Quorum` depend on
//! which broker a client reached: honoured when it talked to the leader,
//! silently downgraded through any other, which is where a failover then lost
//! the record.
use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{ConsistencyLevel, DurableStorage, StreamMetadata};
use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{AckMode, ShardRef};
use tempfile::TempDir;

use super::*;
use crate::replication::quorum::QuorumMarks;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LOCAL: &str = "broker-a";
const GENERATION: u64 = 4;

fn key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

/// A router in which this broker leads the shard with one follower.
fn router() -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [LOCAL, "broker-b", "broker-c"]
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            (
                id.to_string(),
                NodeRef {
                    node_id: id.to_string(),
                    advertise_addr: format!("10.0.0.1:{}", 7001 + i as u16)
                        .parse()
                        .expect("addr"),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    let table = RoutingTable::build(
        [(
            key(),
            LOCAL.to_string(),
            vec!["broker-b".to_string(), "broker-c".to_string()],
            GENERATION,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

async fn broker_with(consistency: ConsistencyLevel) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
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
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: 1,
                consistency,
            },
        )
        .await
        .expect("stream");
    (Arc::new(broker), dir)
}

fn handler(
    broker: Arc<Broker>,
    marks: Option<Arc<QuorumMarks>>,
    timeout: Duration,
) -> ForwardingHandler {
    let router = router();
    let ingress = Arc::new(crate::shard_routing::IngressRouter::new(Arc::clone(
        &router,
    )));
    ingress.publish_servable(
        [(
            crate::shard_watch::ShardKey {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
                shard: 0,
                kind: crate::shard_watch::ShardKind::Stream,
            },
            GENERATION,
        )]
        .into_iter()
        .collect(),
    );
    ForwardingHandler::new(
        broker,
        ingress,
        router,
        "10.0.0.1:7001".to_string(),
        marks,
        timeout,
    )
}

fn forwarded() -> ForwardPublish {
    ForwardPublish {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation: GENERATION,
        },
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"forwarded")],
    }
}

/// **A forwarded publish to a `Quorum` stream is not acknowledged until a
/// majority holds it.** Answering on local durability made the guarantee depend
/// on which broker the client happened to reach.
#[tokio::test]
async fn a_forwarded_quorum_publish_waits_for_the_majority() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    // A mark that never reaches the record: no follower ever stores it.
    marks.publish(
        &crate::shard_watch::ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            kind: crate::shard_watch::ShardKind::Stream,
        },
        GENERATION,
        0,
    );
    let handler = handler(broker, Some(marks), Duration::from_millis(300));

    let answer = handler.apply(forwarded()).await;

    match answer {
        InternalMessage::ForwardPublishError(err) => {
            assert_eq!(err.code, ErrorCode::StorageFailed);
        }
        other => panic!(
            "a forwarded quorum publish was acknowledged without a majority: {:?}",
            other.kind()
        ),
    }
}

/// And it *is* acknowledged once the majority holds it.
#[tokio::test]
async fn a_forwarded_quorum_publish_is_acknowledged_once_the_majority_holds_it() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(
        &crate::shard_watch::ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            kind: crate::shard_watch::ShardKind::Stream,
        },
        GENERATION,
        // Past the record this publish writes.
        1_000,
    );
    let handler = handler(broker, Some(marks), Duration::from_secs(5));

    let answer = handler.apply(forwarded()).await;

    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "expected an acknowledgement, got {:?}",
        answer.kind(),
    );
}

/// A `Leader` stream is unaffected: local durability is the guarantee it offers,
/// so a forwarded publish is acknowledged without waiting for anyone.
#[tokio::test]
async fn a_forwarded_leader_publish_does_not_wait() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let marks = Arc::new(QuorumMarks::new());
    let handler = handler(broker, Some(marks), Duration::from_millis(300));

    let answer = handler.apply(forwarded()).await;

    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "a leader-acknowledged stream waited for a quorum: {:?}",
        answer.kind(),
    );
}
