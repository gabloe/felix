//! What a broker serves for a shard whose records arrived by replication.
//!
//! A follower's records do not come through `publish`. They are written to the
//! shard's log by the replication path, which never touches the in-memory
//! replay ring. When that broker is promoted, everything it holds is on disk
//! and nothing is in the ring — and a subscriber has to be served from disk
//! regardless.
use std::sync::Arc;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StartPosition, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

/// A broker holding a durable stream whose records were written the way
/// replication writes them: straight to the shard log, never through publish.
async fn promoted_broker(records: &[&str]) -> (Arc<Broker>, TempDir) {
    let dir = tempdir().expect("dir");
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

    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
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
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    // The replication path: the shard's log, directly — then telling the stream
    // its tail moved, exactly as the replica handler does.
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open the shard log");
    for record in records {
        log.append(&[payload(record)]).await.expect("append");
    }
    let tail = log.tail_offset().await.expect("tail");
    broker
        .adopt_replicated(TENANT, NAMESPACE, STREAM, tail)
        .await
        .expect("adopt the replicated records");

    (Arc::new(broker), dir)
}

async fn drain(subscription: &mut felix_broker::Subscription, expected: usize) -> Vec<String> {
    let mut seen = Vec::new();
    while seen.len() < expected {
        match tokio::time::timeout(std::time::Duration::from_millis(500), subscription.recv()).await
        {
            Ok(Some(payload)) => seen.push(String::from_utf8(payload.to_vec()).expect("utf8")),
            _ => break,
        }
    }
    seen
}

/// **A promoted broker serves the records it replicated.**
///
/// They are on its disk and nowhere else in its memory. A subscriber asking for
/// the whole stream must get them; returning nothing makes a completed failover
/// look like an empty shard.
#[tokio::test]
async fn a_replicated_stream_replays_from_the_start() {
    let (broker, _dir) = promoted_broker(&["a", "b", "c"]).await;

    let resumed = broker
        .subscribe_from(TENANT, NAMESPACE, STREAM, StartPosition::Earliest)
        .await
        .expect("subscribe from the start");

    let mut from_disk = Vec::new();
    if let Some(range) = resumed.history {
        let records = broker
            .read_durable(TENANT, NAMESPACE, STREAM, range.from_offset, 1024 * 1024)
            .await
            .expect("read history");
        for record in records {
            if record.offset >= range.until_offset {
                break;
            }
            from_disk.push(String::from_utf8(record.payload.to_vec()).expect("utf8"));
        }
    }
    let backlog: Vec<String> = resumed
        .backlog
        .iter()
        .map(|(_, p)| String::from_utf8(p.to_vec()).expect("utf8"))
        .collect();

    let mut everything = from_disk;
    everything.extend(backlog);
    assert_eq!(
        everything,
        vec!["a", "b", "c"],
        "a promoted broker served nothing for records it holds on disk",
    );
}

/// The live edge still works: a record published after promotion is delivered
/// on top of the replicated history rather than instead of it.
#[tokio::test]
async fn a_publish_after_promotion_follows_the_replicated_history() {
    let (broker, _dir) = promoted_broker(&["a", "b"]).await;

    let resumed = broker
        .subscribe_from(TENANT, NAMESPACE, STREAM, StartPosition::Earliest)
        .await
        .expect("subscribe from the start");
    let mut subscription = resumed.subscription;

    broker
        .publish(TENANT, NAMESPACE, STREAM, payload("c"))
        .await
        .expect("publish after promotion");

    assert_eq!(drain(&mut subscription, 1).await, vec!["c"]);
}

/// `Latest` on a replicated stream starts at the disk tail, so a subscriber
/// joining after promotion does not replay what it did not ask for.
#[tokio::test]
async fn latest_starts_at_the_replicated_tail() {
    let (broker, _dir) = promoted_broker(&["a", "b"]).await;

    let resumed = broker
        .subscribe_from(TENANT, NAMESPACE, STREAM, StartPosition::Latest)
        .await
        .expect("subscribe at the tail");

    assert!(resumed.history.is_none());
    assert!(resumed.backlog.is_empty());
}

/// **Raising a stream to `Quorum` takes effect on the next publish.**
///
/// Registering a stream that already exists takes a fast path that only
/// refreshes the metadata map. The live state kept whatever it was built with,
/// so a stream raised to `Quorum` carried on acknowledging on the leader alone
/// while the catalog said a majority was required — until the broker restarted.
/// A `Quorum` that silently behaves as `Leader` is how a failover loses a
/// record the client was told was safe.
#[tokio::test]
async fn a_consistency_change_reaches_a_live_stream() {
    let (broker, _dir) = promoted_broker(&[]).await;
    let handle = broker
        .resolve_stream_handle(TENANT, NAMESPACE, STREAM)
        .await
        .expect("resolve");
    assert_eq!(handle.consistency(), felix_broker::ConsistencyLevel::Leader);

    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: 1,
                consistency: felix_broker::ConsistencyLevel::Quorum,
            },
        )
        .await
        .expect("raise to quorum");

    let handle = broker
        .resolve_stream_handle(TENANT, NAMESPACE, STREAM)
        .await
        .expect("resolve");
    assert_eq!(
        handle.consistency(),
        felix_broker::ConsistencyLevel::Quorum,
        "the live stream kept acknowledging on the leader alone",
    );
}

/// And lowering it takes effect too, so the setting is not one-way.
#[tokio::test]
async fn lowering_the_consistency_also_reaches_a_live_stream() {
    let (broker, _dir) = promoted_broker(&[]).await;
    for level in [
        felix_broker::ConsistencyLevel::Quorum,
        felix_broker::ConsistencyLevel::Leader,
    ] {
        broker
            .register_stream(
                TENANT,
                NAMESPACE,
                STREAM,
                StreamMetadata {
                    durable: true,
                    shards: 1,
                    consistency: level,
                },
            )
            .await
            .expect("register");
        let handle = broker
            .resolve_stream_handle(TENANT, NAMESPACE, STREAM)
            .await
            .expect("resolve");
        assert_eq!(handle.consistency(), level);
    }
}
