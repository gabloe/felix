//! Broker-level behaviour of `durable: true`.
//!
//! The properties under test are the ones the guarantee actually rests on:
//! a durable publish is on disk before it is delivered, a storage failure is
//! never acknowledged as success, a restart brings the records back, and a
//! non-durable stream is untouched by any of it.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{Broker, BrokerError, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

fn log_config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        // Small segments so rollover is covered without writing megabytes.
        segment_size_bytes: 4 * 1024,
        index_spacing_bytes: 256,
        fsync_mode,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker_with_storage(
    dir: &TempDir,
    fsync_mode: FsyncMode,
) -> (Arc<Broker>, DurableStorage) {
    let storage = DurableStorage::open(dir.path(), log_config(fsync_mode)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    (Arc::new(broker), storage)
}

async fn register(broker: &Broker, stream: &str, durable: bool) {
    broker
        .register_stream(
            "t1",
            "default",
            stream,
            StreamMetadata { durable, shards: 1 },
        )
        .await
        .expect("register");
}

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

#[tokio::test]
async fn a_durable_publish_is_on_disk_before_it_is_acknowledged() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    broker
        .publish("t1", "default", "orders", payload("order-1"))
        .await
        .expect("publish");

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream log");
    // `OnCommit` means the device flush happened inside `publish`.
    assert_eq!(log.unsynced_bytes(), 0);
    assert_eq!(log.durable_offset(), 1);

    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload("order-1"));
}

#[tokio::test]
async fn durable_records_survive_a_broker_restart() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;
        for i in 0..50 {
            broker
                .publish("t1", "default", "orders", payload(&format!("order-{i:03}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    // A completely new broker over the same directory: no manual recreation of
    // log files, just the same stream registration the control plane replays.
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream log");
    assert_eq!(log.tail_offset().await.expect("tail"), 50);
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 50);
    assert_eq!(records[0].payload, payload("order-000"));
    assert_eq!(records[49].payload, payload("order-049"));

    // And the recovered stream keeps accepting publishes at the right offset.
    broker
        .publish("t1", "default", "orders", payload("after-restart"))
        .await
        .expect("publish");
    let records = log.read_from(50, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload("after-restart"));
}

#[tokio::test]
async fn a_cursor_taken_after_restart_reflects_the_recovered_tail() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;
        for i in 0..5 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    // The in-memory cursor resumes from the durable tail rather than restarting
    // at zero, so a cursor a client saved before the restart still means the
    // same position.
    let cursor = broker
        .cursor_tail("t1", "default", "orders")
        .await
        .expect("cursor");
    assert_eq!(cursor.next_seq(), 5);
}

#[tokio::test]
async fn durable_publishes_are_delivered_to_subscribers_too() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let mut sub = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", payload("delivered"))
        .await
        .expect("publish");

    let received = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(received, payload("delivered"));
}

#[tokio::test]
async fn a_non_durable_stream_writes_nothing_to_disk() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "ephemeral", false).await;

    let mut sub = broker
        .subscribe("t1", "default", "ephemeral")
        .await
        .expect("subscribe");
    for i in 0..10 {
        broker
            .publish("t1", "default", "ephemeral", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    // Delivery still works...
    let received = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(received, payload("v0"));

    // ...and nothing reached durable storage: no shard directory was created,
    // so the low-latency in-memory path was genuinely untouched.
    assert!(storage.root().exists());
    let shard_dirs = std::fs::read_dir(storage.root())
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .count();
    assert_eq!(shard_dirs, 0, "a non-durable stream created storage");
}

#[tokio::test]
async fn durable_and_non_durable_streams_coexist() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;
    register(&broker, "ephemeral", false).await;

    // Cursors taken before publishing, so the backlog replay below has
    // something to return for each stream.
    let durable_cursor = broker
        .cursor_tail("t1", "default", "orders")
        .await
        .expect("cursor");
    let ephemeral_cursor = broker
        .cursor_tail("t1", "default", "ephemeral")
        .await
        .expect("cursor");

    broker
        .publish("t1", "default", "orders", payload("kept"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "ephemeral", payload("lost"))
        .await
        .expect("publish");

    // Only one of them reached disk...
    let durable = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    assert_eq!(durable.tail_offset().await.expect("tail"), 1);

    // ...but in-memory replay behaves identically for both, which is what keeps
    // the non-durable path unchanged.
    let (durable_backlog, _) = broker
        .subscribe_with_cursor("t1", "default", "orders", durable_cursor)
        .await
        .expect("subscribe");
    assert_eq!(durable_backlog, vec![payload("kept")]);

    let (ephemeral_backlog, _) = broker
        .subscribe_with_cursor("t1", "default", "ephemeral", ephemeral_cursor)
        .await
        .expect("subscribe");
    assert_eq!(ephemeral_backlog, vec![payload("lost")]);
}

#[tokio::test]
async fn registering_a_durable_stream_without_storage_is_rejected() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");

    let err = broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
            },
        )
        .await
        .expect_err("no storage configured");
    assert!(matches!(err, BrokerError::Storage(_)));

    // The stream must not exist: a half-registered durable stream would accept
    // publishes it cannot persist.
    assert!(!broker.stream_exists("t1", "default", "orders").await);

    // A non-durable stream on the same broker is unaffected.
    broker
        .register_stream("t1", "default", "ephemeral", StreamMetadata::default())
        .await
        .expect("register");
    broker
        .publish("t1", "default", "ephemeral", payload("fine"))
        .await
        .expect("publish");
}

#[tokio::test]
async fn periodic_mode_persists_without_blocking_each_publish() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(
        &dir,
        FsyncMode::Periodic {
            interval: Duration::from_millis(20),
        },
    )
    .await;
    register(&broker, "orders", true).await;

    for i in 0..20 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    // The records are readable straight away — they are in the page cache — and
    // the background syncer pushes them to the device within its window.
    assert_eq!(log.read_from(0, usize::MAX).await.expect("read").len(), 20);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while log.durable_offset() < 20 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(log.durable_offset(), 20);

    storage.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_durable_publishes_are_all_persisted_exactly_once() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:03}")))
                .await
                .expect("publish");
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    assert_eq!(log.tail_offset().await.expect("tail"), 64);
    let mut values: Vec<String> = log
        .read_from(0, usize::MAX)
        .await
        .expect("read")
        .into_iter()
        .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(values.len(), 64);
    values.sort();
    values.dedup();
    assert_eq!(values.len(), 64, "duplicate or lost records");
}

#[tokio::test]
async fn a_batch_publish_lands_as_one_contiguous_run() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let batch: Vec<Bytes> = (0..8).map(|i| payload(&format!("b{i}"))).collect();
    broker
        .publish_batch("t1", "default", "orders", &batch)
        .await
        .expect("publish");

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 8);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as u64);
        assert_eq!(record.payload, batch[index]);
    }
}
