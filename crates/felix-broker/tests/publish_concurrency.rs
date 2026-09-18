//! Does the broker let concurrent publishes reach the device flush together?
//!
//! `felix-storage`'s group commit coalesces properly when driven concurrently --
//! `felix-storage/tests/group_commit_fanin.rs` measures a 7.25x speedup at
//! 16-way. But an Azure session measured the fan-in actually achieved in a
//! running cluster (`felix_storage_sync_batch_appends`) at **1.004-1.007** with
//! 48 publishers, which means callers were arriving at `ensure_durable` one at
//! a time rather than together.
//!
//! Somewhere between the wire and the log, publishes are being serialised. This
//! bisects that: it drives `Broker::publish` directly, with no QUIC transport
//! and none of the service's ingress workers in the path.
//!
//! - A speedup here means `felix-broker` is *not* the serialiser, and the cause
//!   is above it in `services/broker` (the per-connection publish workers, which
//!   map a stream handle to exactly one worker via `handle.id() % worker_count`).
//! - No speedup here means the serialisation is in `felix-broker` itself, and
//!   this test is where to debug it.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

const PUBLISHES: usize = 64;
const CONCURRENCY: usize = 16;
const PAYLOAD: usize = 4096;

fn log_config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 256 * 1024 * 1024,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker_with_storage(dir: &TempDir) -> Arc<Broker> {
    let storage = DurableStorage::open(dir.path(), log_config()).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    Arc::new(broker)
}

async fn register(broker: &Broker, stream: &str) {
    broker
        .register_stream(
            "t1",
            "default",
            stream,
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("register");
}

/// **Concurrent publishes to one durable stream should share device flushes.**
///
/// The ratio is the measurement, exactly as in the storage-level test. This one
/// says whether the broker preserves the concurrency the storage layer needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishes_to_one_stream_share_a_flush() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_storage(&dir).await;
    register(&broker, "serial").await;
    register(&broker, "concurrent").await;

    let body = || Bytes::from(vec![b'x'; PAYLOAD]);

    // Warm both streams so segment creation is not inside a measured window.
    broker
        .publish("t1", "default", "serial", body())
        .await
        .expect("warm serial");
    broker
        .publish("t1", "default", "concurrent", body())
        .await
        .expect("warm concurrent");

    let serial_start = Instant::now();
    for _ in 0..PUBLISHES {
        broker
            .publish("t1", "default", "serial", body())
            .await
            .expect("publish");
    }
    let serial = serial_start.elapsed();

    let per_task = PUBLISHES / CONCURRENCY;
    let concurrent_start = Instant::now();
    let mut tasks = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            for _ in 0..per_task {
                broker
                    .publish(
                        "t1",
                        "default",
                        "concurrent",
                        Bytes::from(vec![b'x'; PAYLOAD]),
                    )
                    .await
                    .expect("publish");
            }
        }));
    }
    for task in tasks {
        task.await.expect("task");
    }
    let concurrent = concurrent_start.elapsed();

    let speedup = serial.as_secs_f64() / concurrent.as_secs_f64().max(f64::EPSILON);
    eprintln!(
        "{PUBLISHES} durable publishes: serial {serial:?}, {CONCURRENCY}-way concurrent \
         {concurrent:?} -> speedup {speedup:.2}x"
    );
    eprintln!(
        "  per publish: serial {:?}, concurrent {:?}",
        serial / PUBLISHES as u32,
        concurrent / PUBLISHES as u32
    );

    assert!(
        speedup > 2.0,
        "concurrent publishes were not meaningfully cheaper than serial ones \
         (speedup {speedup:.2}x with {CONCURRENCY} in flight). The broker is serialising \
         publishes before they reach the device flush, so group commit has nothing to \
         coalesce -- which is the fan-in of 1 measured in the Azure sessions."
    );
}
