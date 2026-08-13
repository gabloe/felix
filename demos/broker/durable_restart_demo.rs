//! Restart-and-recover demo for durable streams.
//!
//! # Purpose
//! Shows the M1 guarantee end to end: a durable stream acknowledges a publish
//! only once the record is on disk, so an abrupt process death loses nothing
//! that was acknowledged — while a non-durable stream on the same broker loses
//! everything, which is the trade being made.
//!
//! # What it does
//! 1. Boots a broker with durable storage in a temp directory.
//! 2. Publishes to one durable and one non-durable stream.
//! 3. Drops the broker **without a graceful shutdown**, standing in for a crash:
//!    nothing is flushed on the way out, so only what durability already
//!    guaranteed can survive.
//! 4. Boots a second broker over the same directory and reads both streams back.
//!
//! # Notes
//! Developer-facing demo; it favours clarity over performance. Run it with:
//!
//! ```text
//! cargo run --release -p broker --bin durable-restart-demo
//! ```

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use std::path::Path;

const TENANT: &str = "t1";
const NAMESPACE: &str = "default";
const DURABLE_STREAM: &str = "orders";
const EPHEMERAL_STREAM: &str = "telemetry";
const RECORDS: usize = 500;

/// Small segments so the demo visibly rolls over rather than writing one file.
fn log_config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 32 * 1024,
        index_spacing_bytes: 1024,
        // The strictest policy: an acknowledgement means the bytes are on the
        // device, which is what makes the "lost nothing" claim below checkable.
        fsync_mode: FsyncMode::OnCommit,
        ..LogConfig::default()
    }
}

/// Boot a broker over `dir` with both streams registered.
async fn boot(dir: &Path) -> Result<(Broker, DurableStorage)> {
    let storage = DurableStorage::open(dir, log_config()).context("open durable storage")?;
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());

    broker.register_tenant(TENANT).await?;
    broker.register_namespace(TENANT, NAMESPACE).await?;
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            DURABLE_STREAM,
            StreamMetadata {
                durable: true,
                shards: 1,
            },
        )
        .await?;
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            EPHEMERAL_STREAM,
            StreamMetadata::default(),
        )
        .await?;
    Ok((broker, storage))
}

async fn run_demo() -> Result<()> {
    println!("== Felix Durable Restart Demo ==");
    println!("Goal: show that an acknowledged durable publish survives a crash,");
    println!("      and that a non-durable publish on the same broker does not.\n");

    let dir = tempfile::tempdir().context("create temp dir")?;
    println!("Storage root: {}", dir.path().display());

    // ---- First lifetime -----------------------------------------------------
    println!("\nStep 1/5: booting a broker with durable storage (fsync on commit).");
    let (broker, storage) = boot(dir.path()).await?;

    println!("Step 2/5: publishing {RECORDS} records to each stream.");
    for index in 0..RECORDS {
        let payload = Bytes::from(format!("order-{index:04}"));
        broker
            .publish(TENANT, NAMESPACE, DURABLE_STREAM, payload.clone())
            .await
            .context("publish durable")?;
        broker
            .publish(TENANT, NAMESPACE, EPHEMERAL_STREAM, payload)
            .await
            .context("publish ephemeral")?;
    }

    let log = storage.open_stream(TENANT, NAMESPACE, DURABLE_STREAM, 0)?;
    println!(
        "         durable stream: tail_offset={} durable_offset={} unsynced_bytes={}",
        log.tail_offset().await?,
        log.durable_offset(),
        log.unsynced_bytes(),
    );
    println!(
        "         → every acknowledged record is already on the device, so unsynced_bytes is 0."
    );

    println!("\nStep 3/5: dropping the broker with NO graceful shutdown (simulated crash).");
    // Deliberately not calling `storage.shutdown()`: a crash gets no chance to
    // flush, so anything that survives survived because the durability policy
    // put it there, not because shutdown tidied up.
    drop(log);
    drop(broker);
    drop(storage);

    // ---- Second lifetime ----------------------------------------------------
    println!("Step 4/5: booting a brand new broker over the same directory.");
    let (broker, storage) = boot(dir.path()).await?;
    let log = storage.open_stream(TENANT, NAMESPACE, DURABLE_STREAM, 0)?;

    let recovered = log.read_from(0, usize::MAX).await?;
    let segments = std::fs::read_dir(dir.path())
        .ok()
        .and_then(|entries| entries.flatten().next())
        .map(|shard| {
            std::fs::read_dir(shard.path())
                .map(|files| files.flatten().count())
                .unwrap_or(0)
        })
        .unwrap_or(0);

    println!("\nStep 5/5: results.");
    println!(
        "  durable   `{DURABLE_STREAM}`: {} of {RECORDS} records recovered from disk ({segments} files on disk)",
        recovered.len()
    );

    // The non-durable stream has no disk state at all; a fresh broker starts it
    // empty, which is what "in-memory" means when the process goes away.
    let ephemeral_cursor = broker
        .cursor_tail(TENANT, NAMESPACE, EPHEMERAL_STREAM)
        .await?;
    println!(
        "  ephemeral `{EPHEMERAL_STREAM}`: 0 of {RECORDS} records recovered (cursor restarts at {})",
        ephemeral_cursor.next_seq()
    );

    // ---- Assertions ---------------------------------------------------------
    // A demo that prints the right numbers by accident is worse than no demo, so
    // the claims above are checked rather than narrated.
    if recovered.len() != RECORDS {
        bail!(
            "durability violated: {} of {RECORDS} records survived",
            recovered.len()
        );
    }
    for (index, record) in recovered.iter().enumerate() {
        let expected = format!("order-{index:04}");
        if record.offset != index as u64 || record.payload.as_ref() != expected.as_bytes() {
            bail!(
                "record {index} came back wrong: offset={} payload={:?}, expected offset={index} payload={expected:?}",
                record.offset,
                String::from_utf8_lossy(&record.payload),
            );
        }
    }
    if ephemeral_cursor.next_seq() != 0 {
        bail!(
            "the non-durable stream unexpectedly retained state: cursor at {}",
            ephemeral_cursor.next_seq()
        );
    }

    // Recovery is not read-only: the stream must keep working afterwards.
    broker
        .publish(
            TENANT,
            NAMESPACE,
            DURABLE_STREAM,
            Bytes::from_static(b"after-restart"),
        )
        .await
        .context("publish after restart")?;
    let appended = log.read_from(RECORDS as u64, usize::MAX).await?;
    if appended.len() != 1 || appended[0].payload != Bytes::from_static(b"after-restart") {
        bail!("the recovered stream did not accept a new publish");
    }
    println!("  post-restart publish: accepted at offset {}", RECORDS);

    storage.shutdown().await?;
    println!("\nAll durability claims verified.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_demo().await
}
