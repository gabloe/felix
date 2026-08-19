// End-to-end behaviour of `DiskLog` through the `AppendOnlyLog` trait: the
// durability guarantee each fsync mode actually delivers, restart semantics, and
// the bounds on a range read.

use std::time::Duration;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::log::{AppendOnlyLog, FsyncMode, LogProvider};

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 1_700_000_000,
    }
}

fn records(payloads: &[&str]) -> Vec<AppendRecord> {
    payloads.iter().map(|p| record(p)).collect()
}

/// A small-segment config so rollover is exercised without writing megabytes.
fn config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 120,
        index_spacing_bytes: 48,
        fsync_mode,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn open(dir: &TempDir, fsync_mode: FsyncMode) -> DiskLog {
    DiskLog::open(dir.path(), "t/ns/s/0", config(fsync_mode)).expect("open")
}

async fn read_all(log: &DiskLog, start: Offset) -> Vec<String> {
    log.read_range(ReadRange {
        start,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .into_iter()
    .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
    .collect()
}

#[tokio::test]
async fn appends_are_readable_and_offsets_are_contiguous() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);

    let first = log.append(&records(&["a", "b"])).await.expect("append");
    assert_eq!((first.first_offset, first.last_offset), (0, 1));
    let second = log.append(&records(&["c"])).await.expect("append");
    assert_eq!((second.first_offset, second.last_offset), (2, 2));

    assert_eq!(log.tail_offset().await.expect("tail"), 3);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
    assert_eq!(read_all(&log, 2).await, vec!["c"]);
}

#[tokio::test]
async fn an_empty_append_is_rejected() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    assert!(matches!(
        log.append(&[]).await.expect_err("empty"),
        StorageError::InvalidRange
    ));
    assert_eq!(log.tail_offset().await.expect("tail"), 0);
}

#[tokio::test]
async fn reading_at_the_tail_is_empty_rather_than_an_error() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["only"])).await.expect("append");
    assert!(read_all(&log, 1).await.is_empty());
    assert!(read_all(&log, 500).await.is_empty());
}

#[tokio::test]
async fn a_trimmed_offset_is_distinguishable_from_an_empty_range() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    for i in 0..10 {
        log.append(&records(&[&format!("value-{i}")]))
            .await
            .expect("append");
    }
    // Truncating to zero and re-appending moves the base offset forward.
    log.truncate(0).await.expect("truncate");
    log.append(&records(&["fresh"])).await.expect("append");
    assert_eq!(log.base_offset(), 0);

    // With nothing trimmed yet, a low offset is a valid, non-empty range.
    assert_eq!(read_all(&log, 0).await, vec!["fresh"]);
}

#[tokio::test]
async fn a_read_below_the_base_offset_reports_trimmed() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 60,
            ..config(FsyncMode::None)
        },
    )
    .expect("open");

    for i in 0..20 {
        log.append(&records(&[&format!("v{i:03}")]))
            .await
            .expect("append");
    }
    // Simulate retention by truncating the whole log forward.
    log.truncate(0).await.expect("truncate");
    for i in 0..5 {
        log.append(&records(&[&format!("w{i}")]))
            .await
            .expect("append");
    }
    assert!(read_all(&log, 0).await.len() == 5);

    // A log whose base has advanced reports the trim rather than silently
    // returning a shorter range.
    let inner_base = log.base_offset();
    if inner_base > 0 {
        let err = log
            .read_range(ReadRange {
                start: inner_base - 1,
                max_bytes: usize::MAX,
            })
            .await
            .expect_err("trimmed");
        assert!(matches!(err, StorageError::Trimmed { .. }));
    }
}

#[tokio::test]
async fn reads_are_bounded_by_bytes_and_by_record_count() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            max_records_per_read: 4,
            ..config(FsyncMode::None)
        },
    )
    .expect("open");
    for i in 0..30 {
        log.append(&records(&[&format!("v{i:03}")]))
            .await
            .expect("append");
    }

    // The record cap applies even with an unlimited byte budget.
    let capped = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    assert_eq!(capped.len(), 4);
    assert_eq!(capped[0].offset, 0);
    assert_eq!(capped[3].offset, 3);

    // And the byte budget applies below the record cap.
    let small = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: 8,
        })
        .await
        .expect("read");
    assert_eq!(small.len(), 2);

    // Paging with the returned offsets covers the whole log without gaps.
    let mut seen = Vec::new();
    let mut cursor = 0;
    while cursor < log.tail_offset().await.expect("tail") {
        let page = log
            .read_range(ReadRange {
                start: cursor,
                max_bytes: usize::MAX,
            })
            .await
            .expect("read");
        assert!(!page.is_empty());
        cursor = page.last().expect("last").offset + 1;
        seen.extend(page.into_iter().map(|r| r.offset));
    }
    assert_eq!(seen, (0..30).collect::<Vec<_>>());
}

#[tokio::test]
async fn records_survive_reopening_the_log() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..25 {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append");
        }
        assert!(log.segments().len() > 1, "expected rollovers");
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 25);
    let values = read_all(&log, 0).await;
    assert_eq!(values.len(), 25);
    assert_eq!(values[0], "value-000");
    assert_eq!(values[24], "value-024");

    // And the reopened log keeps accepting writes.
    log.append(&records(&["after"])).await.expect("append");
    assert_eq!(read_all(&log, 25).await, vec!["after"]);
}

#[tokio::test]
async fn background_rollover_keeps_the_log_contiguous_and_reopenable() {
    // `rollover_threshold_percent` is 100 by default -- the background roll is
    // measured to hurt tail latency on a device-level flush, so it ships off.
    // This exercises it explicitly so the path stays covered.
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        rollover_threshold_percent: 60,
        max_overshoot_percent: 200,
        ..config(FsyncMode::OnCommit)
    };
    {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", config.clone()).expect("open");
        for i in 0..60 {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append");
        }
        assert!(log.segments().len() > 1, "expected rollovers");
        log.shutdown().await.expect("shutdown");
    }

    // Reopening is the real assertion: a background roll that left an
    // uninstalled segment, a gap, or a duplicated offset shows up here.
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config).expect("reopen");
    assert_eq!(log.tail_offset().await.expect("tail"), 60);
    let values = read_all(&log, 0).await;
    assert_eq!(values.len(), 60);
    assert_eq!(values[0], "value-000");
    assert_eq!(values[59], "value-059");
}

/// A rollover whose seal fails must never let an `OnCommit` append be
/// acknowledged on the strength of a flush that did not cover it.
///
/// The window this guards: the seal fails, `pending_seal` is cleared, and an
/// append already in flight captures only the new active segment, syncs that,
/// and reports a durable bound spanning both. The retired segment's records
/// were never flushed.
#[tokio::test]
async fn a_failed_seal_stops_the_log_instead_of_acknowledging() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        rollover_threshold_percent: 60,
        max_overshoot_percent: 200,
        ..config(FsyncMode::OnCommit)
    };
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config).expect("open");

    log.inner
        .fail_seal
        .store(true, std::sync::atomic::Ordering::Release);

    // Append until the background rollover has run and failed. Every append
    // either succeeds durably or fails -- what must never happen is an
    // acknowledgement after the seal failed.
    let mut rejected = None;
    for i in 0..200 {
        if let Err(err) = log.append(&records(&[&format!("value-{i:03}")])).await {
            rejected = Some(err);
            break;
        }
    }
    let rejected = rejected.expect("the failed seal should have stopped the log");
    assert!(
        rejected.to_string().contains("rollover failed"),
        "unexpected error: {rejected}",
    );

    // Terminal, on every path that accepts work or reports durability.
    assert!(log.append(&records(&["after"])).await.is_err());
    assert!(log.sync().await.is_err());
    assert!(log.shutdown().await.is_err());

    // And the retired segment is still owed a flush, so no later flush can
    // silently skip it.
    assert!(
        log.inner.pending_seal.lock().is_some(),
        "a failed seal must keep the retired segment on the flush path",
    );
}

#[tokio::test]
async fn on_commit_acknowledges_only_durable_records() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);

    for i in 0..5 {
        let result = log
            .append(&records(&[&format!("v{i}")]))
            .await
            .expect("append");
        // The acknowledgement itself is the durability guarantee: by the time
        // `append` returns, the record is past the device.
        assert!(
            log.durable_offset() > result.last_offset,
            "offset {} not durable at ack",
            result.last_offset
        );
        // Zero, or one segment header. A background rollover installs its
        // replacement by writing the header into the page cache and leaving the
        // flush to the next sync, so an ack can land in the window where those
        // 32 bytes are the only thing outstanding. No *record* is ever
        // unsynced at an ack, which is what the assertion above checks
        // directly.
        assert!(
            log.unsynced_bytes() <= crate::segment::SEGMENT_HEADER_LEN,
            "unsynced record bytes at ack: {}",
            log.unsynced_bytes(),
        );
    }
}

#[tokio::test]
async fn none_mode_acknowledges_without_syncing() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");

    // Nothing has been pushed to the device, which is exactly what `None` buys.
    assert!(log.unsynced_bytes() > 0);
    assert_eq!(log.durable_offset(), 0);

    // An explicit sync still works, and is what shutdown uses.
    log.sync().await.expect("sync");
    assert_eq!(log.unsynced_bytes(), 0);
    assert_eq!(log.durable_offset(), 2);
}

#[tokio::test]
async fn periodic_mode_bounds_unsynced_data_by_its_interval() {
    let dir = tempdir().expect("dir");
    let log = open(
        &dir,
        FsyncMode::Periodic {
            interval: Duration::from_millis(20),
        },
    );
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");

    // The append itself did not wait for the device.
    assert!(log.unsynced_bytes() > 0);

    // Within a few intervals the background syncer catches up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while log.durable_offset() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(log.durable_offset(), 3);
    assert_eq!(log.unsynced_bytes(), 0);

    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_flushes_what_the_policy_had_not() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::None);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        assert!(log.unsynced_bytes() > 0);
        log.shutdown().await.expect("shutdown");
    }
    let log = open(&dir, FsyncMode::None);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn concurrent_on_commit_appends_all_land_exactly_once() {
    let dir = tempdir().expect("dir");
    let log = std::sync::Arc::new(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                segment_size_bytes: 4 * 1024,
                ..config(FsyncMode::OnCommit)
            },
        )
        .expect("open"),
    );

    let mut tasks = Vec::new();
    for i in 0..32u32 {
        let log = std::sync::Arc::clone(&log);
        tasks.push(tokio::spawn(async move {
            log.append(&records(&[&format!("task-{i:02}")]))
                .await
                .expect("append")
        }));
    }
    let mut assigned: Vec<Offset> = Vec::new();
    for task in tasks {
        let result = task.await.expect("join");
        assert_eq!(result.first_offset, result.last_offset);
        assigned.push(result.first_offset);
    }

    // Every append got a distinct offset, and the log holds exactly them.
    assigned.sort_unstable();
    assert_eq!(assigned, (0..32).collect::<Vec<_>>());
    assert_eq!(log.durable_offset(), 32);

    let stored = read_all(&log, 0).await;
    assert_eq!(stored.len(), 32);
    let mut sorted = stored.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 32, "duplicate or lost records");
}

#[tokio::test]
async fn truncate_drops_the_suffix_and_survives_reopen() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..20 {
            log.append(&records(&[&format!("v{i:03}")]))
                .await
                .expect("append");
        }
        log.truncate(6).await.expect("truncate");
        assert_eq!(log.tail_offset().await.expect("tail"), 6);
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 0).await.len(), 6);
    log.append(&records(&["resumed"])).await.expect("append");
    assert_eq!(read_all(&log, 6).await, vec!["resumed"]);
}

#[tokio::test]
async fn on_commit_reflushes_offsets_reused_after_truncation() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);

    log.append(&records(&["zero", "one", "two"]))
        .await
        .expect("initial append");
    assert_eq!(log.durable_offset(), 3);

    log.truncate(1).await.expect("truncate");
    assert_eq!(log.durable_offset(), 1);

    let replacement = log
        .append(&records(&["replacement"]))
        .await
        .expect("replacement append");
    assert_eq!(replacement.first_offset, 1);
    assert_eq!(log.durable_offset(), 2);
    assert_eq!(log.unsynced_bytes(), 0);
}

#[tokio::test]
async fn sealing_reports_a_descriptor_and_checksum() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");

    let sealed = log.seal().await.expect("seal");
    assert_eq!(sealed.descriptor.base_offset, 0);
    assert_eq!(sealed.descriptor.last_offset, 1);
    assert_ne!(sealed.checksum, 0);

    // Sealing rolls, so new appends land in a fresh segment and the sealed one
    // stays immutable.
    log.append(&records(&["c"])).await.expect("append");
    assert!(log.segments().len() > 1);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn an_invalid_config_is_rejected_at_open() {
    let dir = tempdir().expect("dir");
    let err = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::ZERO,
            },
            ..LogConfig::default()
        },
    )
    .expect_err("zero interval");
    assert!(matches!(err, StorageError::InvalidConfig(_)));

    let err = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: 8,
            ..LogConfig::default()
        },
    )
    .expect_err("tiny segment");
    assert!(matches!(err, StorageError::InvalidConfig(_)));
}

#[tokio::test]
async fn the_provider_returns_one_log_per_shard() {
    let root = tempdir().expect("dir");
    let provider = DiskLogProvider::new(root.path(), config(FsyncMode::None)).expect("provider");

    let shard = ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 0,
    };
    let other = ShardKey {
        shard: 1,
        ..shard.clone()
    };

    let first = provider.open(&shard).await.expect("open");
    let again = provider.open(&shard).await.expect("open again");
    let separate = provider.open(&other).await.expect("open other");

    // The same shard must resolve to the same log; two writers over one
    // directory would interleave offsets.
    first.append(&records(&["one"])).await.expect("append");
    again.append(&records(&["two"])).await.expect("append");
    assert_eq!(read_all(&again, 0).await, vec!["one", "two"]);

    // A different shard is genuinely separate.
    separate.append(&records(&["other"])).await.expect("append");
    assert_eq!(read_all(&separate, 0).await, vec!["other"]);
    assert_eq!(provider.open_shards().len(), 2);

    provider.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn provider_logs_reopen_with_their_data() {
    let root = tempdir().expect("dir");
    let shard = ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 3,
    };
    {
        let provider =
            DiskLogProvider::new(root.path(), config(FsyncMode::OnCommit)).expect("provider");
        let log = provider.open(&shard).await.expect("open");
        log.append(&records(&["persisted"])).await.expect("append");
        provider.shutdown().await.expect("shutdown");
    }

    let provider =
        DiskLogProvider::new(root.path(), config(FsyncMode::OnCommit)).expect("provider");
    let log = provider.open(&shard).await.expect("open");
    assert_eq!(read_all(&log, 0).await, vec!["persisted"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_across_a_rollover_stay_contiguous() {
    let dir = tempdir().expect("dir");
    // Segments small enough that this run crosses many boundaries, so the
    // off-thread pre-roll and the in-lock fallback both get exercised, and
    // concurrent publishers race the roll itself.
    let log = std::sync::Arc::new(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 200,
                ..config(FsyncMode::None)
            },
        )
        .expect("open"),
    );

    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let log = std::sync::Arc::clone(&log);
        tasks.push(tokio::spawn(async move {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append")
        }));
    }
    let mut assigned: Vec<Offset> = Vec::new();
    for task in tasks {
        assigned.push(task.await.expect("join").first_offset);
    }
    log.sync().await.expect("sync");

    // Every append got a distinct offset and the log holds exactly them, with
    // no gap or duplicate introduced by a rollover racing an append.
    assigned.sort_unstable();
    assert_eq!(assigned, (0..64).collect::<Vec<_>>());
    assert!(log.segments().len() > 2, "expected several rollovers");

    let stored = read_all(&log, 0).await;
    assert_eq!(stored.len(), 64);
    let mut unique = stored.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        64,
        "duplicate or lost records across a rollover"
    );

    // And it reopens cleanly, which is where a segment left empty or
    // double-rolled would surface.
    log.shutdown().await.expect("shutdown");
    let reopened = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 200,
            ..config(FsyncMode::None)
        },
    )
    .expect("reopen");
    assert_eq!(reopened.tail_offset().await.expect("tail"), 64);
}

#[tokio::test]
async fn a_record_larger_than_a_segment_still_round_trips() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let big = "x".repeat(10_000);
    log.append(&records(&["before", &big, "after"]))
        .await
        .expect("append");
    log.sync().await.expect("sync");

    let values = read_all(&log, 0).await;
    assert_eq!(values, vec!["before".to_string(), big, "after".to_string()]);
}

// --- Retention -------------------------------------------------------------
//
// Retention is the only thing that ever raises `base_offset`, which makes it the
// only thing that can make `Trimmed` reachable. Every test here therefore
// asserts the *observable* consequence — a read that now fails, a base offset
// that moved — rather than just counting files.

fn retention_config(retention_bytes: Option<u64>, retention_age: Option<Duration>) -> LogConfig {
    LogConfig {
        retention_bytes,
        retention_age,
        // Long enough that the background task never fires mid-test; these
        // tests drive retention explicitly via `enforce_retention_now`.
        retention_check_interval: Duration::from_secs(3600),
        ..config(FsyncMode::None)
    }
}

/// A record with an explicit timestamp, for age-based retention.
fn aged_record(payload: &str, timestamp_micros: u64) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros,
    }
}

#[tokio::test]
async fn retention_is_off_by_default() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    for payload in ["a", "b", "c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert_eq!(outcome.segments_deleted, 0);
    assert_eq!(log.base_offset(), 0);
    assert_eq!(read_all(&log, 0).await.len(), 6);
}

#[tokio::test]
async fn size_retention_deletes_oldest_segments_and_raises_base_offset() {
    let dir = tempdir().expect("dir");
    // One segment holds ~2 of these records, so this keeps roughly one sealed
    // segment plus the active one.
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("open");

    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    assert_eq!(log.base_offset(), 0, "nothing trimmed before a sweep");

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(outcome.segments_deleted > 0, "expected a trim");
    assert!(outcome.bytes_reclaimed > 0);
    let base = log.base_offset();
    assert!(
        base > 0,
        "base offset must advance past the deleted records"
    );
    assert_eq!(outcome.base_offset, base);

    // The surviving records still read back, contiguously, from the new base.
    let surviving = read_all(&log, base).await;
    assert!(!surviving.is_empty());
    assert_eq!(surviving.last().map(String::as_str), Some("h"));
}

#[tokio::test]
async fn retention_makes_a_below_base_read_report_trimmed() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("open");
    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.enforce_retention_now().await.expect("retention");
    let base = log.base_offset();
    assert!(base > 0);

    // This is the assertion the whole feature exists to make possible: before
    // retention, `Trimmed` could not fire on a durable log at all.
    let err = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect_err("offset 0 is gone");
    match err {
        StorageError::Trimmed { requested, oldest } => {
            assert_eq!(requested, 0);
            assert_eq!(oldest, base);
        }
        other => panic!("expected Trimmed, got {other:?}"),
    }
}

#[tokio::test]
async fn retention_never_deletes_the_active_segment() {
    let dir = tempdir().expect("dir");
    // A bound far below one segment: retention still cannot empty the log,
    // because the active segment is not a candidate.
    let log = DiskLog::open(dir.path(), "t/ns/s/0", retention_config(Some(1), None)).expect("open");
    for payload in ["a", "b", "c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }

    log.enforce_retention_now().await.expect("retention");
    let base = log.base_offset();
    let tail = log.tail_offset().await.expect("tail");
    assert!(base < tail, "the active segment's records must survive");
    assert!(!read_all(&log, base).await.is_empty());
}

#[tokio::test]
async fn age_retention_uses_the_newest_record_in_a_segment() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(None, Some(Duration::from_secs(60))),
    )
    .expect("open");

    let now = crate::disk_log::retention::now_micros();
    let old = now - Duration::from_secs(3600).as_micros() as u64;
    // Enough expired records to fill whole segments: a segment is only a
    // candidate once *every* record in it has expired, so a handful sharing a
    // segment with fresh ones would (correctly) survive.
    for i in 0..12 {
        log.append(&[aged_record(&format!("old{i:02}"), old)])
            .await
            .expect("append");
    }
    for i in 0..12 {
        log.append(&[aged_record(&format!("new{i:02}"), now)])
            .await
            .expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(outcome.segments_deleted > 0, "expired segments should go");
    let surviving = read_all(&log, log.base_offset()).await;
    assert!(
        !surviving.contains(&"old00".to_string()),
        "the oldest expired record should be gone, got {surviving:?}"
    );
    assert!(
        (0..12).all(|i| surviving.contains(&format!("new{i:02}"))),
        "every fresh record must survive, got {surviving:?}"
    );
}

#[tokio::test]
async fn age_retention_keeps_a_segment_whose_newest_record_is_fresh() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(None, Some(Duration::from_secs(60))),
    )
    .expect("open");

    let now = crate::disk_log::retention::now_micros();
    let old = now - Duration::from_secs(3600).as_micros() as u64;
    // An old record and a fresh one land in the same segment. The newest
    // decides, so nothing is deleted -- the conservative direction.
    log.append(&[aged_record("old", old)])
        .await
        .expect("append");
    log.append(&[aged_record("new", now)])
        .await
        .expect("append");
    for payload in ["x", "y", "z"] {
        log.append(&[aged_record(payload, now)])
            .await
            .expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        outcome.segments_deleted, 0,
        "a segment holding one fresh record must survive"
    );
    assert_eq!(log.base_offset(), 0);
}

#[tokio::test]
async fn a_trimmed_log_reopens_from_its_surviving_base_offset() {
    let dir = tempdir().expect("dir");
    let cfg = retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None);
    let base = {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", cfg.clone()).expect("open");
        for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            log.append(&records(&[payload])).await.expect("append");
        }
        log.enforce_retention_now().await.expect("retention");
        let base = log.base_offset();
        log.shutdown().await.expect("shutdown");
        base
    };
    assert!(base > 0, "the trim must have removed segment 0");

    // Recovery has to derive the base from the lowest *surviving* segment, not
    // assume the directory starts at offset 0.
    let reopened = DiskLog::open(dir.path(), "t/ns/s/0", cfg).expect("reopen");
    assert_eq!(reopened.base_offset(), base);
    assert_eq!(
        read_all(&reopened, base).await.last().map(String::as_str),
        Some("h")
    );
    let err = reopened
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect_err("trimmed offsets stay trimmed across a restart");
    assert!(matches!(err, StorageError::Trimmed { .. }));
}

#[tokio::test]
async fn zero_retention_bounds_are_rejected_at_open() {
    let dir = tempdir().expect("dir");
    assert!(DiskLog::open(dir.path(), "t/ns/s/0", retention_config(Some(0), None)).is_err());
    assert!(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            retention_config(None, Some(Duration::ZERO))
        )
        .is_err()
    );
}
