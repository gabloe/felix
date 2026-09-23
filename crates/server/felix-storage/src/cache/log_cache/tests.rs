//! The cache behaving as a cache, and as a log.
//!
//! The headline is `a_cache_survives_a_restart`: it is what "the cache is a
//! log" buys, and what the in-memory cache could never do.
use std::time::Duration;

use super::*;
use crate::log::AppendOnlyLog;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: crate::log::FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn cache(dir: &std::path::Path) -> LogCache {
    LogCache::open(dir, config()).expect("open")
}

const T: &str = "t1";
const NS: &str = "ns";
const C: &str = "sessions";

#[tokio::test]
async fn a_value_reads_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await;

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.as_deref(),
        Some(&b"v"[..])
    );
}

#[tokio::test]
async fn a_missing_key_reads_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    assert!(cache.get(T, NS, C, 0, "nothing").await.is_none());
}

#[tokio::test]
async fn a_later_write_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"first"), None)
        .await;
    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"second"), None)
        .await;

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.as_deref(),
        Some(&b"second"[..]),
        "the log is append-only, so the *newest* record has to win",
    );
}

#[tokio::test]
async fn a_delete_hides_the_value_and_returns_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await;
    assert_eq!(
        cache.delete(T, NS, C, 0, "k").await.as_deref(),
        Some(&b"v"[..])
    );
    assert!(cache.get(T, NS, C, 0, "k").await.is_none());
}

/// Caches are scoped, so the same key in two of them is two entries.
#[tokio::test]
async fn caches_do_not_share_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, "a", 0, "k", Bytes::from_static(b"a"), None)
        .await;
    cache
        .put(T, NS, "b", 0, "k", Bytes::from_static(b"b"), None)
        .await;

    assert_eq!(
        cache.get(T, NS, "a", 0, "k").await.as_deref(),
        Some(&b"a"[..])
    );
    assert_eq!(
        cache.get(T, NS, "b", 0, "k").await.as_deref(),
        Some(&b"b"[..])
    );
}

/// Tenants are the outermost boundary, and the disk layout has to honour it.
#[tokio::test]
async fn tenants_do_not_share_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put("t1", NS, C, 0, "k", Bytes::from_static(b"one"), None)
        .await;
    cache
        .put("t2", NS, C, 0, "k", Bytes::from_static(b"two"), None)
        .await;

    assert_eq!(
        cache.get("t1", NS, C, 0, "k").await.as_deref(),
        Some(&b"one"[..])
    );
    assert_eq!(
        cache.get("t2", NS, C, 0, "k").await.as_deref(),
        Some(&b"two"[..])
    );
}

#[tokio::test]
async fn an_expired_entry_reads_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(cache.get(T, NS, C, 0, "k").await.is_none());
}

#[tokio::test]
async fn an_unexpired_entry_still_reads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"v"),
            Some(Duration::from_secs(300)),
        )
        .await;

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.as_deref(),
        Some(&b"v"[..])
    );
}

/// **A cache survives a restart.** The point of the whole design: the entries
/// are on disk, and the index is rebuilt from them rather than being the only
/// place they ever lived.
#[tokio::test]
async fn a_cache_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        cache
            .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
            .await;
        cache
            .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
            .await;
        cache
            .put(T, NS, C, 0, "a", Bytes::from_static(b"3"), None)
            .await;
        cache.delete(T, NS, C, 0, "b").await;
        cache.shutdown().await.expect("shutdown");
    }

    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "a").await.as_deref(),
        Some(&b"3"[..]),
        "the newest value for a key has to survive, not the first",
    );
    assert!(
        reopened.get(T, NS, C, 0, "b").await.is_none(),
        "a delete has to survive too, or a restart resurrects deleted keys",
    );
}

/// An expiry that passes while the process is down is still an expiry. This is
/// why the record stores an absolute time rather than a duration: a duration
/// would start its life again on every recovery.
#[tokio::test]
async fn an_expiry_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        cache
            .put(
                T,
                NS,
                C,
                0,
                "k",
                Bytes::from_static(b"v"),
                Some(Duration::from_millis(1)),
            )
            .await;
        cache.shutdown().await.expect("shutdown");
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        cache(dir.path())
            .await
            .get(T, NS, C, 0, "k")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn len_counts_live_entries_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
        .await;
    cache
        .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
        .await;
    cache.delete(T, NS, C, 0, "a").await;
    cache
        .put(
            T,
            NS,
            C,
            0,
            "c",
            Bytes::from_static(b"3"),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(cache.len().await, 1, "only b is live");
    assert!(!cache.is_empty().await);
}

/// **Compaction reclaims overwrites and keeps the data.** Without it the log
/// grows forever and "the cache is a log" is a slow leak rather than a design.
#[tokio::test]
async fn compaction_reclaims_overwritten_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // One key, rewritten until the log is mostly garbage. The value is large
    // enough that the floor is crossed without writing for a minute.
    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        state.index.log_bytes < 40 * value.len() as u64,
        "the log was never compacted: {} bytes for 40 writes of {}",
        state.index.log_bytes,
        value.len(),
    );
    drop(state);

    assert_eq!(
        cache.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len()),
        "compaction must not lose the value it is compacting around",
    );
}

/// Compaction drops expired entries rather than copying them forward.
#[tokio::test]
async fn compaction_drops_expired_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "doomed",
            Bytes::from(vec![b'y'; 32 * 1024]),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        !state.index.entries.contains_key("doomed"),
        "an expired entry was carried through compaction",
    );
}

/// A compacted cache still reopens. Compaction swaps directories, so a bug
/// there would be invisible until the next restart.
#[tokio::test]
async fn a_compacted_cache_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    {
        let cache = cache(dir.path()).await;
        for _ in 0..40 {
            cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
        }
        cache
            .put(T, NS, C, 0, "cold", Bytes::from_static(b"kept"), None)
            .await;
        cache.shutdown().await.expect("shutdown");
    }

    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len())
    );
    assert_eq!(
        reopened.get(T, NS, C, 0, "cold").await.as_deref(),
        Some(&b"kept"[..])
    );
}

/// Compaction must not renumber the log.
///
/// A cache shard is replicated by shipping its records at their offsets, so an
/// offset has to mean the same record on the leader and on every follower, for
/// the life of the shard. A compaction that restarts numbering makes the
/// leader's offset 0 a different record from the follower's, and the two logs
/// have silently diverged with no way to tell.
#[tokio::test]
async fn compaction_does_not_rewind_the_offset_space() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // Enough overwriting of one key to put the log well past the compaction
    // threshold while the live set stays tiny.
    let value = Bytes::from(vec![b'x'; 4096]);
    for _ in 0..64 {
        cache
            .put_checked(T, NS, C, 0, "k", value.clone(), None)
            .await
            .expect("put");
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let before = {
        let state = shard.state.lock().await;
        state.log.tail_offset().await.expect("tail")
    };

    {
        let mut state = shard.state.lock().await;
        shard.compact(&mut state).await.expect("compact");
    }

    let after = {
        let state = shard.state.lock().await;
        state.log.tail_offset().await.expect("tail")
    };

    assert!(
        after >= before,
        "compaction rewound the log from {before} to {after}; \
         every offset a follower already holds now names a different record",
    );
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "k").await.expect("get"),
        Some(value),
        "compaction must keep the live set readable",
    );
}

/// The offset space keeps growing across repeated compactions and a restart.
///
/// One compaction preserving the tail is not enough: the base offset has to
/// survive being written to disk and read back, or the shard rewinds the next
/// time the process starts and a follower's history stops matching.
#[tokio::test]
async fn the_offset_space_survives_compaction_and_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let value = Bytes::from(vec![b'x'; 4096]);
    let mut high_water = 0;

    for round in 0..3 {
        let cache = cache(dir.path()).await;
        for _ in 0..48 {
            cache
                .put_checked(T, NS, C, 0, "k", value.clone(), None)
                .await
                .expect("put");
        }

        let shard = cache.shard(T, NS, C, 0).expect("shard");
        {
            let mut state = shard.state.lock().await;
            shard.compact(&mut state).await.expect("compact");
        }
        let tail = {
            let state = shard.state.lock().await;
            state.log.tail_offset().await.expect("tail")
        };

        assert!(
            tail > high_water,
            "round {round}: tail went from {high_water} to {tail}",
        );
        high_water = tail;

        assert_eq!(
            cache.get_checked(T, NS, C, 0, "k").await.expect("get"),
            Some(value.clone()),
        );
        cache.shutdown().await.expect("shutdown");
    }
}

/// Records can reach a cache's log without going through `put`.
///
/// That is exactly what replication does to a follower: it appends to the log
/// directly, and the follower may later be promoted and asked to serve what it
/// was shipped. An index built once and trusted forever would answer those
/// reads as misses — a value that is on disk, reported absent.
#[tokio::test]
async fn the_index_catches_up_with_records_appended_behind_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // Read once so the index exists and believes it is complete.
    cache
        .put_checked(T, NS, C, 0, "first", Bytes::from_static(b"1"), None)
        .await
        .expect("put");
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "first").await.expect("get"),
        Some(Bytes::from_static(b"1")),
    );

    // Now append straight to the log, the way a replica is shipped records.
    let log = cache.shard_log(T, NS, C, 0).await.expect("shard log");
    let payload = CacheOp::Put {
        key: "shipped".to_string(),
        value: Bytes::from_static(b"2"),
        expires_at_millis: 0,
    }
    .encode();
    log.append(&[AppendRecord {
        payload,
        timestamp_micros: 0,
    }])
    .await
    .expect("append");

    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "shipped")
            .await
            .expect("get"),
        Some(Bytes::from_static(b"2")),
        "a record on disk was reported absent",
    );
    // And the record that was already indexed is still there.
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "first").await.expect("get"),
        Some(Bytes::from_static(b"1")),
    );
}

/// Collects every change it is shown, in the order shown.
#[derive(Debug, Default)]
struct RecordingObserver {
    changes: parking_lot::Mutex<Vec<CacheChange>>,
}

impl CacheObserver for RecordingObserver {
    fn cache_changed(&self, change: CacheChange) {
        self.changes.lock().push(change);
    }
}

/// Every applied write reaches the observer, with the offset the log gave it
/// and the shard's order — a put carries its value, a delete carries none.
#[tokio::test]
async fn the_observer_sees_writes_in_log_order_with_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    cache
        .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
        .await;
    cache
        .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
        .await;
    cache.delete(T, NS, C, 0, "a").await;

    let changes = observer.changes.lock();
    assert_eq!(changes.len(), 3);
    assert_eq!(
        (changes[0].key.as_str(), changes[0].value.as_deref()),
        ("a", Some(&b"1"[..]))
    );
    assert_eq!(
        (changes[1].key.as_str(), changes[1].value.as_deref()),
        ("b", Some(&b"2"[..]))
    );
    assert_eq!(
        (changes[2].key.as_str(), changes[2].value.as_deref()),
        ("a", None),
        "a delete is a change with no value",
    );
    assert_eq!(
        changes.iter().map(|c| c.offset).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "offsets are the log's, in the log's order",
    );
    assert_eq!((changes[0].tenant_id.as_str(), changes[0].shard), (T, 0));
}

/// A delete of a key that was never there appends nothing, so nothing is
/// observed: the observer reports what the log applied, and the log did not
/// change.
#[tokio::test]
async fn a_no_op_delete_is_not_observed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    assert!(cache.delete(T, NS, C, 0, "ghost").await.is_none());

    assert!(
        observer.changes.lock().is_empty(),
        "a delete that appended nothing must observe nothing",
    );
}

/// Compaction rewrites where records live, not what the cache holds, so it
/// must be silent: a watcher told about re-appended live records would see
/// phantom writes nothing performed.
#[tokio::test]
async fn compaction_is_not_observed_as_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        state.index.log_bytes < 40 * value.len() as u64,
        "the log was never compacted, so this test proved nothing",
    );
    drop(state);

    let changes = observer.changes.lock();
    assert_eq!(
        changes.len(),
        40,
        "compaction re-appended records as observable changes",
    );
}

/// The snapshot reports each live key's current value at the offset that
/// defines it, and leaves out deleted and expired keys.
#[tokio::test]
async fn live_entries_reports_current_state_with_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "kept", Bytes::from_static(b"old"), None)
        .await;
    cache
        .put(T, NS, C, 0, "gone", Bytes::from_static(b"x"), None)
        .await;
    cache
        .put(T, NS, C, 0, "kept", Bytes::from_static(b"new"), None)
        .await;
    cache.delete(T, NS, C, 0, "gone").await;
    cache
        .put(
            T,
            NS,
            C,
            0,
            "brief",
            Bytes::from_static(b"y"),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut entries = cache.live_entries(T, NS, C, 0).await.expect("snapshot");
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(entries.len(), 1, "deleted and expired keys are not live");
    assert_eq!(entries[0].key, "kept");
    assert_eq!(&entries[0].value[..], b"new");
    assert_eq!(
        entries[0].offset, 2,
        "the offset is the record that currently defines the key",
    );
}

/// The default store cannot observe writes, and says so — callers must not
/// offer watches over it.
#[tokio::test]
async fn an_ephemeral_cache_refuses_observation() {
    let cache = crate::EphemeralCache::new();
    assert!(!StorageApi::set_change_observer(
        &cache,
        Arc::new(RecordingObserver::default())
    ));
    assert!(cache.live_entries(T, NS, C, 0).await.is_err());
}

/// The group-commit regression (`docs/cache-put-group-commit-plan.md`): the
/// write path must not hold the shard lock across the fsync, or every writer
/// pays a whole device flush and concurrency buys nothing.
///
/// Asserted by counting device flushes: 32 acked `OnCommit` puts from 8
/// concurrent writers must share flushes. The serialised path performed
/// exactly one flush per put — reverting the stage/commit/apply split makes
/// this count 32 again and the assertion fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_fsyncs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(
        LogCache::open(
            dir.path(),
            LogConfig {
                segment_size_bytes: 1024 * 1024,
                fsync_mode: crate::log::FsyncMode::OnCommit,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("open"),
    );

    let writers = 8;
    let puts_per_writer = 4;
    let mut tasks = Vec::new();
    for writer in 0..writers {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for round in 0..puts_per_writer {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        &format!("w{writer}-r{round}"),
                        Bytes::from(format!("v{writer}-{round}")),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }

    let total = (writers * puts_per_writer) as u64;
    let log = cache.shard_log(T, NS, C, 0).await.expect("log");
    let flushes = log.flushes_performed();
    assert!(
        flushes < total,
        "{total} acked OnCommit puts took {flushes} device flushes: \
         every writer paid its own fsync, so the flushes were not shared",
    );

    // Every acked write is readable: sharing flushes must not lose acks.
    for writer in 0..writers {
        for round in 0..puts_per_writer {
            assert_eq!(
                cache
                    .get(T, NS, C, 0, &format!("w{writer}-r{round}"))
                    .await
                    .as_deref(),
                Some(format!("v{writer}-{round}").as_bytes()),
            );
        }
    }
}

/// Stage a write by hand — offset claimed, turn reserved, fsync *not* run —
/// so the window between staging and commit can be held open and examined.
async fn stage_without_committing(
    cache: &LogCache,
    key: &str,
    value: &[u8],
) -> (
    Arc<CacheShard>,
    crate::disk_log::PendingAppend,
    DiskLog,
    CacheOp,
) {
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let mut state = shard.state.lock().await;
    shard.ensure_index(&mut state).await.expect("index");
    let op = CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value),
        expires_at_millis: 0,
    };
    let pending = state
        .log
        .append_pending(&[AppendRecord {
            payload: op.encode(),
            timestamp_micros: now_millis() * 1000,
        }])
        .await
        .expect("stage");
    // Reserve and immediately leak the turn's effect the way a writer's guard
    // would on drop: tests below want the *unresolved* window, so they hold
    // the turn by re-reserving through the shard when they need it. Here the
    // turn is deliberately dropped only by the caller's choice.
    let log = state.log.clone();
    state.sequenced_through = Some(pending.last_offset() + 1);
    drop(state);
    (shard, pending, log, op)
}

/// **Durability before visibility.** A staged write whose fsync has not
/// happened must not be readable and must not reach a watcher — a reader that
/// saw it would be seeing a put a crash could still lose.
#[tokio::test]
async fn a_staged_write_is_invisible_until_committed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let (shard, pending, log, _op) = stage_without_committing(&cache, "k", b"staged").await;
    let turn = shard
        .sequencer
        .reserve(pending.first_offset(), pending.last_offset() + 1);

    // The record is on disk holding its offset, but not committed or applied.
    assert!(
        cache.get(T, NS, C, 0, "k").await.is_none(),
        "a staged, uncommitted write became readable",
    );
    assert!(
        observer.changes.lock().is_empty(),
        "a staged, uncommitted write reached the observer",
    );

    // Once the fsync has run and the turn resolves, the index may fold it.
    log.commit(&pending).await.expect("commit");
    drop(turn);
    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.as_deref(),
        Some(&b"staged"[..]),
        "a committed record must become readable once the sequence passes it",
    );
}

/// A writer stages while an earlier writer's commit is still unfinished —
/// the overlap that lets one fsync serve both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_stages_behind_an_unfinished_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);

    let (shard, pending, log, _op) = stage_without_committing(&cache, "first", b"one").await;
    let turn = shard
        .sequencer
        .reserve(pending.first_offset(), pending.last_offset() + 1);

    // The second writer must be able to claim its offset while the first is
    // uncommitted; it then queues behind the first's turn.
    let second = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            cache
                .put_checked(T, NS, C, 0, "second", Bytes::from_static(b"two"), None)
                .await
        })
    };
    let staged = async {
        loop {
            if log.tail_offset().await.expect("tail") > pending.last_offset() + 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), staged)
        .await
        .expect("the second writer never staged: the lock was held across the commit");
    assert!(
        !second.is_finished(),
        "the second writer overtook the first"
    );

    log.commit(&pending).await.expect("commit");
    drop(turn);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("second writer")
        .expect("join")
        .expect("put");
    assert_eq!(
        cache.get(T, NS, C, 0, "second").await.as_deref(),
        Some(&b"two"[..]),
    );
}

/// A writer abandoned mid-flight — commit never run, turn dropped, the shape
/// of a cancelled future — must not strand the writers behind it.
#[tokio::test]
async fn an_abandoned_write_does_not_strand_the_shard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    let (shard, pending, _log, _op) = stage_without_committing(&cache, "gone", b"never").await;
    drop(
        shard
            .sequencer
            .reserve(pending.first_offset(), pending.last_offset() + 1),
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        cache.put_checked(T, NS, C, 0, "after", Bytes::from_static(b"lives"), None),
    )
    .await
    .expect("the abandoned write stranded the shard")
    .expect("put");
    assert_eq!(
        cache.get(T, NS, C, 0, "after").await.as_deref(),
        Some(&b"lives"[..]),
    );
}

/// Concurrent writers to one key: the index and every watcher settle on the
/// record with the highest offset — disk order, not completion order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_one_key_settle_on_the_highest_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let mut tasks = Vec::new();
    for writer in 0..8 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for round in 0..5 {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        "contended",
                        Bytes::from(format!("w{writer}-r{round}")),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }

    let value = {
        let changes = observer.changes.lock();
        assert_eq!(changes.len(), 40);
        for pair in changes.windows(2) {
            assert!(
                pair[0].offset < pair[1].offset,
                "watch order disagreed with disk order: {} then {}",
                pair[0].offset,
                pair[1].offset,
            );
        }
        let winner = changes.last().expect("changes");
        winner.value.clone().expect("a put carries its value")
    };
    assert_eq!(
        cache.get(T, NS, C, 0, "contended").await.as_deref(),
        Some(&value[..]),
        "the index winner must be the highest offset, as the log reads",
    );
}

/// Compaction and concurrent writers coexist: nothing acked is lost, offsets
/// stay monotone, and the log still gets compacted once the storm passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_under_concurrent_writers_loses_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    // Each writer overwrites its own key with values large enough to push the
    // log well past the compaction threshold.
    let payload = vec![7u8; 64 * 1024];
    let mut tasks = Vec::new();
    for writer in 0..4 {
        let cache = Arc::clone(&cache);
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..40 {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        &format!("key-{writer}"),
                        Bytes::from(payload.clone()),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }

    {
        let changes = observer.changes.lock();
        for pair in changes.windows(2) {
            assert!(pair[0].offset < pair[1].offset, "offsets went backwards");
        }
    }

    for writer in 0..4 {
        let value = cache
            .get(T, NS, C, 0, &format!("key-{writer}"))
            .await
            .expect("key survived");
        assert_eq!(value.len(), payload.len());
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    let written = 4u64 * 40 * payload.len() as u64;
    assert!(
        state.index.log_bytes < written,
        "{} bytes on the log for {written} written: compaction never ran",
        state.index.log_bytes,
    );
}

/// A crash between compaction's two renames must not lose the shard.
///
/// Compaction moves the shard directory to `.retired`, moves the compacted one
/// into its place, then deletes the retired copy. Crash in between and the
/// shard directory is gone while all its data sits in `.retired`. An open that
/// ignored that would start the shard empty, and the next compaction would
/// delete the only copy.
#[tokio::test]
async fn a_shard_interrupted_mid_compaction_is_recovered_from_its_retired_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shard_dir = {
        let cache = cache(dir.path()).await;
        cache
            .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
            .await;
        cache
            .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
            .await;
        let shard_dir = layout::shard_dir(
            cache.root(),
            &crate::log::ShardKey {
                tenant: T.to_string(),
                namespace: NS.to_string(),
                stream: C.to_string(),
                shard: 0,
            },
        );
        cache.shutdown().await.expect("shutdown");
        shard_dir
    };

    // Exactly the state a crash between the renames leaves behind.
    std::fs::rename(&shard_dir, shard_dir.with_extension("retired")).expect("retire");
    assert!(!shard_dir.exists());

    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "a").await.as_deref(),
        Some(&b"1"[..]),
        "the shard came back empty, so the retired copy is now unreferenced and \
         the next compaction deletes it",
    );
    assert_eq!(
        reopened.get(T, NS, C, 0, "b").await.as_deref(),
        Some(&b"2"[..]),
    );
    assert!(
        !shard_dir.with_extension("retired").exists(),
        "the retired copy should have been moved back, not copied",
    );
}
