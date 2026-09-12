//! The cache behaving as a cache, and as a log.
//!
//! The headline is `a_cache_survives_a_restart`: it is what "the cache is a
//! log" buys, and what the in-memory cache could never do.
use super::*;
use std::time::Duration;

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
