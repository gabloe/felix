use super::*;

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
