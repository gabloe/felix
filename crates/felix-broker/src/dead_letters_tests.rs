//! What a group gave up on, and whose it was.
use super::*;

use felix_storage::log::FsyncMode;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key(stream: &str, group: &str) -> GroupKey {
    GroupKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        group: group.to_string(),
    }
}

#[tokio::test]
async fn nothing_is_dead_lettered_to_begin_with() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    assert_eq!(
        dead.list(&key("jobs", "workers")).await.expect("list"),
        Vec::<u64>::new(),
    );
}

#[tokio::test]
async fn recorded_offsets_come_back_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    let key = key("jobs", "workers");

    for offset in [12u64, 3, 40] {
        dead.record(&key, offset).await.expect("record");
    }

    assert_eq!(dead.list(&key).await.expect("list"), vec![3, 12, 40]);
}

/// Recording the same offset twice is one dead letter, not two. A retry of the
/// pass that records it must not double-count.
#[tokio::test]
async fn recording_the_same_offset_twice_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    let key = key("jobs", "workers");

    dead.record(&key, 7).await.expect("record");
    dead.record(&key, 7).await.expect("record");

    assert_eq!(dead.list(&key).await.expect("list"), vec![7]);
}

/// Two groups reading one stream fail on different records. Merging their dead
/// letters would have each answering for work the other gave up on.
#[tokio::test]
async fn two_groups_on_one_stream_keep_separate_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    dead.record(&key("jobs", "alpha"), 1).await.expect("record");
    dead.record(&key("jobs", "beta"), 2).await.expect("record");

    assert_eq!(
        dead.list(&key("jobs", "alpha")).await.expect("list"),
        vec![1]
    );
    assert_eq!(
        dead.list(&key("jobs", "beta")).await.expect("list"),
        vec![2]
    );
}

/// And two streams read by a group of the same name stay separate too.
#[tokio::test]
async fn two_streams_keep_separate_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    dead.record(&key("jobs", "workers"), 1)
        .await
        .expect("record");
    dead.record(&key("orders", "workers"), 2)
        .await
        .expect("record");

    assert_eq!(
        dead.list(&key("jobs", "workers")).await.expect("list"),
        vec![1]
    );
    assert_eq!(
        dead.list(&key("orders", "workers")).await.expect("list"),
        vec![2]
    );
}

/// A dead letter that vanished on restart would leave the record skipped with
/// nothing recording that it was ever tried.
#[tokio::test]
async fn dead_letters_survive_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key("jobs", "workers");

    {
        let dead = DeadLetters::open(dir.path(), config()).expect("open");
        dead.record(&key, 99).await.expect("record");
        dead.shutdown().await.expect("shutdown");
    }

    let reopened = DeadLetters::open(dir.path(), config()).expect("open");
    assert_eq!(reopened.list(&key).await.expect("list"), vec![99]);
}
