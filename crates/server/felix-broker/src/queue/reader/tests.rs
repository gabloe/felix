//! A group reading a real log, with a real cursor underneath it.
use super::*;

use bytes::Bytes;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::durable::{DurableStorage, StreamLog};
use crate::queue::DeadLetters;

const T: &str = "t1";
const NS: &str = "ns";
const S: &str = "orders";
const G: &str = "workers";
const VIS: Duration = Duration::from_secs(30);
/// High enough that the cases below never reach it; the bound has its own tests.
const MANY: u32 = 1_000;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key() -> GroupKey {
    GroupKey {
        tenant_id: T.to_string(),
        namespace: NS.to_string(),
        stream: S.to_string(),
        shard: 0,
        group: G.to_string(),
    }
}

/// A shard log and a group reader over the same directory, the way the broker
/// arranges them: streams under one root, group cursors under another.
struct Fixture {
    log: StreamLog,
    reader: GroupReader,
}

fn open(dir: &std::path::Path) -> Fixture {
    open_with_attempts(dir, MANY)
}

fn open_with_attempts(dir: &std::path::Path, max_attempts: u32) -> Fixture {
    let storage = DurableStorage::open(dir.join("streams"), config()).expect("storage");
    let log = storage.open_stream(T, NS, S, 0).expect("stream log");
    let cursors = Arc::new(ConsumerGroups::open(dir.join("groups"), config()).expect("cursors"));
    let dead = Arc::new(DeadLetters::open(dir.join("dead"), config()).expect("dead letters"));
    Fixture {
        log,
        reader: GroupReader::new(cursors, dead, VIS, max_attempts),
    }
}

async fn publish(log: &StreamLog, payloads: &[&str]) {
    let owned: Vec<Bytes> = payloads
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect();
    log.append(&owned).await.expect("append");
}

fn payloads(claimed: &[Claimed]) -> Vec<String> {
    claimed
        .iter()
        .map(|c| String::from_utf8(c.payload.to_vec()).expect("utf8"))
        .collect()
}

#[tokio::test]
async fn a_group_reads_from_the_beginning_of_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(payloads(&claimed), vec!["a", "b", "c"]);
    assert_eq!(claimed[0].offset, 0);
}

#[tokio::test]
async fn an_empty_log_yields_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert!(claimed.is_empty());
}

/// **The queue property, over a real log.** Two polls do not hand out the same
/// record while the first claim stands.
#[tokio::test]
async fn a_second_poll_does_not_repeat_an_outstanding_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c", "d"]).await;
    let now = Instant::now();

    let first = fx.reader.poll(&key(), &fx.log, 2, now).await.expect("poll");
    let second = fx.reader.poll(&key(), &fx.log, 2, now).await.expect("poll");

    assert_eq!(payloads(&first), vec!["a", "b"]);
    assert_eq!(payloads(&second), vec!["c", "d"]);
}

/// The cursor is written only when a contiguous run closes, so an
/// acknowledgement above a gap leaves nothing on disk to resume from.
#[tokio::test]
async fn the_cursor_is_written_only_when_a_run_closes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;
    let key = key();
    fx.reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    fx.reader.ack(&key, 1).await.expect("ack");
    assert_eq!(
        fx.reader.committed(&key).await.expect("committed"),
        None,
        "the cursor moved past an offset nobody finished",
    );

    fx.reader.ack(&key, 0).await.expect("ack");
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(2));
}

/// **The slice's headline.** Work finished before a restart is not handed out
/// again after one; work still outstanding is.
#[tokio::test]
async fn a_restart_resumes_from_the_cursor_and_redelivers_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();

    {
        let fx = open(dir.path());
        publish(&fx.log, &["a", "b", "c", "d"]).await;
        let claimed = fx
            .reader
            .poll(&key, &fx.log, 10, Instant::now())
            .await
            .expect("poll");
        assert_eq!(claimed.len(), 4);
        // Finish the first two; leave the rest in flight.
        fx.reader.ack(&key, 0).await.expect("ack");
        fx.reader.ack(&key, 1).await.expect("ack");
    }

    // A new reader over the same directory: nothing in memory survives.
    let fx = open(dir.path());
    let after = fx
        .reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(
        payloads(&after),
        vec!["c", "d"],
        "the group either lost finished work or repeated it",
    );
}

/// A consumer that stops answering has its records handed to the next one.
#[tokio::test]
async fn a_lapsed_claim_is_delivered_to_the_next_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let base = Instant::now();
    let key = key();

    let first = fx.reader.poll(&key, &fx.log, 10, base).await.expect("poll");
    assert_eq!(payloads(&first), vec!["a", "b"]);

    let again = fx
        .reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");
    assert_eq!(payloads(&again), vec!["a", "b"]);
}

#[tokio::test]
async fn a_nacked_record_comes_back_on_the_next_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();
    let key = key();

    fx.reader.poll(&key, &fx.log, 10, now).await.expect("poll");
    fx.reader.nack(&key, 0).await.expect("nack");

    let again = fx.reader.poll(&key, &fx.log, 10, now).await.expect("poll");
    assert_eq!(payloads(&again), vec!["a"]);
}

/// Two groups over one shard are independent: each sees every record.
#[tokio::test]
async fn two_groups_each_receive_every_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();

    let mut first = key();
    first.group = "one".to_string();
    let mut second = key();
    second.group = "two".to_string();

    let a = fx
        .reader
        .poll(&first, &fx.log, 10, now)
        .await
        .expect("poll");
    let b = fx
        .reader
        .poll(&second, &fx.log, 10, now)
        .await
        .expect("poll");

    assert_eq!(payloads(&a), vec!["a", "b"]);
    assert_eq!(payloads(&b), vec!["a", "b"], "a group stole another's work");
}

/// A record larger than the per-read budget is still delivered.
///
/// Records are fetched one at a time with a one-byte budget. That works only
/// because a read of a range that holds data never answers empty. If it ever
/// did, this record would be owed for ever and the group would stall here.
#[tokio::test]
async fn a_record_larger_than_the_read_budget_is_delivered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let big = "x".repeat(64 * 1024);
    publish(&fx.log, &["small", &big]).await;

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[1].payload.len(), big.len());
}

/// Every record reaches a consumer exactly once when every claim is answered.
#[tokio::test]
async fn every_record_is_delivered_once_when_all_are_acknowledged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let sent: Vec<String> = (0..50).map(|i| format!("record-{i}")).collect();
    publish(
        &fx.log,
        &sent.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;

    let key = key();
    let mut seen = Vec::new();
    let now = Instant::now();
    loop {
        let batch = fx.reader.poll(&key, &fx.log, 7, now).await.expect("poll");
        if batch.is_empty() {
            break;
        }
        for claimed in batch {
            seen.push(String::from_utf8(claimed.payload.to_vec()).expect("utf8"));
            fx.reader.ack(&key, claimed.offset).await.expect("ack");
        }
    }

    assert_eq!(seen, sent);
    assert_eq!(
        fx.reader.committed(&key).await.expect("committed"),
        Some(50)
    );
}

/// **A poison record does not stall the queue.** After the attempt bound the
/// group gives up on it, records it as a dead letter, and moves on to the work
/// behind it.
#[tokio::test]
async fn a_record_that_is_never_acknowledged_is_dead_lettered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 2);
    publish(&fx.log, &["poison", "good"]).await;
    let key = key();
    let base = Instant::now();

    // Two deliveries of both, each abandoned.
    for round in 0..2 {
        let claimed = fx
            .reader
            .poll(&key, &fx.log, 10, base + Duration::from_secs(round * 31))
            .await
            .expect("poll");
        assert_eq!(claimed.len(), 2, "round {round}");
    }

    // The third poll gives up on both and moves the cursor past them.
    let after = fx
        .reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(3 * 31))
        .await
        .expect("poll");
    assert!(after.is_empty());
    assert_eq!(
        fx.reader.dead_lettered(&key).await.expect("dead letters"),
        vec![0, 1],
    );
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(2));
}

/// The record itself is still in the log at the offset that was recorded, so a
/// dead letter is a pointer rather than a copy — nothing is duplicated, and
/// nothing is lost.
#[tokio::test]
async fn a_dead_lettered_record_is_still_readable_from_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    publish(&fx.log, &["poison"]).await;
    let key = key();
    let base = Instant::now();

    fx.reader.poll(&key, &fx.log, 10, base).await.expect("poll");
    fx.reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");

    let dead = fx.reader.dead_lettered(&key).await.expect("dead letters");
    assert_eq!(dead, vec![0]);

    let records = fx.log.read_from(dead[0], 1).await.expect("read");
    assert_eq!(records[0].payload.as_ref(), b"poison");
}

/// A consumer is told how many times a record has been delivered, so it can
/// treat a retry differently from a first attempt.
#[tokio::test]
async fn a_redelivered_record_reports_its_attempt_number() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["work"]).await;
    let base = Instant::now();

    let first = fx
        .reader
        .poll(&key(), &fx.log, 10, base)
        .await
        .expect("poll");
    assert_eq!(first[0].attempts, 1);

    let again = fx
        .reader
        .poll(&key(), &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");
    assert_eq!(again[0].attempts, 2);
}
