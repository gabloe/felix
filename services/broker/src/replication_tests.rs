//! Shipping, against a follower that answers on command and a real log.
//!
//! The subject is the cursor: where the leader believes a follower is, and what
//! is allowed to move that belief. A real cluster produces the interesting
//! answers rarely and never on demand, so they come from a script.
use std::sync::Mutex;

use felix_broker::DurableStorage;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{ReplicateError, ReplicateOk};
use tempfile::TempDir;

use super::*;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const GENERATION: u64 = 4;

/// A follower that answers from a script and records what it was sent.
struct ScriptedFollower {
    answers: Mutex<std::collections::VecDeque<std::result::Result<InternalMessage, PeerError>>>,
    sent: Mutex<Vec<ReplicateRecords>>,
}

impl ScriptedFollower {
    fn new(
        answers: impl IntoIterator<Item = std::result::Result<InternalMessage, PeerError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            sent: Mutex::new(Vec::new()),
        }
    }

    /// The offsets and payloads of every batch this follower was sent.
    fn sent(&self) -> Vec<(u64, Vec<String>)> {
        self.sent
            .lock()
            .expect("lock")
            .iter()
            .map(|batch| {
                (
                    batch.first_offset,
                    batch
                        .payloads
                        .iter()
                        .map(|p| String::from_utf8(p.to_vec()).expect("utf8"))
                        .collect(),
                )
            })
            .collect()
    }
}

impl PeerRequester for ScriptedFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        match message {
            InternalMessage::ReplicateRecords(batch) => self.sent.lock().expect("lock").push(batch),
            other => panic!("shipping sent a {:?}", other.kind()),
        }
        self.answers
            .lock()
            .expect("lock")
            .pop_front()
            .unwrap_or(Err(PeerError::Unavailable {
                node_id: "broker-b".to_string(),
                detail: "the script ran out".to_string(),
            }))
    }
}

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        generation: GENERATION,
    }
}

fn cursor(next_offset: u64) -> FollowerCursor {
    FollowerCursor::new(
        "broker-b",
        "10.0.0.4:7002".parse().expect("addr"),
        next_offset,
    )
}

fn stored(durable_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateOk(ReplicateOk {
        correlation_id: 0,
        durable_offset,
    })
}

fn refused(code: ErrorCode, expected_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id: 0,
        code,
        expected_offset,
        detail: "no".to_string(),
    })
}

/// A leader's log holding `values` at offsets 0..n.
async fn leader_log(values: &[&str]) -> (StreamLog, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
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
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for value in values {
        log.append(&[Bytes::copy_from_slice(value.as_bytes())])
            .await
            .expect("append");
    }
    (log, dir)
}

const BATCH_BYTES: usize = 1024 * 1024;

/// The ordinary case: ship from where the follower is, and move the cursor to
/// where the follower says it got to.
#[tokio::test]
async fn a_stored_batch_advances_the_cursor_to_what_the_follower_reported() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    let follower = ScriptedFollower::new([Ok(stored(3))]);
    let mut cursor = cursor(0);

    let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(progress, Progress::Stored { durable_offset: 3 });
    assert_eq!(cursor.next_offset, 3);
    assert_eq!(follower.sent(), vec![(0, vec_of(&["a", "b", "c"]))]);
}

/// **The follower's number wins, not the leader's arithmetic.** The follower
/// did the writing, and a partially applied batch would otherwise leave the two
/// sides disagreeing about what is stored.
#[tokio::test]
async fn the_cursor_follows_the_follower_rather_than_the_batch_size() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    // Sent three, but the follower reports holding only two.
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(cursor.next_offset, 2, "the leader trusted its own count");
}

/// **A gap rewinds the cursor.** A follower that lost records, or was rebuilt,
/// names the offset it wants and the leader resumes there — no separate
/// negotiation, and nothing kept on disk.
#[tokio::test]
async fn a_gap_rewinds_the_cursor_and_the_next_batch_starts_there() {
    let (log, _dir) = leader_log(&["a", "b", "c", "d"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogGap, 1)), Ok(stored(4))]);
    let mut cursor = cursor(3);

    let first = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;
    assert_eq!(first, Progress::Resume { offset: 1 });
    assert_eq!(cursor.next_offset, 1);

    ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(
        follower.sent(),
        vec![(3, vec_of(&["d"])), (1, vec_of(&["b", "c", "d"])),],
        "the resend did not start where the follower asked",
    );
}

/// **Divergence stops this follower.** Two logs that disagree about stored
/// bytes do not converge by retrying, and continuing would ship records on top
/// of a log that is already wrong.
#[tokio::test]
async fn divergence_halts_the_follower_and_nothing_more_is_sent() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
    let mut cursor = cursor(0);

    let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;
    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert_eq!(cursor.halted, Some(Halt::Diverged));

    // A halted follower is not shipped to again, even when asked.
    let again = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(again, Progress::Halted(Halt::Diverged));
    assert_eq!(follower.sent().len(), 1, "a halted follower was shipped to");
}

/// **A fenced leader stops shipping.** The follower knows a newer generation,
/// so this broker is not the leader and has no business sending anything.
#[tokio::test]
async fn being_fenced_halts_the_follower() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::FencedEpoch, 0))]);
    let mut cursor = cursor(0);

    let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(progress, Progress::Halted(Halt::Fenced));
    assert_eq!(cursor.halted, Some(Halt::Fenced));
}

/// A refusal that resolves on its own leaves the cursor where it was, so the
/// same batch goes again.
#[tokio::test]
async fn a_transient_refusal_leaves_the_cursor_alone() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::StaleRoute, 0)), Ok(stored(2))]);
    let mut cursor = cursor(0);

    assert_eq!(
        ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await,
        Progress::Retry,
    );
    assert_eq!(
        cursor.next_offset, 0,
        "a transient refusal moved the cursor"
    );

    ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;
    assert_eq!(
        follower.sent(),
        vec![(0, vec_of(&["a", "b"])), (0, vec_of(&["a", "b"]))],
    );
}

/// An unreachable follower is retried, and — importantly — is not mistaken for
/// one that refused. Nothing was stored, so the cursor stays put.
#[tokio::test]
async fn an_unreachable_follower_is_retried_without_moving_the_cursor() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Err(PeerError::Timeout {
        node_id: "broker-b".to_string(),
        timeout: std::time::Duration::from_secs(5),
    })]);
    let mut cursor = cursor(0);

    assert_eq!(
        ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await,
        Progress::Retry,
    );
    assert_eq!(cursor.next_offset, 0);
    assert!(cursor.halted.is_none(), "a timeout halted replication");
}

/// A follower level with the leader is not shipped an empty batch.
#[tokio::test]
async fn a_follower_that_is_level_is_not_shipped_to() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([]);
    let mut cursor = cursor(2);

    let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    assert_eq!(progress, Progress::UpToDate);
    assert!(follower.sent().is_empty(), "an empty batch was shipped");
}

/// **A batch is bounded, so a follower far behind costs one batch of memory
/// rather than the distance it is behind.**
#[tokio::test]
async fn a_batch_is_bounded_rather_than_the_whole_backlog() {
    let (log, _dir) = leader_log(&["a", "b", "c", "d", "e", "f"]).await;
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    // A budget smaller than the backlog's payload bytes, so the read has to
    // stop short of the tail.
    ship_once(&follower, &log, &shard(), &mut cursor, 2).await;

    let (_, payloads) = follower.sent().into_iter().next().expect("a batch");
    assert!(
        !payloads.is_empty(),
        "a budget smaller than one record shipped nothing at all",
    );
    assert!(
        payloads.len() < 6,
        "the whole backlog was read into one batch: {payloads:?}",
    );
}

/// The batch carries the checksum the follower will recompute. If these ever
/// disagreed, every healthy batch would be reported as corrupt.
#[tokio::test]
async fn the_batch_carries_the_checksum_the_follower_will_verify() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

    let batch = follower.sent.lock().expect("lock")[0].clone();
    assert_eq!(batch.checksum, batch_checksum(&batch.payloads));
    assert_eq!(
        batch.shard.generation, GENERATION,
        "the epoch was not carried"
    );
}

fn vec_of(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

/// Reading an answer, on its own. Getting one of these backwards either
/// hammers a diverged follower or abandons a healthy one.
mod answers {
    use super::*;

    #[test]
    fn each_answer_means_one_thing() {
        assert_eq!(
            read_answer(&stored(9)),
            Progress::Stored { durable_offset: 9 },
        );
        assert_eq!(
            read_answer(&refused(ErrorCode::LogGap, 4)),
            Progress::Resume { offset: 4 },
        );
        assert_eq!(
            read_answer(&refused(ErrorCode::LogConflict, 0)),
            Progress::Halted(Halt::Diverged),
        );
        assert_eq!(
            read_answer(&refused(ErrorCode::FencedEpoch, 0)),
            Progress::Halted(Halt::Fenced),
        );
    }

    /// Everything else is this moment rather than this pairing.
    #[test]
    fn the_recoverable_refusals_are_retried() {
        for code in [
            ErrorCode::StaleRoute,
            ErrorCode::Unavailable,
            ErrorCode::Overload,
            ErrorCode::Malformed,
            ErrorCode::StorageFailed,
            ErrorCode::Unauthorized,
        ] {
            assert_eq!(read_answer(&refused(code, 0)), Progress::Retry, "{code:?}");
        }
    }

    /// An answer that is not part of this exchange is retried rather than read
    /// as divergence — stopping replication over a protocol confusion would be
    /// the worse mistake.
    #[test]
    fn an_unexpected_answer_is_retried() {
        assert_eq!(
            read_answer(&InternalMessage::HelloOk(felix_wire::internal::HelloOk {
                correlation_id: 0,
                node_id: "broker-b".to_string(),
            })),
            Progress::Retry,
        );
    }
}

/// The lag figure an operator reads to size the `Leader` loss window.
mod lag {
    use super::*;

    #[test]
    fn lag_is_measured_against_the_slowest_follower() {
        let mut behind = cursor(10);
        behind.node_id = "broker-c".to_string();
        let followers = vec![cursor(90), behind];

        assert_eq!(lag_records(100, &followers), Some(90));
    }

    #[test]
    fn a_caught_up_follower_reports_no_lag() {
        assert_eq!(lag_records(100, &[cursor(100)]), Some(0));
    }

    /// A follower ahead of this leader's tail is not negative lag. It happens
    /// while an answer is in flight, and wrapping would report an enormous one.
    #[test]
    fn a_follower_ahead_of_the_tail_does_not_wrap() {
        assert_eq!(lag_records(100, &[cursor(105)]), Some(0));
    }

    /// **A halted follower is not lagging, it has stopped.** Folding the two
    /// together hides a stopped follower behind a number that merely looks
    /// large, and the two need different responses.
    #[test]
    fn a_halted_follower_is_left_out_of_the_lag() {
        let mut halted = cursor(0);
        halted.halted = Some(Halt::Diverged);

        assert_eq!(lag_records(100, &[cursor(95), halted]), Some(5));
    }

    #[test]
    fn no_followers_means_no_lag_to_report() {
        assert_eq!(lag_records(100, &[]), None);
    }
}
