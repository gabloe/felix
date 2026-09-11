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
    /// Base offsets this follower was offered, as distinct from records sent.
    offered: Mutex<Vec<u64>>,
}

impl ScriptedFollower {
    fn new(
        answers: impl IntoIterator<Item = std::result::Result<InternalMessage, PeerError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            sent: Mutex::new(Vec::new()),
            offered: Mutex::new(Vec::new()),
        }
    }

    /// Base offsets this follower was offered.
    fn offered(&self) -> Vec<u64> {
        self.offered.lock().expect("lock").clone()
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
            InternalMessage::ReplicateBootstrap(request) => {
                self.offered.lock().expect("lock").push(request.base_offset)
            }
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

/// The majority a `Quorum` acknowledgement rests on.
mod quorum {
    use super::*;

    /// A majority always includes the leader, so a set of one is satisfied by
    /// the leader alone — which is why `replication_factor: 1` costs nothing.
    #[test]
    fn a_majority_counts_the_leader() {
        assert_eq!(majority_of(0), 1, "a set of one is the leader alone");
        assert_eq!(majority_of(1), 2, "a set of two needs both");
        assert_eq!(majority_of(2), 2, "a set of three needs two");
        assert_eq!(majority_of(3), 3, "a set of four needs three");
        assert_eq!(majority_of(4), 3, "a set of five needs three");
    }

    /// **A majority is more than half.** Half of an even set is not a majority:
    /// two disjoint halves could each acknowledge a different record, and both
    /// would believe they had a quorum.
    #[test]
    fn half_of_an_even_set_is_not_a_majority() {
        for replicas in [1usize, 3, 5] {
            let set = replicas + 1;
            assert!(
                majority_of(replicas) * 2 > set,
                "a set of {set} accepted {} as a majority",
                majority_of(replicas),
            );
        }
    }

    /// With no followers the leader's own tail is the quorum point, so a
    /// `Quorum` stream on an unreplicated shard behaves exactly like `Leader`
    /// rather than never acknowledging.
    #[test]
    fn the_leader_alone_is_a_quorum_of_one() {
        assert_eq!(quorum_offset(10, &[]), 10);
    }

    /// Two of three: the quorum point is where the faster follower has got to,
    /// not the slower one.
    #[test]
    fn a_set_of_three_advances_with_its_faster_follower() {
        let followers = vec![cursor(8), cursor(3)];

        assert_eq!(quorum_offset(10, &followers), 8);
    }

    /// All of two: the quorum point is the slower of the pair, because a set of
    /// two needs both.
    #[test]
    fn a_set_of_two_waits_for_its_only_follower() {
        assert_eq!(quorum_offset(10, &[cursor(4)]), 4);
    }

    /// A follower reporting past the leader's tail does not drag the quorum
    /// point beyond what the leader actually holds.
    #[test]
    fn a_follower_ahead_of_the_leader_does_not_overstate_the_quorum() {
        assert_eq!(quorum_offset(10, &[cursor(50), cursor(9)]), 10);
    }

    /// **A halted follower counts for nothing.** It has stopped rather than
    /// fallen behind, and letting its last position count toward a majority
    /// makes an acknowledgement mean less than it says.
    #[test]
    fn a_halted_follower_does_not_count_toward_the_majority() {
        let mut halted = cursor(10);
        halted.halted = Some(Halt::Diverged);
        let followers = vec![halted, cursor(3)];

        assert_eq!(
            quorum_offset(10, &followers),
            3,
            "a diverged follower was counted toward the quorum",
        );
    }

    /// Every follower halted leaves the leader alone, which is not a majority
    /// of three — so nothing new reaches the quorum point.
    #[test]
    fn a_shard_with_every_follower_halted_stops_acknowledging() {
        let mut a = cursor(10);
        a.halted = Some(Halt::Diverged);
        let mut b = cursor(10);
        b.halted = Some(Halt::Fenced);

        assert_eq!(
            quorum_offset(10, &[a, b]),
            0,
            "a quorum was claimed with only the leader in a set of three",
        );
    }

    /// A follower that has stored nothing holds offset zero, so a set of three
    /// with one empty follower still has a quorum through the other.
    #[test]
    fn an_empty_follower_holds_nothing_but_still_counts_as_a_member() {
        assert_eq!(quorum_offset(10, &[cursor(0), cursor(7)]), 7);
    }
}

/// A follower asking for records the leader has already trimmed.
///
/// Shipping cannot bridge this: the records are gone from the leader too. The
/// leader offers the follower the one fact it cannot work out for itself —
/// where the surviving log begins — and the follower decides whether it can
/// take it.
mod trimmed_history {
    use super::*;

    /// A leader whose retention has already removed the start of its log.
    async fn trimmed_leader() -> (StreamLog, u64, TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = DurableStorage::open(
            dir.path(),
            LogConfig {
                // Tiny segments and a tight bound, so a handful of records
                // forces the trim a long-lived stream would reach in time.
                segment_size_bytes: 128,
                index_spacing_bytes: 64,
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                retention_bytes: Some(256),
                ..LogConfig::default()
            },
        )
        .expect("storage");
        let log = storage
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        for i in 0..40 {
            log.append(&[Bytes::from(format!("value-{i:03}"))])
                .await
                .expect("append");
        }
        log.enforce_retention_now().await.expect("retention");
        let base = log.base_offset();
        assert!(
            base > 0,
            "retention did not trim, so the case is not set up"
        );
        (log, base, dir)
    }

    /// What the follower was asked, rather than sent.
    fn offers(follower: &ScriptedFollower) -> Vec<u64> {
        follower.offered()
    }

    /// **The leader offers a bootstrap rather than shipping a range it does not
    /// have.** The offer names the oldest offset this leader still holds.
    #[tokio::test]
    async fn a_follower_below_the_leaders_base_is_offered_a_bootstrap() {
        let (log, base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Ok(stored(base))]);
        let mut cursor = cursor(0);

        let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        assert_eq!(progress, Progress::Resume { offset: base });
        assert_eq!(offers(&follower), vec![base]);
        assert!(
            follower.sent().is_empty(),
            "records were shipped from a range the leader no longer holds",
        );
    }

    /// Once the follower has taken the offer, the cursor sits at the surviving
    /// base and ordinary shipping resumes from there.
    #[tokio::test]
    async fn an_accepted_bootstrap_resumes_ordinary_shipping() {
        let (log, base, _dir) = trimmed_leader().await;
        let tail = log.tail_offset().await.expect("tail");
        let follower = ScriptedFollower::new([Ok(stored(base)), Ok(stored(tail))]);
        let mut cursor = cursor(0);

        ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;
        assert_eq!(cursor.next_offset, base);

        let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        assert!(matches!(progress, Progress::Stored { .. }), "{progress:?}");
        let (first, _) = follower.sent().into_iter().next().expect("a batch");
        assert_eq!(first, base, "shipping did not resume at the surviving base");
    }

    /// **A follower that refuses the offer is halted.** It holds records of its
    /// own, so a person has to decide what becomes of them; asking again would
    /// never change the answer.
    #[tokio::test]
    async fn a_refused_bootstrap_halts_the_follower() {
        let (log, _base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
        let mut cursor = cursor(0);

        let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        assert_eq!(progress, Progress::Halted(Halt::NeedsBootstrap));
        assert_eq!(cursor.halted, Some(Halt::NeedsBootstrap));
    }

    /// And it stays halted: a refusal is not retried on the next pass.
    #[tokio::test]
    async fn a_halted_follower_is_not_offered_again() {
        let (log, _base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
        let mut cursor = cursor(0);

        for _ in 0..3 {
            ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;
        }

        assert_eq!(offers(&follower).len(), 1, "a refused offer was repeated");
    }

    /// An unreachable follower keeps its offer open: nothing about the network
    /// says the follower would refuse.
    #[tokio::test]
    async fn an_unreachable_follower_keeps_its_offer_open() {
        let (log, _base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Err(PeerError::Timeout {
            node_id: "broker-b".to_string(),
            timeout: std::time::Duration::from_secs(5),
        })]);
        let mut cursor = cursor(0);

        let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        assert_eq!(progress, Progress::Retry);
        assert!(cursor.halted.is_none(), "a timeout halted replication");
    }

    /// **A follower still being offered a bootstrap counts toward no quorum.**
    /// It is not merely behind; it cannot arrive until it accepts.
    #[tokio::test]
    async fn a_follower_that_refused_counts_toward_no_quorum() {
        let (log, _base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
        let mut cursor = cursor(0);
        ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        let tail = log.tail_offset().await.expect("tail");
        assert_eq!(quorum_offset(tail, &[cursor.clone(), super::cursor(0)]), 0);
    }

    /// A follower at or above the leader's base is ordinary catch-up, not a
    /// bootstrap. The line is exactly the base offset.
    #[tokio::test]
    async fn a_follower_at_the_base_is_ordinary_catch_up() {
        let (log, base, _dir) = trimmed_leader().await;
        let follower = ScriptedFollower::new([Ok(stored(base + 1))]);
        let mut cursor = cursor(base);

        let progress = ship_once(&follower, &log, &shard(), &mut cursor, BATCH_BYTES).await;

        assert!(
            matches!(progress, Progress::Stored { .. }),
            "a follower at the surviving base was offered a bootstrap: {progress:?}",
        );
        assert!(offers(&follower).is_empty());
    }
}

/// Which followers the leader reports as fit to lead.
mod eligibility {
    use super::*;

    #[test]
    fn a_follower_level_with_the_leader_is_caught_up() {
        assert_eq!(caught_up(10, &[cursor(10)]), vec!["broker-b".to_string()]);
    }

    /// **Behind is not caught up.** The bound is zero, so a follower missing
    /// even one record cannot lead: promoting it would lose that record, and a
    /// bound above zero is a decision about how much loss is acceptable that
    /// nobody has made.
    #[test]
    fn a_follower_missing_even_one_record_is_not_caught_up() {
        assert!(caught_up(10, &[cursor(9)]).is_empty());
    }

    /// **A halted follower never qualifies**, however close it was. It has
    /// stopped rather than fallen behind, so its position is not moving toward
    /// the leader's and never will without intervention.
    #[test]
    fn a_halted_follower_is_never_reported_as_caught_up() {
        for halt in [Halt::Diverged, Halt::Fenced, Halt::NeedsBootstrap] {
            let mut stopped = cursor(10);
            stopped.halted = Some(halt);

            assert!(
                caught_up(10, &[stopped]).is_empty(),
                "{halt:?} was reported as fit to lead",
            );
        }
    }

    /// A follower reporting past the leader's tail is caught up rather than
    /// rejected: it happens while an answer is in flight.
    #[test]
    fn a_follower_ahead_of_the_tail_is_caught_up() {
        assert_eq!(caught_up(10, &[cursor(11)]), vec!["broker-b".to_string()]);
    }

    #[test]
    fn only_the_caught_up_followers_are_named() {
        let mut behind = cursor(3);
        behind.node_id = "broker-c".to_string();

        assert_eq!(
            caught_up(10, &[cursor(10), behind]),
            vec!["broker-b".to_string()]
        );
    }

    /// An empty replica set reports nobody, rather than the leader itself.
    #[test]
    fn a_shard_with_no_followers_reports_nobody() {
        assert!(caught_up(10, &[]).is_empty());
    }
}
