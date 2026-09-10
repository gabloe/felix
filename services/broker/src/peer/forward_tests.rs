//! The retry rules, tested against an owner that answers on command.
//!
//! The question every case here asks is the one at the top of this module:
//! *could the owner already have applied this batch?* A real cluster produces
//! the "yes" answers rarely and never on demand, which is exactly why they are
//! driven from a script rather than from a peer.
use std::sync::Mutex;

use felix_wire::internal::{ErrorCode, ForwardPublishError, ForwardPublishOk, HelloOk, NotLeader};

use super::*;

/// An owner that answers from a script and records what it was asked.
struct ScriptedOwner {
    answers: Mutex<std::collections::VecDeque<std::result::Result<InternalMessage, PeerError>>>,
    asked: Mutex<Vec<(String, u64)>>,
}

impl ScriptedOwner {
    fn new(
        answers: impl IntoIterator<Item = std::result::Result<InternalMessage, PeerError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            asked: Mutex::new(Vec::new()),
        }
    }

    /// Who was asked, and at which generation.
    fn asked(&self) -> Vec<(String, u64)> {
        self.asked.lock().expect("lock").clone()
    }

    fn attempts(&self) -> usize {
        self.asked.lock().expect("lock").len()
    }
}

impl PeerRequester for ScriptedOwner {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let generation = match &message {
            InternalMessage::ForwardPublish(publish) => publish.shard.generation,
            other => panic!("forwarding sent a {:?}", other.kind()),
        };
        self.asked
            .lock()
            .expect("lock")
            .push((node_id.to_string(), generation));
        self.answers
            .lock()
            .expect("lock")
            .pop_front()
            .unwrap_or(Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "the script ran out".to_string(),
            }))
    }
}

fn target() -> ForwardTarget {
    ForwardTarget {
        node_id: "broker-b".to_string(),
        advertise_addr: "10.0.0.4:7002".parse().expect("addr"),
        generation: 4,
    }
}

fn key() -> ForwardKey {
    ForwardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
    }
}

fn accepted(first: u64, last: u64) -> InternalMessage {
    InternalMessage::ForwardPublishOk(ForwardPublishOk {
        correlation_id: 0,
        first_offset: first,
        last_offset: last,
    })
}

fn moved_to(node_id: &str, addr: &str, generation: u64) -> InternalMessage {
    InternalMessage::NotLeader(NotLeader {
        correlation_id: 0,
        node_id: node_id.to_string(),
        advertise_addr: addr.to_string(),
        generation,
    })
}

fn refused(code: ErrorCode) -> InternalMessage {
    InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id: 0,
        code,
        detail: "not now".to_string(),
    })
}

async fn forward(owner: &ScriptedOwner) -> std::result::Result<Option<(u64, u64)>, ForwardError> {
    forward_publish(
        owner,
        &target(),
        &key(),
        AckMode::OnCommit,
        vec![Bytes::from_static(b"an order")],
    )
    .await
}

/// The ordinary path: the owner accepts, and the offsets it assigned come back.
#[tokio::test]
async fn an_accepted_batch_returns_the_owners_offsets() {
    let owner = ScriptedOwner::new([Ok(accepted(10, 12))]);

    assert_eq!(forward(&owner).await.expect("accepted"), Some((10, 12)));
    assert_eq!(owner.attempts(), 1, "an accepted batch was sent twice");
    assert_eq!(owner.asked(), vec![("broker-b".to_string(), 4)]);
}

/// A redirect is followed, at the generation the new owner named — the refusal
/// itself is the evidence nothing was applied, so re-sending is a repair.
#[tokio::test]
async fn a_redirect_is_followed_to_the_new_owner() {
    let owner = ScriptedOwner::new([
        Ok(moved_to("broker-c", "10.0.0.5:7002", 5)),
        Ok(accepted(1, 1)),
    ]);

    assert_eq!(forward(&owner).await.expect("accepted"), Some((1, 1)));
    assert_eq!(
        owner.asked(),
        vec![("broker-b".to_string(), 4), ("broker-c".to_string(), 5)],
    );
}

/// **A redirect that does not advance the generation is refused.** Following it
/// would send the batch back where it came from, and the two brokers would
/// bounce it between them until the attempt budget ran out.
#[tokio::test]
async fn a_redirect_that_does_not_advance_the_generation_is_refused() {
    for generation in [3, 4] {
        let owner = ScriptedOwner::new([Ok(moved_to("broker-c", "10.0.0.5:7002", generation))]);

        let err = forward(&owner).await.expect_err("should be refused");
        assert!(
            matches!(err, ForwardError::Refused { .. }),
            "generation {generation}: {err}",
        );
        assert_eq!(owner.attempts(), 1, "the batch was bounced back");
    }
}

/// A redirect to an address that does not parse is a dead end, not something to
/// keep trying.
#[tokio::test]
async fn a_redirect_to_an_unusable_address_is_refused() {
    let owner = ScriptedOwner::new([Ok(moved_to("broker-c", "not-an-address", 5))]);

    let err = forward(&owner).await.expect_err("should be refused");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err}");
    assert!(err.to_string().contains("broker-c"), "{err}");
}

/// A refusal the owner can recover from is retried, within the budget.
#[tokio::test]
async fn a_retryable_refusal_is_retried() {
    let owner = ScriptedOwner::new([Ok(refused(ErrorCode::Unavailable)), Ok(accepted(3, 3))]);

    assert_eq!(forward(&owner).await.expect("accepted"), Some((3, 3)));
    assert_eq!(owner.attempts(), 2);
}

/// **A storage failure is never retried.** The owner accepted the batch and
/// then failed to write it, so it knows about the batch; sending it again is a
/// second write, not a repair.
#[tokio::test]
async fn a_write_that_failed_on_the_owner_is_not_retried() {
    let owner = ScriptedOwner::new([Ok(refused(ErrorCode::StorageFailed))]);

    let err = forward(&owner).await.expect_err("should be refused");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err}");
    assert_eq!(owner.attempts(), 1, "a failed write was sent again");
}

/// **A lost answer is never retried, for any stream.** The batch may be on the
/// owner's disk and this broker cannot tell, so it says so rather than risking
/// a duplicate the client cannot see.
#[tokio::test]
async fn a_lost_answer_is_reported_as_indeterminate_rather_than_retried() {
    for lost in [
        PeerError::Disconnected {
            node_id: "broker-b".to_string(),
        },
        PeerError::Timeout {
            node_id: "broker-b".to_string(),
            timeout: Duration::from_secs(5),
        },
    ] {
        let owner = ScriptedOwner::new([Err(lost)]);

        let err = forward(&owner).await.expect_err("should be indeterminate");
        assert!(
            matches!(err, ForwardError::Indeterminate { .. }),
            "a batch that may have landed was reported as {err}",
        );
        assert_eq!(
            owner.attempts(),
            1,
            "a batch that may have landed was resent"
        );
    }
}

/// Nothing was sent, so the batch can go again.
#[tokio::test]
async fn a_request_that_was_never_sent_is_retried() {
    let owner = ScriptedOwner::new([
        Err(PeerError::Unavailable {
            node_id: "broker-b".to_string(),
            detail: "in backoff".to_string(),
        }),
        Ok(accepted(9, 9)),
    ]);

    assert_eq!(forward(&owner).await.expect("accepted"), Some((9, 9)));
    assert_eq!(owner.attempts(), 2);
}

/// A handshake refusal is a configuration problem, not a transient one. It ends
/// the forward rather than consuming the budget.
#[tokio::test]
async fn a_refused_handshake_ends_the_forward() {
    let owner = ScriptedOwner::new([Err(PeerError::Handshake {
        node_id: "broker-b".to_string(),
        detail: "unknown peer".to_string(),
    })]);

    let err = forward(&owner).await.expect_err("should be refused");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err}");
    assert_eq!(owner.attempts(), 1);
}

/// **Retries are bounded.** A shard being reassigned faster than a publish can
/// complete fails explicitly rather than chasing ownership around the cluster.
#[tokio::test]
async fn retries_are_bounded_rather_than_chasing_ownership() {
    let owner = ScriptedOwner::new((0..10).map(|_| Ok(refused(ErrorCode::Unavailable))));

    let err = forward(&owner).await.expect_err("should give up");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err}");
    assert_eq!(owner.attempts(), MAX_ATTEMPTS as usize);
    assert!(err.to_string().contains("orders"), "{err}");
}

/// An answer that is not part of this exchange is refused rather than read as
/// success. Nothing else in the protocol answers a forwarded publish.
#[tokio::test]
async fn an_unexpected_answer_is_refused() {
    let owner = ScriptedOwner::new([Ok(InternalMessage::HelloOk(HelloOk {
        correlation_id: 0,
        node_id: "broker-b".to_string(),
    }))]);

    let err = forward(&owner).await.expect_err("should be refused");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err}");
    assert_eq!(owner.attempts(), 1);
}

/// The pause between attempts grows, and stays small: the client is waiting,
/// and the attempt budget is the real bound.
#[test]
fn the_pause_between_attempts_grows_but_stays_short() {
    assert!(retry_delay(0) < retry_delay(1));
    assert!(retry_delay(1) < retry_delay(2));
    assert!(
        retry_delay(u32::MAX) <= Duration::from_millis(100),
        "the backoff outgrew the request the client is waiting on",
    );
}
