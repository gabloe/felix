//! The pool's policy, tested without a peer on the other end.
//!
//! What a real peer would add here is a socket, and the rules below do not
//! depend on one: they decide which connection a request lands on, when a
//! refused peer may be dialled again, and which connections a reaper may take
//! away. Those are the parts that misbehave under load, and they are all
//! decisions about local state.
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// A connection with no socket behind it, whose liveness, pending work, and age
/// are set by the test rather than by a peer.
struct FakeConnection {
    live: AtomicBool,
    pending: AtomicBool,
    idle_for: Duration,
    closed_with: Mutex<Option<String>>,
}

impl FakeConnection {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            pending: AtomicBool::new(false),
            idle_for: Duration::ZERO,
            closed_with: Mutex::new(None),
        })
    }

    fn idle(idle_for: Duration) -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            pending: AtomicBool::new(false),
            idle_for,
            closed_with: Mutex::new(None),
        })
    }

    fn die(&self) {
        self.live.store(false, Ordering::Release);
    }

    fn closed(&self) -> Option<String> {
        self.closed_with.lock().clone()
    }
}

impl Pooled for FakeConnection {
    fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    fn idle_for(&self) -> Duration {
        self.idle_for
    }

    fn close(&self, reason: &str) {
        *self.closed_with.lock() = Some(reason.to_string());
    }
}

fn state(connections: Vec<Arc<FakeConnection>>) -> PeerState<FakeConnection> {
    PeerState {
        connections,
        ..Default::default()
    }
}

/// Which connection a request lands on.
mod selection {
    use super::*;

    /// Growing to the configured size takes precedence over reuse. A pool of
    /// one available connection would otherwise never open a second, because
    /// the first is always there to hand back.
    #[test]
    fn a_pool_below_its_target_asks_for_another_connection() {
        let mut state = state(vec![FakeConnection::new()]);
        assert!(state.next(2).is_none());
    }

    #[test]
    fn a_full_pool_hands_back_a_connection() {
        let connection = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&connection)]);
        assert!(Arc::ptr_eq(
            &state.next(1).expect("a connection"),
            &connection
        ));
    }

    /// Requests spread across the connections rather than piling onto one: a
    /// QUIC stream is ordered, so a large batch on the same connection holds up
    /// everything queued behind it.
    #[test]
    fn requests_are_spread_across_the_connections() {
        let a = FakeConnection::new();
        let b = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&a), Arc::clone(&b)]);

        let handed_out: Vec<Arc<FakeConnection>> = (0..4)
            .map(|_| state.next(2).expect("a connection"))
            .collect();

        assert!(Arc::ptr_eq(&handed_out[0], &a));
        assert!(Arc::ptr_eq(&handed_out[1], &b));
        assert!(Arc::ptr_eq(&handed_out[2], &a), "the cursor should wrap");
        assert!(Arc::ptr_eq(&handed_out[3], &b));
    }

    /// The cursor is only ever advanced, so it has to survive passing `usize`'s
    /// end without indexing out of the pool.
    #[test]
    fn a_wrapped_cursor_still_selects_a_connection() {
        let mut state = state(vec![FakeConnection::new(), FakeConnection::new()]);
        state.cursor = usize::MAX;
        assert!(state.next(2).is_some());
        assert!(state.next(2).is_some(), "the cursor wrapped past the end");
    }

    /// An empty pool asks for a connection rather than dividing by its own
    /// length. A target of zero cannot reach here from configuration, which
    /// filters it out, but the remainder in `next` is one step from a panic on
    /// the forwarding path either way.
    #[test]
    fn an_empty_pool_has_nothing_to_hand_back() {
        let mut state = state(Vec::new());
        assert!(state.next(1).is_none());
        assert!(
            state.next(0).is_none(),
            "an empty pool divided by its own length"
        );
    }
}

/// Connections the pool must stop handing out.
mod reaping {
    use super::*;

    #[test]
    fn a_dead_connection_is_dropped_and_the_live_ones_stay() {
        let dead = FakeConnection::new();
        let live = FakeConnection::new();
        dead.die();
        let mut state = state(vec![Arc::clone(&dead), Arc::clone(&live)]);

        state.drop_dead();

        assert_eq!(state.connections.len(), 1);
        assert!(Arc::ptr_eq(&state.connections[0], &live));
    }

    #[test]
    fn a_dead_connection_is_not_counted_as_live() {
        let dead = FakeConnection::new();
        dead.die();
        let state = state(vec![dead, FakeConnection::new()]);
        assert_eq!(state.live_connections(), 1);
    }

    #[test]
    fn an_idle_connection_is_closed_and_evicted() {
        let idle = FakeConnection::idle(Duration::from_secs(60));
        let mut state = state(vec![Arc::clone(&idle)]);

        state.evict_idle(Duration::from_secs(30));

        assert!(state.connections.is_empty());
        assert_eq!(idle.closed().as_deref(), Some("idle"));
    }

    #[test]
    fn a_connection_younger_than_the_timeout_is_kept() {
        let young = FakeConnection::idle(Duration::from_secs(10));
        let mut state = state(vec![Arc::clone(&young)]);

        state.evict_idle(Duration::from_secs(30));

        assert_eq!(state.connections.len(), 1);
        assert_eq!(young.closed(), None);
    }

    /// **A connection with work outstanding is not idle**, however long ago it
    /// was last handed out. Reaping it would fail waiters that a healthy peer is
    /// still answering.
    #[test]
    fn a_connection_awaiting_a_response_is_not_reaped_however_old() {
        let waiting = FakeConnection::idle(Duration::from_secs(600));
        waiting.pending.store(true, Ordering::Release);
        let mut state = state(vec![Arc::clone(&waiting)]);

        state.evict_idle(Duration::from_secs(30));

        assert_eq!(
            state.connections.len(),
            1,
            "a connection with a waiter was reaped"
        );
        assert_eq!(waiting.closed(), None);
    }

    #[test]
    fn closing_the_peer_closes_every_connection_with_the_reason() {
        let a = FakeConnection::new();
        let b = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&a), Arc::clone(&b)]);

        state.close_all("shutting down");

        assert!(state.connections.is_empty());
        assert_eq!(a.closed().as_deref(), Some("shutting down"));
        assert_eq!(b.closed().as_deref(), Some("shutting down"));
    }
}

/// The window in which a refused peer is not dialled again.
mod backoff {
    use super::*;

    #[test]
    fn a_peer_that_has_not_failed_is_not_in_backoff() {
        let state = state(Vec::new());
        assert!(!state.in_backoff());
        assert_eq!(state.backoff_remaining(), None);
    }

    #[test]
    fn a_peer_inside_its_window_is_in_backoff() {
        let mut state = state(Vec::new());
        state.backoff_until = Some(Instant::now() + Duration::from_secs(30));

        assert!(state.in_backoff());
        assert!(state.backoff_remaining().expect("remaining") <= Duration::from_secs(30));
    }

    /// An elapsed deadline reads as "dial again", not as a huge remaining wait.
    /// Subtracting the other way round would leave a peer unreachable for as
    /// long as it had been unreachable already.
    #[test]
    fn an_elapsed_window_is_over_rather_than_wrapping() {
        let mut state = state(Vec::new());
        state.backoff_until = Some(Instant::now() - Duration::from_secs(30));

        assert!(!state.in_backoff());
        assert_eq!(state.backoff_remaining(), None);
    }
}

/// The error a caller sees, and what it is allowed to do next.
mod errors {
    use super::*;

    /// **Only a request that was never sent may be retried.** `Disconnected`
    /// and `Timeout` mean the peer may have applied the write before the answer
    /// was lost, so an automatic retry is a duplicate publish, not a repair.
    #[test]
    fn only_a_request_that_was_never_sent_is_retryable() {
        assert!(
            PeerError::Unavailable {
                node_id: "broker-b".to_string(),
                detail: "in backoff".to_string(),
            }
            .is_retryable()
        );

        for error in [
            PeerError::Disconnected {
                node_id: "broker-b".to_string(),
            },
            PeerError::Timeout {
                node_id: "broker-b".to_string(),
                timeout: Duration::from_secs(5),
            },
            PeerError::Handshake {
                node_id: "broker-b".to_string(),
                detail: "refused".to_string(),
            },
            PeerError::ShuttingDown,
        ] {
            assert!(!error.is_retryable(), "{error} should not be retryable");
        }
    }

    /// The labels separate the failures an operator responds to differently:
    /// nothing was sent, the handshake was refused, the connection dropped
    /// mid-request, or no answer came. `ShuttingDown` shares "nothing was sent"
    /// with `Unavailable` on purpose — to the caller they are the same answer,
    /// and a shutdown is already visible elsewhere.
    #[test]
    fn the_labels_separate_the_failures_an_operator_acts_on() {
        let nothing_sent = PeerError::Unavailable {
            node_id: "b".to_string(),
            detail: String::new(),
        }
        .outcome();
        assert_eq!(PeerError::ShuttingDown.outcome(), nothing_sent);

        let distinct = [
            nothing_sent,
            PeerError::Handshake {
                node_id: "b".to_string(),
                detail: String::new(),
            }
            .outcome(),
            PeerError::Disconnected {
                node_id: "b".to_string(),
            }
            .outcome(),
            PeerError::Timeout {
                node_id: "b".to_string(),
                timeout: Duration::ZERO,
            }
            .outcome(),
        ];
        let mut unique = distinct.to_vec();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            distinct.len(),
            "two failures an operator acts on differently share a label: {distinct:?}"
        );
    }

    /// The peer's identity has to survive into the message; an operator reading
    /// one of these needs to know which broker it is about.
    #[test]
    fn the_peer_is_named_in_the_message() {
        let error = PeerError::Timeout {
            node_id: "broker-b".to_string(),
            timeout: Duration::from_secs(5),
        };
        assert!(error.to_string().contains("broker-b"), "{error}");
    }
}

/// Correlation ids belong to the connection, not to the caller.
mod correlation {
    use super::*;
    use felix_wire::internal::*;

    fn correlation_of(message: &InternalMessage) -> u64 {
        match message {
            InternalMessage::ForwardPublish(m) => m.correlation_id,
            InternalMessage::ForwardPublishOk(m) => m.correlation_id,
            InternalMessage::ForwardPublishError(m) => m.correlation_id,
            InternalMessage::NotLeader(m) => m.correlation_id,
            InternalMessage::Hello(m) => m.correlation_id,
            InternalMessage::HelloOk(m) => m.correlation_id,
        }
    }

    fn shard() -> ShardRef {
        ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation: 4,
        }
    }

    /// Every variant, each carrying the caller's id.
    fn every_variant() -> Vec<InternalMessage> {
        vec![
            InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 7,
                shard: shard(),
                ack: AckMode::OnCommit,
                payloads: vec![Bytes::from_static(b"hello")],
            }),
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id: 7,
                first_offset: 1,
                last_offset: 1,
            }),
            InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: 7,
                code: ErrorCode::StaleRoute,
                detail: "elsewhere".to_string(),
            }),
            InternalMessage::NotLeader(NotLeader {
                correlation_id: 7,
                node_id: "broker-c".to_string(),
                advertise_addr: "10.0.0.5:7002".to_string(),
                generation: 5,
            }),
            InternalMessage::Hello(Hello {
                correlation_id: 7,
                node_id: "broker-a".to_string(),
            }),
            InternalMessage::HelloOk(HelloOk {
                correlation_id: 7,
                node_id: "broker-b".to_string(),
            }),
        ]
    }

    /// **Every variant must be stamped.** One that kept the caller's id would be
    /// matched to the wrong waiter, which is worse than losing it: a response
    /// would be handed to a different request.
    #[test]
    fn every_message_carries_the_connections_id_rather_than_the_callers() {
        for message in every_variant() {
            let stamped = with_correlation(message, 99);
            assert_eq!(
                correlation_of(&stamped),
                99,
                "{stamped:?} kept the caller's id"
            );
        }
    }

    /// Stamping changes the id and nothing else.
    #[test]
    fn stamping_leaves_the_rest_of_the_message_alone() {
        let stamped = with_correlation(
            InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 7,
                shard: shard(),
                ack: AckMode::OnCommit,
                payloads: vec![Bytes::from_static(b"hello")],
            }),
            99,
        );
        let InternalMessage::ForwardPublish(stamped) = stamped else {
            panic!("the variant changed");
        };
        assert_eq!(stamped.correlation_id, 99);
        assert_eq!(stamped.shard, shard());
        assert_eq!(stamped.ack, AckMode::OnCommit);
        assert_eq!(stamped.payloads, vec![Bytes::from_static(b"hello")]);
    }
}

/// How a connection loss is labelled for metrics.
mod close_labels {
    use super::*;

    #[test]
    fn each_close_reason_has_its_own_label() {
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::TimedOut),
            "timeout"
        );
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::LocallyClosed),
            "closed_locally"
        );
        assert_eq!(close_reason_label(&quinn::ConnectionError::Reset), "reset");
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::VersionMismatch),
            "other"
        );
    }
}
