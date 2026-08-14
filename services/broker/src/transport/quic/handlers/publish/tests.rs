// Unit tests for the publish path: admission, sharding, enqueue policy, depth
// accounting, ack handling, and the control/uni handler entry points.

use super::*;
use bytes::Bytes;
use felix_authz::PermissionMatcher;
use felix_storage::EphemeralCache;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

// These publish-path tests don't exercise subscription delivery; this just gives
// `PublishContext::lane_manager` a real (if unused) instance to satisfy the type.
fn test_lane_manager() -> Arc<WriterLaneManager> {
    WriterLaneManager::new(&crate::config::BrokerConfig::default())
}

fn reset_global_ack_depth() {
    GLOBAL_ACK_DEPTH.store(0, Ordering::Relaxed);
}

fn make_publish_context(
    buffer: usize,
) -> (
    PublishContext,
    mpsc::Receiver<PublishJob>,
    mpsc::Sender<PublishJob>,
) {
    let (tx, rx) = mpsc::channel(buffer);
    let context = PublishContext {
        workers: Arc::new(vec![tx.clone()]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(100),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    (context, rx, tx)
}

fn make_job() -> PublishJob {
    PublishJob {
        target: PublishTarget::Named {
            tenant_id: "tenant".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        response: None,
        admission_permit: None,
    }
}

#[tokio::test]
async fn publish_admission_bounds_shared_inflight_bytes() {
    let admission = PublishAdmission::new(4);
    let permit = admission.acquire(4).await.expect("initial permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
            .await
            .is_err()
    );
    drop(permit);
    let _permit = admission.acquire(1).await.expect("released permit");
}

#[tokio::test]
async fn publish_admission_try_acquire_fails_when_exhausted() {
    let admission = PublishAdmission::new(4);
    let _permit = admission.try_acquire(4).expect("initial permit");
    assert!(admission.try_acquire(1).is_err());
}

#[tokio::test]
async fn enqueue_publish_drop_sheds_load_when_byte_budget_exhausted() {
    let (tx, _rx) = mpsc::channel(8);
    let ctx = PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
        admission: Arc::new(PublishAdmission::new(4)),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    // Queue depth (8) has room, but the shared byte budget (4 bytes) does not fit this
    // 7-byte payload, so the job must be shed even though the item-count queue is empty.
    let mut job = make_job();
    job.payloads = vec![Bytes::from_static(b"payload")];
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_drop_sheds_load_when_conn_byte_budget_exhausted() {
    let (tx, _rx) = mpsc::channel(8);
    let ctx = PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
        // Shared budget is generous; this connection's own share is not.
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    let mut job = make_job();
    job.payloads = vec![Bytes::from_static(b"payload")];
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_conn_budget_does_not_starve_other_connections() {
    let (tx, mut rx) = mpsc::channel(8);
    // Two connections sharing one global budget, each with its own conn_admission.
    let admission = Arc::new(PublishAdmission::new(8));
    let ctx_a = PublishContext {
        workers: Arc::new(vec![tx.clone()]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
        admission: Arc::clone(&admission),
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    let ctx_b = PublishContext {
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ..ctx_a.clone()
    };

    // Connection A tries to claim more than its own share (would fit in the global budget
    // alone) and must be shed by its own per-connection gate, not the global one.
    let mut big_job = make_job();
    big_job.payloads = vec![Bytes::from_static(b"01234567")]; // 8 bytes > A's 4-byte share
    assert!(
        !enqueue_publish(&ctx_a, big_job, EnqueuePolicy::Drop, None)
            .await
            .unwrap()
    );

    // Connection B is unaffected: its own share is untouched by A's rejected attempt.
    let mut small_job = make_job();
    small_job.payloads = vec![Bytes::from_static(b"ok")]; // 2 bytes, fits B's 4-byte share
    assert!(
        enqueue_publish(&ctx_b, small_job, EnqueuePolicy::Drop, None)
            .await
            .unwrap()
    );
    assert!(rx.recv().await.is_some());
}

#[test]
fn publish_worker_index_is_deterministic() {
    let first = publish_worker_index("tenant", "ns", "stream", 3);
    let second = publish_worker_index("tenant", "ns", "stream", 3);
    assert_eq!(first, second);
    assert!(first < 3);
}

#[test]
fn publish_worker_index_returns_zero_with_no_workers() {
    assert_eq!(publish_worker_index("tenant", "ns", "stream", 0), 0);
}

#[test]
fn ack_timeout_state_tracks_and_resets() {
    let start = Instant::now();
    let mut state = AckTimeoutState::new(start);
    assert_eq!(state.register_timeout(start), 1);
    assert_eq!(state.register_timeout(start + Duration::from_millis(10)), 2);
    let later = start + ACK_TIMEOUT_WINDOW + Duration::from_millis(1);
    assert_eq!(state.register_timeout(later), 1);
    let reset_at = later + Duration::from_secs(1);
    state.reset(reset_at);
    assert_eq!(state.register_timeout(reset_at), 1);
}

#[test]
fn decrement_depth_returns_none_when_empty() {
    let depth = Arc::new(AtomicUsize::new(0));
    let global = AtomicUsize::new(0);
    assert!(decrement_depth(&depth, &global, "test").is_none());
}

#[test]
fn decrement_depth_decreases_both_counters() {
    let depth = Arc::new(AtomicUsize::new(2));
    let global = AtomicUsize::new(3);
    let result = decrement_depth(&depth, &global, "test");
    assert!(result.is_some());
    let (prev, cur) = result.unwrap();
    assert_eq!(prev, 2);
    assert_eq!(cur, 1);
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    assert_eq!(global.load(Ordering::Relaxed), 2);
}

#[test]
fn decrement_depth_handles_global_underflow() {
    let depth = Arc::new(AtomicUsize::new(1));
    let global = AtomicUsize::new(0);
    let result = decrement_depth(&depth, &global, "test");
    assert!(result.is_some());
    let (prev, cur) = result.unwrap();
    assert_eq!(prev, 1);
    assert_eq!(cur, 0);
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(global.load(Ordering::Relaxed), 0);
}

#[test]
fn reset_local_depth_only_conciles_global_counter() {
    let depth = Arc::new(AtomicUsize::new(4));
    let global = AtomicUsize::new(10);
    reset_local_depth_only(&depth, &global, "test");
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(global.load(Ordering::Relaxed), 6);
}

#[tokio::test]
async fn enqueue_publish_drop_returns_false_when_full() {
    let (ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let job = make_job();
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_fail_returns_error_when_full() {
    let (ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let job = make_job();
    let err = enqueue_publish(&ctx, job, EnqueuePolicy::Fail, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish queue full"));
}

#[tokio::test]
async fn enqueue_publish_wait_enqueues_when_receiver_ready() {
    let (ctx, mut rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
        let _ = rx.recv().await;
    });
    let job = make_job();
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Wait, None)
        .await
        .unwrap();
    assert!(result);
    handle.await.unwrap();
}

#[tokio::test]
async fn enqueue_publish_wait_times_out_when_queue_full() {
    let (tx, _rx) = mpsc::channel(1);
    tx.try_send(make_job()).unwrap();
    let ctx = PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(5),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Wait, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish enqueue timed out"));
}

#[tokio::test]
async fn enqueue_publish_returns_error_when_queue_closed() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let ctx = PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(10),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Fail, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish queue closed"));
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_critical_increments_depth() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
    });
    let result = send_outgoing_critical(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Error {
            message: "e".to_string(),
        }),
    )
    .await;
    handle.await.unwrap();
    assert!(result.is_ok());
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    assert!(GLOBAL_ACK_DEPTH.load(Ordering::Relaxed) >= 1);
    assert!(!*throttle_rx.borrow());
}

#[tokio::test]
async fn send_outgoing_critical_triggers_throttle_at_hi_water() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(ACK_HI_WATER.saturating_sub(1)));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
    });
    let result = send_outgoing_critical(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Ok),
    )
    .await;
    handle.await.unwrap();
    assert!(result.is_ok());
    assert!(*throttle_rx.borrow());
}

#[tokio::test]
async fn send_outgoing_best_effort_reports_full() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, _rx) = mpsc::channel(1);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let _ = tx
        .send(Outgoing::Message(Message::Error {
            message: "f".to_string(),
        }))
        .await;
    let err = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Error {
            message: "overflow".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, AckEnqueueError::Full));
}

#[tokio::test]
async fn send_outgoing_best_effort_triggers_throttle_at_hi_water() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(ACK_HI_WATER.saturating_sub(1)));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let result = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Ok),
    )
    .await;
    assert!(result.is_ok());
    assert!(*throttle_rx.borrow());
    let _ = rx.recv().await;
}

#[tokio::test]
async fn send_outgoing_best_effort_reports_closed() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let err = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Error {
            message: "closed".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, AckEnqueueError::Closed));
}

#[tokio::test]
async fn handle_ack_enqueue_timeout_threshold_triggers_cancel() {
    let state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    for _ in 0..(ACK_TIMEOUT_THRESHOLD - 1) {
        assert!(
            handle_ack_enqueue_result(
                Err(AckEnqueueError::Timeout),
                &state,
                &throttle_tx,
                &cancel_tx
            )
            .await
            .is_ok()
        );
    }
    let result = handle_ack_enqueue_result(
        Err(AckEnqueueError::Timeout),
        &state,
        &throttle_tx,
        &cancel_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(*throttle_rx.borrow());
    assert!(*cancel_rx.borrow());
}

#[tokio::test]
async fn handle_ack_enqueue_full_returns_error() {
    let state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let err =
        handle_ack_enqueue_result(Err(AckEnqueueError::Full), &state, &throttle_tx, &cancel_tx)
            .await
            .expect_err("full");
    assert!(err.to_string().contains("ack queue full"));
}

#[tokio::test]
async fn handle_ack_enqueue_closed_shutdowns_stream() {
    let state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let result = handle_ack_enqueue_result(
        Err(AckEnqueueError::Closed),
        &state,
        &throttle_tx,
        &cancel_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(!*throttle_rx.borrow());
    assert!(*cancel_rx.borrow());
}

fn make_auth_ctx(tenant_id: &str, perms: &[&str]) -> AuthContext {
    let patterns = perms.iter().map(|p| (*p).to_string()).collect::<Vec<_>>();
    let matcher = PermissionMatcher::from_strings(&patterns).expect("parse perms");
    AuthContext {
        tenant_id: tenant_id.to_string(),
        matcher,
    }
}

fn make_binary_publish_frame(tenant_id: &str, namespace: &str, stream: &str) -> Frame {
    let payloads = vec![b"payload".to_vec()];
    felix_wire::binary::encode_publish_batch(tenant_id, namespace, stream, &payloads)
        .expect("encode publish batch")
}

#[tokio::test]
async fn handle_binary_publish_batch_control_requires_auth() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        None,
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("auth required");
    assert!(err.to_string().contains("auth required"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_tenant_mismatch() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("other", &["stream.publish:stream:tenant/ns/stream"]);
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("tenant mismatch");
    assert!(err.to_string().contains("tenant mismatch"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_forbidden() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.subscribe:stream:tenant/ns/stream"]);
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("forbidden");
    assert!(err.to_string().contains("forbidden"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_missing_stream_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, mut rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
    let recv = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await;
    assert!(recv.is_err(), "publish should not be enqueued");
}

#[tokio::test]
async fn handle_binary_publish_batch_control_enqueue_dropped_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn handle_binary_publish_batch_control_enqueue_error_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn handle_publish_message_throttled_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("throttled path");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 7);
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_throttled_without_request_id_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("throttled path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message }) => {
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_missing_request_id_returns_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("missing request id");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message }) => {
            assert!(message.contains("missing request_id"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_drop_when_queue_full_and_ack_none() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(felix_wire::AckMode::None),
        false,
    )
    .await
    .expect("publish");

    let recv = tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await;
    assert!(recv.is_err(), "no ack expected");
}

#[tokio::test]
async fn handle_publish_message_enqueue_error_reports_publish_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        Some(44),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 44);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_stream_not_found_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "missing".to_string(),
        vec![1, 2, 3],
        Some(42),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("stream not found");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 42);
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_sends_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2],
        Some(5),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishOk { request_id }) => {
            assert_eq!(request_id, 5);
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiters_exhausted() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(0));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 7);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_full() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);

    let permit = ack_waiters.clone().acquire_owned().await.expect("permit");
    ack_waiter_tx
        .try_send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 99,
            payload_len: 1,
            start: crate::transport::quic::telemetry::t_instant_now(),
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");
    drop(ack_waiter_rx);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        Some(8),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 8);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_full_with_permit() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(2));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    let permit = ack_waiters.clone().acquire_owned().await.expect("permit");
    ack_waiter_tx
        .try_send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 101,
            payload_len: 1,
            start: crate::transport::quic::telemetry::t_instant_now(),
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        Some(9),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 9);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_closed() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    drop(ack_waiter_rx);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        Some(10),
        Some(felix_wire::AckMode::PerMessage),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 10);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_requires_auth() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let result =
        handle_binary_publish_batch_uni(&broker, &mut cache, &mut key, &publish_ctx, &frame, None)
            .await
            .expect("auth required");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_tenant_mismatch() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("other", &["stream.publish:stream:tenant/ns/stream"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("tenant mismatch");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_forbidden() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.subscribe:stream:tenant/ns/stream"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("forbidden");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_decode_error_returns_err() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = Frame::new(0, Bytes::from_static(b"bad")).expect("frame");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let err = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect_err("decode error");
    assert!(err.to_string().contains("decode binary publish batch"));
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("drop");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_message_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_message_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_publish_message_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
    )
    .await
    .expect("drop");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
    )
    .await
    .expect("drop");
    assert!(result);
}

#[tokio::test]
async fn resolve_stream_cached_uses_cached_entry_until_cleared() {
    let broker = Broker::new(EphemeralCache::new().into());
    let mut cache = HashMap::new();
    let mut key = String::new();

    let handle = resolve_stream_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
    assert!(handle.is_none(), "no tenant/namespace yet");

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let cached = resolve_stream_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
    assert!(
        cached.is_none(),
        "cached miss should be returned until cache expires or clears"
    );

    cache.clear();
    let refreshed =
        resolve_stream_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
    assert!(refreshed.is_some(), "cache refresh should see stream");
}

#[tokio::test]
async fn handle_publish_batch_missing_request_id_returns_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("missing request id");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message }) => {
            assert!(message.contains("missing request_id"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_stream_not_found_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(9),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("stream missing path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 9);
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_enqueue_full_reports_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(11),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("enqueue full path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 11);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_enqueue_ok_sends_ack() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(13),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish batch");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishOk { request_id }) => {
            assert_eq!(request_id, 13);
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_drop_when_queue_full_and_ack_none() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(felix_wire::AckMode::None),
        false,
    )
    .await
    .expect("publish");

    let recv = tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await;
    assert!(recv.is_err(), "no ack expected");
}

#[tokio::test]
async fn handle_publish_batch_message_enqueue_error_reports_publish_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(45),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 45);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_on_commit_sends_waiter_message() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, _out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, mut ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(46),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish");

    let msg = ack_waiter_rx.recv().await.expect("waiter msg");
    match msg {
        AckWaiterMessage::PublishBatch { request_id, .. } => {
            assert_eq!(request_id, 46);
        }
        _ => panic!("unexpected waiter message"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_throttled_with_request_id_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(21),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("throttled path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 21);
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_throttled_without_request_id_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("throttled path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message }) => {
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_waiters_exhausted() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(0));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(22),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 22);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_waiter_queue_full() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(2));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    let permit = ack_waiters.clone().acquire_owned().await.expect("permit");
    ack_waiter_tx
        .try_send(AckWaiterMessage::PublishBatch {
            encoding: AckEncoding::Json,
            request_id: 99,
            payload_bytes: vec![1],
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(23),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 23);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_waiter_queue_closed() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    drop(ack_waiter_rx);

    handle_publish_batch_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        Some(24),
        Some(felix_wire::AckMode::PerBatch),
        false,
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
        }) => {
            assert_eq!(request_id, 24);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

// --- ingress waiting semantics -------------------------------------------
//
// Three properties, one per defect that used to be here:
//   1. `Wait` spends one `wait_timeout` across both stages, not one each.
//   2. `Backpressure` never sheds — it waits for capacity however long that takes.
//   3. `Backpressure` still ends promptly when the connection is torn down.

/// Admission and the queue send each used to get a full `wait_timeout`, so the
/// worst case was twice the configured budget. Here admission is held for most of
/// the budget and the queue is left full, so the send has to wait too; the whole
/// call must still finish within one budget.
#[tokio::test(start_paused = true)]
async fn wait_policy_spends_one_budget_across_both_stages() {
    let payload = b"payload".len();
    let (mut ctx, _rx, _tx) = make_publish_context(1);
    ctx.wait_timeout = Duration::from_millis(100);
    // Room for the primed job plus one held permit, so a third job must wait for
    // admission *and then* find the queue still full.
    ctx.admission = Arc::new(PublishAdmission::new(payload * 2));
    ctx.conn_admission = Arc::new(PublishAdmission::new(payload * 2));

    // Fill the only queue slot. This job keeps its admission permit while queued.
    enqueue_publish(&ctx, make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    // Hold the remaining admission and release it partway through the budget.
    let held = ctx
        .admission
        .clone()
        .acquire(payload)
        .await
        .expect("hold admission");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(held);
    });

    let start = tokio::time::Instant::now();
    let result = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Wait, None).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_err(),
        "the queue never drains, so this must time out rather than enqueue"
    );
    assert!(
        elapsed >= Duration::from_millis(60),
        "admission should have blocked until the held permit was released, took {elapsed:?}"
    );
    // Before the fix each stage got its own budget, so this was ~160ms.
    assert!(
        elapsed <= Duration::from_millis(100),
        "Wait must not exceed one wait_timeout across both stages, took {elapsed:?}"
    );
}

/// The whole point of `Backpressure`: overload becomes slowness, never loss. A
/// timer here would have turned this into a silent drop, with no ack channel to
/// report it on.
#[tokio::test(start_paused = true)]
async fn backpressure_waits_for_capacity_instead_of_shedding() {
    let (ctx, mut rx, _tx) = make_publish_context(1);
    // Fill the single queue slot so the next enqueue has to wait.
    enqueue_publish(&ctx, make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    // Drain long after any plausible timeout would have fired.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let _ = rx.recv().await;
        // Hold the receiver open so the channel does not close under the waiter.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        drop(rx);
    });

    let accepted = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Backpressure, None)
        .await
        .expect("backpressure must not fail on a full queue");
    assert!(
        accepted,
        "backpressure must enqueue once capacity frees, never report a drop"
    );
}

/// Unbounded does not mean unstoppable: teardown is what ends the wait, which is
/// why the policy needs the connection's cancel signal rather than a clock.
#[tokio::test(start_paused = true)]
async fn backpressure_gives_up_when_the_connection_is_cancelled() {
    let (ctx, _rx, _tx) = make_publish_context(1);
    enqueue_publish(&ctx, make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    let (cancel_tx, cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = cancel_tx.send(true);
    });

    let err = enqueue_publish(
        &ctx,
        make_job(),
        EnqueuePolicy::Backpressure,
        Some(cancel_rx),
    )
    .await
    .expect_err("cancellation must surface as an error, not a silent drop");
    assert!(
        err.to_string().contains("cancelled"),
        "unexpected error: {err}"
    );
}
