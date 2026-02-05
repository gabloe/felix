//! Publish path (ingress) helpers for the QUIC transport.
//!
//! This module is the “publish ingestion glue” between QUIC stream handlers and the broker core.
//! It owns:
//! - **Ingress enqueue policy** (Drop/Fail/Wait) into the publish worker queues.
//! - **Worker sharding** (deterministic hashing of tenant/namespace/stream to pick a worker).
//! - **Ack semantics + backpressure** for control-stream publishes (including commit-ack waiting).
//! - **Depth tracking** for ingress and outbound-ack queues (local + global gauges).
//!
//! Publish arrives in two main shapes:
//! - **Control stream publish** (bi-directional): publish messages may request an ack (`ack != None`)
//!   and can be configured as either enqueue-ack or commit-ack (`ack_on_commit`).
//! - **Uni-directional publish stream** (ingress-only): fire-and-forget publishes with **no acks**.
//!
//! Ack meaning depends on configuration:
//! - `ack_on_commit = false` → **enqueue-ack**: an ack means “accepted into the ingress queue”.
//!   Lowest latency, but does not guarantee the publish ultimately commits.
//! - `ack_on_commit = true` → **commit-ack**: an ack means “the publish job completed/committed”.
//!   Higher latency; bounded by `ack_waiters` and `ack_waiter_tx` to avoid unbounded in-flight acks.
//!
//! Backpressure strategy:
//! - Ingress queue uses `EnqueuePolicy` (Drop/Fail/Wait) to shed load or apply bounded waiting.
//! - Outbound ack queue maintains a high-water throttle signal (`ack_throttle_tx`) and records
//!   enqueue failures/timeouts to decide when to cooperatively cancel the control stream.
//! - Depth counters are tracked both per-stream and globally to support observability and tuning.
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::{Frame, Message};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};

use crate::auth::AuthContext;
use crate::timings;

use crate::transport::quic::errors::AckEnqueueError;
use crate::transport::quic::telemetry::{log_decode_error, t_consume_instant, t_now_if};
use crate::transport::quic::{
    ACK_ENQUEUE_TIMEOUT, ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
    GLOBAL_INGRESS_DEPTH, STREAM_CACHE_TTL,
};

/// Work item consumed by publish workers.
///
/// A publish job is the unit the broker’s ingress pipeline processes:
/// - It identifies the target stream (`tenant_id`, `namespace`, `stream`).
/// - It carries one or more payloads (single publish or batch).
/// - `response` is **only** used when the publish was received on the control stream and the
///   client requested an ack in commit-ack mode (`ack_on_commit = true`).
///
/// For uni-stream publishes and enqueue-ack mode, `response` is `None`.
pub(crate) struct PublishJob {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) stream: String,
    pub(crate) payloads: Vec<Bytes>,
    pub(crate) response: Option<oneshot::Sender<Result<()>>>,
}

/// Items pushed to the outbound writer loop (ack/response path).
///
/// The writer loop is the single owner of the QUIC `SendStream` for the control stream.
/// All responses/acks are funneled into that task to avoid concurrent writes.
///
/// Variants:
/// - `Message`: normal control responses (PublishOk/Error, SubscribeOk, etc.).
/// - `CacheMessage`: cache fast-path replies that may be encoded differently or routed
///   separately from general control responses (depending on the writer implementation).
pub(crate) enum Outgoing {
    Message(Message),
    CacheMessage(Message),
}

/// Admission policy when the ingress publish queue is full.
///
/// - `Drop`: shed load silently (best for fire-and-forget / non-acked traffic).
/// - `Fail`: reject immediately with an error (best for acked traffic when you prefer fast failure).
/// - `Wait`: apply bounded backpressure by waiting up to `publish_ctx.wait_timeout`.
///   Used for commit-ack publishes so the client is less likely to be stranded by overload.
pub(crate) enum EnqueuePolicy {
    Drop,
    Fail,
    Wait,
}

/// Result reported by the ack-waiter task for commit-ack publishes.
///
/// The waiter task is responsible for awaiting the worker completion signal (oneshot),
/// applying timeouts, and producing a normalized result for the response writer.
pub(crate) enum AckWaiterResult {
    Publish {
        request_id: u64,
        payload_len: u64,
        start: crate::transport::quic::telemetry::TelemetryInstant,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishTimeout {
        request_id: u64,
        start: crate::transport::quic::telemetry::TelemetryInstant,
    },
    PublishBatch {
        request_id: u64,
        payload_bytes: Vec<usize>,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishBatchTimeout {
        request_id: u64,
        payload_bytes: Vec<usize>,
    },
}

/// Message sent to the ack-waiter task to track one in-flight commit-ack request.
///
/// Carries the oneshot receiver and a semaphore permit (`ack_waiters`) which bounds the number of
/// in-flight commit acks. Releasing the permit signals “this commit-ack slot is free again”.
pub(crate) enum AckWaiterMessage {
    Publish {
        request_id: u64,
        payload_len: u64,
        start: crate::transport::quic::telemetry::TelemetryInstant,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
    PublishBatch {
        request_id: u64,
        payload_bytes: Vec<usize>,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
}

/// Shared publish-ingress configuration and worker queue handles.
///
/// - `workers`: per-worker `mpsc::Sender<PublishJob>` queues.
/// - `worker_count`: cached length for fast hashing.
/// - `depth`: best-effort local depth tracking for this publish queue set.
/// - `wait_timeout`: bound used by `EnqueuePolicy::Wait`.
#[derive(Clone)]
pub(crate) struct PublishContext {
    pub(crate) workers: Arc<Vec<mpsc::Sender<PublishJob>>>,
    pub(crate) worker_count: usize,
    pub(crate) depth: Arc<AtomicUsize>,
    pub(crate) wait_timeout: Duration,
}

/// Tracks consecutive outbound-ack enqueue timeouts in a sliding time window.
///
/// This is a defensive mechanism: if we cannot enqueue responses for too long, the control stream
/// is likely unhealthy (client not reading, writer wedged, or extreme overload). In that case we
/// throttle and eventually cancel the stream cooperatively.
pub(crate) struct AckTimeoutState {
    window_start: Instant,
    count: u32,
}

impl AckTimeoutState {
    /// Create a new timeout state window starting at `now`.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            count: 0,
        }
    }

    /// Reset the window and streak counters (typically after a successful enqueue).
    pub(crate) fn reset(&mut self, now: Instant) {
        self.window_start = now;
        self.count = 0;
    }

    /// Record one timeout and return the current streak count within the active window.
    ///
    /// If the window has elapsed, we start a new window and reset the streak to 1.
    pub(crate) fn register_timeout(&mut self, now: Instant) -> u32 {
        if now.duration_since(self.window_start) > ACK_TIMEOUT_WINDOW {
            self.window_start = now;
            self.count = 1;
        } else {
            self.count = self.count.saturating_add(1);
        }
        self.count
    }
}

/// Deterministically map (tenant, namespace, stream) to a publish worker index.
///
/// Goal: keep ordering locality and cache locality for a given stream by always hashing to
/// the same worker, while distributing streams across workers reasonably well.
///
/// This *must* be stable across processes for predictable performance; it does not need to be
/// cryptographically secure.
pub(crate) fn publish_worker_index(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    worker_count: usize,
) -> usize {
    if worker_count == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    namespace.hash(&mut hasher);
    stream.hash(&mut hasher);
    (hasher.finish() as usize) % worker_count
}

/// Enqueue a publish job into the appropriate worker queue with explicit overload semantics.
///
/// Return value:
/// - `Ok(true)`  → job enqueued
/// - `Ok(false)` → job intentionally dropped (policy = Drop)
/// - `Err(...)`  → failure to enqueue (policy = Fail, closed queue, timeout, etc.)
///
/// Implementation detail:
/// - We try `try_send` first to keep the common path allocation-free and to make overload observable
///   (`Full` vs `Closed`). Only `Wait` does an async `send` with a timeout.
/// - Any path that successfully enqueues must increment both local and global depth **exactly once**.
pub(crate) async fn enqueue_publish(
    publish_ctx: &PublishContext,
    job: PublishJob,
    policy: EnqueuePolicy,
) -> Result<bool> {
    let worker_index = publish_worker_index(
        &job.tenant_id,
        &job.namespace,
        &job.stream,
        publish_ctx.worker_count,
    );
    let worker = publish_ctx
        .workers
        .get(worker_index)
        .ok_or_else(|| anyhow!("publish worker index out of range"))?;
    #[cfg(feature = "perf_debug")]
    let enqueue_wait_start = Instant::now();
    // We use try_send first to keep the fast path allocation-free and to make overload observable
    // (Full vs Closed). Only the Wait policy performs an async send with a timeout.
    // IMPORTANT: Any code path that successfully enqueues MUST increment depth counters exactly once.
    // Best-effort enqueue with metrics and optional backpressure/err.
    match worker.try_send(job) {
        Ok(()) => {
            let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
            let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
            #[cfg(feature = "perf_debug")]
            {
                let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                metrics::histogram!(
                    "felix_perf_publish_enqueue_wait_ns",
                    "worker" => worker_index.to_string()
                )
                .record(wait_ns as f64);
                metrics::counter!(
                    "felix_perf_publish_enqueue_ok_total",
                    "worker" => worker_index.to_string()
                )
                .increment(1);
            }
            Ok(true)
        }
        Err(mpsc::error::TrySendError::Full(job)) => {
            t_counter!("felix_broker_ingress_queue_full_total").increment(1);
            match policy {
                EnqueuePolicy::Drop => {
                    t_counter!("felix_broker_ingress_dropped_total").increment(1);
                    Ok(false)
                }
                EnqueuePolicy::Fail => {
                    t_counter!("felix_broker_ingress_rejected_total").increment(1);
                    Err(anyhow!("publish queue full"))
                }
                EnqueuePolicy::Wait => {
                    // Acked publishes with ack_on_commit use Wait; otherwise we Fail/Drop.
                    t_counter!("felix_broker_ingress_waited_total").increment(1);
                    let send_result =
                        tokio::time::timeout(publish_ctx.wait_timeout, worker.send(job))
                            .await
                            .map_err(|_| anyhow!("publish enqueue timed out"))?;
                    send_result.map_err(|_| anyhow!("publish queue closed"))?;
                    // Local depth is per publish context; global depth is used for cross-connection observability.
                    let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
                    let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
                    t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
                    #[cfg(feature = "perf_debug")]
                    {
                        let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                        metrics::histogram!(
                            "felix_perf_publish_enqueue_wait_ns",
                            "worker" => worker_index.to_string()
                        )
                        .record(wait_ns as f64);
                        metrics::counter!(
                            "felix_perf_publish_enqueue_wait_total",
                            "worker" => worker_index.to_string()
                        )
                        .increment(1);
                    }
                    Ok(true)
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("publish queue closed")),
    }
}

// Adjust queue depth gauges safely when send fails or work completes.
pub(crate) fn decrement_depth(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) -> Option<(usize, usize)> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // Depth tracking is intentionally best-effort: we avoid panicking on underflow and tolerate drift.
    // Drift can occur if a task exits unexpectedly or if multiple teardown paths reset counters.
    // We record drift metrics and rely on `reset_local_depth_only` to reconcile on teardown.
    if let Ok(prev) = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        if value == 0 { None } else { Some(value - 1) }
    }) {
        let cur = prev.saturating_sub(1);
        let updated = match global.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            if value == 0 { None } else { Some(value - 1) }
        }) {
            Ok(value) => value - 1,
            Err(_) => {
                t_counter!("felix_queue_depth_drift_total", "queue" => gauge).increment(1);
                global.load(Ordering::Relaxed)
            }
        };
        t_gauge!(gauge).set(updated as f64);
        return Some((prev, cur));
    }
    None
}

pub(crate) fn reset_local_depth_only(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    let remaining = depth.swap(0, Ordering::Relaxed);
    if remaining == 0 {
        return;
    }
    let mut prev = global.load(Ordering::Relaxed);
    loop {
        let next = prev.saturating_sub(remaining);
        match global.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                t_gauge!(gauge).set(next as f64);
                break;
            }
            Err(updated) => prev = updated,
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use felix_authz::PermissionMatcher;
    use felix_storage::EphemeralCache;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use tokio::sync::{mpsc, watch};

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
        };
        (context, rx, tx)
    }

    fn make_job() -> PublishJob {
        PublishJob {
            tenant_id: "tenant".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
            payloads: vec![Bytes::from_static(b"payload")],
            response: None,
        }
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
        let result = enqueue_publish(&ctx, job, EnqueuePolicy::Drop)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn enqueue_publish_fail_returns_error_when_full() {
        let (ctx, _rx, tx) = make_publish_context(1);
        tx.try_send(make_job()).unwrap();
        let job = make_job();
        let err = enqueue_publish(&ctx, job, EnqueuePolicy::Fail)
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
        let result = enqueue_publish(&ctx, job, EnqueuePolicy::Wait)
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
        };
        let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Wait)
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
        };
        let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Fail)
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
        let result = handle_binary_publish_batch_uni(
            &broker,
            &mut cache,
            &mut key,
            &publish_ctx,
            &frame,
            None,
        )
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
    async fn stream_exists_cached_uses_cached_entry_until_cleared() {
        let broker = Broker::new(EphemeralCache::new().into());
        let mut cache = HashMap::new();
        let mut key = String::new();

        let exists =
            stream_exists_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
        assert!(!exists, "no tenant/namespace yet");

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

        let cached =
            stream_exists_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
        assert!(
            !cached,
            "cached miss should be returned until cache expires or clears"
        );

        cache.clear();
        let refreshed =
            stream_exists_cached(&broker, &mut cache, &mut key, "t1", "ns", "stream").await;
        assert!(refreshed, "cache refresh should see stream");
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
}

pub(crate) async fn send_outgoing_critical(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<AtomicUsize>,
    gauge: &'static str,
    throttle_tx: &watch::Sender<bool>,
    message: Outgoing,
) -> std::result::Result<(), AckEnqueueError> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // "Critical" means: if we cannot enqueue within a short bound, we prefer to cancel/close the
    // control stream rather than allow an acked request to wait forever.
    // This is used for acks/responses where the client is very likely waiting.
    // Critical enqueue with timeout; cancel the stream if it cannot be queued.
    let send_result = tokio::time::timeout(ACK_ENQUEUE_TIMEOUT, tx.send(message)).await;
    match send_result {
        Ok(Ok(())) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Ok(Err(_)) => Err(AckEnqueueError::Closed),
        Err(_) => {
            t_counter!("felix_broker_out_ack_timeout_total").increment(1);
            Err(AckEnqueueError::Timeout)
        }
    }
}

pub(crate) async fn send_outgoing_best_effort(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<AtomicUsize>,
    gauge: &'static str,
    throttle_tx: &watch::Sender<bool>,
    message: Outgoing,
) -> std::result::Result<(), AckEnqueueError> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // Best-effort is used when the server is already overloaded and we are shedding load.
    // NOTE: Dropping an ack/error for a request that asked for an ack can strand the client until
    // it times out. If this becomes problematic, switch these paths to `send_outgoing_critical`
    // or close the stream when enqueue fails.
    // Best-effort enqueue; fail fast if the ack queue is full to avoid deadlocks.
    match tx.try_send(message) {
        Ok(()) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            t_counter!("felix_broker_out_ack_full_total").increment(1);
            Err(AckEnqueueError::Full)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(AckEnqueueError::Closed),
    }
}

pub(crate) async fn handle_ack_enqueue_result(
    result: std::result::Result<(), AckEnqueueError>,
    state: &Arc<Mutex<AckTimeoutState>>,
    throttle_tx: &watch::Sender<bool>,
    cancel_tx: &watch::Sender<bool>,
) -> Result<()> {
    match result {
        Ok(()) => {
            let mut guard = state.lock().await;
            guard.reset(Instant::now());
            Ok(())
        }
        Err(AckEnqueueError::Timeout) => {
            let _ = throttle_tx.send(true);
            let now = Instant::now();
            let mut guard = state.lock().await;
            let count = guard.register_timeout(now);
            if count >= ACK_TIMEOUT_THRESHOLD {
                crate::transport::quic::errors::record_ack_enqueue_failure_metrics(
                    "ack_queue_timeout",
                );
                let _ = cancel_tx.send(true);
                return Err(anyhow!(
                    "closing control stream: ack enqueue timeout streak"
                ));
            }
            Ok(())
        }
        Err(AckEnqueueError::Full) => Err(anyhow!("ack queue full")),
        Err(AckEnqueueError::Closed) => {
            crate::transport::quic::errors::record_ack_enqueue_failure_metrics("ack_queue_closed");
            let _ = throttle_tx.send(false);
            let _ = cancel_tx.send(true);
            Err(anyhow!("closing control stream: ack_queue_closed"))
        }
    }
}

pub(crate) async fn handle_binary_publish_batch_control(
    broker: &Broker,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
    sample: bool,
) -> Result<()> {
    let decode_start = t_now_if(sample);
    let batch = match felix_wire::binary::decode_publish_batch(frame)
        .context("decode binary publish batch")
    {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = auth_ctx.ok_or_else(|| anyhow!("auth required"))?;
    if auth_ctx.tenant_id != batch.tenant_id {
        return Err(anyhow!("tenant mismatch"));
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Err(anyhow!("forbidden"));
    }
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
    }
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_decode_ns(decode_ns);
        t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
    }
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &batch.tenant_id,
        &batch.namespace,
        &batch.stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        return Ok(());
    }
    let span = tracing::info_span!(
        "publish_batch_binary",
        tenant_id = %batch.tenant_id,
        namespace = %batch.namespace,
        stream = %batch.stream,
        count = batch.payloads.len()
    );
    let _enter = span.enter();
    let payloads = batch
        .payloads
        .into_iter()
        .map(Bytes::from)
        .collect::<Vec<_>>();
    let fanout_start = t_now_if(sample);
    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id: batch.tenant_id,
            namespace: batch.namespace,
            stream: batch.stream,
            payloads,
            response: None,
        },
        EnqueuePolicy::Drop,
    )
    .await;
    match r {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            tracing::warn!(error = %err, "publish enqueue failed");
        }
    }

    if let Some(start) = fanout_start {
        let fanout_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(fanout_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message(
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    ack_wait_timeout: Duration,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
) -> Result<()> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            } else {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            }
        }
        t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        return Ok(());
    }
    // Publish protocol (control stream):
    // - Client sends Publish { payload, request_id?, ack }.
    // - Broker enqueues payload (fanout path) and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerMessage -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let start = crate::transport::quic::telemetry::t_instant_now();
    t_consume_instant(start);
    let payload_len = payload.len();
    let enqueue_start = t_now_if(sample);
    // Ack mode determines if we wait for broker commit or reply immediately.
    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
    // Protocol invariant: any acked publish must include request_id, because acks
    // may be out-of-order and request_id is the only correlator.
    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "missing request_id for acked publish".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(());
    }
    let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && ack_on_commit {
        let (response_tx, response_rx) = oneshot::channel();
        (Some(response_tx), Some(response_rx))
    } else {
        (None, None)
    };
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        if ack_mode != felix_wire::AckMode::None {
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: format!(
                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                        ),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        return Ok(());
    }
    let enqueue_result = enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            payloads: vec![Bytes::from(payload)],
            response: response_tx,
        },
        if ack_mode == felix_wire::AckMode::None {
            EnqueuePolicy::Drop
        } else if ack_on_commit {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Fail
        },
    )
    .await;
    if let Some(start) = enqueue_start {
        let enqueue_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(enqueue_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(enqueue_ns as f64);
    }
    let span = tracing::trace_span!(
        "publish",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    let _enter = span.enter();
    match enqueue_result {
        Ok(true) => {
            if ack_mode == felix_wire::AckMode::None {
                t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
            }
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: "ingress overloaded".to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: err.to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
    }
    if ack_mode == felix_wire::AckMode::None {
        return Ok(());
    }
    if !ack_on_commit {
        // Enqueue-ack mode:
        // Ack means "accepted into the ingress queue", not "committed". This keeps
        // latency low but can report success even if a later broker error occurs.
        // Fire-and-forget ack after enqueue when configured.
        let request_id = request_id.expect("request id checked");
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishOk { request_id }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        t_counter!("felix_publish_requests_total", "result" => "ok").increment(1);
        t_counter!("felix_publish_bytes_total").increment(payload_len as u64);
        #[cfg(feature = "telemetry")]
        {
            t_histogram!("felix_publish_latency_ms", "mode" => "enqueue")
                .record(start.elapsed().as_secs_f64() * 1000.0);
        }
        return Ok(());
    }
    let request_id = request_id.expect("request id checked");
    let response_rx = response_rx.expect("response rx available");
    let payload_len_for_metrics = payload_len as u64;
    // Commit-ack mode:
    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
    // Correctness note: failing after enqueue means the publish may still commit;
    // the client will see an error/overload even though the publish succeeded.
    // If that is unacceptable, we must enforce admission *before* enqueue.
    let permit = match Arc::clone(ack_waiters).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
            return Ok(());
        }
    };
    let msg = AckWaiterMessage::Publish {
        request_id,
        payload_len: payload_len_for_metrics,
        start,
        response_rx,
        permit,
    };
    match ack_waiter_tx.try_send(msg) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
    }
    let _ = ack_wait_timeout;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message(
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
) -> Result<()> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            } else {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            }
        }
        t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        return Ok(());
    }
    // PublishBatch protocol (control stream):
    // - Client sends PublishBatch { payloads, request_id?, ack }.
    // - Broker enqueues all payloads as one unit and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerBatch -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let span = tracing::trace_span!(
        "publish_batch",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream,
        count = payloads.len()
    );
    let _enter = span.enter();
    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
    // Protocol invariant: any acked publish must include request_id, because acks
    // may be out-of-order and request_id is the only correlator.
    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "missing request_id for acked publish batch".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(());
    }
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        if ack_mode != felix_wire::AckMode::None {
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: format!(
                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                        ),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        return Ok(());
    }
    let payload_bytes = payloads
        .iter()
        .map(|payload| payload.len())
        .collect::<Vec<_>>();
    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
    let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && ack_on_commit {
        let (response_tx, response_rx) = oneshot::channel();
        (Some(response_tx), Some(response_rx))
    } else {
        (None, None)
    };
    let fanout_start = t_now_if(sample);
    let enqueue_result = enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            payloads,
            response: response_tx,
        },
        if ack_mode == felix_wire::AckMode::None {
            EnqueuePolicy::Drop
        } else if ack_on_commit {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Fail
        },
    )
    .await;
    if let Some(start) = fanout_start {
        let fanout_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(fanout_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
    }
    match enqueue_result {
        Ok(true) => {
            if ack_mode == felix_wire::AckMode::None {
                t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
            }
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: "ingress overloaded".to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: err.to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
    }
    if ack_mode == felix_wire::AckMode::None {
        return Ok(());
    }
    if !ack_on_commit {
        // Enqueue-ack mode:
        // Ack means "accepted into the ingress queue", not "committed". This keeps
        // latency low but can report success even if a later broker error occurs.
        // Batch ack can be sent once enqueued if commit acks are disabled.
        let request_id = request_id.expect("request id checked");
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishOk { request_id }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        t_counter!("felix_publish_requests_total", "result" => "ok").increment(1);
        for bytes in &payload_bytes {
            t_counter!("felix_publish_bytes_total").increment(*bytes as u64);
        }
        return Ok(());
    }
    let request_id = request_id.expect("request id checked");
    let response_rx = response_rx.expect("response rx available");
    let payload_bytes_for_metrics = payload_bytes;
    // Commit-ack mode:
    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
    // Correctness note: failing after enqueue means the publish may still commit;
    // the client will see an error/overload even though the publish succeeded.
    // If that is unacceptable, we must enforce admission *before* enqueue.
    let permit = match Arc::clone(ack_waiters).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
            return Ok(());
        }
    };
    let msg = AckWaiterMessage::PublishBatch {
        request_id,
        payload_bytes: payload_bytes_for_metrics,
        response_rx,
        permit,
    };
    match ack_waiter_tx.try_send(msg) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
    }
    Ok(())
}

pub(crate) async fn handle_binary_publish_batch_uni(
    broker: &Broker,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
) -> Result<bool> {
    let batch = match felix_wire::binary::decode_publish_batch(frame)
        .context("decode binary publish batch")
    {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("uni_binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = match auth_ctx {
        Some(ctx) => ctx,
        None => return Ok(false),
    };
    if auth_ctx.tenant_id != batch.tenant_id {
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Ok(false);
    }
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
    }
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &batch.tenant_id,
        &batch.namespace,
        &batch.stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        return Ok(true);
    }
    let payloads = batch
        .payloads
        .into_iter()
        .map(Bytes::from)
        .collect::<Vec<_>>();
    match enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id: batch.tenant_id,
            namespace: batch.namespace,
            stream: batch.stream,
            payloads,
            response: None,
        },
        EnqueuePolicy::Drop,
    )
    .await
    {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(_) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message_uni(
    broker: &Broker,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
) -> Result<bool> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
    }
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        return Ok(true);
    }

    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id,
            namespace,
            stream,
            payloads: vec![Bytes::from(payload)],
            response: None,
        },
        EnqueuePolicy::Drop,
    )
    .await;
    match r {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            tracing::warn!(error = %err, "publish enqueue failed");
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message_uni(
    broker: &Broker,
    stream_cache: &mut HashMap<String, (bool, Instant)>,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
) -> Result<bool> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
    }
    if !stream_exists_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        return Ok(true);
    }
    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();

    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            tenant_id,
            namespace,
            stream,
            payloads,
            response: None,
        },
        EnqueuePolicy::Drop,
    )
    .await;
    match r {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            tracing::warn!(error = %err, "publish enqueue failed");
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn stream_exists_cached(
    broker: &Broker,
    cache: &mut HashMap<String, (bool, Instant)>,
    key_scratch: &mut String,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> bool {
    // Short-lived cache to avoid repeated stream lookups on hot paths.
    key_scratch.clear();
    let needed = tenant_id.len() + namespace.len() + stream.len() + 2;
    if key_scratch.capacity() < needed {
        key_scratch.reserve(needed - key_scratch.capacity());
    }
    key_scratch.push_str(tenant_id);
    key_scratch.push('\0');
    key_scratch.push_str(namespace);
    key_scratch.push('\0');
    key_scratch.push_str(stream);
    if let Some((exists, expires)) = cache.get(key_scratch.as_str())
        && *expires > Instant::now()
    {
        return *exists;
    }
    let exists = broker.stream_exists(tenant_id, namespace, stream).await;
    cache.insert(
        key_scratch.clone(),
        (exists, Instant::now() + STREAM_CACHE_TTL),
    );
    exists
}
