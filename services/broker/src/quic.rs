// QUIC transport adapter for the broker.
// Decodes felix-wire frames, enforces stream scope, and fans out events to subscribers.
/*
QUIC transport protocol overview:
- Bidirectional streams are the control plane: clients send Publish/PublishBatch, Subscribe,
  and Cache requests, and the broker returns acks/responses on the same stream.
- Subscribe on the control stream causes the broker to open a uni stream for event delivery;
  that uni stream starts with EventStreamHello and then carries Event/EventBatch frames.
- Client-initiated uni streams are publish-only (no acks); they accept Publish/PublishBatch
  and are treated as fire-and-forget ingress.
- Frames use felix-wire; high-throughput paths may carry binary batch frames to avoid JSON
  encode/decode overhead.
- Acked publishes require request_id; PublishOk/PublishError echo it and may arrive out of order.
*/
/*
DESIGN NOTES (why this file is structured this way)

High level goals
- Keep QUIC SendStream writes single-threaded: Quinn's SendStream is not safe/efficient under many
  concurrent writers (it can serialize internally and/or create heavy contention). We therefore funnel
  all outbound control-plane responses/acks through a single writer task per control stream.
- Keep broker mutation serialized per connection: a single publish worker per QUIC connection drains
  a bounded ingress queue. This avoids many tasks mutating broker state concurrently and reduces
  lock contention inside the broker.
- Make overload behavior explicit and observable: bounded queues + metrics + throttling.

Key queues
- Ingress publish queue (PUBLISH_QUEUE_DEPTH): work items sent to a per-connection publish worker.
- Outbound ack/response queue (ACK_QUEUE_DEPTH): Outgoing messages drained by the single writer.
- Ack waiter queue (ACK_WAITERS_MAX): only used when ack_on_commit is enabled; tracks acks that must
  wait until broker commit completes.

Ack modes & policies
- Wire-level ack mode is per-message/per-batch/none; server policy `ack_on_commit` optionally delays
  acks until the publish worker finishes.
- When ack_on_commit=true, the enqueue policy is Wait for acked publishes so we preserve the
  semantic that an ack implies the broker accepted work and (eventually) committed.
- When ack_on_commit=false, we can respond immediately after enqueue.

Potential issues / edge cases to be aware of
- Task lifecycle: writer + ack-waiter tasks must be joined or aborted on stream close. If one task
  finishes and the other is dropped without abort/join, it can continue running detached.
  (This is easy to accidentally introduce when using `tokio::select!` during shutdown.)
- Overload + acked publishes: if we accept a request that expects an ack, but later drop/skip the
  error ack because the outbound queue is full, clients may hang until their own timeout.
  Preferred policy is either "always respond" (critical enqueue / close on failure) or "hard close".
- Backpressure interactions: waiting to enqueue ingress (Wait policy) can propagate latency back to
  the control stream read loop; this is intentional for commit-acked publishes but must be bounded.
- Queue depth gauges are best-effort; under races they can drift. We track drift counters and reset
  local depths on teardown.
- Ordering: acks may be out-of-order relative to requests because publish jobs complete out-of-order
  (and ack waiters emit as they complete). This is allowed by protocol, but clients must treat
  request_id as the correlator.
- Cancellation: cancel signals are delivered via watch channels and are cooperative; code must check
  them in all long waits to avoid hanging tasks.
*/
use crate::config::BrokerConfig;
use crate::timings;
use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
#[cfg(test)]
use felix_broker::CacheMetadata;
use felix_broker::timings as broker_publish_timings;
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::{Frame, FrameHeader, Message};
use futures::{StreamExt, stream::FuturesUnordered};
use quinn::{ReadExactError, RecvStream, SendStream};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
#[cfg(feature = "telemetry")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, oneshot, watch};

#[cfg(feature = "telemetry")]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        metrics::counter!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        NoopCounter
    };
}

#[cfg(feature = "telemetry")]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        NoopHistogram
    };
}

#[cfg(feature = "telemetry")]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        metrics::gauge!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        NoopGauge
    };
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
struct NoopCounter;

#[cfg(not(feature = "telemetry"))]
impl NoopCounter {
    fn increment(&self, _value: u64) {}
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
impl NoopHistogram {
    fn record(&self, _value: f64) {}
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
struct NoopGauge;

#[cfg(not(feature = "telemetry"))]
impl NoopGauge {
    fn set(&self, _value: f64) {}
}

#[cfg(feature = "telemetry")]
#[inline]
fn t_should_sample() -> bool {
    timings::should_sample()
}

#[cfg(not(feature = "telemetry"))]
#[inline]
fn t_should_sample() -> bool {
    false
}

#[cfg(feature = "telemetry")]
#[inline]
fn t_now_if(sample: bool) -> Option<Instant> {
    sample.then(Instant::now)
}

#[cfg(not(feature = "telemetry"))]
#[inline]
fn t_now_if(_sample: bool) -> Option<Instant> {
    None
}

#[cfg(feature = "telemetry")]
type TelemetryInstant = Instant;

#[cfg(not(feature = "telemetry"))]
type TelemetryInstant = ();

#[cfg(feature = "telemetry")]
#[inline]
fn t_instant_now() -> TelemetryInstant {
    Instant::now()
}

#[cfg(not(feature = "telemetry"))]
#[inline]
fn t_instant_now() -> TelemetryInstant {
    ()
}

const PUBLISH_QUEUE_DEPTH: usize = 1024;
const ACK_QUEUE_DEPTH: usize = 2048;
const ACK_WAITERS_MAX: usize = 1024;
const ACK_HI_WATER: usize = ACK_QUEUE_DEPTH * 3 / 4;
const ACK_LO_WATER: usize = ACK_QUEUE_DEPTH / 2;
const ACK_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const ACK_TIMEOUT_WINDOW: Duration = Duration::from_millis(200);
const ACK_TIMEOUT_THRESHOLD: u32 = 3;
const DEFAULT_EVENT_QUEUE_DEPTH: usize = 1024;
const STREAM_CACHE_TTL: Duration = Duration::from_secs(2);
// Event single-binary frame opt-in constants.
const EVENT_SINGLE_BINARY_MIN_BYTES_DEFAULT: usize = 512;
const EVENT_SINGLE_BINARY_ENV: &str = "FELIX_BINARY_SINGLE_EVENT";
const EVENT_SINGLE_BINARY_MIN_BYTES_ENV: &str = "FELIX_BINARY_SINGLE_EVENT_MIN_BYTES";
static SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
static GLOBAL_INGRESS_DEPTH: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
static GLOBAL_ACK_DEPTH: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
static DECODE_ERROR_LOGS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
const DECODE_ERROR_LOG_LIMIT: usize = 20;

// Work item for the publish worker; response is only used for per-message acks.
struct PublishJob {
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Bytes>,
    response: Option<oneshot::Sender<Result<()>>>,
}

// Outbound responses pushed onto the ack/write task.
enum Outgoing {
    Message(Message),
    CacheMessage(Message),
}

// What to do when the publish queue is full.
enum EnqueuePolicy {
    Drop,
    Fail,
    Wait,
}

enum AckWaiterResult {
    Publish {
        request_id: u64,
        payload_len: u64,
        start: TelemetryInstant,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishTimeout {
        request_id: u64,
        start: TelemetryInstant,
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

enum AckWaiterMessage {
    Publish {
        request_id: u64,
        payload_len: u64,
        start: TelemetryInstant,
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

#[derive(Clone)]
struct PublishContext {
    tx: mpsc::Sender<PublishJob>,
    depth: Arc<std::sync::atomic::AtomicUsize>,
    wait_timeout: Duration,
}

#[cfg(feature = "telemetry")]
#[derive(Default)]
struct FrameCounters {
    frames_in_ok: AtomicU64,
    frames_in_err: AtomicU64,
    frames_out_ok: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    pub_frames_in_ok: AtomicU64,
    pub_frames_in_err: AtomicU64,
    pub_items_in_ok: AtomicU64,
    pub_items_in_err: AtomicU64,
    pub_batches_in_ok: AtomicU64,
    pub_batches_in_err: AtomicU64,
    ack_frames_out_ok: AtomicU64,
    ack_items_out_ok: AtomicU64,
    sub_frames_out_ok: AtomicU64,
    sub_items_out_ok: AtomicU64,
    sub_batches_out_ok: AtomicU64,
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Default)]
struct FrameCounters;

#[derive(Debug, Clone)]
pub struct FrameCountersSnapshot {
    pub frames_in_ok: u64,
    pub frames_in_err: u64,
    pub frames_out_ok: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub pub_frames_in_ok: u64,
    pub pub_frames_in_err: u64,
    pub pub_items_in_ok: u64,
    pub pub_items_in_err: u64,
    pub pub_batches_in_ok: u64,
    pub pub_batches_in_err: u64,
    pub ack_frames_out_ok: u64,
    pub ack_items_out_ok: u64,
    pub sub_frames_out_ok: u64,
    pub sub_items_out_ok: u64,
    pub sub_batches_out_ok: u64,
}

#[cfg(feature = "telemetry")]
static FRAME_COUNTERS: OnceLock<Arc<FrameCounters>> = OnceLock::new();

#[cfg(feature = "telemetry")]
fn frame_counters() -> Arc<FrameCounters> {
    FRAME_COUNTERS
        .get_or_init(|| Arc::new(FrameCounters::default()))
        .clone()
}

#[cfg(feature = "telemetry")]
pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    let counters = frame_counters();
    FrameCountersSnapshot {
        frames_in_ok: counters.frames_in_ok.load(Ordering::Relaxed),
        frames_in_err: counters.frames_in_err.load(Ordering::Relaxed),
        frames_out_ok: counters.frames_out_ok.load(Ordering::Relaxed),
        bytes_in: counters.bytes_in.load(Ordering::Relaxed),
        bytes_out: counters.bytes_out.load(Ordering::Relaxed),
        pub_frames_in_ok: counters.pub_frames_in_ok.load(Ordering::Relaxed),
        pub_frames_in_err: counters.pub_frames_in_err.load(Ordering::Relaxed),
        pub_items_in_ok: counters.pub_items_in_ok.load(Ordering::Relaxed),
        pub_items_in_err: counters.pub_items_in_err.load(Ordering::Relaxed),
        pub_batches_in_ok: counters.pub_batches_in_ok.load(Ordering::Relaxed),
        pub_batches_in_err: counters.pub_batches_in_err.load(Ordering::Relaxed),
        ack_frames_out_ok: counters.ack_frames_out_ok.load(Ordering::Relaxed),
        ack_items_out_ok: counters.ack_items_out_ok.load(Ordering::Relaxed),
        sub_frames_out_ok: counters.sub_frames_out_ok.load(Ordering::Relaxed),
        sub_items_out_ok: counters.sub_items_out_ok.load(Ordering::Relaxed),
        sub_batches_out_ok: counters.sub_batches_out_ok.load(Ordering::Relaxed),
    }
}

#[cfg(not(feature = "telemetry"))]
pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    FrameCountersSnapshot {
        frames_in_ok: 0,
        frames_in_err: 0,
        frames_out_ok: 0,
        bytes_in: 0,
        bytes_out: 0,
        pub_frames_in_ok: 0,
        pub_frames_in_err: 0,
        pub_items_in_ok: 0,
        pub_items_in_err: 0,
        pub_batches_in_ok: 0,
        pub_batches_in_err: 0,
        ack_frames_out_ok: 0,
        ack_items_out_ok: 0,
        sub_frames_out_ok: 0,
        sub_items_out_ok: 0,
        sub_batches_out_ok: 0,
    }
}

#[cfg(feature = "telemetry")]
pub fn reset_frame_counters() {
    let counters = frame_counters();
    counters.frames_in_ok.store(0, Ordering::Relaxed);
    counters.frames_in_err.store(0, Ordering::Relaxed);
    counters.frames_out_ok.store(0, Ordering::Relaxed);
    counters.bytes_in.store(0, Ordering::Relaxed);
    counters.bytes_out.store(0, Ordering::Relaxed);
    counters.pub_frames_in_ok.store(0, Ordering::Relaxed);
    counters.pub_frames_in_err.store(0, Ordering::Relaxed);
    counters.pub_items_in_ok.store(0, Ordering::Relaxed);
    counters.pub_items_in_err.store(0, Ordering::Relaxed);
    counters.pub_batches_in_ok.store(0, Ordering::Relaxed);
    counters.pub_batches_in_err.store(0, Ordering::Relaxed);
    counters.ack_frames_out_ok.store(0, Ordering::Relaxed);
    counters.ack_items_out_ok.store(0, Ordering::Relaxed);
    counters.sub_frames_out_ok.store(0, Ordering::Relaxed);
    counters.sub_items_out_ok.store(0, Ordering::Relaxed);
    counters.sub_batches_out_ok.store(0, Ordering::Relaxed);
}

#[cfg(not(feature = "telemetry"))]
pub fn reset_frame_counters() {}

// Publish queue helper with explicit backpressure and timeout semantics.
async fn enqueue_publish(
    publish_ctx: &PublishContext,
    job: PublishJob,
    policy: EnqueuePolicy,
) -> Result<bool> {
    // We use try_send first to keep the fast path allocation-free and to make overload observable
    // (Full vs Closed). Only the Wait policy performs an async send with a timeout.
    // IMPORTANT: Any code path that successfully enqueues MUST increment depth counters exactly once.
    // Best-effort enqueue with metrics and optional backpressure/err.
    match publish_ctx.tx.try_send(job) {
        Ok(()) => {
            let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
            let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
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
                        tokio::time::timeout(publish_ctx.wait_timeout, publish_ctx.tx.send(job))
                            .await
                            .map_err(|_| anyhow!("publish enqueue timed out"))?;
                    send_result.map_err(|_| anyhow!("publish queue closed"))?;
                    let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
                    let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
                    t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
                    Ok(true)
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("publish queue closed")),
    }
}

// Adjust queue depth gauges safely when send fails or work completes.
fn decrement_depth(
    depth: &Arc<std::sync::atomic::AtomicUsize>,
    global: &std::sync::atomic::AtomicUsize,
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

async fn send_outgoing_critical(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<std::sync::atomic::AtomicUsize>,
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

async fn send_outgoing_best_effort(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<std::sync::atomic::AtomicUsize>,
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

struct AckTimeoutState {
    window_start: Instant,
    count: u32,
}

impl AckTimeoutState {
    fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            count: 0,
        }
    }

    fn reset(&mut self, now: Instant) {
        self.window_start = now;
        self.count = 0;
    }

    fn register_timeout(&mut self, now: Instant) -> u32 {
        if now.duration_since(self.window_start) > ACK_TIMEOUT_WINDOW {
            self.window_start = now;
            self.count = 1;
        } else {
            self.count = self.count.saturating_add(1);
        }
        self.count
    }
}

async fn handle_ack_enqueue_result(
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
                record_ack_enqueue_failure_metrics("ack_queue_timeout");
                let _ = cancel_tx.send(true);
                return Err(anyhow!(
                    "closing control stream: ack enqueue timeout streak"
                ));
            }
            Ok(())
        }
        Err(AckEnqueueError::Full) => Err(anyhow!("ack queue full")),
        Err(AckEnqueueError::Closed) => {
            record_ack_enqueue_failure_metrics("ack_queue_closed");
            let _ = throttle_tx.send(false);
            let _ = cancel_tx.send(true);
            Err(anyhow!("closing control stream: ack_queue_closed"))
        }
    }
}

#[derive(Debug)]
enum AckEnqueueError {
    Full,
    Closed,
    Timeout,
}

fn record_ack_enqueue_failure_metrics(reason: &'static str) {
    t_counter!(
        "felix_broker_control_stream_closed_total",
        "reason" => reason
    )
    .increment(1);
    tracing::info!(reason = %reason, "closing control stream due to ack enqueue failure");
}

fn ack_enqueue_reason(err: &AckEnqueueError) -> &'static str {
    match err {
        AckEnqueueError::Full => "ack_queue_full",
        AckEnqueueError::Closed => "ack_queue_closed",
        AckEnqueueError::Timeout => "ack_queue_timeout",
    }
}

fn record_ack_enqueue_failure(err: AckEnqueueError) -> anyhow::Error {
    let reason = ack_enqueue_reason(&err);
    record_ack_enqueue_failure_metrics(reason);
    anyhow!("closing control stream: {reason}")
}

fn reset_local_depth_only(
    depth: &Arc<std::sync::atomic::AtomicUsize>,
    global: &std::sync::atomic::AtomicUsize,
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

async fn stream_exists_cached(
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

pub async fn serve(
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    config: BrokerConfig,
) -> Result<()> {
    // Main accept loop: spawn a task per incoming QUIC connection.
    if config.disable_timings {
        timings::set_enabled(false);
        broker_publish_timings::set_enabled(false);
    }
    loop {
        let connection = server.accept().await?;
        let broker = Arc::clone(&broker);
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(broker, connection, config).await {
                tracing::warn!(error = %err, "quic connection handler failed");
            }
        });
    }
}

async fn handle_connection(
    broker: Arc<Broker>,
    connection: QuicConnection,
    config: BrokerConfig,
) -> Result<()> {
    // One QUIC connection can multiplex multiple streams.
    let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(PUBLISH_QUEUE_DEPTH);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queue_depth_worker = Arc::clone(&queue_depth);
    let broker_for_worker = Arc::clone(&broker);
    // Per-connection publish worker:
    // - Serializes calls into broker.publish_batch (reduces internal contention).
    // - Provides a single place to account ingress queue depth and to complete commit-ack oneshots.
    // - Note: this is per-connection, not global. Global fairness is handled upstream by QUIC
    //   scheduling and per-connection backpressure.
    tokio::spawn(async move {
        while let Some(job) = publish_rx.recv().await {
            let _ = decrement_depth(
                &queue_depth_worker,
                &GLOBAL_INGRESS_DEPTH,
                "felix_broker_ingress_queue_depth",
            );
            let result = broker_for_worker
                .publish_batch(&job.tenant_id, &job.namespace, &job.stream, &job.payloads)
                .await
                .map(|_| ())
                .map_err(Into::into);
            if let Some(response) = job.response {
                let _ = response.send(result);
            }
        }
        reset_local_depth_only(
            &queue_depth_worker,
            &GLOBAL_INGRESS_DEPTH,
            "felix_broker_ingress_queue_depth",
        );
    });
    let publish_ctx = PublishContext {
        tx: publish_tx.clone(),
        depth: Arc::clone(&queue_depth),
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
    };
    loop {
        // Accept both bidirectional control streams and uni-directional publish streams.
        tokio::select! {
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        reset_local_depth_only(
                            &queue_depth,
                            &GLOBAL_INGRESS_DEPTH,
                            "felix_broker_ingress_queue_depth",
                        );
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let connection = connection.clone();
                let config = config.clone();
                let publish_ctx = publish_ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(
                        broker,
                        connection,
                        config,
                        publish_ctx,
                        send,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic stream handler failed");
                    }
                });
            }
            result = connection.accept_uni() => {
                let recv = match result {
                    Ok(recv) => recv,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        reset_local_depth_only(
                            &queue_depth,
                            &GLOBAL_INGRESS_DEPTH,
                            "felix_broker_ingress_queue_depth",
                        );
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let config = config.clone();
                let publish_ctx = publish_ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_uni_stream(
                        broker,
                        config,
                        publish_ctx,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic uni stream handler failed");
                    }
                });
            }
        }
    }
}

async fn handle_stream(
    broker: Arc<Broker>,
    connection: QuicConnection,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // Bi-directional stream: carries control-plane requests and their acks/responses.
    // Payload fanout to subscribers happens on separate uni streams opened per subscription.
    // Outbound response queue:
    // We never write to `send` directly from the read loop. All responses go through this queue and
    // are drained by a single writer task to avoid concurrent SendStream writes.
    // Single-writer response path: enqueue Outgoing messages, one task serializes writes.
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ack_on_commit = config.ack_on_commit;
    let mut stream_cache = HashMap::new();
    let mut stream_cache_key = String::new();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
    let (ack_waiter_tx, mut ack_waiter_rx) = mpsc::channel::<AckWaiterMessage>(ACK_WAITERS_MAX);
    let ack_wait_timeout = Duration::from_millis(config.ack_wait_timeout_ms);

    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let cancel_tx_writer = cancel_tx.clone();
    let mut cancel_rx_writer = cancel_rx.clone();
    let ack_throttle_tx_writer = ack_throttle_tx.clone();
    let out_ack_tx_waiter = out_ack_tx.clone();
    let out_ack_depth_waiter = Arc::clone(&out_ack_depth);
    let ack_throttle_tx_waiter = ack_throttle_tx.clone();
    let ack_timeout_state_waiter = Arc::clone(&ack_timeout_state);
    let cancel_tx_waiter = cancel_tx.clone();
    let mut cancel_rx_waiter = cancel_rx.clone();
    // Single writer task:
    // - Owns the SendStream and performs *all* writes, ensuring ordered, non-overlapping writes.
    // - Applies backpressure signaling via ack_throttle when the outbound queue crosses watermarks.
    // - On any write/encode error, we cancel the control stream (cooperative shutdown) and finish.
    // Correctness note: if the writer exits, any pending Outgoing messages in the channel are dropped;
    // teardown must reconcile depth gauges via `reset_local_depth_only`.
    let writer_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = cancel_rx_writer.changed() => {
                    if changed.is_err() || *cancel_rx_writer.borrow() {
                        break;
                    }
                }
                outgoing = out_ack_rx.recv() => {
                    let Some(outgoing) = outgoing else { break };
                    match outgoing {
                        Outgoing::Message(message) => {
                            let sample = t_should_sample();
                            let is_publish_ack = matches!(
                                message,
                                Message::PublishOk { .. } | Message::PublishError { .. }
                            );
                            #[cfg(not(feature = "telemetry"))]
                            let _ = is_publish_ack;
                            let write_start = t_now_if(sample);
                            if let Err(err) = write_message(&mut send, message).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
                                let _ = ack_throttle_tx_writer.send(false);
                                let _ = cancel_tx_writer.send(true);
                                break;
                            }
                            let depth_update = decrement_depth(
                                &out_ack_depth_worker,
                                &GLOBAL_ACK_DEPTH,
                                "felix_broker_out_ack_depth",
                            );
                            if let Some((prev, cur)) = depth_update
                                && prev >= ACK_HI_WATER
                                && cur < ACK_LO_WATER
                            {
                                let _ = ack_throttle_tx_writer.send(false);
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_quic_write_ns(write_ns);
                                t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                            }
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                if is_publish_ack {
                                    counters.ack_items_out_ok.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Outgoing::CacheMessage(message) => {
                            let sample = t_should_sample();
                            let encode_start = t_now_if(sample);
                            let frame = match message.encode().context("encode message") {
                                Ok(frame) => frame,
                                Err(err) => {
                                    // Dequeue happened; decrement depth even if encode fails.
                                    let _ = decrement_depth(
                                        &out_ack_depth_worker,
                                        &GLOBAL_ACK_DEPTH,
                                        "felix_broker_out_ack_depth",
                                    );
                                    tracing::info!(error = %err, "encode cache response failed");
                                    let _ = ack_throttle_tx_writer.send(false);
                                    let _ = cancel_tx_writer.send(true);
                                    break;
                                }
                            };
                            if let Some(start) = encode_start {
                                let encode_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_encode_ns(encode_ns);
                            }
                            let write_start = t_now_if(sample);
                            if let Err(err) = write_frame(&mut send, &frame).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
                                let _ = ack_throttle_tx_writer.send(false);
                                let _ = cancel_tx_writer.send(true);
                                break;
                            }
                            let depth_update = decrement_depth(
                                &out_ack_depth_worker,
                                &GLOBAL_ACK_DEPTH,
                                "felix_broker_out_ack_depth",
                            );
                            if let Some((prev, cur)) = depth_update
                                && prev >= ACK_HI_WATER
                                && cur < ACK_LO_WATER
                            {
                                let _ = ack_throttle_tx_writer.send(false);
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_write_ns(write_ns);
                            }
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
        let _ = send.finish();
    });

    let ack_waiter_handle = tokio::spawn(async move {
        // Ack waiter task (only meaningful when ack_on_commit=true):
        // We decouple "publish committed" from the control stream read loop.
        // - Each acked publish creates a oneshot receiver that completes when the publish worker
        //   finishes the broker call.
        // - FuturesUnordered allows acks to be emitted as commits complete (out-of-order allowed).
        // - A semaphore bounds the number of in-flight waiters to avoid unbounded memory growth.
        // Correctness note: every waiter must drop its semaphore permit on *all* paths.
        let mut pending = FuturesUnordered::new();
        let mut closed = false;
        loop {
            tokio::select! {
                changed = cancel_rx_waiter.changed() => {
                    if changed.is_err() || *cancel_rx_waiter.borrow() {
                        break;
                    }
                }
                message = ack_waiter_rx.recv() => {
                    match message {
                        Some(message) => {
                            let mut cancel_rx = cancel_rx_waiter.clone();
                            let fut = async move {
                                match message {
                                    AckWaiterMessage::Publish {
                                        request_id,
                                        payload_len,
                                        start,
                                        response_rx,
                                        permit,
                                    } => {
                                        let response = tokio::select! {
                                            _ = cancel_rx.changed() => return None,
                                            response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                        };
                                        drop(permit);
                                        match response {
                                            Ok(response) => Some(AckWaiterResult::Publish {
                                                request_id,
                                                payload_len,
                                                start,
                                                response,
                                            }),
                                            Err(_) => Some(AckWaiterResult::PublishTimeout {
                                                request_id,
                                                start,
                                            }),
                                        }
                                    }
                                    AckWaiterMessage::PublishBatch {
                                        request_id,
                                        payload_bytes,
                                        response_rx,
                                        permit,
                                    } => {
                                        let response = tokio::select! {
                                            _ = cancel_rx.changed() => return None,
                                            response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                        };
                                        drop(permit);
                                        match response {
                                            Ok(response) => Some(AckWaiterResult::PublishBatch {
                                                request_id,
                                                payload_bytes,
                                                response,
                                            }),
                                            Err(_) => Some(AckWaiterResult::PublishBatchTimeout {
                                                request_id,
                                                payload_bytes,
                                            }),
                                        }
                                    }
                                }
                            };
                            pending.push(fut);
                        }
                        None => {
                            closed = true;
                        }
                    }
                }
                result = pending.next(), if !pending.is_empty() => {
                    if let Some(result) = result.flatten() {
                        match result {
                            AckWaiterResult::Publish {
                                request_id,
                                payload_len,
                                start,
                                response,
                            } => {
                                #[cfg(not(feature = "telemetry"))]
                                let _ = start;
                                match response {
                                    Ok(Ok(_)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "ok")
                                            .increment(1);
                                        t_counter!("felix_publish_bytes_total")
                                            .increment(payload_len);
                                        #[cfg(feature = "telemetry")]
                                        {
                                            t_histogram!(
                                                "felix_publish_latency_ms",
                                                "mode" => "commit"
                                            )
                                            .record(start.elapsed().as_secs_f64() * 1000.0);
                                        }
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishOk { request_id }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: err.to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Err(_) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: "publish worker dropped response".to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                }
                            }
                            AckWaiterResult::PublishTimeout { request_id, start } => {
                                #[cfg(not(feature = "telemetry"))]
                                let _ = start;
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "error"
                                )
                                .increment(1);
                                t_counter!("felix_broker_ack_waiter_timeout_total")
                                    .increment(1);
                                #[cfg(feature = "telemetry")]
                                {
                                    t_histogram!(
                                        "felix_publish_latency_ms",
                                        "mode" => "commit"
                                    )
                                    .record(start.elapsed().as_secs_f64() * 1000.0);
                                }
                                if let Err(err) = handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx_waiter,
                                        &out_ack_depth_waiter,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx_waiter,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "publish commit timeout".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state_waiter,
                                    &ack_throttle_tx_waiter,
                                    &cancel_tx_waiter,
                                )
                                .await
                                {
                                    tracing::info!(error = %err, "ack enqueue failed");
                                }
                            }
                            AckWaiterResult::PublishBatch {
                                request_id,
                                payload_bytes,
                                response,
                            } => {
                                match response {
                                    Ok(Ok(_)) => {
                                        t_counter!(
                                            "felix_publish_requests_total",
                                            "result" => "ok"
                                        )
                                        .increment(1);
                                        for bytes in &payload_bytes {
                                            t_counter!("felix_publish_bytes_total")
                                                .increment(*bytes as u64);
                                        }
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishOk { request_id }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: err.to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Err(_) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: "publish batch worker dropped response".to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                }
                            }
                            AckWaiterResult::PublishBatchTimeout {
                                request_id,
                                payload_bytes,
                            } => {
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "error"
                                )
                                .increment(1);
                                t_counter!("felix_broker_ack_waiter_timeout_total")
                                    .increment(1);
                                for bytes in &payload_bytes {
                                    t_counter!("felix_publish_bytes_total")
                                        .increment(*bytes as u64);
                                }
                                if let Err(err) = handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx_waiter,
                                        &out_ack_depth_waiter,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx_waiter,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "publish commit timeout".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state_waiter,
                                    &ack_throttle_tx_waiter,
                                    &cancel_tx_waiter,
                                )
                                .await
                                {
                                    tracing::info!(error = %err, "ack enqueue failed");
                                }
                            }
                        }
                    }
                }
            }
            if closed && pending.is_empty() {
                break;
            }
        }
    });
    // Read loop: decode control-plane frames and enqueue work; acks flow back on this stream.
    // Shutdown/drain note:
    // This function spawns background tasks (writer + ack-waiter). On exit we must ensure:
    // - both tasks are either joined or aborted
    // - local depth counters are reconciled
    // Otherwise we can leak detached tasks or leave gauges permanently high.
    let result: Result<bool> = (async {
        let mut graceful_close = false;
        let mut cancel_rx_read = cancel_rx.clone();
        loop {
            if *cancel_rx_read.borrow() {
                break;
            }
            // Throttling is driven by outbound ack queue depth. When outbound acks back up, we shed
            // load by rejecting/dropping new publish requests early (before doing expensive work).
            // This protects the single writer from unbounded backlog.
            let throttled = *ack_throttle_rx.borrow();
            let sample = t_should_sample();
            let read_start = t_now_if(sample);
            let frame = tokio::select! {
                changed = cancel_rx_read.changed() => {
                    if changed.is_err() || *cancel_rx_read.borrow() {
                        break;
                    }
                    continue;
                }
                frame = read_frame_limited_into(&mut recv, config.max_frame_bytes, &mut frame_scratch) => {
                    match frame? {
                        Some(frame) => frame,
                        None => {
                            graceful_close = true;
                            break;
                        }
                    }
                }
            };
            let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
            // Binary batches bypass JSON decoding for high-throughput publish.
            if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
                let decode_start = t_now_if(sample);
                let batch = match felix_wire::binary::decode_publish_batch(&frame)
                    .context("decode binary publish batch")
                {
                    Ok(batch) => batch,
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                            counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                        }
                        log_decode_error("binary_publish_batch", &err, &frame);
                        return Err(err);
                    }
                };
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
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
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &batch.tenant_id,
                    &batch.namespace,
                    &batch.stream,
                )
                .await
                {
                    t_counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
                    continue;
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
                    &publish_ctx,
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
                        t_counter!("felix_publish_requests_total", "result" => "accepted")
                            .increment(1);
                    }
                    Ok(false) => {
                        t_counter!("felix_publish_requests_total", "result" => "dropped")
                            .increment(1);
                    }
                    Err(err) => {
                        t_counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        tracing::warn!(error = %err, "publish enqueue failed");
                    }
                }

                if let Some(start) = fanout_start {
                    let fanout_ns = start.elapsed().as_nanos() as u64;
                    timings::record_fanout_ns(fanout_ns);
                    t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
                }
                continue;
            }
            let decode_start = t_now_if(sample);
            // Control-plane protocol: decode one frame into a typed Message, then route below.
            // Publish vs PublishBatch determines whether we ingest one payload or a batch and
            // which ack mode we honor (none, per-message, per-batch). If ack_on_commit is set,
            // we delay the ack until the publish worker completes (server-side policy, not a
            // distinct wire AckMode).
            let message = match Message::decode(frame.clone()).context("decode message") {
                Ok(message) => message,
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    log_decode_error("control_message", &err, &frame);
                    return Err(err);
                }
            };
            let decode_ns = decode_start.map(|start| start.elapsed().as_nanos() as u64);
            if let Some(decode_ns) = decode_ns {
                timings::record_decode_ns(decode_ns);
                t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
            }
            match message {
                Message::Publish {
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                    request_id,
                    ack,
                } => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
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
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "server overloaded".to_string(),
                                    }),
                                )
                                .await;
                                if !matches!(result, Err(AckEnqueueError::Full)) {
                                    handle_ack_enqueue_result(
                                        result,
                                        &ack_timeout_state,
                                        &ack_throttle_tx,
                                        &cancel_tx,
                                    )
                                    .await?;
                                }
                            } else {
                                let result = send_outgoing_best_effort(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::Error {
                                        message: "server overloaded".to_string(),
                                    }),
                                )
                                .await;
                                if !matches!(result, Err(AckEnqueueError::Full)) {
                                    handle_ack_enqueue_result(
                                        result,
                                        &ack_timeout_state,
                                        &ack_throttle_tx,
                                        &cancel_tx,
                                    )
                                    .await?;
                                }
                            }
                        }
                        t_counter!(
                            "felix_publish_requests_total",
                            "result" => "dropped"
                        )
                        .increment(1);
                        continue;
                    }
                    // Publish protocol (control stream):
                    // - Client sends Publish { payload, request_id?, ack }.
                    // - Broker enqueues payload (fanout path) and responds:
                    //   - AckMode::None -> no response.
                    //   - AckMode::PerMessage -> PublishOk/PublishError with request_id (acks may be out of order).
                    // - request_id is required for any acked publish.
                    let start = t_instant_now();
                    #[cfg(not(feature = "telemetry"))]
                    let _ = start;
                    let payload_len = payload.len();
                    let enqueue_start = t_now_if(sample);
                    // Ack mode determines if we wait for broker commit or reply immediately.
                    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
                    // Protocol invariant: any acked publish must include request_id, because acks
                    // may be out-of-order and request_id is the only correlator.
                    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::Error {
                                    message: "missing request_id for acked publish".to_string(),
                                }),
                            )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    continue;
                    }
                    let (response_tx, response_rx) =
                        if ack_mode != felix_wire::AckMode::None && ack_on_commit {
                            let (response_tx, response_rx) = oneshot::channel();
                            (Some(response_tx), Some(response_rx))
                        } else {
                            (None, None)
                        };
                    if !stream_exists_cached(
                        &broker,
                        &mut stream_cache,
                        &mut stream_cache_key,
                        &tenant_id,
                        &namespace,
                        &stream,
                    )
                    .await
                    {
                        t_counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        if ack_mode != felix_wire::AckMode::None {
                            let request_id = request_id.expect("request id checked");
                            handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: format!(
                                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                                        ),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await?;
                        }
                        continue;
                    }
                    let enqueue_result = enqueue_publish(
                        &publish_ctx,
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
                        t_histogram!("felix_broker_ingress_enqueue_ns")
                            .record(enqueue_ns as f64);
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
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "accepted"
                                )
                                .increment(1);
                            }
                        }
                        Ok(false) => {
                            t_counter!("felix_publish_requests_total", "result" => "dropped")
                            .increment(1);
                            if ack_mode != felix_wire::AckMode::None {
                                let request_id = request_id.expect("request id checked");
                                handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx,
                                        &out_ack_depth,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "ingress overloaded".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state,
                                    &ack_throttle_tx,
                                    &cancel_tx,
                                )
                                .await?;
                            }
                            continue;
                        }
                        Err(err) => {
                            t_counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if ack_mode != felix_wire::AckMode::None {
                                let request_id = request_id.expect("request id checked");
                                handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx,
                                        &out_ack_depth,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: err.to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state,
                                    &ack_throttle_tx,
                                    &cancel_tx,
                                )
                                .await?;
                            }
                            continue;
                        }
                    }
                    if ack_mode == felix_wire::AckMode::None {
                        continue;
                    }
                    if !ack_on_commit {
                        // Enqueue-ack mode:
                        // Ack means "accepted into the ingress queue", not "committed". This keeps
                        // latency low but can report success even if a later broker error occurs.
                        // Fire-and-forget ack after enqueue when configured.
                        let request_id = request_id.expect("request id checked");
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishOk { request_id }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        t_counter!("felix_publish_requests_total", "result" => "ok")
                            .increment(1);
                        t_counter!("felix_publish_bytes_total")
                            .increment(payload_len as u64);
                        #[cfg(feature = "telemetry")]
                        {
                            t_histogram!("felix_publish_latency_ms", "mode" => "enqueue")
                                .record(start.elapsed().as_secs_f64() * 1000.0);
                        }
                        continue;
                    }
                    let request_id = request_id.expect("request id checked");
                    let response_rx = response_rx.expect("response rx available");
                    let payload_len_for_metrics = payload_len as u64;
                    // Commit-ack mode:
                    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
                    // Correctness note: failing after enqueue means the publish may still commit;
                    // the client will see an error/overload even though the publish succeeded.
                    // If that is unacceptable, we must enforce admission *before* enqueue.
                    let permit = match Arc::clone(&ack_waiters).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            t_counter!(
                                "felix_broker_ack_waiters_exhausted_total"
                            )
                            .increment(1);
                            continue;
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
                            t_counter!("felix_broker_ack_waiter_queue_full_total")
                                .increment(1);
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            continue;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
                            drop(match msg {
                                AckWaiterMessage::Publish { permit, .. }
                                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
                            });
                            t_counter!("felix_broker_ack_waiter_queue_full_total")
                                .increment(1);
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            continue;
                        }
                    }
                }
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    request_id,
                    ack,
                } => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
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
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "server overloaded".to_string(),
                                    }),
                                )
                                .await;
                                if !matches!(result, Err(AckEnqueueError::Full)) {
                                    handle_ack_enqueue_result(
                                        result,
                                        &ack_timeout_state,
                                        &ack_throttle_tx,
                                        &cancel_tx,
                                    )
                                    .await?;
                                }
                            } else {
                                let result = send_outgoing_best_effort(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::Error {
                                        message: "server overloaded".to_string(),
                                    }),
                                )
                                .await;
                                if !matches!(result, Err(AckEnqueueError::Full)) {
                                    handle_ack_enqueue_result(
                                        result,
                                        &ack_timeout_state,
                                        &ack_throttle_tx,
                                        &cancel_tx,
                                    )
                                    .await?;
                                }
                            }
                        }
                        t_counter!(
                            "felix_publish_requests_total",
                            "result" => "dropped"
                        )
                        .increment(1);
                        continue;
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
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::Error {
                                    message: "missing request_id for acked publish batch".to_string(),
                                }),
                            )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    continue;
                    }
                    if !stream_exists_cached(
                        &broker,
                        &mut stream_cache,
                        &mut stream_cache_key,
                        &tenant_id,
                        &namespace,
                        &stream,
                    )
                    .await
                    {
                        t_counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        if ack_mode != felix_wire::AckMode::None {
                            let request_id = request_id.expect("request id checked");
                            handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: format!(
                                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                                        ),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await?;
                        }
                        continue;
                    }
                    let payload_bytes = payloads
                        .iter()
                        .map(|payload| payload.len())
                        .collect::<Vec<_>>();
                    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
                    let (response_tx, response_rx) =
                        if ack_mode != felix_wire::AckMode::None && ack_on_commit {
                            let (response_tx, response_rx) = oneshot::channel();
                            (Some(response_tx), Some(response_rx))
                        } else {
                            (None, None)
                        };
                    let fanout_start = t_now_if(sample);
                    let enqueue_result = enqueue_publish(
                        &publish_ctx,
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
                        t_histogram!("felix_broker_ingress_enqueue_ns")
                            .record(fanout_ns as f64);
                    }
                    match enqueue_result {
                        Ok(true) => {
                            if ack_mode == felix_wire::AckMode::None {
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "accepted"
                                )
                                .increment(1);
                            }
                        }
                        Ok(false) => {
                            t_counter!("felix_publish_requests_total", "result" => "dropped")
                            .increment(1);
                            if ack_mode != felix_wire::AckMode::None {
                                let request_id = request_id.expect("request id checked");
                                handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx,
                                        &out_ack_depth,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "ingress overloaded".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state,
                                    &ack_throttle_tx,
                                    &cancel_tx,
                                )
                                .await?;
                            }
                            continue;
                        }
                        Err(err) => {
                            t_counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if ack_mode != felix_wire::AckMode::None {
                                let request_id = request_id.expect("request id checked");
                                handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx,
                                        &out_ack_depth,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: err.to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state,
                                    &ack_throttle_tx,
                                    &cancel_tx,
                                )
                                .await?;
                            }
                            continue;
                        }
                    }
                    if ack_mode == felix_wire::AckMode::None {
                        continue;
                    }
                    if !ack_on_commit {
                        // Enqueue-ack mode:
                        // Ack means "accepted into the ingress queue", not "committed". This keeps
                        // latency low but can report success even if a later broker error occurs.
                        // Batch ack can be sent once enqueued if commit acks are disabled.
                        let request_id = request_id.expect("request id checked");
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishOk { request_id }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        t_counter!("felix_publish_requests_total", "result" => "ok")
                            .increment(1);
                        for bytes in &payload_bytes {
                            t_counter!("felix_publish_bytes_total").increment(*bytes as u64);
                        }
                        continue;
                    }
                    let request_id = request_id.expect("request id checked");
                    let response_rx = response_rx.expect("response rx available");
                    let payload_bytes_for_metrics = payload_bytes;
                    // Commit-ack mode:
                    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
                    // Correctness note: failing after enqueue means the publish may still commit;
                    // the client will see an error/overload even though the publish succeeded.
                    // If that is unacceptable, we must enforce admission *before* enqueue.
                    let permit = match Arc::clone(&ack_waiters).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            t_counter!(
                                "felix_broker_ack_waiters_exhausted_total"
                            )
                            .increment(1);
                            continue;
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
                            t_counter!("felix_broker_ack_waiter_queue_full_total")
                                .increment(1);
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            continue;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
                            drop(match msg {
                                AckWaiterMessage::Publish { permit, .. }
                                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
                            });
                            t_counter!("felix_broker_ack_waiter_queue_full_total")
                                .increment(1);
                            let _ = send_outgoing_best_effort(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::PublishError {
                                    request_id,
                                    message: "server overloaded".to_string(),
                                }),
                            )
                            .await;
                            continue;
                        }
                    }
                }
                Message::Subscribe {
                    tenant_id,
                    namespace,
                    stream,
                    subscription_id,
                } => {
                    // Subscribe is control-plane: ack/metadata stay on this stream, events go to a new uni stream.
                    let span = tracing::trace_span!(
                        "subscribe",
                        tenant_id = %tenant_id,
                        namespace = %namespace,
                        stream = %stream
                    );
                    let _enter = span.enter();
                    let subscription_id = subscription_id
                        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed));
                    let mut receiver = match broker.subscribe(&tenant_id, &namespace, &stream).await
                    {
                        Ok(receiver) => receiver,
                        Err(err) => {
                            t_counter!("felix_subscribe_requests_total", "result" => "error")
                            .increment(1);
                            handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::Error {
                                        message: err.to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await?;
                            return Ok(true);
                        }
                    };
                    let mut event_send = match connection.open_uni().await {
                        Ok(send) => send,
                        Err(err) => {
                            t_counter!("felix_subscribe_requests_total", "result" => "error")
                            .increment(1);
                            handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::Error {
                                        message: err.to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await?;
                            return Ok(true);
                        }
                    };
                    t_counter!("felix_subscribe_requests_total", "result" => "ok")
                        .increment(1);
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Subscribed { subscription_id }),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    // First write a hello on the uni stream so the client can bind subscription_id -> stream.
                    if let Err(err) = write_message(
                        &mut event_send,
                        Message::EventStreamHello { subscription_id },
                    )
                    .await
                    {
                        tracing::info!(error = %err, "subscription event stream closed");
                        return Ok(true);
                    }

                    let queue_depth = std::env::var("FELIX_EVENT_QUEUE_DEPTH")
                        .ok()
                        .and_then(|value| value.parse::<usize>().ok())
                        .filter(|value| *value > 0)
                        .unwrap_or(DEFAULT_EVENT_QUEUE_DEPTH);
                    let (event_tx, event_rx) = mpsc::channel(queue_depth);
                    tokio::spawn(async move {
                        loop {
                            match receiver.recv().await {
                                Ok(payload) => match event_tx.try_send(EventEnvelope {
                                    payload,
                                    enqueue_at: t_instant_now(),
                                }) {
                                    Ok(()) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        t_counter!("felix_subscribe_dropped_total")
                                            .increment(1);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        break;
                                    }
                                },
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    t_counter!("felix_subscribe_dropped_total").increment(1);
                                }
                            }
                        }
                    });

                    let batch_size = config.fanout_batch_size;
                    let max_events = config.event_batch_max_events.min(batch_size.max(1));
                    let max_bytes = config.event_batch_max_bytes.max(1);
                    let flush_delay = Duration::from_micros(config.event_batch_max_delay_us);

                    // Opt-in: allow using the existing binary EventBatch encoding even when batch_size==1,
                    // to avoid payload.to_vec() in the hot path. Gated by env so we don't break older clients.
                    let binary_single_enabled = std::env::var(EVENT_SINGLE_BINARY_ENV)
                        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                        .unwrap_or(false);
                    let binary_single_min_bytes = std::env::var(EVENT_SINGLE_BINARY_MIN_BYTES_ENV)
                        .ok()
                        .and_then(|v| v.parse::<usize>().ok())
                        .unwrap_or(EVENT_SINGLE_BINARY_MIN_BYTES_DEFAULT);

                    let tenant_id = Arc::<str>::from(tenant_id);
                    let namespace = Arc::<str>::from(namespace);
                    let stream = Arc::<str>::from(stream);

                    tokio::spawn(async move {
                        let writer_config = EventWriterConfig {
                            subscription_id,
                            tenant_id,
                            namespace,
                            stream,
                            batch_size,
                            max_events,
                            max_bytes,
                            flush_delay,
                            binary_single_enabled,
                            binary_single_min_bytes,
                        };
                        if let Err(err) = run_event_writer(
                            event_send,
                            event_rx,
                            writer_config,
                        )
                        .await
                        {
                            tracing::info!(error = %err, "subscription event stream closed");
                        }
                    });
                    return Ok(true);
                }
                Message::CachePut {
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    value,
                    request_id,
                    ttl_ms,
                } => {
                    // Cache ops can be single-response (request_id set) or stream-terminating.
                    if let Some(read_ns) = read_ns {
                        timings::record_cache_read_ns(read_ns);
                    }
                    if let Some(decode_ns) = decode_ns {
                        timings::record_cache_decode_ns(decode_ns);
                    }
                    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!(
                                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                                    ),
                                }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        if request_id.is_none() {
                            return Ok(true);
                        }
                        continue;
                    }
                    let ttl = ttl_ms.map(Duration::from_millis);
                    let lookup_start = t_now_if(sample);
                    broker
                        .cache()
                        .put(
                            tenant_id.as_str(),
                            namespace.as_str(),
                            cache.as_str(),
                            key.as_str(),
                            value,
                            ttl,
                        )
                        .await;
                    if let Some(start) = lookup_start {
                        let lookup_ns = start.elapsed().as_nanos() as u64;
                        timings::record_cache_insert_ns(lookup_ns);
                    }
                    if let Some(request_id) = request_id {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::CacheOk { request_id }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        continue;
                    }
                    match send_outgoing_best_effort(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::CacheMessage(Message::Ok),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(AckEnqueueError::Full) => {}
                        Err(err) => return Err(record_ack_enqueue_failure(err)),
                    }
                    return Ok(true);
                }
                Message::CacheGet {
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    request_id,
                } => {
                    // Cache gets return CacheValue and optionally finish the stream.
                    if let Some(read_ns) = read_ns {
                        timings::record_cache_read_ns(read_ns);
                    }
                    if let Some(decode_ns) = decode_ns {
                        timings::record_cache_decode_ns(decode_ns);
                    }
                    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!(
                                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                                    ),
                                }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        if request_id.is_none() {
                            return Ok(true);
                        }
                        continue;
                    }
                    let lookup_start = t_now_if(sample);
                    let value = broker
                        .cache()
                        .get(&tenant_id, &namespace, &cache, &key)
                        .await;
                    if let Some(start) = lookup_start {
                        let lookup_ns = start.elapsed().as_nanos() as u64;
                        timings::record_cache_lookup_ns(lookup_ns);
                    }
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::CacheMessage(Message::CacheValue {
                                tenant_id,
                                namespace,
                                cache,
                                key,
                                value,
                                request_id,
                            }),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    if request_id.is_none() {
                        return Ok(true);
                    }
                }
                Message::CacheValue { .. }
                | Message::CacheOk { .. }
                | Message::Event { .. }
                | Message::EventBatch { .. }
                | Message::Subscribed { .. }
                | Message::EventStreamHello { .. }
                | Message::PublishOk { .. }
                | Message::PublishError { .. }
                | Message::Ok => {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: "unexpected message type".to_string(),
                            }),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    return Ok(false);
                }
                Message::Error { .. } => {
                    return Ok(false);
                }
            }
        }
        Ok(graceful_close)
    })
    .await;
    let graceful_close = matches!(result, Ok(true));
    if !graceful_close {
        let _ = cancel_tx.send(true);
    }
    drop(out_ack_tx);
    drop(ack_waiter_tx);
    #[cfg(feature = "telemetry")]
    let drain_start = Instant::now();
    let drain_timeout = Duration::from_millis(config.control_stream_drain_timeout_ms);
    let mut writer_handle = writer_handle;
    let mut ack_waiter_handle = ack_waiter_handle;
    let mut timed_out = false;
    tokio::select! {
        _ = tokio::time::sleep(drain_timeout) => {
            timed_out = true;
        }
        _ = &mut writer_handle => {}
        _ = &mut ack_waiter_handle => {}
    }
    #[cfg(feature = "telemetry")]
    {
        t_histogram!("felix_broker_control_stream_drain_ms")
            .record(drain_start.elapsed().as_secs_f64() * 1000.0);
    }
    if timed_out {
        t_counter!("felix_broker_control_stream_drain_timeout_total").increment(1);
        writer_handle.abort();
        ack_waiter_handle.abort();
    }
    reset_local_depth_only(
        &out_ack_depth,
        &GLOBAL_ACK_DEPTH,
        "felix_broker_out_ack_depth",
    );
    result.map(|_| ())
}

async fn handle_uni_stream(
    broker: Arc<Broker>,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut recv: RecvStream,
) -> Result<()> {
    // Uni-directional streams are payload-only: fire-and-forget publishes with no acks/responses.
    let mut stream_cache = HashMap::new();
    let mut stream_cache_key = String::new();
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
    // Read loop: accept publish payloads only; control/ack traffic never appears here.
    loop {
        let frame =
            match read_frame_limited_into(&mut recv, config.max_frame_bytes, &mut frame_scratch)
                .await?
            {
                Some(frame) => frame,
                None => break,
            };
        // Uni streams are publish-only; responses are not sent.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let batch = match felix_wire::binary::decode_publish_batch(&frame)
                .context("decode binary publish batch")
            {
                Ok(batch) => batch,
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    log_decode_error("uni_binary_publish_batch", &err, &frame);
                    return Err(err);
                }
            };
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                counters
                    .pub_items_in_ok
                    .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
            }
            if !stream_exists_cached(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &batch.tenant_id,
                &batch.namespace,
                &batch.stream,
            )
            .await
            {
                t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
                continue;
            }
            let payloads = batch
                .payloads
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>();
            match enqueue_publish(
                &publish_ctx,
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
                    break;
                }
            }
            continue;
        }
        // Publish-only protocol: decode one frame into a typed Message, then route below.
        // Only Publish/PublishBatch are honored on uni streams; no acks are sent.
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                log_decode_error("uni_message", &err, &frame);
                return Err(err);
            }
        };
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
                }
                if !stream_exists_cached(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &tenant_id,
                    &namespace,
                    &stream,
                )
                .await
                {
                    t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
                    continue;
                }

                let r = enqueue_publish(
                    &publish_ctx,
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
                        t_counter!("felix_publish_requests_total", "result" => "accepted")
                            .increment(1);
                    }
                    Ok(false) => {
                        t_counter!("felix_publish_requests_total", "result" => "dropped")
                            .increment(1);
                    }
                    Err(err) => {
                        t_counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        tracing::warn!(error = %err, "publish enqueue failed");
                        break;
                    }
                }
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ..
            } => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters
                        .pub_items_in_ok
                        .fetch_add(payloads.len() as u64, Ordering::Relaxed);
                }
                if !stream_exists_cached(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &tenant_id,
                    &namespace,
                    &stream,
                )
                .await
                {
                    t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
                    continue;
                }
                let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();

                let r = enqueue_publish(
                    &publish_ctx,
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
                        t_counter!("felix_publish_requests_total", "result" => "accepted")
                            .increment(1);
                    }
                    Ok(false) => {
                        t_counter!("felix_publish_requests_total", "result" => "dropped")
                            .increment(1);
                    }
                    Err(err) => {
                        t_counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        tracing::warn!(error = %err, "publish enqueue failed");
                        break;
                    }
                }
            }
            _ => {
                t_counter!(
                    "felix_quic_protocol_violation_total",
                    "stream" => "uni"
                )
                .increment(1);
                tracing::debug!("closing uni stream after non-publish message");
                break;
            }
        }
    }
    Ok(())
}

// Helper for tests and small control flows with an explicit frame cap.
pub async fn read_message_limited(
    recv: &mut RecvStream,
    max_frame_bytes: usize,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    let frame = match read_frame_limited_into(recv, max_frame_bytes, frame_scratch).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
}

// Helper to encode + write a single message.
pub async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame(send, &frame).await
}

// Low-level frame reader with a max payload cap.
async fn read_frame_limited_into(
    recv: &mut RecvStream,
    max_payload_bytes: usize,
    scratch: &mut BytesMut,
) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    if length > max_payload_bytes {
        return Err(anyhow!(
            "frame length {length} exceeds max_payload_bytes {max_payload_bytes}"
        ));
    }
    scratch.clear();
    scratch.resize(length, 0u8);
    recv.read_exact(&mut scratch[..])
        .await
        .context("read frame payload")?;
    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.fetch_add(1, Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }
    Ok(Some(frame))
}

// Low-level frame writer for QUIC streams.
async fn write_frame(send: &mut SendStream, frame: &Frame) -> Result<()> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    frame.header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes)
        .await
        .context("write frame header")?;
    send.write_all(&frame.payload)
        .await
        .context("write frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }
    Ok(())
}

// Write raw pre-encoded bytes (used for cached responses).
async fn write_frame_bytes(send: &mut SendStream, bytes: Bytes) -> Result<()> {
    send.write_all(&bytes)
        .await
        .context("write frame")
        .map(|_| {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                counters
                    .bytes_out
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
        })
}

struct EventWriterConfig {
    subscription_id: u64,
    tenant_id: Arc<str>,
    namespace: Arc<str>,
    stream: Arc<str>,
    batch_size: usize,
    max_events: usize,
    max_bytes: usize,
    flush_delay: Duration,
    binary_single_enabled: bool,
    binary_single_min_bytes: usize,
}

struct EventEnvelope {
    payload: Bytes,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    enqueue_at: TelemetryInstant,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct EventMessageRef<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
    #[serde(with = "base64_bytes_slice")]
    payload: &'a [u8],
}

mod base64_bytes_slice {
    use base64::Engine;

    pub fn serialize<S>(value: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }
}

fn encode_event_json_frame(config: &EventWriterConfig, payload: &[u8]) -> Result<Frame> {
    let message = EventMessageRef {
        kind: "event",
        tenant_id: config.tenant_id.as_ref(),
        namespace: config.namespace.as_ref(),
        stream: config.stream.as_ref(),
        payload,
    };
    let payload = serde_json::to_vec(&message).context("encode event message")?;
    Frame::new(0, Bytes::from(payload)).context("encode event frame")
}

#[cfg(feature = "telemetry")]
fn log_decode_error(context: &str, err: &anyhow::Error, frame: &Frame) {
    let count = DECODE_ERROR_LOGS.fetch_add(1, Ordering::Relaxed);
    if count >= DECODE_ERROR_LOG_LIMIT {
        return;
    }
    let preview_len = frame.payload.len().min(64);
    let preview = &frame.payload[..preview_len];
    let hex = preview
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let printable = preview
        .iter()
        .map(|b| {
            let c = *b as char;
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '.'
            }
        })
        .collect::<String>();
    tracing::warn!(
        error = %err,
        context,
        frame_len = frame.header.length,
        payload_len = frame.payload.len(),
        preview_hex = %hex,
        preview_printable = %printable,
        "frame decode error"
    );
}

#[cfg(not(feature = "telemetry"))]
fn log_decode_error(_context: &str, _err: &anyhow::Error, _frame: &Frame) {}

#[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
async fn run_event_writer(
    mut event_send: SendStream,
    mut rx: mpsc::Receiver<EventEnvelope>,
    config: EventWriterConfig,
) -> Result<()> {
    let mut pending: Option<EventEnvelope> = None;
    if config.batch_size <= 1 {
        loop {
            let sample = t_should_sample();
            let queue_start = t_now_if(sample);
            let envelope = match rx.recv().await {
                Some(payload) => payload,
                None => break,
            };
            #[cfg(feature = "telemetry")]
            let enqueue_at = envelope.enqueue_at;
            let payload = envelope.payload;
            t_counter!("felix_subscribe_bytes_total").increment(payload.len() as u64);
            if let Some(start) = queue_start {
                let queue_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_queue_wait_ns(queue_ns);
                t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
            }
            let write_start = t_now_if(sample);
            #[cfg(not(feature = "telemetry"))]
            let _ = write_start;
            if config.binary_single_enabled && payload.len() >= config.binary_single_min_bytes {
                let batch = vec![payload];
                let frame_bytes =
                    felix_wire::binary::encode_event_batch_bytes(config.subscription_id, &batch)
                        .context("encode binary event batch")?;
                t_histogram!("broker_sub_frame_bytes").record(frame_bytes.len() as f64);
                write_frame_bytes(&mut event_send, frame_bytes).await?;
            } else {
                let frame = encode_event_json_frame(&config, &payload)?;
                t_histogram!("broker_sub_frame_bytes")
                    .record((FrameHeader::LEN + frame.payload.len()) as f64);
                write_frame(&mut event_send, &frame).await?;
            }
            #[cfg(feature = "telemetry")]
            let write_end = Instant::now();
            #[cfg(feature = "telemetry")]
            {
                let delivery_ns = write_end.duration_since(enqueue_at).as_nanos() as u64;
                t_histogram!("broker_sub_delivery_latency_ns").record(delivery_ns as f64);
                timings::record_sub_delivery_ns(delivery_ns);
            }
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.sub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                counters.sub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                counters.sub_items_out_ok.fetch_add(1, Ordering::Relaxed);
            }
            #[cfg(feature = "telemetry")]
            if let Some(start) = write_start {
                let write_ns = write_end.duration_since(start).as_nanos() as u64;
                timings::record_sub_write_ns(write_ns);
                timings::record_quic_write_ns(write_ns);
                t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
                t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
            }
        }
        let _ = event_send.finish();
        return Ok(());
    }

    // Batch mode: coalesce events by count, size, or deadline.
    loop {
        let sample = t_should_sample();
        let queue_start = t_now_if(sample);
        let first = match pending.take() {
            Some(payload) => payload,
            None => match rx.recv().await {
                Some(payload) => payload,
                None => break,
            },
        };
        if let Some(start) = queue_start {
            let queue_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_queue_wait_ns(queue_ns);
            t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
        }

        let mut batch = Vec::with_capacity(config.max_events.max(1));
        let mut batch_bytes = 0usize;
        let mut closed = false;
        #[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
        #[allow(unused_assignments)]
        let mut flush_reason = "idle";

        batch_bytes += first.payload.len();
        batch.push(first);
        let deadline = tokio::time::Instant::now() + config.flush_delay;
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);

        if batch.len() >= config.max_events {
            flush_reason = "count";
        } else if batch_bytes >= config.max_bytes {
            flush_reason = "bytes";
        }

        while flush_reason == "idle" && !closed {
            while batch.len() < config.max_events && batch_bytes < config.max_bytes {
                match rx.try_recv() {
                    Ok(payload) => {
                        if batch_bytes.saturating_add(payload.payload.len()) > config.max_bytes {
                            pending = Some(payload);
                            flush_reason = "bytes";
                            break;
                        }
                        batch_bytes += payload.payload.len();
                        batch.push(payload);
                        if batch.len() >= config.max_events {
                            flush_reason = "count";
                            break;
                        }
                        if batch_bytes >= config.max_bytes {
                            flush_reason = "bytes";
                            break;
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        closed = true;
                        flush_reason = "idle";
                        break;
                    }
                }
            }

            if flush_reason != "idle" || closed {
                break;
            }

            tokio::select! {
                recv = rx.recv() => {
                    match recv {
                        Some(payload) => {
                            if batch_bytes.saturating_add(payload.payload.len()) > config.max_bytes {
                                pending = Some(payload);
                                flush_reason = "bytes";
                                break;
                            }
                            batch_bytes += payload.payload.len();
                            batch.push(payload);
                            if batch.len() >= config.max_events {
                                flush_reason = "count";
                                break;
                            }
                            if batch_bytes >= config.max_bytes {
                                flush_reason = "bytes";
                                break;
                            }
                        }
                        None => {
                            closed = true;
                            flush_reason = "idle";
                            break;
                        }
                    }
                }
                _ = &mut deadline_sleep => {
                    flush_reason = "deadline";
                    break;
                }
            }
        }

        let write_start = t_now_if(sample);
        #[cfg(not(feature = "telemetry"))]
        let _ = write_start;
        t_counter!(
            "felix_broker_event_batch_flush_reason_total",
            "reason" => flush_reason
        )
        .increment(1);
        t_histogram!("felix_broker_event_batch_size_bytes").record(batch_bytes as f64);
        let payloads = batch
            .iter()
            .map(|event| event.payload.clone())
            .collect::<Vec<_>>();
        let frame_bytes =
            felix_wire::binary::encode_event_batch_bytes(config.subscription_id, &payloads)
                .context("encode binary event batch")?;
        t_counter!("felix_subscribe_bytes_total").increment(batch_bytes as u64);
        t_histogram!("broker_sub_frame_bytes").record(frame_bytes.len() as f64);
        write_frame_bytes(&mut event_send, frame_bytes).await?;
        #[cfg(feature = "telemetry")]
        let write_end = Instant::now();
        #[cfg(feature = "telemetry")]
        for event in &batch {
            let delivery_ns = write_end.duration_since(event.enqueue_at).as_nanos() as u64;
            t_histogram!("broker_sub_delivery_latency_ns").record(delivery_ns as f64);
            timings::record_sub_delivery_ns(delivery_ns);
        }
        #[cfg(feature = "telemetry")]
        {
            let counters = frame_counters();
            counters.sub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
            counters.sub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
            counters
                .sub_items_out_ok
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = write_start {
            let write_ns = write_end.duration_since(start).as_nanos() as u64;
            timings::record_sub_write_ns(write_ns);
            timings::record_quic_write_ns(write_ns);
            t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
            t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
            t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
        }
        if closed {
            break;
        }
    }
    let _ = event_send.finish();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicClient, TransportConfig};
    use quinn::ClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;

    #[tokio::test]
    async fn cache_put_get_round_trip() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_cache("t1", "default", "primary", CacheMetadata)
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(serve(Arc::clone(&server), Arc::clone(&broker), config));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CachePut {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: Bytes::from_static(b"cached"),
                request_id: None,
                ttl_ms: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch).await?;
        assert_eq!(response, Some(Message::Ok));

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CacheGet {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                request_id: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch).await?;
        assert_eq!(
            response,
            Some(Message::CacheValue {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: Some(Bytes::from_static(b"cached")),
                request_id: None,
            })
        );

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_rejects_unknown_stream() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(serve(Arc::clone(&server), Arc::clone(&broker), config));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(1),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch).await?;
        assert!(matches!(
            response,
            Some(Message::PublishError { request_id: 1, .. })
        ));

        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "missing", Default::default())
            .await
            .expect("register");
        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(2),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch).await?;
        assert_eq!(response, Some(Message::PublishOk { request_id: 2 }));

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_ack_on_commit_smoke() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "updates", Default::default())
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let mut config = BrokerConfig::from_env()?;
        config.ack_on_commit = true;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(serve(Arc::clone(&server), Arc::clone(&broker), config));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "updates".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(1),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch).await?;
        assert_eq!(response, Some(Message::PublishOk { request_id: 1 }));

        drop(connection);
        server_task.abort();
        Ok(())
    }

    fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
                .context("build server config")?;
        Ok((server_config, cert_der))
    }

    fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert)?;
        Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
    }

    #[test]
    fn ack_timeout_state_resets_after_window() {
        let now = Instant::now();
        let mut state = AckTimeoutState::new(now);
        assert_eq!(state.register_timeout(now), 1);
        assert_eq!(state.register_timeout(now), 2);
        let later = now + ACK_TIMEOUT_WINDOW + Duration::from_millis(1);
        assert_eq!(state.register_timeout(later), 1);
    }
}
