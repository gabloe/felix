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
use crate::config::BrokerConfig;
use crate::timings;
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use felix_broker::Broker;
#[cfg(test)]
use felix_broker::CacheMetadata;
use felix_broker::timings as broker_publish_timings;
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, oneshot, watch};

const PUBLISH_QUEUE_DEPTH: usize = 1024;
const ACK_QUEUE_DEPTH: usize = 256;
const ACK_WAITERS_MAX: usize = 1024;
const ACK_HI_WATER: usize = ACK_QUEUE_DEPTH * 3 / 4;
const ACK_LO_WATER: usize = ACK_QUEUE_DEPTH / 2;
const ACK_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const ACK_TIMEOUT_WINDOW: Duration = Duration::from_millis(200);
const ACK_TIMEOUT_THRESHOLD: u32 = 3;
const STREAM_CACHE_TTL: Duration = Duration::from_secs(2);
static SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
static GLOBAL_INGRESS_DEPTH: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
static GLOBAL_ACK_DEPTH: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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

#[derive(Clone)]
struct PublishContext {
    tx: mpsc::Sender<PublishJob>,
    depth: Arc<std::sync::atomic::AtomicUsize>,
    wait_timeout: Duration,
}

// Publish queue helper with explicit backpressure and timeout semantics.
async fn enqueue_publish(
    publish_ctx: &PublishContext,
    job: PublishJob,
    policy: EnqueuePolicy,
) -> Result<bool> {
    // Best-effort enqueue with metrics and optional backpressure/err.
    match publish_ctx.tx.try_send(job) {
        Ok(()) => {
            let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
            let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::gauge!("felix_broker_ingress_queue_depth").set(global as f64);
            Ok(true)
        }
        Err(mpsc::error::TrySendError::Full(job)) => {
            metrics::counter!("felix_broker_ingress_queue_full_total").increment(1);
            match policy {
                EnqueuePolicy::Drop => {
                    metrics::counter!("felix_broker_ingress_dropped_total").increment(1);
                    Ok(false)
                }
                EnqueuePolicy::Fail => {
                    metrics::counter!("felix_broker_ingress_rejected_total").increment(1);
                    Err(anyhow!("publish queue full"))
                }
                EnqueuePolicy::Wait => {
                    // Acked publishes with ack_on_commit use Wait; otherwise we Fail/Drop.
                    metrics::counter!("felix_broker_ingress_waited_total").increment(1);
                    let send_result =
                        tokio::time::timeout(publish_ctx.wait_timeout, publish_ctx.tx.send(job))
                            .await
                            .map_err(|_| anyhow!("publish enqueue timed out"))?;
                    send_result.map_err(|_| anyhow!("publish queue closed"))?;
                    let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
                    let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
                    metrics::gauge!("felix_broker_ingress_queue_depth").set(global as f64);
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
    if let Ok(prev) = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        if value == 0 { None } else { Some(value - 1) }
    }) {
        let cur = prev.saturating_sub(1);
        let updated = global
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value == 0 { None } else { Some(value - 1) }
            })
            .map(|value| {
                let next = value - 1;
                debug_assert!(next <= value, "global ack depth underflow");
                next
            })
            .unwrap_or(0);
        metrics::gauge!(gauge).set(updated as f64);
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
    // Critical enqueue with timeout; cancel the stream if it cannot be queued.
    let send_result = tokio::time::timeout(ACK_ENQUEUE_TIMEOUT, tx.send(message)).await;
    match send_result {
        Ok(Ok(())) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Ok(Err(_)) => Err(AckEnqueueError::Closed),
        Err(_) => {
            metrics::counter!("felix_broker_out_ack_timeout_total").increment(1);
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
    // Best-effort enqueue; fail fast if the ack queue is full to avoid deadlocks.
    match tx.try_send(message) {
        Ok(()) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics::counter!("felix_broker_out_ack_full_total").increment(1);
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
                record_ack_enqueue_failure_metrics("ack_queue_timeout_streak");
                let _ = cancel_tx.send(true);
                return Err(anyhow!("closing control stream: ack_queue_timeout_streak"));
            }
            Ok(())
        }
        Err(err) => {
            let err = record_ack_enqueue_failure(err);
            let _ = cancel_tx.send(true);
            Err(err)
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
    metrics::counter!(
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

fn reset_depth(
    depth: &Arc<std::sync::atomic::AtomicUsize>,
    global: &std::sync::atomic::AtomicUsize,
    gauge: &'static str,
) {
    let outstanding = depth.swap(0, Ordering::Relaxed);
    if outstanding == 0 {
        return;
    }
    let mut prev = global.load(Ordering::Relaxed);
    loop {
        let next = prev.saturating_sub(outstanding);
        match global.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                metrics::gauge!(gauge).set(next as f64);
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
    // Per-connection publish worker to serialize writes into broker state.
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
        reset_depth(
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
                        reset_depth(
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
                        reset_depth(
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
    // Single-writer response path: enqueue Outgoing messages, one task serializes writes.
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ack_on_commit = config.ack_on_commit;
    let mut stream_cache = HashMap::new();
    let mut stream_cache_key = String::new();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));
    let (ack_throttle_tx, mut ack_throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));

    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let cancel_tx_writer = cancel_tx.clone();
    let mut cancel_rx_writer = cancel_rx.clone();
    let ack_throttle_tx_writer = ack_throttle_tx.clone();
    // Single writer drains the queue to keep QUIC writes ordered and non-overlapping.
    tokio::spawn(async move {
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
                            let sample = timings::should_sample();
                            let write_start = sample.then(Instant::now);
                            if let Err(err) = write_message(&mut send, message).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
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
                                metrics::histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                            }
                        }
                        Outgoing::CacheMessage(message) => {
                            let sample = timings::should_sample();
                            let encode_start = sample.then(Instant::now);
                            let frame = match message.encode().context("encode message") {
                                Ok(frame) => frame,
                                Err(err) => {
                                    let _ = decrement_depth(
                                        &out_ack_depth_worker,
                                        &GLOBAL_ACK_DEPTH,
                                        "felix_broker_out_ack_depth",
                                    );
                                    tracing::info!(error = %err, "encode cache response failed");
                                    let _ = cancel_tx_writer.send(true);
                                    break;
                                }
                            };
                            if let Some(start) = encode_start {
                                let encode_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_encode_ns(encode_ns);
                            }
                            let write_start = sample.then(Instant::now);
                            if let Err(err) = write_frame(&mut send, frame).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
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
                        }
                    }
                }
            }
        }
        let _ = send.finish();
    });
    // Read loop: decode control-plane frames and enqueue work; acks flow back on this stream.
    let result: Result<()> = (async {
        let mut cancel_rx_read = cancel_rx.clone();
        loop {
            if *cancel_rx_read.borrow() {
                break;
            }
            while *ack_throttle_rx.borrow() {
                tokio::select! {
                    changed = cancel_rx_read.changed() => {
                        if changed.is_err() || *cancel_rx_read.borrow() {
                            return Ok(());
                        }
                    }
                    changed = ack_throttle_rx.changed() => {
                        if changed.is_err() {
                            return Ok(());
                        }
                        if !*ack_throttle_rx.borrow() {
                            let mut guard = ack_timeout_state.lock().await;
                            guard.reset(Instant::now());
                        }
                    }
                }
            }
            let sample = timings::should_sample();
            let read_start = sample.then(Instant::now);
            let frame = tokio::select! {
                changed = cancel_rx_read.changed() => {
                    if changed.is_err() || *cancel_rx_read.borrow() {
                        break;
                    }
                    continue;
                }
                frame = read_frame_limited(&mut recv, config.max_frame_bytes) => {
                    match frame? {
                        Some(frame) => frame,
                        None => break,
                    }
                }
            };
            let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
            // Binary batches bypass JSON decoding for high-throughput publish.
            if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
                let decode_start = sample.then(Instant::now);
                let batch = felix_wire::binary::decode_publish_batch(&frame)
                    .context("decode binary publish batch")?;
                if let Some(start) = decode_start {
                    let decode_ns = start.elapsed().as_nanos() as u64;
                    timings::record_decode_ns(decode_ns);
                    metrics::histogram!("felix_broker_decode_ns").record(decode_ns as f64);
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
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
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
                let fanout_start = sample.then(Instant::now);
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

                emit_send_telemetry(r);

                if let Some(start) = fanout_start {
                    let fanout_ns = start.elapsed().as_nanos() as u64;
                    timings::record_fanout_ns(fanout_ns);
                    metrics::histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
                }
                continue;
            }
            let decode_start = sample.then(Instant::now);
            // Control-plane protocol: decode one frame into a typed Message, then route below.
            // Publish vs PublishBatch determines whether we ingest one payload or a batch and
            // which ack mode we honor (none, per-message, per-batch). If ack_on_commit is set,
            // we delay the ack until the publish worker completes (server-side policy, not a
            // distinct wire AckMode).
            let message = Message::decode(frame).context("decode message")?;
            let decode_ns = decode_start.map(|start| start.elapsed().as_nanos() as u64);
            if let Some(decode_ns) = decode_ns {
                timings::record_decode_ns(decode_ns);
                metrics::histogram!("felix_broker_decode_ns").record(decode_ns as f64);
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
                    // Publish protocol (control stream):
                    // - Client sends Publish { payload, request_id?, ack }.
                    // - Broker enqueues payload (fanout path) and responds:
                    //   - AckMode::None -> no response.
                    //   - AckMode::PerMessage -> PublishOk/PublishError with request_id (acks may be out of order).
                    // - request_id is required for any acked publish.
                    let start = Instant::now();
                    let payload_len = payload.len();
                    let enqueue_start = sample.then(Instant::now);
                    // Ack mode determines if we wait for broker commit or reply immediately.
                    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
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
                        return Ok(());
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
                        metrics::counter!("felix_publish_requests_total", "result" => "error")
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
                        metrics::histogram!("felix_broker_ingress_enqueue_ns")
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
                            metrics::counter!("felix_publish_requests_total", "result" => "success")
                            .increment(1);
                        }
                        Ok(false) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "dropped")
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
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
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
                        metrics::counter!("felix_publish_requests_total", "result" => "ok")
                            .increment(1);
                        metrics::counter!("felix_publish_bytes_total")
                            .increment(payload_len as u64);
                        metrics::histogram!("felix_publish_latency_ms")
                            .record(start.elapsed().as_secs_f64() * 1000.0);
                        continue;
                    }
                    let out_tx = out_ack_tx.clone();
                    let out_ack_depth = Arc::clone(&out_ack_depth);
                    let ack_waiters = Arc::clone(&ack_waiters);
                    let ack_throttle_tx = ack_throttle_tx.clone();
                    let ack_timeout_state = Arc::clone(&ack_timeout_state);
                    let mut cancel_rx = cancel_rx.clone();
                    let cancel_tx = cancel_tx.clone();
                    let request_id = request_id.expect("request id checked");
                    let response_rx = response_rx.expect("response rx available");
                    let permit = match ack_waiters.try_acquire_owned() {
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
                            metrics::counter!(
                                "felix_broker_control_stream_closed_total",
                                "reason" => "ack_waiters_full"
                            )
                            .increment(1);
                            tracing::info!("closing control stream due to ack waiter overflow");
                            let _ = cancel_tx.send(true);
                            return Ok(());
                        }
                    };
                    // Wait for broker publish result before acknowledging.
                    tokio::spawn(async move {
                        let _permit = permit;
                        if *cancel_rx.borrow() {
                            return;
                        }
                        // NOTE: acks may be emitted out of publish order; correlate by request_id.
                        let response = tokio::select! {
                            _ = cancel_rx.changed() => return,
                            response = response_rx => response,
                        };
                        match response {
                            Ok(Ok(())) => {
                                metrics::counter!("felix_publish_requests_total", "result" => "ok")
                                    .increment(1);
                                metrics::counter!("felix_publish_bytes_total")
                                    .increment(payload_len as u64);
                                metrics::histogram!("felix_publish_latency_ms")
                                    .record(start.elapsed().as_secs_f64() * 1000.0);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
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
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                            Ok(Err(err)) => {
                                metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
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
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                            Err(_) => {
                                metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "publish worker dropped response".to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                        }
                    });
                }
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    request_id,
                    ack,
                } => {
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
                        return Ok(());
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
                        metrics::counter!("felix_publish_requests_total", "result" => "error")
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
                    let fanout_start = sample.then(Instant::now);
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
                        metrics::histogram!("felix_broker_ingress_enqueue_ns")
                            .record(fanout_ns as f64);
                    }
                    match enqueue_result {
                        Ok(true) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "success")
                            .increment(1);
                        }
                        Ok(false) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "dropped")
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
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
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
                        for bytes in &payload_bytes {
                            metrics::counter!("felix_publish_requests_total", "result" => "ok")
                                .increment(1);
                            metrics::counter!("felix_publish_bytes_total").increment(*bytes as u64);
                        }
                        continue;
                    }
                    let out_tx = out_ack_tx.clone();
                    let out_ack_depth = Arc::clone(&out_ack_depth);
                    let ack_waiters = Arc::clone(&ack_waiters);
                    let ack_throttle_tx = ack_throttle_tx.clone();
                    let ack_timeout_state = Arc::clone(&ack_timeout_state);
                    let mut cancel_rx = cancel_rx.clone();
                    let cancel_tx = cancel_tx.clone();
                    let request_id = request_id.expect("request id checked");
                    let response_rx = response_rx.expect("response rx available");
                    let permit = match ack_waiters.try_acquire_owned() {
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
                            metrics::counter!(
                                "felix_broker_control_stream_closed_total",
                                "reason" => "ack_waiters_full"
                            )
                            .increment(1);
                            tracing::info!("closing control stream due to ack waiter overflow");
                            let _ = cancel_tx.send(true);
                            return Ok(());
                        }
                    };
                    // Wait for broker publish result before acknowledging.
                    tokio::spawn(async move {
                        let _permit = permit;
                        if *cancel_rx.borrow() {
                            return;
                        }
                        // NOTE: acks may be emitted out of publish order; correlate by request_id.
                        let response = tokio::select! {
                            _ = cancel_rx.changed() => return,
                            response = response_rx => response,
                        };
                        match response {
                            Ok(Ok(())) => {
                                for bytes in &payload_bytes {
                                    metrics::counter!(
                                        "felix_publish_requests_total",
                                        "result" => "ok"
                                    )
                                    .increment(1);
                                    metrics::counter!("felix_publish_bytes_total")
                                        .increment(*bytes as u64);
                                }
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
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
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                            Ok(Err(err)) => {
                                metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
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
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                            Err(_) => {
                                metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_tx,
                                    &out_ack_depth,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "publish batch worker dropped response"
                                            .to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state,
                                &ack_throttle_tx,
                                &cancel_tx,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                            }
                        }
                    });
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
                            metrics::counter!("felix_subscribe_requests_total", "result" => "error")
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
                            return Ok(());
                        }
                    };
                    let mut event_send = match connection.open_uni().await {
                        Ok(send) => send,
                        Err(err) => {
                            metrics::counter!("felix_subscribe_requests_total", "result" => "error")
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
                            return Ok(());
                        }
                    };
                    metrics::counter!("felix_subscribe_requests_total", "result" => "ok")
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
                        return Ok(());
                    }

                    let batch_size = config.fanout_batch_size;
                    let max_events = config.event_batch_max_events.min(batch_size.max(1));
                    let max_bytes = config.event_batch_max_bytes.max(1);
                    let flush_delay = Duration::from_micros(config.event_batch_max_delay_us);
                    let mut pending: Option<Bytes> = None;

                    if batch_size <= 1 {
                        // Single-event mode: write each event as it arrives.
                        loop {
                            match receiver.recv().await {
                                Ok(payload) => {
                                    let sample = timings::should_sample();
                                    let queue_start = sample.then(Instant::now);
                                    let message = Message::Event {
                                        tenant_id: tenant_id.clone(),
                                        namespace: namespace.clone(),
                                        stream: stream.clone(),
                                        payload: payload.to_vec(),
                                    };
                                    metrics::counter!("felix_subscribe_bytes_total")
                                        .increment(payload.len() as u64);
                                    if let Some(start) = queue_start {
                                        let queue_ns = start.elapsed().as_nanos() as u64;
                                        timings::record_sub_queue_wait_ns(queue_ns);
                                        metrics::histogram!("felix_broker_sub_recv_wait_ns")
                                            .record(queue_ns as f64);
                                    }
                                    let write_start = sample.then(Instant::now);
                                    if let Err(err) = write_message(&mut event_send, message).await
                                    {
                                        tracing::info!(error = %err, "subscription event stream closed");
                                        return Ok(());
                                    }
                                    if let Some(start) = write_start {
                                        let write_ns = start.elapsed().as_nanos() as u64;
                                        timings::record_sub_write_ns(write_ns);
                                        timings::record_quic_write_ns(write_ns);
                                        metrics::histogram!("felix_broker_sub_write_ns")
                                            .record(write_ns as f64);
                                        metrics::histogram!("felix_broker_quic_write_ns")
                                            .record(write_ns as f64);
                                    }
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                    continue;
                                }
                            }
                        }
                        return Ok(());
                    }

                    // Batch mode: coalesce events by count, size, or deadline.
                    loop {
                        let sample = timings::should_sample();
                        let queue_start = sample.then(Instant::now);
                        let first = match pending.take() {
                            Some(payload) => payload,
                            None => match receiver.recv().await {
                                Ok(payload) => payload,
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                    continue;
                                }
                            },
                        };
                        if let Some(start) = queue_start {
                            let queue_ns = start.elapsed().as_nanos() as u64;
                            timings::record_sub_queue_wait_ns(queue_ns);
                            metrics::histogram!("felix_broker_sub_recv_wait_ns")
                                .record(queue_ns as f64);
                        }

                        let mut batch = Vec::with_capacity(max_events.max(1));
                        let mut batch_bytes = 0usize;
                        let mut closed = false;
                        let mut flush_reason = "idle";

                        batch_bytes += first.len();
                        batch.push(first);
                        let deadline = tokio::time::Instant::now() + flush_delay;
                        let deadline_sleep = tokio::time::sleep_until(deadline);
                        tokio::pin!(deadline_sleep);

                        if batch.len() >= max_events {
                            flush_reason = "count";
                        } else if batch_bytes >= max_bytes {
                            flush_reason = "bytes";
                        }

                        while flush_reason == "idle" && !closed {
                            while batch.len() < max_events && batch_bytes < max_bytes {
                                match receiver.try_recv() {
                                    Ok(payload) => {
                                        if batch_bytes.saturating_add(payload.len()) > max_bytes {
                                            pending = Some(payload);
                                            flush_reason = "bytes";
                                            break;
                                        }
                                        batch_bytes += payload.len();
                                        batch.push(payload);
                                        if batch.len() >= max_events {
                                            flush_reason = "count";
                                            break;
                                        }
                                        if batch_bytes >= max_bytes {
                                            flush_reason = "bytes";
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::TryRecvError::Lagged(_)) => {
                                        metrics::counter!("felix_subscribe_dropped_total")
                                            .increment(1);
                                    }
                                    Err(broadcast::error::TryRecvError::Empty) => break,
                                    Err(broadcast::error::TryRecvError::Closed) => {
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
                                recv = receiver.recv() => {
                                    match recv {
                                        Ok(payload) => {
                                            if batch_bytes.saturating_add(payload.len()) > max_bytes {
                                                pending = Some(payload);
                                                flush_reason = "bytes";
                                                break;
                                            }
                                            batch_bytes += payload.len();
                                            batch.push(payload);
                                            if batch.len() >= max_events {
                                                flush_reason = "count";
                                                break;
                                            }
                                            if batch_bytes >= max_bytes {
                                                flush_reason = "bytes";
                                                break;
                                            }
                                        }
                                        Err(broadcast::error::RecvError::Lagged(_)) => {
                                            metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                        }
                                        Err(broadcast::error::RecvError::Closed) => {
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

                        let write_start = sample.then(Instant::now);
                        metrics::counter!(
                            "felix_broker_event_batch_flush_reason_total",
                            "reason" => flush_reason
                        )
                        .increment(1);
                        metrics::histogram!("felix_broker_event_batch_size_bytes")
                            .record(batch_bytes as f64);
                        if batch.len() == 1 {
                            let payload = batch.pop().expect("payload");
                            let payload_len = payload.len();
                            let message = Message::Event {
                                tenant_id: tenant_id.clone(),
                                namespace: namespace.clone(),
                                stream: stream.clone(),
                                payload: payload.to_vec(),
                            };
                            metrics::counter!("felix_subscribe_bytes_total")
                                .increment(payload_len as u64);
                            if let Err(err) = write_message(&mut event_send, message).await {
                                tracing::info!(error = %err, "subscription event stream closed");
                                return Ok(());
                            }
                        } else {
                            let frame_bytes = felix_wire::binary::encode_event_batch_bytes(
                                subscription_id,
                                &batch,
                            )
                            .context("encode binary event batch")?;
                            metrics::counter!("felix_subscribe_bytes_total")
                                .increment(batch_bytes as u64);
                            if let Err(err) = write_frame_bytes(&mut event_send, frame_bytes).await
                            {
                                tracing::info!(error = %err, "subscription event stream closed");
                                return Ok(());
                            }
                        }
                        if let Some(start) = write_start {
                            let write_ns = start.elapsed().as_nanos() as u64;
                            timings::record_sub_write_ns(write_ns);
                            timings::record_quic_write_ns(write_ns);
                            metrics::histogram!("felix_broker_sub_write_ns")
                                .record(write_ns as f64);
                            metrics::histogram!("felix_broker_quic_write_ns")
                                .record(write_ns as f64);
                        }
                        if closed {
                            break;
                        }
                    }
                    let _ = event_send.finish();
                    return Ok(());
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
                            return Ok(());
                        }
                        continue;
                    }
                    let ttl = ttl_ms.map(Duration::from_millis);
                    let lookup_start = sample.then(Instant::now);
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
                    send_outgoing_best_effort(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::CacheMessage(Message::Ok),
                    )
                    .await
                    .map_err(record_ack_enqueue_failure)?;
                    return Ok(());
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
                            return Ok(());
                        }
                        continue;
                    }
                    let lookup_start = sample.then(Instant::now);
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
                        return Ok(());
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
                    return Ok(());
                }
                Message::Error { .. } => {
                    return Ok(());
                }
            }
        }
        Ok(())
    })
    .await;
    let _ = cancel_tx.send(true);
    reset_depth(
        &out_ack_depth,
        &GLOBAL_ACK_DEPTH,
        "felix_broker_out_ack_depth",
    );
    result
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
    // Read loop: accept publish payloads only; control/ack traffic never appears here.
    loop {
        let frame = match read_frame_limited(&mut recv, config.max_frame_bytes).await? {
            Some(frame) => frame,
            None => break,
        };
        // Uni streams are publish-only; responses are not sent.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let batch = felix_wire::binary::decode_publish_batch(&frame)
                .context("decode binary publish batch")?;
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
                metrics::counter!("felix_publish_requests_total", "result" => "error").increment(1);
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
                    metrics::counter!("felix_publish_requests_total", "result" => "success")
                        .increment(1);
                }
                Ok(false) => {
                    metrics::counter!("felix_publish_requests_total", "result" => "dropped")
                        .increment(1);
                }
                Err(_) => {
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
                    break;
                }
            }
            continue;
        }
        // Publish-only protocol: decode one frame into a typed Message, then route below.
        // Only Publish/PublishBatch are honored on uni streams; no acks are sent.
        let message = Message::decode(frame).context("decode message")?;
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
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
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
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

                if emit_send_telemetry(r) {
                    break;
                }
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ..
            } => {
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
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
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

                if emit_send_telemetry(r) {
                    break;
                }
            }
            _ => {
                metrics::counter!(
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
) -> Result<Option<Message>> {
    let frame = match read_frame_limited(recv, max_frame_bytes).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
}

// Helper to encode + write a single message.
pub async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame(send, frame).await
}

// Low-level frame reader with a max payload cap.
async fn read_frame_limited(
    recv: &mut RecvStream,
    max_payload_bytes: usize,
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
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

// Low-level frame writer for QUIC streams.
async fn write_frame(send: &mut SendStream, frame: Frame) -> Result<()> {
    send.write_all(&frame.encode()).await.context("write frame")
}

// Write raw pre-encoded bytes (used for cached responses).
async fn write_frame_bytes(send: &mut SendStream, bytes: Bytes) -> Result<()> {
    send.write_all(&bytes).await.context("write frame")
}

fn emit_send_telemetry(result: Result<bool>) -> bool {
    match result {
        Ok(true) => {
            metrics::counter!("felix_publish_requests_total", "result" => "success").increment(1);
            false
        }
        Ok(false) => {
            metrics::counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
            false
        }
        Err(err) => {
            metrics::counter!("felix_publish_requests_total", "result" => "error").increment(1);
            tracing::warn!(error = %err, "publish enqueue failed");
            true
        }
    }
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
        let response = read_message_limited(&mut recv, max_frame_bytes).await?;
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
        let response = read_message_limited(&mut recv, max_frame_bytes).await?;
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
        let response = read_message_limited(&mut recv, max_frame_bytes).await?;
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
        let response = read_message_limited(&mut recv, max_frame_bytes).await?;
        assert_eq!(response, Some(Message::PublishOk { request_id: 2 }));

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
}
