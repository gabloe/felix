// QUIC transport adapter for the broker.
// Decodes felix-wire frames, enforces stream scope, and fans out events to subscribers.
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
use tokio::sync::{broadcast, mpsc, oneshot};

const PUBLISH_QUEUE_DEPTH: usize = 1024;
const ACK_QUEUE_DEPTH: usize = 256;
const STREAM_CACHE_TTL: Duration = Duration::from_secs(2);
static SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

struct PublishJob {
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Bytes>,
    response: Option<oneshot::Sender<Result<()>>>,
}

enum Outgoing {
    Message(Message),
    CacheMessage(Message),
    Finish,
}

enum EnqueuePolicy {
    Drop,
    Fail,
    Wait,
}

/// This will publish the job to the channel and handle the result. There is no real
/// logic in this file.
async fn enqueue_publish(
    publish_tx: &mpsc::Sender<PublishJob>,
    queue_depth: &Arc<std::sync::atomic::AtomicUsize>,
    job: PublishJob,
    policy: EnqueuePolicy,
) -> Result<bool> {
    match publish_tx.try_send(job) {
        Ok(()) => {
            let depth = queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::gauge!("felix_broker_ingress_queue_depth").set(depth as f64);
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
                    metrics::counter!("felix_broker_ingress_waited_total").increment(1);
                    publish_tx
                        .send(job)
                        .await
                        .map_err(|_| anyhow!("publish queue closed"))?;
                    let depth = queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
                    metrics::gauge!("felix_broker_ingress_queue_depth").set(depth as f64);
                    Ok(true)
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("publish queue closed")),
    }
}

fn decrement_depth(depth: &Arc<std::sync::atomic::AtomicUsize>, gauge: &'static str) {
    let mut current = depth.load(Ordering::Relaxed);
    while current > 0 {
        match depth.compare_exchange(current, current - 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                metrics::gauge!(gauge).set((current - 1) as f64);
                return;
            }
            Err(next) => current = next,
        }
    }
}

async fn send_outgoing(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<std::sync::atomic::AtomicUsize>,
    gauge: &'static str,
    message: Outgoing,
) -> bool {
    let next = depth.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::gauge!(gauge).set(next as f64);
    if tx.send(message).await.is_err() {
        decrement_depth(depth, gauge);
        return false;
    }
    true
}

fn stream_cache_key(tenant_id: &str, namespace: &str, stream: &str) -> String {
    format!("{tenant_id}\0{namespace}\0{stream}")
}

async fn stream_exists_cached(
    broker: &Broker,
    cache: &mut HashMap<String, (bool, Instant)>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> bool {
    let key = stream_cache_key(tenant_id, namespace, stream);
    if let Some((exists, expires)) = cache.get(&key)
        && *expires > Instant::now()
    {
        return *exists;
    }
    let exists = broker.stream_exists(tenant_id, namespace, stream).await;
    cache.insert(key, (exists, Instant::now() + STREAM_CACHE_TTL));
    exists
}

pub async fn serve(
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    config: BrokerConfig,
) -> Result<()> {
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
    loop {
        // Accept both bidirectional control streams and uni-directional publish streams.
        tokio::select! {
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let connection = connection.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(broker, connection, config, send, recv).await {
                        tracing::warn!(error = %err, "quic stream handler failed");
                    }
                });
            }
            result = connection.accept_uni() => {
                let recv = match result {
                    Ok(recv) => recv,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_uni_stream(broker, config, recv).await {
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
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(PUBLISH_QUEUE_DEPTH);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ack_on_commit = config.ack_on_commit;
    let broker_for_worker = Arc::clone(&broker);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queue_depth_worker = Arc::clone(&queue_depth);
    let mut stream_cache = HashMap::new();

    let _reader_job = tokio::spawn(async move {
        while let Some(job) = publish_rx.recv().await {
            queue_depth_worker.fetch_sub(1, Ordering::Relaxed);
            metrics::gauge!("felix_broker_ingress_queue_depth")
                .set(queue_depth_worker.load(Ordering::Relaxed) as f64);
            let result = broker_for_worker
                .publish_batch(&job.tenant_id, &job.namespace, &job.stream, &job.payloads)
                .await
                .map(|_| ())
                .map_err(Into::into);
            if let Some(response) = job.response {
                let _ = response.send(result);
            }
        }
    });

    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let _writer_job = tokio::spawn(async move {
        let mut ack_done = false;
        let mut last_cache = false;
        while !ack_done {
            match out_ack_rx.recv().await {
                Some(outgoing) => {
                    decrement_depth(&out_ack_depth_worker, "felix_broker_out_ack_depth");
                    match outgoing {
                        Outgoing::Message(message) => {
                            last_cache = false;
                            let sample = timings::should_sample();
                            let write_start = sample.then(Instant::now);
                            if let Err(err) = write_message(&mut send, message).await {
                                tracing::info!(error = %err, "quic response stream closed");
                                break;
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_quic_write_ns(write_ns);
                                metrics::histogram!("felix_broker_quic_write_ns")
                                    .record(write_ns as f64);
                            }
                        }
                        Outgoing::CacheMessage(message) => {
                            last_cache = true;
                            let sample = timings::should_sample();
                            let encode_start = sample.then(Instant::now);
                            let frame = match message.encode().context("encode message") {
                                Ok(frame) => frame,
                                Err(err) => {
                                    tracing::info!(error = %err, "encode cache response failed");
                                    break;
                                }
                            };
                            if let Some(start) = encode_start {
                                let encode_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_encode_ns(encode_ns);
                            }
                            let write_start = sample.then(Instant::now);
                            if let Err(err) = write_frame(&mut send, frame).await {
                                tracing::info!(error = %err, "quic response stream closed");
                                break;
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_write_ns(write_ns);
                            }
                        }
                        Outgoing::Finish => {
                            ack_done = true;
                        }
                    }
                }
                None => {
                    ack_done = true;
                }
            }
        }
        if last_cache {
            let sample = timings::should_sample();
            let finish_start = sample.then(Instant::now);
            let _ = send.finish();
            if let Some(start) = finish_start {
                let finish_ns = start.elapsed().as_nanos() as u64;
                timings::record_cache_finish_ns(finish_ns);
            }
        } else {
            let _ = send.finish();
        }
        let _ = send.stopped().await;
    });
    loop {
        let sample = timings::should_sample();
        let read_start = sample.then(Instant::now);
        let frame = match read_frame(&mut recv).await? {
            Some(frame) => frame,
            None => break,
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
                &batch.tenant_id,
                &batch.namespace,
                &batch.stream,
            )
            .await
            {
                metrics::counter!("felix_publish_requests_total", "result" => "error").increment(1);
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
                &publish_tx,
                &queue_depth,
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
                ack,
            } => {
                // Per-message publish path with optional ack.
                let start = Instant::now();
                let (response_tx, response_rx) = oneshot::channel();
                let payload_len = payload.len();
                let enqueue_start = sample.then(Instant::now);
                let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
                if !stream_exists_cached(
                    &broker,
                    &mut stream_cache,
                    &tenant_id,
                    &namespace,
                    &stream,
                )
                .await
                {
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
                    if ack_mode != felix_wire::AckMode::None {
                        let _ = out_ack_tx
                            .send(Outgoing::Message(Message::Error {
                                message: format!(
                                    "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                                ),
                            }))
                            .await;
                    }
                    continue;
                }
                let enqueue_result = enqueue_publish(
                    &publish_tx,
                    &queue_depth,
                    PublishJob {
                        tenant_id: tenant_id.clone(),
                        namespace: namespace.clone(),
                        stream: stream.clone(),
                        payloads: vec![Bytes::from(payload)],
                        response: Some(response_tx),
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
                            let _ = out_ack_tx
                                .send(Outgoing::Message(Message::Error {
                                    message: "ingress overloaded".to_string(),
                                }))
                                .await;
                        }
                        continue;
                    }
                    Err(err) => {
                        metrics::counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        if ack_mode != felix_wire::AckMode::None {
                            let _ = out_ack_tx
                                .send(Outgoing::Message(Message::Error {
                                    message: err.to_string(),
                                }))
                                .await;
                        }
                        continue;
                    }
                }
                if ack_mode == felix_wire::AckMode::None {
                    continue;
                }
                if !ack_on_commit {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::Message(Message::Ok),
                    )
                    .await;
                    metrics::counter!("felix_publish_requests_total", "result" => "ok")
                        .increment(1);
                    metrics::counter!("felix_publish_bytes_total").increment(payload_len as u64);
                    metrics::histogram!("felix_publish_latency_ms")
                        .record(start.elapsed().as_secs_f64() * 1000.0);
                    continue;
                }
                let out_tx = out_ack_tx.clone();
                let out_ack_depth = Arc::clone(&out_ack_depth);
                tokio::spawn(async move {
                    match response_rx.await {
                        Ok(Ok(())) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "ok")
                                .increment(1);
                            metrics::counter!("felix_publish_bytes_total")
                                .increment(payload_len as u64);
                            metrics::histogram!("felix_publish_latency_ms")
                                .record(start.elapsed().as_secs_f64() * 1000.0);
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Ok),
                            )
                            .await;
                        }
                        Ok(Err(err)) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Error {
                                    message: err.to_string(),
                                }),
                            )
                            .await;
                        }
                        Err(_) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Error {
                                    message: "publish worker dropped response".to_string(),
                                }),
                            )
                            .await;
                        }
                    }
                });
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ack,
            } => {
                // Batch publish trades latency for throughput, returns a single ack.
                let span = tracing::trace_span!(
                    "publish_batch",
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream,
                    count = payloads.len()
                );
                let _enter = span.enter();
                let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
                if !stream_exists_cached(
                    &broker,
                    &mut stream_cache,
                    &tenant_id,
                    &namespace,
                    &stream,
                )
                .await
                {
                    metrics::counter!("felix_publish_requests_total", "result" => "error")
                        .increment(1);
                    if ack_mode != felix_wire::AckMode::None {
                        let _ = out_ack_tx
                            .send(Outgoing::Message(Message::Error {
                                message: format!(
                                    "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                                ),
                            }))
                            .await;
                    }
                    continue;
                }
                let payload_bytes = payloads
                    .iter()
                    .map(|payload| payload.len())
                    .collect::<Vec<_>>();
                let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
                let (response_tx, response_rx) = oneshot::channel();
                let fanout_start = sample.then(Instant::now);
                let enqueue_result = enqueue_publish(
                    &publish_tx,
                    &queue_depth,
                    PublishJob {
                        tenant_id: tenant_id.clone(),
                        namespace: namespace.clone(),
                        stream: stream.clone(),
                        payloads,
                        response: Some(response_tx),
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
                    metrics::histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
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
                            let _ = out_ack_tx
                                .send(Outgoing::Message(Message::Error {
                                    message: "ingress overloaded".to_string(),
                                }))
                                .await;
                        }
                        continue;
                    }
                    Err(err) => {
                        metrics::counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        if ack_mode != felix_wire::AckMode::None {
                            let _ = out_ack_tx
                                .send(Outgoing::Message(Message::Error {
                                    message: err.to_string(),
                                }))
                                .await;
                        }
                        continue;
                    }
                }
                if ack_mode == felix_wire::AckMode::None {
                    continue;
                }
                if !ack_on_commit {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::Message(Message::Ok),
                    )
                    .await;
                    for bytes in &payload_bytes {
                        metrics::counter!("felix_publish_requests_total", "result" => "ok")
                            .increment(1);
                        metrics::counter!("felix_publish_bytes_total").increment(*bytes as u64);
                    }
                    continue;
                }
                let out_tx = out_ack_tx.clone();
                let out_ack_depth = Arc::clone(&out_ack_depth);
                tokio::spawn(async move {
                    match response_rx.await {
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
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Ok),
                            )
                            .await;
                        }
                        Ok(Err(err)) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Error {
                                    message: err.to_string(),
                                }),
                            )
                            .await;
                        }
                        Err(_) => {
                            metrics::counter!("felix_publish_requests_total", "result" => "error")
                                .increment(1);
                            let _ = send_outgoing(
                                &out_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                Outgoing::Message(Message::Error {
                                    message: "publish batch worker dropped response".to_string(),
                                }),
                            )
                            .await;
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
                // Subscribe keeps the control stream for acks and uses a uni stream for events.
                let span = tracing::trace_span!(
                    "subscribe",
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream
                );
                let _enter = span.enter();
                let subscription_id = subscription_id
                    .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed));
                let mut receiver = match broker.subscribe(&tenant_id, &namespace, &stream).await {
                    Ok(receiver) => receiver,
                    Err(err) => {
                        metrics::counter!("felix_subscribe_requests_total", "result" => "error")
                            .increment(1);
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Message(Message::Error {
                                message: err.to_string(),
                            }),
                        )
                        .await;
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Finish,
                        )
                        .await;
                        return Ok(());
                    }
                };
                let mut event_send = match connection.open_uni().await {
                    Ok(send) => send,
                    Err(err) => {
                        metrics::counter!("felix_subscribe_requests_total", "result" => "error")
                            .increment(1);
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Message(Message::Error {
                                message: err.to_string(),
                            }),
                        )
                        .await;
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Finish,
                        )
                        .await;
                        return Ok(());
                    }
                };
                metrics::counter!("felix_subscribe_requests_total", "result" => "ok").increment(1);
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::Message(Message::Subscribed { subscription_id }),
                )
                .await;
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
                                    metrics::histogram!("felix_broker_sub_queue_wait_ns")
                                        .record(queue_ns as f64);
                                }
                                let write_start = sample.then(Instant::now);
                                if let Err(err) = write_message(&mut event_send, message).await {
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
                        metrics::histogram!("felix_broker_sub_queue_wait_ns")
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
                                    metrics::counter!("felix_subscribe_dropped_total").increment(1);
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
                        let frame_bytes =
                            felix_wire::binary::encode_event_batch_bytes(subscription_id, &batch)
                                .context("encode binary event batch")?;
                        metrics::counter!("felix_subscribe_bytes_total")
                            .increment(batch_bytes as u64);
                        if let Err(err) = write_frame_bytes(&mut event_send, frame_bytes).await {
                            tracing::info!(error = %err, "subscription event stream closed");
                            return Ok(());
                        }
                    }
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_sub_write_ns(write_ns);
                        timings::record_quic_write_ns(write_ns);
                        metrics::histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
                        metrics::histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
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
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::CacheMessage(Message::Error {
                            message: format!(
                                "cache scope not found: {tenant_id}/{namespace}/{cache}"
                            ),
                        }),
                    )
                    .await;
                    if request_id.is_none() {
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Finish,
                        )
                        .await;
                        return Ok(());
                    }
                    continue;
                }
                let ttl = ttl_ms.map(Duration::from_millis);
                let lookup_start = sample.then(Instant::now);
                broker
                    .cache()
                    .put(tenant_id, namespace, cache, key, value, ttl)
                    .await;
                if let Some(start) = lookup_start {
                    let lookup_ns = start.elapsed().as_nanos() as u64;
                    timings::record_cache_insert_ns(lookup_ns);
                }
                if let Some(request_id) = request_id {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::CacheMessage(Message::CacheOk { request_id }),
                    )
                    .await;
                    continue;
                }
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::CacheMessage(Message::Ok),
                )
                .await;
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::Finish,
                )
                .await;
                return Ok(());
            }
            Message::CacheGet {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::CacheMessage(Message::Error {
                            message: format!(
                                "cache scope not found: {tenant_id}/{namespace}/{cache}"
                            ),
                        }),
                    )
                    .await;
                    if request_id.is_none() {
                        let _ = send_outgoing(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            Outgoing::Finish,
                        )
                        .await;
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
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::CacheMessage(Message::CacheValue {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        value,
                        request_id,
                    }),
                )
                .await;
                if request_id.is_none() {
                    let _ = send_outgoing(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        Outgoing::Finish,
                    )
                    .await;
                    return Ok(());
                }
            }
            Message::CacheValue { .. }
            | Message::CacheOk { .. }
            | Message::Event { .. }
            | Message::EventBatch { .. }
            | Message::Subscribed { .. }
            | Message::EventStreamHello { .. }
            | Message::Ok => {
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::Message(Message::Error {
                        message: "unexpected message type".to_string(),
                    }),
                )
                .await;
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::Finish,
                )
                .await;
                return Ok(());
            }
            Message::Error { .. } => {
                let _ = send_outgoing(
                    &out_ack_tx,
                    &out_ack_depth,
                    "felix_broker_out_ack_depth",
                    Outgoing::Finish,
                )
                .await;
                return Ok(());
            }
        }
    }
    let _ = send_outgoing(
        &out_ack_tx,
        &out_ack_depth,
        "felix_broker_out_ack_depth",
        Outgoing::Finish,
    )
    .await;
    Ok(())
}

async fn handle_uni_stream(
    broker: Arc<Broker>,
    _config: BrokerConfig,
    mut recv: RecvStream,
) -> Result<()> {
    let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(PUBLISH_QUEUE_DEPTH);
    let broker_for_worker = Arc::clone(&broker);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queue_depth_worker = Arc::clone(&queue_depth);
    let mut stream_cache = HashMap::new();
    tokio::spawn(async move {
        while let Some(job) = publish_rx.recv().await {
            queue_depth_worker.fetch_sub(1, Ordering::Relaxed);
            metrics::gauge!("felix_broker_ingress_queue_depth")
                .set(queue_depth_worker.load(Ordering::Relaxed) as f64);
            let _ = broker_for_worker
                .publish_batch(&job.tenant_id, &job.namespace, &job.stream, &job.payloads)
                .await;
        }
    });
    loop {
        let frame = match read_frame(&mut recv).await? {
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
                &publish_tx,
                &queue_depth,
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
                    &publish_tx,
                    &queue_depth,
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
                    &publish_tx,
                    &queue_depth,
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
                tracing::debug!("ignored non-publish message on uni stream");
            }
        }
    }
    Ok(())
}

pub async fn read_message(recv: &mut RecvStream) -> Result<Option<Message>> {
    let frame = match read_frame(recv).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
}

pub async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame(send, frame).await
}

async fn read_frame(recv: &mut RecvStream) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

async fn write_frame(send: &mut SendStream, frame: Frame) -> Result<()> {
    send.write_all(&frame.encode()).await.context("write frame")
}

async fn write_frame_bytes(send: &mut SendStream, bytes: Bytes) -> Result<()> {
    send.write_all(&bytes).await.context("write frame")
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
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
        let response = read_message(&mut recv).await?;
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
        let response = read_message(&mut recv).await?;
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
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
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
                ack: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert!(matches!(response, Some(Message::Error { .. })));

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
                ack: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert_eq!(response, Some(Message::Ok));

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
