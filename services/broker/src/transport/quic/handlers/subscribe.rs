// Subscribe path logic and event writer for subscription streams.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
use felix_wire::{Frame, FrameHeader, Message};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "telemetry")]
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};

use crate::timings;

use super::publish::{Outgoing, send_outgoing_critical};
use crate::transport::quic::codec::{write_frame, write_frame_bytes, write_message};
use crate::transport::quic::telemetry::{t_instant_now, t_now_if, t_should_sample};
use crate::transport::quic::{
    DEFAULT_EVENT_QUEUE_DEPTH, EVENT_SINGLE_BINARY_ENV, EVENT_SINGLE_BINARY_MIN_BYTES_DEFAULT,
    EVENT_SINGLE_BINARY_MIN_BYTES_ENV, SUBSCRIPTION_ID,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_subscribe_message(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: crate::config::BrokerConfig,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
    tenant_id: String,
    namespace: String,
    stream: String,
    subscription_id: Option<u64>,
) -> Result<bool> {
    // Subscribe is control-plane: ack/metadata stay on this stream, events go to a new uni stream.
    let span = tracing::trace_span!(
        "subscribe",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    let _enter = span.enter();
    let subscription_id = subscription_id
        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    let mut receiver = match broker.subscribe(&tenant_id, &namespace, &stream).await {
        Ok(receiver) => receiver,
        Err(err) => {
            t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
            super::publish::handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: err.to_string(),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(true);
        }
    };
    let mut event_send = match connection.open_uni().await {
        Ok(send) => send,
        Err(err) => {
            t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
            super::publish::handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: err.to_string(),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(true);
        }
    };
    t_counter!("felix_subscribe_requests_total", "result" => "ok").increment(1);
    super::publish::handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::Subscribed { subscription_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
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
                        t_counter!("felix_subscribe_dropped_total").increment(1);
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
        if let Err(err) = run_event_writer(event_send, event_rx, writer_config).await {
            tracing::info!(error = %err, "subscription event stream closed");
        }
    });
    Ok(true)
}

pub(crate) struct EventWriterConfig {
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

pub(crate) struct EventEnvelope {
    payload: Bytes,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    enqueue_at: crate::transport::quic::telemetry::TelemetryInstant,
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

#[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
pub(crate) async fn run_event_writer(
    mut event_send: quinn::SendStream,
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
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters
                    .sub_frames_out_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_batches_out_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_items_out_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            let counters = crate::transport::quic::telemetry::frame_counters();
            counters
                .sub_frames_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .sub_batches_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .sub_items_out_ok
                .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
