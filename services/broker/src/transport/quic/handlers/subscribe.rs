//! QUIC subscribe handling and event-stream writer.
//!
//! ## High-level flow
//! Subscriptions are requested on the **bi-directional control stream** (the same stream used
//! for publish/control-plane operations). Once accepted, the broker opens a **new uni-directional
//! stream** (broker → client) dedicated to delivering events for that subscription.
//!
//! This file is responsible for:
//! - Handling the `Message::Subscribe` request on the control stream.
//! - Sending `Message::Subscribed` back on the control stream as the acknowledgement.
//! - Opening a uni stream and sending a `Message::EventStreamHello` so the client can bind
//!   `subscription_id -> stream`.
//! - Bridging the broker’s broadcast receiver into an internal `mpsc` queue to decouple:
//!   - broker event production (broadcast)
//!   - QUIC write pacing / batching / framing (writer task)
//! - Running the event writer in either single-event mode or batch mode.
//!
//! ## Buffering and drops
//! The broker’s pub/sub fanout uses a broadcast channel. A broadcast receiver can lag and
//! will report `Lagged`, and it can also close. To avoid blocking the broadcast path on a
//! slow network writer, we forward events into a bounded `mpsc` queue via `try_send`:
//! - If the queue is full, we **drop** the event and increment `felix_subscribe_dropped_total`.
//! - This keeps backpressure localized to the subscriber rather than stalling the broker.
//!
//! ## Encoding / batching
//! Events are framed as binary `EventBatch` using
//! `felix_wire::binary::encode_event_batch_bytes`.
//!
//! In batch mode, we coalesce by whichever triggers first:
//! - max events (`max_events`)
//! - max bytes (`max_bytes`)
//! - deadline (`flush_delay`)
//!
//! ## Telemetry
//! Telemetry is compiled out unless `--features telemetry` is enabled. When enabled, we record:
//! - queue wait latency
//! - write latency
//! - end-to-end delivery latency (enqueue → write completion)
//! - frame/batch/item counters

// Subscribe path logic and event writer for subscription streams.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
use felix_wire::Message;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "telemetry")]
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};

use crate::timings;

use super::publish::{Outgoing, send_outgoing_critical};
use crate::transport::quic::SUBSCRIPTION_ID;
use crate::transport::quic::codec::{write_frame_bytes, write_message};
#[cfg(feature = "telemetry")]
use crate::transport::quic::telemetry::t_instant_now;
use crate::transport::quic::telemetry::{t_now_if, t_should_sample};

/// Handle a subscribe request received on the bi-directional control stream.
///
/// This function is invoked from the control stream read loop when a `Message::Subscribe`
/// is decoded.
///
/// Responsibilities:
/// - Allocate/derive a `subscription_id`.
/// - Ask the broker core for a subscription receiver.
/// - Acknowledge the subscribe on the control stream (`Message::Subscribed` or `Message::Error`).
/// - Open a uni stream for event delivery, send `EventStreamHello`, and spawn the event writer.
///
/// Return value semantics:
/// - `Ok(true)` means: “handled; keep the control stream alive / continue.”
///   (This is consistent with the caller’s pattern where subscribe is a control-plane operation.)
/// - `Err(_)` bubbles up unexpected failures (e.g., encoding errors).
///
/// Error handling strategy:
/// - If we can’t subscribe or can’t open the event stream, we **reply with `Message::Error`**
///   on the control stream (via the outbound ack queue) and return `Ok(true)` so the control
///   loop can continue / cleanly terminate based on higher-level policy.
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
    // Subscribe is a control-plane request: acknowledgements/metadata stay on this bi stream.
    // Actual event delivery happens on a fresh uni stream (broker -> client).
    let span = tracing::trace_span!(
        "subscribe",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    let _enter = span.enter();

    // Client may provide an explicit subscription_id (useful for idempotency/testing).
    // If absent, we allocate one from a global atomic counter.
    let subscription_id = subscription_id
        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    // Ask broker core for a broadcast receiver.
    // On failure, respond on the control stream (through the ack queue) and keep the stream alive.
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

    // Open a uni stream for event delivery. If this fails, respond with error on control stream.
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

    // Control-plane acknowledgement: subscriber is registered.
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

    // First write a hello on the uni stream so the client can bind:
    //   subscription_id -> this uni stream
    // before any events arrive. If this fails, we treat it as the subscriber being gone.
    if let Err(err) = write_message(
        &mut event_send,
        Message::EventStreamHello { subscription_id },
    )
    .await
    {
        tracing::info!(error = %err, "subscription event stream closed");
        return Ok(true);
    }

    // Local queue decouples broadcast receive from network writes.
    // `try_send` on this queue defines our drop policy for slow subscribers.
    let queue_depth = config.event_queue_depth.max(1);
    let (event_tx, event_rx) = mpsc::channel(queue_depth);

    // Forwarder task:
    // - reads from broker broadcast receiver
    // - attempts to enqueue into bounded mpsc without awaiting
    // - increments drop counter on overflow or lag
    // This isolates the broker from slow subscriber I/O.
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(payload) => match event_tx.try_send(EventEnvelope {
                    payload,
                    #[cfg(feature = "telemetry")]
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

    // Batching configuration.
    // Note: `batch_size` is the “fanout batch size” (publisher -> broker internal),
    // but for subscriber delivery we compute and enforce independent limits.
    let batch_size = config.fanout_batch_size;
    let max_events = config.event_batch_max_events.min(batch_size.max(1));
    let max_bytes = config.event_batch_max_bytes.max(1);
    let flush_delay = Duration::from_micros(config.event_batch_max_delay_us);

    // Event writer task:
    // Owns the uni SendStream and is the *only* writer to it.
    // It drains `event_rx`, frames/batches, and writes to QUIC.
    tokio::spawn(async move {
        let writer_config = EventWriterConfig {
            subscription_id,
            batch_size,
            max_events,
            max_bytes,
            flush_delay,
        };
        if let Err(err) = run_event_writer(event_send, event_rx, writer_config).await {
            tracing::info!(error = %err, "subscription event stream closed");
        }
    });
    Ok(true)
}

/// Configuration for the subscription event writer.
///
/// Fields are chosen to make the event writer a pure “I/O + framing” component:
/// it doesn’t need the broker, only identifiers and batching policy.
///
/// Batching behavior:
/// - If `batch_size <= 1`, we operate in single-event mode (no coalescing).
/// - Otherwise, we coalesce into a binary EventBatch and flush based on:
///   - `max_events`
///   - `max_bytes`
///   - `flush_delay`
///
/// Note: `max_events` is clamped elsewhere to be at least 1.
pub(crate) struct EventWriterConfig {
    /// Stable identifier for this subscription; used by the client to route events.
    subscription_id: u64,

    /// Controls whether we’re in “single” vs “batch” mode.
    batch_size: usize,

    /// Max number of events per flush in batch mode.
    max_events: usize,

    /// Max total payload bytes per flush in batch mode.
    max_bytes: usize,

    /// Deadline for flushing a partially-filled batch.
    flush_delay: Duration,
}

/// Wrapper around event payload that can carry extra metadata.
///
/// We keep telemetry timestamps here so the forwarder can stamp the enqueue time once,
/// and the writer can compute end-to-end delivery latency without sharing state.
pub(crate) struct EventEnvelope {
    payload: Bytes,
    #[cfg(feature = "telemetry")]
    enqueue_at: crate::transport::quic::telemetry::TelemetryInstant,
}

/// Drain subscriber events from an `mpsc` queue and write them onto a uni QUIC stream.
///
/// Modes:
/// - **Single mode** (`config.batch_size <= 1`): write each event immediately as a binary batch.
/// - **Batch mode**: coalesce into a binary EventBatch with flush triggers:
///   - count (`max_events`)
///   - bytes (`max_bytes`)
///   - deadline (`flush_delay`)
///
/// Internal detail: `pending`
/// In batch mode, we may read one event that would exceed `max_bytes` if appended. We keep that
/// event in `pending` so it becomes the first element of the next batch (instead of dropping it).
#[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
pub(crate) async fn run_event_writer(
    mut event_send: quinn::SendStream,
    mut rx: mpsc::Receiver<EventEnvelope>,
    config: EventWriterConfig,
) -> Result<()> {
    // `pending` holds the first event of the next batch when the current batch is byte-limited.
    let mut pending: Option<EventEnvelope> = None;

    // ---- Single-event mode ---------------------------------------------------
    if config.batch_size <= 1 {
        loop {
            // Sampling is used to reduce overhead when telemetry is enabled.
            let sample = t_should_sample();
            let queue_start = t_now_if(sample);

            // In single mode we simply await the next envelope.
            let envelope = match rx.recv().await {
                Some(payload) => payload,
                None => break,
            };
            #[cfg(feature = "telemetry")]
            let enqueue_at = envelope.enqueue_at;
            let payload = envelope.payload;
            t_counter!("felix_subscribe_bytes_total").increment(payload.len() as u64);

            // Measure time spent waiting in the subscriber queue (local buffering).
            if let Some(start) = queue_start {
                let queue_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_queue_wait_ns(queue_ns);
                t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
            }
            let write_start = t_now_if(sample);
            #[cfg(not(feature = "telemetry"))]
            let _ = write_start;

            let batch = vec![payload];
            let frame_bytes =
                felix_wire::binary::encode_event_batch_bytes(config.subscription_id, &batch)
                    .context("encode binary event batch")?;
            t_histogram!("broker_sub_frame_bytes").record(frame_bytes.len() as f64);
            write_frame_bytes(&mut event_send, frame_bytes).await?;

            // Telemetry: end-to-end delivery for this subscriber (enqueue -> write completion).
            #[cfg(feature = "telemetry")]
            let write_end = Instant::now();
            #[cfg(feature = "telemetry")]
            {
                let delivery_ns = write_end.duration_since(enqueue_at).as_nanos() as u64;
                t_histogram!("broker_sub_delivery_latency_ns").record(delivery_ns as f64);
                timings::record_sub_delivery_ns(delivery_ns);
            }

            // Counters:
            // - `sub_frames_out_ok`: frames written successfully
            // - `sub_batches_out_ok`: batches written successfully (we treat single as a batch too)
            // - `sub_items_out_ok`: number of events delivered
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

            // Telemetry: write await time (time spent awaiting QUIC send).
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

        // Graceful shutdown: finish the uni stream so the peer sees end-of-stream.
        let _ = event_send.finish();
        return Ok(());
    }

    // ---- Batch mode ----------------------------------------------------------
    // Coalesce events by count, bytes, or deadline.
    loop {
        let sample = t_should_sample();
        let queue_start = t_now_if(sample);

        // Prefer `pending` (carried over when we hit `max_bytes` on the previous batch).
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

        // Why we track `flush_reason`:
        // - helps diagnose which limiter is dominant under different workloads.
        #[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
        #[allow(unused_assignments)]
        let mut flush_reason = "idle";

        // Seed the batch with the first element.
        batch_bytes += first.payload.len();
        batch.push(first);

        // Deadline for flushing a partially full batch.
        let deadline = tokio::time::Instant::now() + config.flush_delay;
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);

        // Immediate flush if already at a threshold.
        if batch.len() >= config.max_events {
            flush_reason = "count";
        } else if batch_bytes >= config.max_bytes {
            flush_reason = "bytes";
        }

        // Keep collecting until we hit a flush condition or the channel closes.
        while flush_reason == "idle" && !closed {
            // First, drain whatever is immediately available without awaiting:
            // this maximizes batching without introducing extra latency.
            while batch.len() < config.max_events && batch_bytes < config.max_bytes {
                match rx.try_recv() {
                    Ok(payload) => {
                        // If this payload would exceed the byte budget, hold it for next batch.
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
                        // No more senders: flush what we have, then exit after writing.
                        closed = true;
                        flush_reason = "idle";
                        break;
                    }
                }
            }

            if flush_reason != "idle" || closed {
                break;
            }

            // Otherwise, wait for either:
            // - next event
            // - deadline
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

        // We clone `Bytes` into a Vec for encoding; `Bytes` clone is cheap (refcounted),
        // but we still allocate the Vec. This keeps the encoder API simple.
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

        // Telemetry: attribute delivery latency to each item in the batch.
        #[cfg(feature = "telemetry")]
        let write_end = Instant::now();
        #[cfg(feature = "telemetry")]
        for event in &batch {
            let delivery_ns = write_end.duration_since(event.enqueue_at).as_nanos() as u64;
            t_histogram!("broker_sub_delivery_latency_ns").record(delivery_ns as f64);
            timings::record_sub_delivery_ns(delivery_ns);
        }

        // Counters:
        // - one frame per flush
        // - one batch per flush
        // - items = batch.len()
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

        // If the channel was closed and we flushed the last batch, exit.
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
    use crate::transport::quic::handlers::publish::AckTimeoutState;
    use anyhow::Context;
    use bytes::{Bytes, BytesMut};
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;

    fn make_server_config() -> anyhow::Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])
            .context("generate self-signed cert")?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config = quinn::ServerConfig::with_single_cert(
            vec![cert_der.clone()],
            PrivateKeyDer::Pkcs8(key_der),
        )?;
        Ok((server_config, cert_der))
    }

    fn make_client_config(cert: CertificateDer<'static>) -> anyhow::Result<quinn::ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert).context("add root cert")?;
        Ok(quinn::ClientConfig::with_root_certificates(
            std::sync::Arc::new(roots),
        )?)
    }

    fn test_config() -> crate::config::BrokerConfig {
        crate::config::BrokerConfig {
            quic_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            metrics_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            controlplane_url: None,
            controlplane_sync_interval_ms: 2000,
            ack_on_commit: false,
            max_frame_bytes: 16 * 1024 * 1024,
            publish_queue_wait_timeout_ms: 2000,
            ack_wait_timeout_ms: 2000,
            disable_timings: false,
            control_stream_drain_timeout_ms: 50,
            cache_conn_recv_window: 256 * 1024 * 1024,
            cache_stream_recv_window: 64 * 1024 * 1024,
            cache_send_window: 256 * 1024 * 1024,
            event_batch_max_events: 1,
            event_batch_max_bytes: 64 * 1024,
            event_batch_max_delay_us: 250,
            fanout_batch_size: 1,
            pub_workers_per_conn: 1,
            pub_queue_depth: 8,
            event_queue_depth: 8,
        }
    }

    fn make_envelope(payload: &[u8]) -> EventEnvelope {
        EventEnvelope {
            payload: Bytes::from(payload.to_vec()),
            #[cfg(feature = "telemetry")]
            enqueue_at: crate::transport::quic::telemetry::t_instant_now(),
        }
    }

    async fn spawn_event_writer(
        rx: mpsc::Receiver<EventEnvelope>,
        config: EventWriterConfig,
    ) -> Result<(
        tokio::task::JoinHandle<Result<()>>,
        felix_transport::QuicConnection,
    )> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let event_send = connection.open_uni().await?;
            run_event_writer(event_send, rx, config).await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        Ok((server_task, connection))
    }

    #[tokio::test]
    async fn run_event_writer_single_closes_on_channel_close() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 1,
            batch_size: 1,
            max_events: 1,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(10),
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_envelope(b"hello")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 1);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"hello");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_single_binary_uses_batch_encoding() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 9,
            batch_size: 1,
            max_events: 1,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(10),
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_envelope(b"bin")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 9);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"bin");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_batches_with_pending_payload() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 7,
            batch_size: 10,
            max_events: 10,
            max_bytes: 5,
            flush_delay: Duration::from_millis(50),
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_envelope(b"aaaa")).await?;
        tx.send(make_envelope(b"bbb")).await?;
        tx.send(make_envelope(b"c")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
        assert_eq!(batch1.subscription_id, 7);
        assert_eq!(batch1.payloads.len(), 1);
        assert_eq!(batch1.payloads[0].as_ref(), b"aaaa");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
        assert_eq!(batch2.subscription_id, 7);
        assert_eq!(batch2.payloads.len(), 2);
        assert_eq!(batch2.payloads[0].as_ref(), b"bbb");
        assert_eq!(batch2.payloads[1].as_ref(), b"c");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_sends_event_stream_binary_batch() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                felix_broker::StreamMetadata::default(),
            )
            .await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let handled = handle_subscribe_message(
                broker_for_server,
                connection,
                test_config(),
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(7),
            )
            .await?;
            Result::<bool>::Ok(handled)
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;

        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Subscribed { subscription_id }) => {
                assert_eq!(subscription_id, 7);
            }
            _ => panic!("unexpected ack"),
        }

        let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let hello = crate::transport::quic::codec::read_message_limited(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("hello");
        match hello {
            Message::EventStreamHello { subscription_id } => {
                assert_eq!(subscription_id, 7);
            }
            other => panic!("unexpected hello: {other:?}"),
        }

        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"hello"),
            )
            .await?;

        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 7);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"hello");

        let handled = server_task.await.context("server join")??;
        assert!(handled);
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_errors_when_stream_missing() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server,
                connection,
                test_config(),
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "missing".to_string(),
                Some(11),
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let _connection = client.connect(addr, "localhost").await?;

        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Error { message }) => {
                assert!(message.contains("stream not found"));
            }
            _ => panic!("unexpected ack"),
        }

        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_batches_by_bytes() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                felix_broker::StreamMetadata::default(),
            )
            .await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let mut config = test_config();
        config.fanout_batch_size = 10;
        config.event_batch_max_events = 10;
        config.event_batch_max_bytes = 6;

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server,
                connection,
                config,
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(21),
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;

        let _ = out_ack_rx.recv().await;
        let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let _ = crate::transport::quic::codec::read_message_limited(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("hello");

        broker
            .publish("t1", "default", "orders", bytes::Bytes::from_static(b"aa"))
            .await?;
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"bbbb"),
            )
            .await?;
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"ccccc"),
            )
            .await?;

        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("batch1");
        assert_eq!(batch1.subscription_id, 21);
        assert_eq!(batch1.payloads.len(), 2);
        assert_eq!(batch1.payloads[0].as_ref(), b"aa");
        assert_eq!(batch1.payloads[1].as_ref(), b"bbbb");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("batch2");
        assert_eq!(batch2.subscription_id, 21);
        assert_eq!(batch2.payloads.len(), 1);
        assert_eq!(batch2.payloads[0].as_ref(), b"ccccc");

        server_task.await.context("server join")??;
        Ok(())
    }
}
