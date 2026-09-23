// Legacy direct-to-stream event writer, retained for tests.

use anyhow::{Context, Result};
#[cfg(feature = "telemetry")]
use std::time::Instant;
use tokio::sync::mpsc;

use crate::timings;
use crate::transport::quic::handlers::subscribe::config::EventWriterConfig;
use crate::transport::quic::handlers::subscribe::writer::write_parts;
use crate::transport::quic::telemetry::{t_now_if, t_should_sample};

/// Drain subscriber events from an `mpsc` queue and write them onto a uni QUIC stream.
///
/// Internal detail: `pending`
/// In batch mode, we may read one event that would exceed `max_bytes` if appended. We keep that
/// event in `pending` so it becomes the first element of the next batch (instead of dropping it).
#[allow(unused_assignments)]
#[cfg(test)]
pub(crate) async fn run_event_writer(
    mut event_send: quinn::SendStream,
    mut rx: mpsc::Receiver<bytes::Bytes>,
    config: EventWriterConfig,
) -> Result<()> {
    // `pending` holds the first event of the next batch when the current batch is byte-limited.
    let mut pending: Option<bytes::Bytes> = None;
    let max_events = config.max_events.max(1);
    let max_bytes = config.max_bytes.max(1);

    if config.single_event_mode {
        loop {
            let sample = t_should_sample();
            let queue_start = t_now_if(sample);

            let first_payload = match rx.recv().await {
                Some(payload) => payload,
                None => break,
            };
            if let Some(start) = queue_start {
                let queue_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_queue_wait_ns(queue_ns);
                t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
            }

            let mut payloads = Vec::with_capacity(max_events);
            payloads.push(first_payload);
            while payloads.len() < max_events {
                match rx.try_recv() {
                    Ok(payload) => payloads.push(payload),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            let write_start = t_now_if(sample);
            #[cfg(not(feature = "telemetry"))]
            let _ = write_start;

            let mut payload_bytes = 0usize;
            let mut frames_written = 0u64;
            let mut bytes_written = 0u64;
            for payload in &payloads {
                payload_bytes += payload.len();
                let prefix_start = t_now_if(sample);
                let parts = felix_wire::binary::encode_event_batch_parts(
                    config.subscription_id,
                    std::slice::from_ref(payload),
                )
                .context("encode binary event batch parts")?;
                if let Some(start) = prefix_start {
                    let prefix_ns = start.elapsed().as_nanos() as u64;
                    timings::record_sub_prefix_ns(prefix_ns);
                    t_histogram!(
                        "felix_broker_sub_prefix_build_ns",
                        "payload_bytes" => payload.len().to_string(),
                        "batch_events" => "1"
                    )
                    .record(prefix_ns as f64);
                }
                t_histogram!("broker_sub_frame_bytes").record(parts.frame_len() as f64);
                bytes_written = bytes_written.saturating_add(parts.frame_len() as u64);
                let write_await_start = t_now_if(sample);
                write_parts(&mut event_send, parts).await?;
                if let Some(start) = write_await_start {
                    let write_await_ns = start.elapsed().as_nanos() as u64;
                    timings::record_sub_write_await_ns(write_await_ns);
                    t_histogram!("broker_sub_write_blocked_ns").record(write_await_ns as f64);
                    t_histogram!(
                        "felix_broker_sub_write_await_ns",
                        "payload_bytes" => payload.len().to_string(),
                        "batch_events" => "1"
                    )
                    .record(write_await_ns as f64);
                }
                frames_written = frames_written.saturating_add(1);
            }
            t_counter!("felix_subscribe_bytes_total").increment(payload_bytes as u64);
            t_histogram!(
                "felix_broker_sub_flush_payload_bytes",
                "batch_events" => payloads.len().to_string()
            )
            .record(payload_bytes as f64);
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters
                    .frames_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .bytes_out
                    .fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_frames_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_batches_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_items_out_ok
                    .fetch_add(payloads.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            #[cfg(feature = "telemetry")]
            if let Some(start) = write_start {
                let write_end = Instant::now();
                let write_ns = write_end.duration_since(start).as_nanos() as u64;
                timings::record_sub_write_ns(write_ns);
                timings::record_quic_write_ns(write_ns);
                t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
                t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
                t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
            }
        }
        let _ = event_send.finish();
        return Ok(());
    }

    // Coalesce events by count, bytes, or deadline; each flush writes exactly one binary EventBatch frame.
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

        let mut batch = Vec::with_capacity(max_events);
        let mut batch_bytes = 0usize;
        let mut closed = false;

        // Why we track `flush_reason`:
        // - helps diagnose which limiter is dominant under different workloads.
        #[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
        #[allow(unused_assignments)]
        let mut flush_reason = "idle";

        // Seed the batch with the first element.
        batch_bytes += first.len();
        batch.push(first);

        // Deadline for flushing a partially full batch.
        let deadline = tokio::time::Instant::now() + config.flush_delay;
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);

        // Immediate flush if already at a threshold.
        if batch.len() >= max_events {
            flush_reason = "count";
        } else if batch_bytes >= max_bytes {
            flush_reason = "bytes";
        }

        // Keep collecting until we hit a flush condition or the channel closes.
        while flush_reason == "idle" && !closed {
            // First, drain whatever is immediately available without awaiting:
            // this maximizes batching without introducing extra latency.
            while batch.len() < max_events && batch_bytes < max_bytes {
                match rx.try_recv() {
                    Ok(payload) => {
                        // If this payload would exceed the byte budget, hold it for next batch.
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

        let prefix_start = t_now_if(sample);
        let frame_parts =
            felix_wire::binary::encode_event_batch_parts(config.subscription_id, &batch)
                .context("encode binary event batch parts")?;
        if let Some(start) = prefix_start {
            let prefix_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_prefix_ns(prefix_ns);
            t_histogram!(
                "felix_broker_sub_prefix_build_ns",
                "payload_bytes" => batch_bytes.to_string(),
                "batch_events" => batch.len().to_string()
            )
            .record(prefix_ns as f64);
        }

        t_counter!("felix_subscribe_bytes_total").increment(batch_bytes as u64);
        t_histogram!("broker_sub_frame_bytes").record(frame_parts.frame_len() as f64);
        t_histogram!(
            "felix_broker_sub_flush_payload_bytes",
            "batch_events" => batch.len().to_string()
        )
        .record(batch_bytes as f64);
        let write_await_start = t_now_if(sample);
        let frame_len = frame_parts.frame_len() as u64;
        write_parts(&mut event_send, frame_parts).await?;
        #[cfg(not(feature = "telemetry"))]
        let _ = frame_len;
        if let Some(start) = write_await_start {
            let write_await_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_write_await_ns(write_await_ns);
            t_histogram!("broker_sub_write_blocked_ns").record(write_await_ns as f64);
            t_histogram!(
                "felix_broker_sub_write_await_ns",
                "payload_bytes" => batch_bytes.to_string(),
                "batch_events" => batch.len().to_string()
            )
            .record(write_await_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        {
            let counters = crate::transport::quic::telemetry::frame_counters();
            counters
                .frames_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .bytes_out
                .fetch_add(frame_len, std::sync::atomic::Ordering::Relaxed);
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
            let write_end = Instant::now();
            let write_ns = write_end.duration_since(start).as_nanos() as u64;
            timings::record_sub_write_ns(write_ns);
            timings::record_quic_write_ns(write_ns);
            t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
            t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
            t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
            t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
        }

        // If the channel was closed and we flushed the last batch, exit.
        if closed {
            break;
        }
    }
    let _ = event_send.finish();
    Ok(())
}
