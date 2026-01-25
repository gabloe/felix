//! QUIC response writer loop.
//!
//! This module owns the *single-writer* side of a broker control stream.
//! The broker reads control frames (publish, subscribe, cache put/get, etc.) on the receive side,
//! and enqueues responses/acks onto an `mpsc` channel. A dedicated task runs this writer loop and
//! is the *only* code that ever writes to the `quinn::SendStream`.
//!
//! Why a single writer?
//! - `quinn::SendStream` does not support concurrent writes safely without external coordination.
//! - Serializing writes in one task eliminates interleaving/corruption and avoids mutex contention.
//! - Backpressure is applied via queue depth + throttle signals rather than blocking many tasks.
//!
//! Responsibilities:
//! - Drain `Outgoing` items (either JSON `Message` or binary `Frame` for cache fast-path replies).
//! - Maintain per-stream and global ack queue depth gauges.
//! - Toggle `ack_throttle` when depth crosses high/low watermarks.
//! - On any write/encode failure, initiate cooperative shutdown by signaling `cancel`.
//!
//! This file is intentionally small and hot-path oriented: comments explain invariants and
//! cancellation/backpressure behavior, while logic stays straightforward.

// Writer loop owns the SendStream and serializes all outbound responses.
use felix_wire::Message;
use quinn::SendStream;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

use crate::timings;
use crate::transport::quic::GLOBAL_ACK_DEPTH;
use crate::transport::quic::handlers::publish::{Outgoing, decrement_depth};
use crate::transport::quic::telemetry::{t_histogram, t_now_if, t_should_sample};

use super::hooks::{
    encode_cache_message_with_hook, should_reset_throttle, write_frame_with_hook,
    write_message_with_hook,
};

// Drains outgoing responses, updates depth counters, and handles shutdown on error.
pub(super) async fn run_writer_loop(
    mut send: SendStream,
    mut out_ack_rx: mpsc::Receiver<Outgoing>,
    out_ack_depth_worker: Arc<AtomicUsize>,
    ack_throttle_tx_writer: watch::Sender<bool>,
    cancel_tx_writer: watch::Sender<bool>,
    mut cancel_rx_writer: watch::Receiver<bool>,
) {
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
                        if let Err(err) = write_message_with_hook(&mut send, message).await {
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
                        if should_reset_throttle(depth_update) {
                            let _ = ack_throttle_tx_writer.send(false);
                        }
                        if let Some(start) = write_start {
                            let write_ns = start.elapsed().as_nanos() as u64;
                            timings::record_quic_write_ns(write_ns);
                            t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                        }
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = super::super::telemetry::frame_counters();
                            counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            if is_publish_ack {
                                counters.ack_items_out_ok.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Outgoing::CacheMessage(message) => {
                        let sample = t_should_sample();
                        let encode_start = t_now_if(sample);
                        let frame = match encode_cache_message_with_hook(message) {
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
                        if let Err(err) = write_frame_with_hook(&mut send, &frame).await {
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
                        if should_reset_throttle(depth_update) {
                            let _ = ack_throttle_tx_writer.send(false);
                        }
                        if let Some(start) = write_start {
                            let write_ns = start.elapsed().as_nanos() as u64;
                            timings::record_cache_write_ns(write_ns);
                        }
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = super::super::telemetry::frame_counters();
                            counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }
    let _ = send.finish();
}
