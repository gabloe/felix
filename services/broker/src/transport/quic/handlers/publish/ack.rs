// Ack protocol: outgoing envelopes, waiter messages, the timeout window, and the
// helpers that push acks onto the writer with the right backpressure policy.

use anyhow::{Result, anyhow};
use felix_wire::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::transport::quic::errors::AckEnqueueError;
use crate::transport::quic::{
    ACK_ENQUEUE_TIMEOUT, ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
};

/// Items pushed to the outbound writer loop (ack/response path).
///
/// The writer loop is the single owner of the QUIC `SendStream` for the control stream.
/// All responses/acks are funneled into that task to avoid concurrent writes.
///
/// Variants:
/// - `Message`: normal control responses (PublishOk/Error, SubscribeOk, etc.).
/// - `CacheMessage`: cache fast-path replies that may be encoded differently or routed
///   separately from general control responses (depending on the writer implementation).
/// - `PublishAck`: binary acknowledgement for a publish that arrived on the binary
///   acked path. Kept as a distinct variant rather than reusing `Message` because
///   the reply encoding has to match the request encoding — a client that sent a
///   `FLAG_BINARY_PUBLISH_ACKED` frame is reading a binary ack frame, not JSON.
pub(crate) enum Outgoing {
    Message(Message),
    CacheMessage(Message),
    PublishAck {
        request_id: u64,
        /// `None` acknowledges success; `Some` reports failure.
        error: Option<String>,
    },
}

/// Which encoding a pending ack must be answered in.
///
/// The reply has to match the request. A client that sent a JSON `PublishBatch`
/// is blocked reading a JSON `PublishOk`; a client that sent a binary
/// `FLAG_BINARY_PUBLISH_ACKED` frame is blocked reading a binary ack frame.
/// Answering in the wrong one strands the client on a frame it cannot parse,
/// which is why this is carried all the way to the emit site rather than decided
/// there — the commit-ack path emits from the ack-waiter task, long after the
/// request frame itself is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckEncoding {
    Json,
    Binary,
}

impl AckEncoding {
    /// Build a success ack in this encoding.
    pub(crate) fn ok(self, request_id: u64) -> Outgoing {
        match self {
            AckEncoding::Json => Outgoing::Message(Message::PublishOk { request_id }),
            AckEncoding::Binary => Outgoing::PublishAck {
                request_id,
                error: None,
            },
        }
    }

    /// Build a failure ack in this encoding.
    pub(crate) fn error(self, request_id: u64, message: impl Into<String>) -> Outgoing {
        let message = message.into();
        match self {
            AckEncoding::Json => Outgoing::Message(Message::PublishError {
                request_id,
                message,
            }),
            AckEncoding::Binary => Outgoing::PublishAck {
                request_id,
                error: Some(message),
            },
        }
    }
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
        encoding: AckEncoding,
        payload_len: u64,
        start: crate::transport::quic::telemetry::TelemetryInstant,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishTimeout {
        request_id: u64,
        encoding: AckEncoding,
        start: crate::transport::quic::telemetry::TelemetryInstant,
    },
    PublishBatch {
        request_id: u64,
        encoding: AckEncoding,
        payload_bytes: Vec<usize>,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishBatchTimeout {
        request_id: u64,
        encoding: AckEncoding,
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
        encoding: AckEncoding,
        payload_len: u64,
        start: crate::transport::quic::telemetry::TelemetryInstant,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
    PublishBatch {
        request_id: u64,
        encoding: AckEncoding,
        payload_bytes: Vec<usize>,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
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
