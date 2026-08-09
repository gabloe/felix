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

// Submodules:
// - `admission`: byte-budget admission control and the subscription cap.
// - `ingress`: worker sharding, bounded enqueue, in-flight depth accounting.
// - `ack`: ack envelopes, waiter protocol, and the ack timeout window.
// - `control`: acked publish handlers on the bi-directional control stream.
// - `uni`: fire-and-forget publish handlers on uni-directional streams.
//
// The connection and stream layers address these through the re-exports below,
// so `handlers::publish::<name>` stays the stable path for the whole transport.

mod ack;
mod admission;
mod control;
mod ingress;
mod uni;

#[cfg(test)]
mod tests;

pub(crate) use ack::{
    AckTimeoutState, AckWaiterMessage, AckWaiterResult, Outgoing, handle_ack_enqueue_result,
    send_outgoing_best_effort, send_outgoing_critical,
};
pub(crate) use admission::{PublishAdmission, SubscriptionLimiter};

use ack::EnqueuePolicy;
use admission::AdmissionPermit;
pub(crate) use control::{
    handle_binary_publish_batch_control, handle_publish_batch_message, handle_publish_message,
};
pub(crate) use ingress::{PublishTarget, decrement_depth, reset_local_depth_only};
pub(crate) use uni::{
    handle_binary_publish_batch_uni, handle_publish_batch_message_uni, handle_publish_message_uni,
};

// Re-exported so the test module (and its `use super::*`) reaches the internals it
// exercises directly, without widening them for the rest of the crate.
#[cfg(test)]
use crate::auth::AuthContext;
#[cfg(test)]
use crate::transport::quic::errors::AckEnqueueError;
#[cfg(test)]
use crate::transport::quic::{
    ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
};
#[cfg(test)]
use felix_wire::{Frame, Message};
#[cfg(test)]
use ingress::{enqueue_publish, publish_worker_index};
#[cfg(test)]
use tokio::sync::{Mutex, Semaphore};

use anyhow::Result;
use bytes::Bytes;
use felix_broker::{Broker, StreamHandle};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

use super::subscribe::WriterLaneManager;

use crate::transport::quic::STREAM_CACHE_TTL;

pub(crate) type StreamHandleCache = HashMap<String, (Option<StreamHandle>, Instant)>;

/// Work item consumed by publish workers.
///
/// A publish job is the unit the broker’s ingress pipeline processes:
/// - It identifies the target stream with a resolved handle.
/// - It carries one or more payloads (single publish or batch).
/// - `response` is **only** used when the publish was received on the control stream and the
///   client requested an ack in commit-ack mode (`ack_on_commit = true`).
///
/// For uni-stream publishes and enqueue-ack mode, `response` is `None`.
pub(crate) struct PublishJob {
    pub(crate) target: PublishTarget,
    pub(crate) payloads: Vec<Bytes>,
    pub(crate) response: Option<oneshot::Sender<Result<()>>>,
    /// Held from `enqueue_publish` admission until this job finishes processing (or is dropped
    /// without ever being enqueued). See [`PublishAdmission`].
    pub(crate) admission_permit: Option<AdmissionPermit>,
}

/// Shared publish-ingress configuration and worker queue handles.
///
/// - `workers`: per-worker `mpsc::Sender<PublishJob>` queues.
/// - `worker_count`: cached length for fast hashing.
/// - `depth`: best-effort local depth tracking for this publish queue set.
/// - `wait_timeout`: bound used by `EnqueuePolicy::Wait`.
/// - `admission`: shared in-flight-byte budget across all workers (see [`PublishAdmission`]).
/// - `conn_admission`: this connection's slice of `admission`. `workers`/`admission`/`depth` are
///   intentionally process-wide (see `build_publish_context`'s note on avoiding per-connection
///   worker pools), but that means nothing bounds how much of the shared budget one connection
///   can occupy. `conn_admission` is constructed fresh per connection
///   (`handle_connection`) and closes that gap without touching the shared worker pool.
#[derive(Clone)]
pub(crate) struct PublishContext {
    pub(crate) workers: Arc<Vec<mpsc::Sender<PublishJob>>>,
    pub(crate) worker_count: usize,
    pub(crate) depth: Arc<AtomicUsize>,
    pub(crate) wait_timeout: Duration,
    pub(crate) admission: Arc<PublishAdmission>,
    pub(crate) conn_admission: Arc<PublishAdmission>,
    /// This connection's subscription-count limiter (see [`SubscriptionLimiter`]). Bundled here
    /// because `PublishContext` is already the per-connection context threaded down to the
    /// control-stream loop that handles `Subscribe` messages.
    pub(crate) subscriptions: Arc<SubscriptionLimiter>,
    /// This connection's writer-lane manager for subscription delivery (see
    /// [`WriterLaneManager`]). One instance per connection, constructed fresh in
    /// `handle_connection` — see that type's doc comment for why it's no longer a
    /// process-wide cache.
    pub(crate) lane_manager: Arc<WriterLaneManager>,
    /// When true, un-acked publishes wait (bounded) for ingress capacity instead
    /// of being shed. Production keeps this off so fire-and-forget load sheds
    /// visibly under overload; benchmarks and lossless pipelines turn it on so
    /// backpressure propagates through QUIC flow control to the publisher.
    pub(crate) ingress_wait: bool,
}

impl PublishContext {
    /// Overflow policy for publishes that carry no ack (fire-and-forget).
    pub(crate) fn overflow_policy(&self) -> EnqueuePolicy {
        if self.ingress_wait {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Drop
        }
    }
}

pub(crate) async fn resolve_stream_cached(
    broker: &Broker,
    cache: &mut StreamHandleCache,
    key_scratch: &mut String,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> Option<StreamHandle> {
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
    if let Some((handle, expires)) = cache.get(key_scratch.as_str())
        && *expires > Instant::now()
        && handle.as_ref().is_none_or(StreamHandle::is_active)
    {
        return handle.clone();
    }
    let handle = broker
        .resolve_stream_handle(tenant_id, namespace, stream)
        .await
        .ok();
    cache.insert(
        key_scratch.clone(),
        (handle.clone(), Instant::now() + STREAM_CACHE_TTL),
    );
    handle
}
