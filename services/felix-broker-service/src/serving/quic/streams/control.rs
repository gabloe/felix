// Control stream (bi-directional QUIC stream)
//
// This module implements the *read side* of the broker's bidirectional control stream.
// The control stream is the request/response path used by clients for:
//   - Publish / PublishBatch (optionally requesting an ack)
//   - Subscribe (establishing a subscription and spawning a uni-directional event stream)
//   - CachePut / CacheGet (request/response cache API)
//
// Key design points:
//   1) Single-writer response path (implemented elsewhere): the read loop never writes to the
//      SendStream directly; it enqueues `Outgoing` responses into an outbound channel drained by a
//      dedicated writer task.
//
//   2) Fast-path binary batching: when a frame is marked with FLAG_BINARY_PUBLISH_BATCH we bypass
//      JSON decoding and dispatch to a specialized handler. This keeps the hot path cheap.
//
//   3) Cooperative cancellation: `cancel_rx_read` (watch) allows the writer or other tasks to
//      request the control loop stop (e.g., writer detects the peer closed, or backpressure logic
//      decides to tear down).
//
//   4) Backpressure / throttling coordination: `ack_throttle_rx/tx` is a watch channel used to
//      communicate whether the outbound response queue is in a throttled state (watermarks are
//      enforced in the writer/enqueue helpers). The control loop passes the current throttled state
//      into publish handlers so they can adjust behavior.
//
//   5) Ack-on-commit mode: when `config.ack_on_commit` is enabled, publish handlers may defer the
//      ack until the publish worker commits. `ack_waiters` bounds in-flight waiters and
//      `ack_waiter_tx` delivers waiter work to the background ack-waiter task.
//
// Return value convention:
//   Ok(true)  => graceful close / stream should be considered "done" (no error)
//   Ok(false) => protocol error or peer sent Error/unexpected message
//   Err(_)    => hard failure (decode/IO/etc.)
use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_authz::{
    Action, CacheScope, Namespace, StreamName, TenantId, cache_resource, stream_resource,
};
use felix_broker::Broker;
use felix_wire::Message;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::BrokerConfig;
use crate::observability::timings;
use crate::serving::auth::{AuthContext, BrokerAuth};
use crate::serving::quic::errors::{AckEnqueueError, record_ack_enqueue_failure};
use crate::serving::quic::handlers::publish::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, Outgoing, PublishContext, StreamHandleCache,
    handle_ack_enqueue_result, handle_acked_binary_publish_batch_control,
    handle_binary_publish_batch_control, handle_publish_batch_message, handle_publish_message,
    send_outgoing_best_effort, send_outgoing_critical,
};
use crate::serving::quic::handlers::subscribe::handle_subscribe_message;
use crate::serving::quic::telemetry::{t_histogram, t_now_if, t_should_sample};

use super::frame_source::FrameSource;

// The loop is intentionally structured as:
//   read frame -> (optional fast-path) -> decode -> dispatch.
//
// Important parameters:
//   - `source`: abstract frame source (RecvStream in prod, test doubles in unit tests).
//   - `stream_cache` / `stream_cache_key`: per-connection cache of stream scope lookups used by
//     publish handlers to avoid repeatedly touching shared metadata for hot streams.
//   - `out_ack_tx` / `out_ack_depth`: outbound response queue + depth gauge used for backpressure.
//   - `ack_throttle_rx/tx`: shared throttling state; this loop reads current state, handlers/writer
//     update it.
//   - `ack_timeout_state`: shared state used to detect/report ack enqueue timeouts.
//   - `ack_waiters` / `ack_waiter_tx`: bounds and routes "ack when commit finishes" work.
//   - `frame_scratch`: reusable buffer to avoid per-frame allocations.
// Main control-loop: read frames, decode messages, and dispatch to handlers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_control_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    publish_ctx: PublishContext,
    mut stream_cache: StreamHandleCache,
    mut stream_cache_key: String,
    out_ack_tx: mpsc::Sender<Outgoing>,
    out_ack_depth: Arc<AtomicUsize>,
    ack_throttle_rx: watch::Receiver<bool>,
    ack_throttle_tx: watch::Sender<bool>,
    ack_timeout_state: Arc<Mutex<AckTimeoutState>>,
    cancel_tx: watch::Sender<bool>,
    mut cancel_rx_read: watch::Receiver<bool>,
    ack_waiters: Arc<Semaphore>,
    ack_waiter_tx: mpsc::Sender<AckWaiterMessage>,
    ack_wait_timeout: Duration,
    frame_scratch: &mut BytesMut,
) -> Result<bool> {
    // If we observe EOF from the peer (source returns None), we treat it as a graceful close.
    // Otherwise, we will cancel downstream tasks and tear down the connection cooperatively.
    let mut graceful_close = false;
    let mut auth_ctx: Option<AuthContext> = None;
    // Frame-flag bits the client understands. Narrowed to the pre-negotiation
    // set until an `Auth` says otherwise.
    let mut peer_flags = felix_wire::ORIGINAL_V1_FLAGS;
    // Optional messages this client understands. Nothing until an `Auth` says
    // otherwise.
    let mut peer_features = 0u32;
    let authz_ctx = AuthzResponseContext {
        out_ack_tx: &out_ack_tx,
        out_ack_depth: &out_ack_depth,
        ack_throttle_tx: &ack_throttle_tx,
        ack_timeout_state: &ack_timeout_state,
        cancel_tx: &cancel_tx,
    };
    loop {
        if *cancel_rx_read.borrow() {
            break;
        }
        // Snapshot current throttling state (set by writer/enqueue helpers when outbound queue
        // crosses watermarks). Handlers may use this to shed work or alter ack behavior.
        let throttled = *ack_throttle_rx.borrow();
        let sample = t_should_sample();
        let read_start = t_now_if(sample);
        // We need to be responsive to cancellation even while blocked on network reads.
        // `watch::Receiver::changed()` wakes when the cancel flag flips.
        let frame = tokio::select! {
            changed = cancel_rx_read.changed() => {
                if changed.is_err() || *cancel_rx_read.borrow() {
                    break;
                }
                continue;
            }
            frame = source.next_frame(config.max_frame_bytes, frame_scratch) => {
                match frame? {
                    Some(frame) => frame,
                    // EOF: the peer cleanly finished the control stream.
                    None => {
                        graceful_close = true;
                        break;
                    }
                }
            }
        };
        let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
        // Flag bits select the payload layout, so an unrecognised bit means we do
        // not know how to parse the body. Reject rather than mask it off and
        // misparse — see `felix_wire::KNOWN_FLAGS`.
        //
        // This is a per-frame error, not a stream-fatal one: the frame reader has
        // already consumed exactly `header.length` bytes, so the stream is sitting
        // on the next frame boundary and stays parseable. Answering and continuing
        // means the peer actually receives the diagnostic — tearing the stream down
        // here would race the writer task and usually deliver EOF instead.
        if felix_wire::has_unknown_flags(frame.header.flags) {
            send_control_error(
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "unsupported frame flags",
            )
            .await?;
            continue;
        }
        // Fast-path: binary publish batch frames avoid JSON decode/allocations.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let acked = frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0;
            if auth_ctx.is_none() {
                send_control_error(
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    "auth required",
                )
                .await?;
                return Ok(false);
            }
            if acked {
                handle_acked_binary_publish_batch_control(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    &frame,
                    auth_ctx.as_ref(),
                    throttled,
                    config.ack_on_commit,
                    sample,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    &ack_waiters,
                    &ack_waiter_tx,
                    peer_flags,
                )
                .await?;
            } else {
                handle_binary_publish_batch_control(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    &frame,
                    auth_ctx.as_ref(),
                    sample,
                    &cancel_tx,
                )
                .await?;
            }
            continue;
        }
        // Slow-path: decode JSON control message. Decode errors are considered fatal protocol
        // violations and terminate the stream.
        let decode_start = t_now_if(sample);
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::serving::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                crate::serving::quic::telemetry::log_decode_error("control_message", &err, &frame);
                return Err(err);
            }
        };
        let decode_ns = decode_start.map(|start| start.elapsed().as_nanos() as u64);
        if let Some(decode_ns) = decode_ns {
            timings::record_decode_ns(decode_ns);
            t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
        }
        // Dispatch by message type. Most handlers are responsible for enqueuing responses into
        // `out_ack_tx` rather than writing directly to the network.
        match message {
            Message::Auth {
                tenant_id,
                token,
                client_flags,
                client_features,
            } => {
                if auth_ctx.is_some() {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "auth already established",
                    )
                    .await?;
                    return Ok(false);
                }
                match auth.authenticate(&tenant_id, &token).await {
                    Ok(ctx) => {
                        auth_ctx = Some(ctx);
                        // Remembered, not just answered: delivery paths need to
                        // know which optional frame shapes this client can read.
                        // Absent means a pre-negotiation client, and the only
                        // safe reading of that silence is the original bits.
                        peer_flags = client_flags.unwrap_or(felix_wire::ORIGINAL_V1_FLAGS);
                        // Which optional messages this client can decode.
                        // Absent means none: a broker that guessed would send a
                        // frame the client cannot parse, and an undecodable
                        // frame costs the connection.
                        peer_features = client_features.unwrap_or(0);
                        // Advertise our flag set only to a client that offered its
                        // own. A client that sent no `client_flags` predates
                        // negotiation and would not understand `AuthOk`, so it must
                        // keep receiving the plain `Ok` it expects.
                        let response = match client_flags {
                            Some(_) => Message::AuthOk {
                                server_flags: felix_wire::KNOWN_FLAGS,
                                // Only what this broker can actually answer.
                                //
                                // The cluster-shaped features are gated on there
                                // being a cluster: a broker with no topology to
                                // report would have to refuse the question it
                                // had invited. Cache delete is not one of those
                                // -- it works the same on a single node -- so
                                // gating it too would leave every standalone
                                // broker unable to offer a request it can serve.
                                server_features: Some(
                                    felix_wire::FEATURE_CACHE_DELETE
                                        // Only when the cache store can observe
                                        // its writes. A watch's contract is
                                        // built on log offsets, so a broker
                                        // whose cache has no log has nothing to
                                        // anchor a resume to and must not
                                        // invite one. Retained delivery rides
                                        // the same machinery — the snapshot is
                                        // the index the log already maintains —
                                        // so the two bits travel together here.
                                        | match broker.cache_watches() {
                                            Some(_) => {
                                                felix_wire::FEATURE_CACHE_WATCH
                                                    | felix_wire::FEATURE_CACHE_WATCH_RETAINED
                                            }
                                            None => 0,
                                        }
                                        | match publish_ctx.client_endpoints {
                                            Some(_) => {
                                                felix_wire::FEATURE_TOPOLOGY
                                                    | felix_wire::FEATURE_REDIRECT
                                            }
                                            None => 0,
                                        }
                                        // Only when there is somewhere to keep a
                                        // group's position. Without durable
                                        // storage a group would restart from the
                                        // beginning on every reconnect, so
                                        // offering the feature would invite work
                                        // this broker cannot do.
                                        | match broker.group_reader() {
                                            Some(_) => {
                                                felix_wire::FEATURE_CONSUMER_GROUP
                                                    | felix_wire::FEATURE_GROUP_DEAD_LETTERS
                                            }
                                            None => 0,
                                        }
                                        // Only when there is somewhere to write
                                        // the counter log. A sum any restart
                                        // resets is worse than refusing to
                                        // count at all.
                                        | match broker.counters() {
                                            Some(_) => felix_wire::FEATURE_COUNTERS,
                                            None => 0,
                                        }
                                        // Advertised unconditionally. A broker
                                        // with no routing snapshot answers 1,
                                        // which is the truth for a single-node
                                        // deployment rather than a guess.
                                        | felix_wire::FEATURE_STREAM_SHARDS
                                        | felix_wire::FEATURE_CACHE_SHARDS
                                        // Advertised unconditionally: the
                                        // sequences live with the shard's
                                        // leader, which every broker is for
                                        // the shards it leads.
                                        | felix_wire::FEATURE_IDEMPOTENT_PRODUCER,
                                ),
                                // Only when there is more than one. A single
                                // listener is the default, and saying so
                                // explicitly would change the bytes every
                                // existing deployment puts on the wire to say
                                // nothing a client does not already know.
                                listener_ports: (config.quic_listeners > 1).then(|| {
                                    config.quic_binds().iter().map(|a| a.port()).collect()
                                }),
                            },
                            None => Message::Ok,
                        };
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(response),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "auth failed");
                        send_control_error(
                            &out_ack_tx,
                            &out_ack_depth,
                            &ack_throttle_tx,
                            &ack_timeout_state,
                            &cancel_tx,
                            "auth failed",
                        )
                        .await?;
                        return Ok(false);
                    }
                }
            }
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                key,
                request_id,
                ack,
            } => {
                if !authorize_stream(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamPublish,
                    &namespace,
                    &stream,
                    request_id,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                handle_publish_message(
                    &broker,
                    &publish_ctx,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    throttled,
                    config.ack_on_commit,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    &ack_waiters,
                    &ack_waiter_tx,
                    ack_wait_timeout,
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                    key,
                    request_id,
                    ack,
                    sample,
                    auth_ctx
                        .as_ref()
                        .map_or_else(String::new, |ctx| ctx.token.clone()),
                )
                .await?;
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                key,
                request_id,
                ack,
            } => {
                if !authorize_stream(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamPublish,
                    &namespace,
                    &stream,
                    request_id,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                handle_publish_batch_message(
                    peer_flags,
                    &broker,
                    &publish_ctx,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    throttled,
                    config.ack_on_commit,
                    AckEncoding::Json,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    &ack_waiters,
                    &ack_waiter_tx,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    key,
                    request_id,
                    ack,
                    sample,
                    auth_ctx
                        .as_ref()
                        .map_or_else(String::new, |ctx| ctx.token.clone()),
                    None,
                )
                .await?;
            }
            Message::PublishIdempotent {
                tenant_id,
                namespace,
                stream,
                payloads,
                key,
                request_id,
                producer_id,
                sequence,
            } => {
                if !authorize_stream(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamPublish,
                    &namespace,
                    &stream,
                    Some(request_id),
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                handle_publish_batch_message(
                    peer_flags,
                    &broker,
                    &publish_ctx,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    throttled,
                    config.ack_on_commit,
                    AckEncoding::Idempotent,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    &ack_waiters,
                    &ack_waiter_tx,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    key,
                    Some(request_id),
                    // Always acknowledged: a producer that never learns the
                    // answer cannot know what to send next.
                    Some(felix_wire::AckMode::PerBatch),
                    sample,
                    auth_ctx
                        .as_ref()
                        .map_or_else(String::new, |ctx| ctx.token.clone()),
                    Some((producer_id, sequence)),
                )
                .await?;
            }
            Message::ProducerInit { request_id } => {
                // Authenticated like everything else on this stream. The id
                // itself carries no authority: a batch under it is authorised
                // against the stream it names, like any other.
                if auth_ctx.is_none() {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "not authenticated",
                    )
                    .await?;
                    return Ok(false);
                }
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::ProducerInitOk {
                            request_id,
                            producer_id: broker.new_producer_id(),
                        }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::Topology => {
                // Authenticated like everything else on this stream: the
                // addresses are not secret, but who may ask a broker anything
                // at all is still the tenant boundary.
                if auth_ctx.is_none() {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "not authenticated",
                    )
                    .await?;
                    return Ok(false);
                }
                let brokers = publish_ctx
                    .client_endpoints
                    .as_ref()
                    .map(|endpoints| endpoints.snapshot().as_ref().clone())
                    .unwrap_or_default();
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::TopologyView { brokers }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::StreamShards {
                tenant_id,
                namespace,
                stream,
                request_id,
            } => {
                // Authenticated, and scoped: a client may ask about the shape
                // of streams in its own tenant, not another's.
                let Some(ctx) = auth_ctx.as_ref() else {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "not authenticated",
                    )
                    .await?;
                    return Ok(false);
                };
                if ctx.tenant_id != tenant_id {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "tenant mismatch",
                    )
                    .await?;
                    return Ok(false);
                }
                // Read from the routing snapshot, which is an `ArcSwap` load.
                // A broker that has never heard of the stream answers 0 rather
                // than guessing 1: "I do not know" and "exactly one shard" are
                // different answers, and a client that assumed the latter would
                // silently read a fraction of a stream.
                let shards = match publish_ctx.ingress.as_deref() {
                    Some(ingress) => ingress
                        .placed_shards_for(
                            crate::shards::ShardKind::Stream,
                            &tenant_id,
                            &namespace,
                            &stream,
                        )
                        .unwrap_or(0),
                    // No routing snapshot to consult, so the registry is the
                    // only thing that knows whether the stream exists. It is
                    // served here and unplaced, which is one shard.
                    None => u32::from(broker.stream_exists(&tenant_id, &namespace, &stream).await),
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::StreamShardsView { shards, request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::CacheShards {
                tenant_id,
                namespace,
                cache,
                request_id,
            } => {
                let Some(ctx) = auth_ctx.as_ref() else {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "not authenticated",
                    )
                    .await?;
                    return Ok(false);
                };
                if ctx.tenant_id != tenant_id {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "tenant mismatch",
                    )
                    .await?;
                    return Ok(false);
                }
                // A registered cache the snapshot has not placed is served
                // here as one shard, which is how `cache_watch` resolves it
                // too. A cache nobody knows is 0, not 1.
                let placed = publish_ctx.ingress.as_deref().and_then(|ingress| {
                    ingress.placed_shards_for(
                        crate::shards::ShardKind::Cache,
                        &tenant_id,
                        &namespace,
                        &cache,
                    )
                });
                let shards = match placed {
                    Some(shards) => shards,
                    None => u32::from(broker.cache_exists(&tenant_id, &namespace, &cache).await),
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::CacheShardsView { shards, request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::Subscribe {
                tenant_id,
                namespace,
                stream,
                subscription_id,
                start,
                shard,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                // Where the shard actually lives.
                //
                // Without this a subscription for a shard this broker does not
                // own is accepted and then delivers nothing, for as long as the
                // application is willing to wait -- the worst of the available
                // answers, because it is indistinguishable from a quiet stream.
                // `docs/subscribe-routing.md` records the decision: redirect to
                // the owner rather than proxy for it.
                if let Some(answer) = crate::serving::quic::handlers::redirect::redirect_for(
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.client_endpoints.as_deref(),
                    &tenant_id,
                    &namespace,
                    &stream,
                    // Ownership is per shard, so the redirect answers for the
                    // shard being subscribed to: different shards of one stream
                    // can have different owners.
                    shard.unwrap_or(0),
                    crate::shards::ShardKind::Stream,
                    peer_features,
                ) {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(answer),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    continue;
                }
                // Subscribe establishes server-side subscription state and typically spawns a
                // uni-directional event stream back to the client for delivery.
                let done = handle_subscribe_message(
                    Arc::clone(&broker),
                    connection.clone(),
                    config.clone(),
                    &publish_ctx.subscriptions,
                    &publish_ctx.lane_manager,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    tenant_id,
                    namespace,
                    stream,
                    subscription_id,
                    start,
                    shard,
                    peer_flags,
                )
                .await?;
                if done {
                    return Ok(true);
                }
            }
            // Broker -> client only; a client sending one is a protocol error.
            Message::SubscribeCursorError { .. } => {
                send_control_error(
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    "subscribe_cursor_error is a server-to-client message",
                )
                .await?;
                return Ok(false);
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
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheWrite,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                // Cache scope validation: cache operations are rejected if the cache isn't
                // registered for the (tenant, namespace, cache) triple.
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
                        // When request_id is None, the client is using a "best effort" cache API and the
                        // stream is closed after the single request/response completes.
                        return Ok(true);
                    }
                    continue;
                }
                let ttl = ttl_ms.map(Duration::from_millis);
                let lookup_start = t_now_if(sample);
                // Routed, not applied locally: exactly one broker owns this
                // key's shard, and a write served here instead would be the
                // second copy nothing reconciles.
                let applied = crate::serving::cache_routing::apply_cache_op(
                    &broker,
                    (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.peers.as_deref(),
                    auth_ctx.as_ref().map_or("", |ctx| ctx.token.as_str()),
                    tenant_id.as_str(),
                    namespace.as_str(),
                    cache.as_str(),
                    key.as_str(),
                    crate::serving::cache_routing::put_request(value, ttl),
                )
                .await;
                if let Some(start) = lookup_start {
                    let lookup_ns = start.elapsed().as_nanos() as u64;
                    timings::record_cache_insert_ns(lookup_ns);
                }
                if let Err(reason) = applied {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::CacheMessage(Message::Error {
                                message: format!("cache put not served: {reason}"),
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
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheRead,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                // Cache scope validation: cache operations are rejected if the cache isn't
                // registered for the (tenant, namespace, cache) triple.
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
                        // When request_id is None, the client is using a "best effort" cache API and the
                        // stream is closed after the single request/response completes.
                        return Ok(true);
                    }
                    continue;
                }
                let lookup_start = t_now_if(sample);
                let read = crate::serving::cache_routing::apply_cache_op(
                    &broker,
                    (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.peers.as_deref(),
                    auth_ctx.as_ref().map_or("", |ctx| ctx.token.as_str()),
                    &tenant_id,
                    &namespace,
                    &cache,
                    &key,
                    crate::serving::forward::CacheRequest::Get,
                )
                .await;
                if let Some(start) = lookup_start {
                    let lookup_ns = start.elapsed().as_nanos() as u64;
                    timings::record_cache_lookup_ns(lookup_ns);
                }
                let value = match read {
                    Ok(value) => value,
                    Err(reason) => {
                        // A read this broker cannot route is an error, never an
                        // empty answer: reporting a miss would let a client
                        // conclude the key does not exist when it does, on the
                        // owner.
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!("cache get not served: {reason}"),
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
                };
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
                    // When request_id is None, the client is using a "best effort" cache API and the
                    // stream is closed after the single request/response completes.
                    return Ok(true);
                }
            }
            Message::CacheWatch {
                tenant_id,
                namespace,
                cache,
                key,
                prefix,
                shard,
                from_offset,
                retained,
                subscription_id,
            } => {
                // A watch is a read of the cache, and is authorized as one.
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheRead,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                crate::serving::quic::handlers::cache_watch::handle_cache_watch_message(
                    Arc::clone(&broker),
                    connection.clone(),
                    config.clone(),
                    &publish_ctx,
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    },
                    crate::serving::quic::handlers::cache_watch::WatchRequest {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        prefix,
                        shard,
                        from_offset,
                        retained,
                        subscription_id,
                    },
                    peer_features,
                )
                .await?;
            }
            Message::CounterAdd {
                tenant_id,
                namespace,
                cache,
                key,
                delta,
                request_id,
            } => {
                // An add is a write, authorized as one.
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheWrite,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
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
                    continue;
                }
                let applied = crate::serving::cache_routing::apply_counter_op(
                    &broker,
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.peers.as_deref(),
                    auth_ctx.as_ref().map_or("", |ctx| ctx.token.as_str()),
                    &tenant_id,
                    &namespace,
                    &cache,
                    &key,
                    crate::serving::forward::CacheRequest::CounterAdd { delta },
                )
                .await;
                let value = match applied {
                    Ok(value) => value,
                    Err(reason) => {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!("counter add not served: {reason}"),
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
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::CacheMessage(Message::CounterValue { value, request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::CounterGet {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheRead,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
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
                    continue;
                }
                let read = crate::serving::cache_routing::apply_counter_op(
                    &broker,
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.peers.as_deref(),
                    auth_ctx.as_ref().map_or("", |ctx| ctx.token.as_str()),
                    &tenant_id,
                    &namespace,
                    &cache,
                    &key,
                    crate::serving::forward::CacheRequest::CounterGet,
                )
                .await;
                let value = match read {
                    Ok(value) => value,
                    Err(reason) => {
                        // A read this broker cannot route is an error, never
                        // "no counter": absence is an answer about the data,
                        // and this is an answer about the broker.
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!("counter get not served: {reason}"),
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
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::CacheMessage(Message::CounterValue { value, request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupPoll {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                max_records,
                wait_ms,
                request_id,
            } => {
                // A group is a read position over a stream, so it is authorized
                // as a read of that stream.
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                let polled = crate::serving::group_ops::poll(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    max_records as usize,
                    // Capped, so a client cannot hold a broker stream open for
                    // as long as it likes.
                    Duration::from_millis(wait_ms.min(config.group_max_wait_ms)),
                )
                .await;
                let records = match polled {
                    Ok(records) => records,
                    Err(reason) => {
                        // Refused rather than answered with an empty batch: a
                        // consumer told "nothing available" would poll for ever
                        // against a shard this broker does not lead.
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::Error {
                                    message: format!("group poll not served: {reason}"),
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
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::GroupRecords {
                            records,
                            request_id,
                        }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupAck {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                if let Err(reason) = crate::serving::group_ops::settle(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    offset,
                    true,
                )
                .await
                {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: format!("group ack not served: {reason}"),
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
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::CacheOk { request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupNack {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                if let Err(reason) = crate::serving::group_ops::settle(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    offset,
                    false,
                )
                .await
                {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: format!("group nack not served: {reason}"),
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
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::CacheOk { request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupDeadLetters {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                request_id,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                let listed = crate::serving::group_ops::dead_letters(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                )
                .await;
                let offsets = match listed {
                    Ok(offsets) => offsets,
                    Err(reason) => {
                        // Refused rather than answered with an empty list: an
                        // operator told there are no dead letters would stop
                        // looking.
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::Message(Message::Error {
                                    message: format!("dead letters not served: {reason}"),
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
                };
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::GroupDeadLetterList {
                            offsets,
                            request_id,
                        }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupDiscard {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                if let Err(reason) = crate::serving::group_ops::manage_dead_letter(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    offset,
                    false,
                )
                .await
                {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: format!("group discard not served: {reason}"),
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
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::CacheOk { request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::GroupRedrive {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(answer) = group_redirect(
                    &publish_ctx,
                    peer_features,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                ) {
                    crate::serving::quic::handlers::cache_watch::WatchResponder {
                        out_ack_tx: &out_ack_tx,
                        out_ack_depth: &out_ack_depth,
                        ack_throttle_tx: &ack_throttle_tx,
                        ack_timeout_state: &ack_timeout_state,
                        cancel_tx: &cancel_tx,
                    }
                    .send(answer)
                    .await?;
                    continue;
                }
                if let Err(reason) = crate::serving::group_ops::manage_dead_letter(
                    &broker,
                    &publish_ctx,
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    offset,
                    true,
                )
                .await
                {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: format!("group redrive not served: {reason}"),
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
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(Message::CacheOk { request_id }),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
            }
            Message::CacheDelete {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                // A delete is a write, so it is authorized as one. Letting it
                // through on `CacheRead` would make read-only credentials able
                // to destroy data.
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheWrite,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
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

                let removed = crate::serving::cache_routing::apply_cache_op(
                    &broker,
                    (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
                    publish_ctx.ingress.as_deref(),
                    publish_ctx.peers.as_deref(),
                    auth_ctx.as_ref().map_or("", |ctx| ctx.token.as_str()),
                    &tenant_id,
                    &namespace,
                    &cache,
                    &key,
                    crate::serving::forward::CacheRequest::Delete,
                )
                .await;

                let value = match removed {
                    Ok(value) => value,
                    Err(reason) => {
                        // Refused rather than reported as "nothing was there".
                        // A client told the key is gone when the owner still
                        // holds it would be worse than a plain failure.
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!("cache delete not served: {reason}"),
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
                };

                // Answered with the value that was removed, so a caller learns
                // whether the key was there without a second round trip.
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
            Message::GroupRecords { .. }
            | Message::GroupDeadLetterList { .. }
            | Message::CacheValue { .. }
            | Message::CacheOk { .. }
            | Message::CounterValue { .. }
            | Message::CacheWatchStarted { .. }
            | Message::CacheEvent { .. }
            | Message::CacheWatchLagged { .. }
            | Message::Event { .. }
            | Message::EventBatch { .. }
            | Message::Subscribed { .. }
            | Message::EventStreamHello { .. }
            | Message::PublishOk { .. }
            | Message::PublishError { .. }
            | Message::PublishRefused { .. }
            | Message::ProducerInitOk { .. }
            | Message::AuthOk { .. }
            | Message::TopologyView { .. }
            | Message::StreamShardsView { .. }
            | Message::CacheShardsView { .. }
            | Message::NotLeader { .. }
            | Message::Ok => {
                // Protocol hygiene: these message types should never arrive on the control stream
                // from the client. Treat as a protocol violation and close.
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
    // `graceful_close` only tracks EOF from the peer. Any other early-exit path returns false
    // (protocol error) or Err (hard failure).
    Ok(graceful_close)
}

/// A group operation for a shard another broker leads, answered with where to
/// go instead of refused.
///
/// Only a client that offered `FEATURE_REDIRECT` gets it; the rest keep the
/// plain refusal they always had. Every group request travels on its own
/// stream, so a `NotLeader` there answers exactly that request.
fn group_redirect(
    publish_ctx: &PublishContext,
    peer_features: u32,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Option<Message> {
    if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT) {
        return None;
    }
    match crate::serving::quic::handlers::redirect::redirect_for(
        publish_ctx.ingress.as_deref(),
        publish_ctx.client_endpoints.as_deref(),
        tenant_id,
        namespace,
        stream,
        shard,
        crate::shards::ShardKind::Stream,
        peer_features,
    ) {
        answer @ Some(Message::NotLeader { .. }) => answer,
        _ => None,
    }
}

async fn send_control_error(
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    message: &str,
) -> Result<()> {
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::Error {
                message: message.to_string(),
            }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await
}

struct AuthzResponseContext<'a> {
    out_ack_tx: &'a mpsc::Sender<Outgoing>,
    out_ack_depth: &'a Arc<AtomicUsize>,
    ack_throttle_tx: &'a watch::Sender<bool>,
    ack_timeout_state: &'a Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &'a watch::Sender<bool>,
}

async fn authorize_stream(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    request_id: Option<u64>,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    let outgoing = match request_id {
        Some(request_id) => Outgoing::Message(Message::PublishError {
            request_id,
            message: "forbidden".to_string(),
        }),
        None => Outgoing::Message(Message::Error {
            message: "forbidden".to_string(),
        }),
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            "felix_broker_out_ack_depth",
            ctx.ack_throttle_tx,
            outgoing,
        )
        .await,
        ctx.ack_timeout_state,
        ctx.ack_throttle_tx,
        ctx.cancel_tx,
    )
    .await?;
    Ok(false)
}

async fn authorize_stream_simple(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        "forbidden",
    )
    .await?;
    Ok(false)
}

async fn authorize_cache(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    cache: &str,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = cache_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &CacheScope::new(cache),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        "forbidden",
    )
    .await?;
    Ok(false)
}
