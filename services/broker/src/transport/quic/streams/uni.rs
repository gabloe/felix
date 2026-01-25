//! This module implements the uni-directional publish stream loop for the QUIC transport in the broker.
//!
//! In this protocol, a uni-directional publish stream is a stream initiated by a client solely for sending
//! publish-only traffic to the broker. Unlike bi-directional streams, this stream does not expect or send
//! any responses back to the client. This design simplifies the protocol for publish-only clients and reduces
//! overhead on the broker side.
//!
//! The stream expects to receive frames that encode publish messages or publish batches, either in binary or
//! JSON format. The loop continuously reads frames, decodes messages, and forwards publish events to the broker.
//! Any frame or message that does not conform to the expected publish-only pattern is treated as a protocol
//! violation, causing the stream to be closed. This ensures strict adherence to the publish-only contract
//! and prevents misuse or protocol confusion.

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_broker::Broker;
use felix_wire::Message;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use crate::config::BrokerConfig;
use crate::transport::quic::handlers::publish::{
    PublishContext, handle_binary_publish_batch_uni, handle_publish_batch_message_uni,
    handle_publish_message_uni,
};
use crate::transport::quic::telemetry::t_counter;

use super::frame_source::FrameSource;

/// Runs the uni-directional publish stream loop.
///
/// This function manages the lifecycle of a uni-directional stream from a client that sends publish-only
/// traffic. The contract for this uni stream is that it only carries publish messages or publish batches
/// (in binary or JSON format), and no responses are sent back on this stream.
///
/// The `stream_cache` and `stream_cache_key` are used to track stream-specific state and optimize handling
/// of repeated streams or topics. They are passed in and updated by the handlers to maintain continuity
/// across frames on this stream.
///
/// The `frame_scratch` buffer is used as temporary storage for frame payloads to avoid repeated allocations
/// and improve performance during frame reads.
///
/// The loop terminates when:
/// - The source reaches EOF (no more frames),
/// - A protocol violation is detected (e.g., a non-publish message is received),
/// - Any handler returns `handled = false` indicating the stream should close,
/// - Or a decode error occurs.
///
/// # Parameters
/// - `source`: Abstracts the frame source, allowing different underlying transport implementations.
/// - `broker`: Shared broker instance to which publish events are forwarded.
/// - `config`: Broker configuration with limits such as max frame size.
/// - `publish_ctx`: Context for publish handling.
/// - `stream_cache`: Mutable cache for stream state keyed by `stream_cache_key`.
/// - `stream_cache_key`: Key identifying the stream in the cache.
/// - `frame_scratch`: Mutable buffer used to read frames efficiently.
///
/// # Returns
/// Returns `Ok(())` on clean stream closure, or an error if a decode or processing failure occurs.
pub(super) async fn run_uni_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut stream_cache: HashMap<String, (bool, std::time::Instant)>,
    mut stream_cache_key: String,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    loop {
        // Read the next frame from the source. The FrameSource abstraction allows reading frames from
        // different underlying transport mechanisms while enforcing max frame size limits.
        let frame = match source
            .next_frame(config.max_frame_bytes, frame_scratch)
            .await?
        {
            Some(frame) => frame,
            None => break, // EOF reached, close stream gracefully
        };

        // Fast-path: if the frame has the FLAG_BINARY_PUBLISH_BATCH flag set, handle it directly without
        // decoding as JSON. This optimization avoids unnecessary decode overhead for binary encoded batches.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let handled = handle_binary_publish_batch_uni(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &publish_ctx,
                &frame,
            )
            .await?;
            // If handler indicates not handled, close the stream loop
            if !handled {
                break;
            }
            continue;
        }

        // Decode the frame payload into a Message. This may fail if the payload is malformed or invalid.
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                // On decode error, increment telemetry counters to track error rates for monitoring.
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::transport::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                // Log the decode error with context for debugging and analysis.
                crate::transport::quic::telemetry::log_decode_error("uni_message", &err, &frame);
                return Err(err);
            }
        };

        // Handle the decoded message according to its variant.
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
                // Handle a single publish message. The handler returns whether the message was handled
                // successfully and whether to continue processing.
                let handled = handle_publish_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                )
                .await?;
                // If not handled, close the stream loop.
                if !handled {
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
                // Handle a batch of publish messages similarly.
                let handled = handle_publish_batch_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                )
                .await?;
                // Close the stream if handler indicates not handled.
                if !handled {
                    break;
                }
            }
            // Receiving any other message type on a uni-directional publish stream is a protocol violation.
            _ => {
                // Increment telemetry counter for protocol violations on uni streams.
                t_counter!(
                    "felix_quic_protocol_violation_total",
                    "stream" => "uni"
                )
                .increment(1);
                tracing::debug!("closing uni stream after non-publish message");
                // Close the stream loop due to protocol violation.
                break;
            }
        }
    }
    Ok(())
}
