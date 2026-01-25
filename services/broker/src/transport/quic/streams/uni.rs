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
        let frame = match source
            .next_frame(config.max_frame_bytes, frame_scratch)
            .await?
        {
            Some(frame) => frame,
            None => break,
        };
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let handled = handle_binary_publish_batch_uni(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &publish_ctx,
                &frame,
            )
            .await?;
            if !handled {
                break;
            }
            continue;
        }
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::transport::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                crate::transport::quic::telemetry::log_decode_error("uni_message", &err, &frame);
                return Err(err);
            }
        };
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
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
                if !handled {
                    break;
                }
            }
            _ => {
                t_counter!(
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
