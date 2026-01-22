// Ack parsing and publish ack handling helpers.
use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::{AckMode, Message};
use quinn::RecvStream;

#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::frame_io::read_frame_into;

pub(crate) async fn read_ack_message_with_timing(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    #[cfg(feature = "telemetry")]
    let sample = crate::t_should_sample();
    #[cfg(feature = "telemetry")]
    let read_start = crate::t_now_if(sample);
    let frame = match read_frame_into(recv, frame_scratch, false).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_read_wait_ns(read_ns);
        t_histogram!("client_ack_read_wait_ns").record(read_ns as f64);
    }
    #[cfg(feature = "telemetry")]
    let decode_start = crate::t_now_if(sample);
    let message = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_decode_ns(decode_ns);
        t_histogram!("client_ack_decode_ns").record(decode_ns as f64);
    }
    Ok(Some(message))
}

pub(crate) async fn maybe_wait_for_ack(
    recv: &mut RecvStream,
    ack: AckMode,
    request_id: Option<u64>,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    // AckMode::None is fire-and-forget; otherwise wait for PublishOk/PublishError.
    if ack == AckMode::None {
        return Ok(());
    }
    let request_id =
        request_id.ok_or_else(|| anyhow::anyhow!("missing request_id for acked publish"))?;
    let response = read_ack_message_with_timing(recv, frame_scratch).await?;
    match response {
        Some(Message::PublishOk { request_id: ack_id }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        }
        Some(Message::PublishError {
            request_id: ack_id,
            message,
        }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(anyhow::anyhow!("publish failed: {message}"))
        }
        other => Err(anyhow::anyhow!("publish failed: {other:?}")),
    }
}
