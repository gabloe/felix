// Low-level frame IO for felix-wire over QUIC streams.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};

use crate::config::runtime_config;
#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::decode_log::log_decode_error;

pub(crate) async fn read_message(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    let frame = match read_frame_into(recv, frame_scratch, false).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    match Message::decode(frame.clone()).context("decode message") {
        Ok(message) => Ok(Some(message)),
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .frames_in_err
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            log_decode_error("read_message", &err, &frame);
            Err(err)
        }
    }
}

pub(crate) async fn read_frame_into(
    recv: &mut RecvStream,
    scratch: &mut BytesMut,
    record_read_await: bool,
) -> Result<Option<Frame>> {
    #[cfg(not(feature = "telemetry"))]
    let _ = record_read_await;
    #[cfg(feature = "telemetry")]
    let sample = record_read_await && crate::t_should_sample();
    #[cfg(feature = "telemetry")]
    let header_start = crate::t_now_if(sample);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    #[cfg(feature = "telemetry")]
    let header_await = header_start.map(|start| start.elapsed().as_nanos() as u64);

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;

    // Safety: we enforce a max frame size (`FELIX_MAX_FRAME_BYTES`) before allocating.
    let max_frame_bytes = runtime_config().max_frame_bytes;
    if length > max_frame_bytes {
        return Err(anyhow::anyhow!(
            "frame too large: {length} bytes (cap {max_frame_bytes}); refusing"
        ));
    }

    // Reuse the scratch buffer to avoid per-frame allocations.
    scratch.clear();
    scratch.resize(length, 0u8);
    #[cfg(feature = "telemetry")]
    let payload_start = crate::t_now_if(sample);
    recv.read_exact(&mut scratch[..])
        .await
        .context("read frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let payload_await = payload_start.map(|start| start.elapsed().as_nanos() as u64);
        if let Some(header_ns) = header_await {
            let payload_ns = payload_await.unwrap_or(0);
            let total = header_ns.saturating_add(payload_ns);
            timings::record_sub_read_await_ns(total);
            t_histogram!("client_sub_read_await_ns").record(total as f64);
        }
    }

    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters
            .frames_in_ok
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters
            .bytes_in
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(Some(frame))
}

pub(crate) async fn read_frame_cache_timed_into(
    recv: &mut RecvStream,
    sample: bool,
    scratch: &mut BytesMut,
) -> Result<Option<Frame>> {
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    #[cfg(feature = "telemetry")]
    let read_start = crate::t_now_if(sample);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_wait_ns(read_ns);
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;

    // Safety: we enforce a max frame size (`FELIX_MAX_FRAME_BYTES`) before allocating.
    let max_frame_bytes = runtime_config().max_frame_bytes;
    if length > max_frame_bytes {
        return Err(anyhow::anyhow!(
            "frame too large: {length} bytes (cap {max_frame_bytes}); refusing"
        ));
    }

    #[cfg(feature = "telemetry")]
    let drain_start = crate::t_now_if(sample);
    scratch.clear();
    scratch.resize(length, 0u8);
    recv.read_exact(&mut scratch[..])
        .await
        .context("read frame payload")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = drain_start {
        let drain_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_drain_ns(drain_ns);
    }

    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters
            .frames_in_ok
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters
            .bytes_in
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(Some(frame))
}

pub(crate) async fn write_frame_parts(send: &mut SendStream, frame: &Frame) -> Result<()> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    frame.header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes)
        .await
        .context("write frame header")?;
    send.write_all(&frame.payload)
        .await
        .context("write frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters
            .frames_out_ok
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters
            .bytes_out
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

pub(crate) async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame_parts(send, &frame).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_has_reasonable_defaults() {
        let config = runtime_config();
        assert!(config.max_frame_bytes > 0);
        assert!(config.max_frame_bytes <= 64 * 1024 * 1024); // Sanity check
    }
}
