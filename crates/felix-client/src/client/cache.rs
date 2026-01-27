//! Cache client worker implementations and request handling.
//!
//! # Purpose
//! Manages the per-connection cache stream, serializing cache requests and
//! returning responses to callers while recording optional timings.
//!
//! # Design notes
//! A single bi-directional stream is used per cache worker to preserve request
//! ordering and simplify response matching. Backpressure is handled by the
//! mpsc queue and per-connection inflight counters.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::Message;
use quinn::{RecvStream, SendStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::{read_frame_cache_timed_into, write_frame_parts};

pub(crate) struct CacheWorker {
    pub(crate) tx: mpsc::Sender<CacheRequest>,
    pub(crate) conn_index: usize,
}

pub(crate) enum CacheRequest {
    Put {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<()>>,
    },
    Get {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<Option<Bytes>>>,
    },
}

pub(crate) async fn run_cache_worker(
    conn_index: usize,
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<CacheRequest>,
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
) {
    // Single writer for a cache stream; handles sequential request/response pairs.
    let sample = crate::t_should_sample();
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    #[cfg(feature = "telemetry")]
    if sample {
        let open_ns = 0;
        timings::record_cache_open_stream_ns(open_ns);
    }
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
    debug!(conn_index, "cache worker started");
    while let Some(request) = rx.recv().await {
        debug!(conn_index, "cache worker received request");
        let result = handle_cache_request(&mut send, &mut recv, request, &mut frame_scratch).await;

        // Decrement "in-flight ops" gauge for this connection, saturating at 0.
        let counter = &cache_conn_counts[conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
                        .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }

        if let Err(err) = result {
            debug!(conn_index, error = %err, "cache worker request failed");
            break;
        }
    }
    let _ = send.finish();
    debug!(conn_index, "cache worker exited");
    // We should probably use a connection-level atomic that we decremented when work completes so
    // that we can track inflight ops more accurately.
}

async fn handle_cache_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
    request: CacheRequest,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    let sample = crate::t_should_sample();
    match request {
        CacheRequest::Put {
            request_id,
            message,
            response,
        } => {
            let result =
                cache_round_trip(send, recv, message, sample, request_id, frame_scratch).await;
            match result {
                Ok(_) => {
                    let _ = response.send(Ok(()));
                    Ok(())
                }
                Err(err) => {
                    let _ = response.send(Err(err));
                    Err(anyhow::anyhow!("cache stream failed"))
                }
            }
        }
        CacheRequest::Get {
            request_id,
            message,
            response,
        } => {
            let result =
                cache_round_trip(send, recv, message, sample, request_id, frame_scratch).await;
            match result {
                Ok(value) => {
                    let _ = response.send(Ok(value));
                    Ok(())
                }
                Err(err) => {
                    let _ = response.send(Err(err));
                    Err(anyhow::anyhow!("cache stream failed"))
                }
            }
        }
    }
}

async fn cache_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Bytes>> {
    // Encode -> write -> read -> decode in one stream round trip.
    let encode_start = crate::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = encode_start;
    let frame = message.encode().context("encode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = encode_start {
        let encode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_encode_ns(encode_ns);
    }
    let write_start = crate::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = write_start;
    write_frame_parts(send, &frame).await?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = write_start {
        let write_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_write_ns(write_ns);
    }
    let frame = match read_frame_cache_timed_into(recv, sample, frame_scratch).await? {
        Some(frame) => frame,
        None => return Err(anyhow::anyhow!("cache response closed")),
    };
    let decode_start = crate::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = decode_start;
    let response = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_decode_ns(decode_ns);
    }
    match response {
        Message::CacheOk {
            request_id: resp_id,
        } => {
            if resp_id != request_id {
                return Err(anyhow::anyhow!("cache put request id mismatch"));
            }
            Ok(None)
        }
        Message::Ok => Err(anyhow::anyhow!(
            "cache response missing request id (protocol violation)"
        )),
        Message::CacheValue {
            value,
            request_id: resp_id,
            ..
        } => {
            if let Some(resp_id) = resp_id
                && resp_id != request_id
            {
                return Err(anyhow::anyhow!("cache get request id mismatch"));
            }
            Ok(value)
        }
        Message::Error { message } => Err(anyhow::anyhow!("cache error: {message}")),
        other => Err(anyhow::anyhow!("cache response unexpected: {other:?}")),
    }
}
