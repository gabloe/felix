// Timestamp helpers for embedded publish timing and e2e latency sampling.
use bytes::Bytes;
#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
#[cfg(feature = "telemetry")]
use std::time::Instant;

#[cfg(feature = "telemetry")]
use crate::timings;

#[cfg(feature = "telemetry")]
pub(crate) fn bench_embed_ts_enabled() -> bool {
    crate::config::runtime_config().bench_embed_ts
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn bench_embed_ts_enabled() -> bool {
    false
}

#[cfg(feature = "telemetry")]
fn bench_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

#[cfg(feature = "telemetry")]
pub(crate) fn maybe_append_publish_ts(mut payload: Vec<u8>) -> Vec<u8> {
    if !bench_embed_ts_enabled() {
        return payload;
    }
    let ts = bench_now_ns().to_le_bytes();
    payload.extend_from_slice(&ts);
    payload
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn maybe_append_publish_ts(payload: Vec<u8>) -> Vec<u8> {
    payload
}

#[cfg(feature = "telemetry")]
pub(crate) fn maybe_append_publish_ts_batch(payloads: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    if !bench_embed_ts_enabled() {
        return payloads;
    }
    payloads.into_iter().map(maybe_append_publish_ts).collect()
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn maybe_append_publish_ts_batch(payloads: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    payloads
}

#[cfg(feature = "telemetry")]
pub(crate) fn record_e2e_latency(payload: &Bytes) {
    if !bench_embed_ts_enabled() || payload.len() < std::mem::size_of::<u64>() {
        return;
    }
    let mut ts_bytes = [0u8; 8];
    let start = payload.len() - std::mem::size_of::<u64>();
    ts_bytes.copy_from_slice(&payload[start..]);
    let publish_ts = u64::from_le_bytes(ts_bytes);
    let now = bench_now_ns();
    if now >= publish_ts {
        let delta = now - publish_ts;
        t_histogram!("client_e2e_latency_ns").record(delta as f64);
        timings::record_e2e_latency_ns(delta);
    }
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn record_e2e_latency(_payload: &Bytes) {}
