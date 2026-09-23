//! Benchmark timestamps: a publish can carry its send time in the last eight
//! bytes of the payload, and a subscriber turns that into an end-to-end
//! latency sample. Only with `bench_embed_ts` and the `telemetry` feature.

#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
#[cfg(feature = "telemetry")]
use std::time::Instant;

use bytes::Bytes;

#[cfg(feature = "telemetry")]
use crate::timings;

#[cfg(feature = "telemetry")]
fn bench_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

#[cfg(feature = "telemetry")]
pub(crate) fn maybe_append_publish_ts(mut payload: Vec<u8>, enabled: bool) -> Vec<u8> {
    if !enabled {
        return payload;
    }
    let ts = bench_now_ns().to_le_bytes();
    payload.extend_from_slice(&ts);
    payload
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn maybe_append_publish_ts(payload: Vec<u8>, _enabled: bool) -> Vec<u8> {
    payload
}

#[cfg(feature = "telemetry")]
pub(crate) fn maybe_append_publish_ts_batch(payloads: Vec<Vec<u8>>, enabled: bool) -> Vec<Vec<u8>> {
    if !enabled {
        return payloads;
    }
    payloads
        .into_iter()
        .map(|payload| maybe_append_publish_ts(payload, true))
        .collect()
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn maybe_append_publish_ts_batch(
    payloads: Vec<Vec<u8>>,
    _enabled: bool,
) -> Vec<Vec<u8>> {
    payloads
}

#[cfg(feature = "telemetry")]
pub(crate) fn record_e2e_latency(payload: &Bytes, enabled: bool) {
    if !enabled || payload.len() < std::mem::size_of::<u64>() {
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
