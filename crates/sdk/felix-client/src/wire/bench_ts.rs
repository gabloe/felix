// Timestamp helpers for embedded publish timing and e2e latency sampling.
use bytes::Bytes;
#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
#[cfg(feature = "telemetry")]
use std::time::Instant;

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

#[cfg(all(test, not(feature = "telemetry")))]
mod tests {
    use super::*;

    #[test]
    fn maybe_append_publish_ts_returns_unchanged() {
        let payload = vec![1, 2, 3, 4];
        let result = maybe_append_publish_ts(payload.clone(), true);
        assert_eq!(result, payload);
    }

    #[test]
    fn maybe_append_publish_ts_batch_returns_unchanged() {
        let payloads = vec![vec![1, 2], vec![3, 4]];
        let result = maybe_append_publish_ts_batch(payloads.clone(), true);
        assert_eq!(result, payloads);
    }

    #[test]
    fn record_e2e_latency_does_not_panic() {
        let payload = Bytes::from_static(b"test payload");
        record_e2e_latency(&payload);
    }
}

#[cfg(all(test, feature = "telemetry"))]
mod telemetry_tests {
    use super::*;

    #[test]
    fn maybe_append_publish_ts_appends() {
        let payload = vec![1, 2, 3];
        let result = maybe_append_publish_ts(payload.clone(), true);
        assert_eq!(&result[..payload.len()], payload.as_slice());
        assert_eq!(result.len(), payload.len() + 8);
        let _ts = u64::from_le_bytes(result[result.len() - 8..].try_into().expect("ts"));
    }

    #[test]
    fn maybe_append_publish_ts_batch_appends() {
        let payloads = vec![vec![1, 2], vec![3, 4, 5]];
        let result = maybe_append_publish_ts_batch(payloads.clone(), true);
        assert_eq!(result.len(), payloads.len());
        assert_eq!(result[0].len(), payloads[0].len() + 8);
        assert_eq!(result[1].len(), payloads[1].len() + 8);
    }

    #[test]
    fn record_e2e_latency_paths() {
        crate::timings::enable_collection(1);
        let payload = maybe_append_publish_ts(vec![9, 9], true);
        record_e2e_latency(&Bytes::from(payload), true);
        let short = Bytes::from_static(b"short");
        record_e2e_latency(&short, true);
        let mut future = vec![0u8; 8];
        future.copy_from_slice(&u64::MAX.to_le_bytes());
        record_e2e_latency(&Bytes::from(future), true);

        let disabled_payload = vec![1, 2, 3];
        let disabled = Bytes::from_static(b"disabled");
        assert_eq!(
            maybe_append_publish_ts(disabled_payload.clone(), false),
            disabled_payload
        );
        assert_eq!(
            maybe_append_publish_ts_batch(vec![disabled_payload.clone()], false),
            vec![disabled_payload]
        );
        record_e2e_latency(&disabled, false);
    }
}
