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

#[cfg(all(test, not(feature = "telemetry")))]
mod tests {
    use super::*;

    #[test]
    fn bench_embed_ts_enabled_returns_false() {
        assert!(!bench_embed_ts_enabled());
    }

    #[test]
    fn maybe_append_publish_ts_returns_unchanged() {
        let payload = vec![1, 2, 3, 4];
        let result = maybe_append_publish_ts(payload.clone());
        assert_eq!(result, payload);
    }

    #[test]
    fn maybe_append_publish_ts_batch_returns_unchanged() {
        let payloads = vec![vec![1, 2], vec![3, 4]];
        let result = maybe_append_publish_ts_batch(payloads.clone());
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
    use serial_test::serial;

    use crate::config::{
        ClientRuntimeConfig, DEFAULT_EVENT_ROUTER_MAX_PENDING, DEFAULT_MAX_FRAME_BYTES,
        install_runtime_config_for_tests, reset_runtime_config_for_tests,
    };

    fn install_bench_config(enabled: bool) {
        reset_runtime_config_for_tests();
        install_runtime_config_for_tests(ClientRuntimeConfig {
            event_router_max_pending: DEFAULT_EVENT_ROUTER_MAX_PENDING,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            bench_embed_ts: enabled,
        });
    }

    #[test]
    #[serial]
    fn bench_embed_ts_enabled_true() {
        install_bench_config(true);
        assert!(bench_embed_ts_enabled());
    }

    #[test]
    #[serial]
    fn maybe_append_publish_ts_appends() {
        install_bench_config(true);
        let payload = vec![1, 2, 3];
        let result = maybe_append_publish_ts(payload.clone());
        assert_eq!(&result[..payload.len()], payload.as_slice());
        assert_eq!(result.len(), payload.len() + 8);
        let _ts = u64::from_le_bytes(result[result.len() - 8..].try_into().expect("ts"));
    }

    #[test]
    #[serial]
    fn maybe_append_publish_ts_batch_appends() {
        install_bench_config(true);
        let payloads = vec![vec![1, 2], vec![3, 4, 5]];
        let result = maybe_append_publish_ts_batch(payloads.clone());
        assert_eq!(result.len(), payloads.len());
        assert_eq!(result[0].len(), payloads[0].len() + 8);
        assert_eq!(result[1].len(), payloads[1].len() + 8);
    }

    #[test]
    #[serial]
    fn record_e2e_latency_paths() {
        install_bench_config(true);
        crate::timings::enable_collection(1);
        let payload = maybe_append_publish_ts(vec![9, 9]);
        record_e2e_latency(&Bytes::from(payload));
        let short = Bytes::from_static(b"short");
        record_e2e_latency(&short);
        let mut future = vec![0u8; 8];
        future.copy_from_slice(&u64::MAX.to_le_bytes());
        record_e2e_latency(&Bytes::from(future));

        install_bench_config(false);
        let disabled_payload = vec![1, 2, 3];
        let disabled = Bytes::from_static(b"disabled");
        assert_eq!(
            maybe_append_publish_ts(disabled_payload.clone()),
            disabled_payload
        );
        assert_eq!(
            maybe_append_publish_ts_batch(vec![disabled_payload.clone()]),
            vec![disabled_payload]
        );
        record_e2e_latency(&disabled);
    }
}
