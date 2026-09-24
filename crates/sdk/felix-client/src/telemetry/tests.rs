use std::sync::Mutex;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use bytes::Bytes;
use felix_wire::{Frame, FrameHeader};

#[cfg(not(feature = "telemetry"))]
use super::macros::{NoopCounter, NoopGauge, NoopHistogram};
use super::*;

static COUNTER_TEST_GUARD: Mutex<()> = Mutex::new(());

#[test]
#[cfg(not(feature = "telemetry"))]
fn t_should_sample_returns_false() {
    assert!(!t_should_sample());
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn t_now_if_returns_none() {
    assert!(t_now_if(true).is_none());
    assert!(t_now_if(false).is_none());
}

#[test]
#[cfg(feature = "telemetry")]
fn t_now_if_with_telemetry() {
    // When telemetry is enabled, t_now_if(true) returns Some
    assert!(t_now_if(true).is_some());
    // and t_now_if(false) returns None
    assert!(t_now_if(false).is_none());
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn noop_counter_does_not_panic() {
    let counter = NoopCounter;
    counter.increment(100);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn noop_histogram_does_not_panic() {
    let histogram = NoopHistogram;
    histogram.record(100.0);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn noop_gauge_does_not_panic() {
    let gauge = NoopGauge;
    gauge.set(50.0);
}

#[test]
fn frame_counters_snapshot_returns_values() {
    let _guard = COUNTER_TEST_GUARD.lock().expect("counter guard");
    reset_frame_counters();
    let snapshot = frame_counters_snapshot();
    #[cfg(not(feature = "telemetry"))]
    {
        // Just verify it returns a snapshot (all zeros in non-telemetry mode).
        assert_eq!(snapshot.frames_in_ok, 0);
        assert_eq!(snapshot.frames_in_err, 0);
        assert_eq!(snapshot.frames_out_ok, 0);
    }
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.fetch_add(2, Ordering::Relaxed);
        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
        counters.frames_out_ok.fetch_add(3, Ordering::Relaxed);
        let updated = frame_counters_snapshot();
        assert!(updated.frames_in_ok >= snapshot.frames_in_ok + 2);
        assert!(updated.frames_in_err > snapshot.frames_in_err);
        assert!(updated.frames_out_ok >= snapshot.frames_out_ok + 3);
    }
}

#[test]
fn reset_frame_counters_does_not_panic() {
    let _guard = COUNTER_TEST_GUARD.lock().expect("counter guard");
    reset_frame_counters();
    // Just ensure it doesn't panic
}

#[test]
fn frame_counters_snapshot_has_all_fields() {
    let snapshot = frame_counters_snapshot();
    // Verify all fields are accessible
    let _ = snapshot.frames_in_ok;
    let _ = snapshot.frames_in_err;
    let _ = snapshot.frames_out_ok;
    let _ = snapshot.bytes_in;
    let _ = snapshot.bytes_out;
    let _ = snapshot.pub_frames_out_ok;
    let _ = snapshot.pub_frames_out_err;
    let _ = snapshot.sub_frames_in_ok;
    let _ = snapshot.ack_frames_in_ok;
    let _ = snapshot.pub_items_out_ok;
    let _ = snapshot.pub_items_out_err;
    let _ = snapshot.pub_batches_out_ok;
    let _ = snapshot.pub_batches_out_err;
    let _ = snapshot.sub_items_in_ok;
    let _ = snapshot.sub_batches_in_ok;
    let _ = snapshot.ack_items_in_ok;
    let _ = snapshot.binary_encode_reallocs;
    let _ = snapshot.text_encode_reallocs;
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn maybe_append_publish_ts_returns_unchanged() {
    let payload = vec![1, 2, 3, 4];
    let result = maybe_append_publish_ts(payload.clone(), true);
    assert_eq!(result, payload);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn maybe_append_publish_ts_batch_returns_unchanged() {
    let payloads = vec![vec![1, 2], vec![3, 4]];
    let result = maybe_append_publish_ts_batch(payloads.clone(), true);
    assert_eq!(result, payloads);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn record_e2e_latency_does_not_panic() {
    let payload = Bytes::from_static(b"test payload");
    record_e2e_latency(&payload);
}

#[test]
#[cfg(feature = "telemetry")]
fn maybe_append_publish_ts_appends() {
    let payload = vec![1, 2, 3];
    let result = maybe_append_publish_ts(payload.clone(), true);
    assert_eq!(&result[..payload.len()], payload.as_slice());
    assert_eq!(result.len(), payload.len() + 8);
    let _ts = u64::from_le_bytes(result[result.len() - 8..].try_into().expect("ts"));
}

#[test]
#[cfg(feature = "telemetry")]
fn maybe_append_publish_ts_batch_appends() {
    let payloads = vec![vec![1, 2], vec![3, 4, 5]];
    let result = maybe_append_publish_ts_batch(payloads.clone(), true);
    assert_eq!(result.len(), payloads.len());
    assert_eq!(result[0].len(), payloads[0].len() + 8);
    assert_eq!(result[1].len(), payloads[1].len() + 8);
}

#[test]
#[cfg(feature = "telemetry")]
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

#[test]
fn log_decode_error_does_not_panic() {
    let frame = Frame {
        header: FrameHeader {
            magic: 0x42,
            version: 1,
            flags: 0,
            length: 10,
        },
        payload: Bytes::from_static(b"test error"),
    };
    let err = anyhow::anyhow!("test error");
    log_decode_error("test_context", &err, &frame);
}

#[test]
fn log_decode_error_with_non_printable_bytes() {
    let frame = Frame {
        header: FrameHeader {
            magic: 0x42,
            version: 1,
            flags: 0,
            length: 10,
        },
        payload: Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, b'A', b'B', b'C']),
    };
    let err = anyhow::anyhow!("test error");
    log_decode_error("binary_data", &err, &frame);
}

#[test]
fn log_decode_error_with_large_payload() {
    let mut data = vec![b'X'; 100];
    data[50] = 0x00; // Add non-printable char
    let frame = Frame {
        header: FrameHeader {
            magic: 0x42,
            version: 1,
            flags: 0,
            length: 100,
        },
        payload: Bytes::from(data),
    };
    let err = anyhow::anyhow!("test error");
    log_decode_error("large_payload", &err, &frame);
}
