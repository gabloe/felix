use super::*;
use anyhow::anyhow;
use bytes::Bytes;
use felix_wire::Frame;

#[test]
fn helpers_return_noop_defaults_when_telemetry_disabled() {
    #[cfg(not(feature = "telemetry"))]
    {
        assert!(!t_should_sample());
        assert_eq!(t_now_if(true), None);
        assert_eq!(t_now_if(false), None);
        let instant = t_instant_now();
        t_consume_instant(instant);
        assert_eq!(instant, 0);
    }
    #[cfg(feature = "telemetry")]
    {
        t_should_sample();
        t_now_if(true);
        t_now_if(false);
        let instant = t_instant_now();
        t_consume_instant(instant);
    }
}

#[test]
fn metrics_macros_return_noop_structs() {
    let counter = t_counter!("felix_test_counter", "result" => "ok");
    counter.increment(1);
    let histogram = t_histogram!("felix_test_histogram");
    histogram.record(1.5);
    let gauge = t_gauge!("felix_test_gauge");
    gauge.set(3.0);
}

#[test]
fn reset_and_snapshot_are_zeroed() {
    reset_frame_counters();
    let snapshot = frame_counters_snapshot();
    assert_eq!(snapshot.frames_in_ok, 0);
    assert_eq!(snapshot.frames_out_ok, 0);
    assert_eq!(snapshot.bytes_in, 0);
    assert_eq!(snapshot.bytes_out, 0);
    assert_eq!(snapshot.pub_frames_in_ok, 0);
    assert_eq!(snapshot.pub_items_in_ok, 0);
    assert_eq!(snapshot.pub_batches_in_ok, 0);
    assert_eq!(snapshot.ack_frames_out_ok, 0);
    assert_eq!(snapshot.sub_frames_out_ok, 0);
    assert_eq!(snapshot.sub_items_out_ok, 0);
    assert_eq!(snapshot.sub_batches_out_ok, 0);
}

#[test]
fn log_decode_error_noops_without_telemetry() {
    let payload = Bytes::from_static(b"payload");
    let frame = Frame::new(0, payload).expect("create frame");
    log_decode_error("context", &anyhow!("boom"), &frame);
}
