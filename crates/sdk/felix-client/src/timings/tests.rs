#[cfg(feature = "telemetry")]
use serial_test::serial;

#[cfg(feature = "telemetry")]
use super::collector::reset_collector_for_tests;
use super::*;

#[test]
fn enable_collection_does_not_panic() {
    enable_collection(10);
}

#[test]
fn set_enabled_does_not_panic() {
    set_enabled(true);
    set_enabled(false);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn should_sample_returns_false() {
    assert!(!should_sample());
}

#[test]
fn record_functions_do_not_panic() {
    record_encode_ns(100);
    record_binary_encode_ns(200);
    record_text_encode_ns(300);
    record_text_batch_build_ns(400);
    record_publish_enqueue_wait_ns(500);
    record_write_ns(600);
    record_send_await_ns(700);
    record_sub_read_wait_ns(800);
    record_sub_read_await_ns(900);
    record_sub_queue_wait_ns(950);
    record_sub_decode_ns(1000);
    record_sub_dispatch_ns(1100);
    record_sub_consumer_gap_ns(1200);
    record_sub_poll_gap_ns(1250);
    record_sub_time_in_queue_ns(1275);
    record_sub_runtime_gap_ns(1280);
    record_sub_delivery_chan_wait_ns(1290);
    record_e2e_latency_ns(1300);
    record_ack_read_wait_ns(1400);
    record_ack_decode_ns(1500);
    record_cache_encode_ns(1600);
    record_cache_open_stream_ns(1700);
    record_cache_write_ns(1800);
    record_cache_finish_ns(1900);
    record_cache_read_wait_ns(2000);
    record_cache_read_drain_ns(2100);
    record_cache_decode_ns(2200);
    record_cache_validate_ns(2300);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn take_samples_returns_none() {
    assert!(take_samples().is_none());
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn take_cache_samples_returns_none() {
    assert!(take_cache_samples().is_none());
}

#[test]
#[serial]
#[cfg(feature = "telemetry")]
fn collector_none_paths() {
    reset_collector_for_tests();
    assert!(!should_sample());
    set_enabled(true);
    set_enabled(false);
    record_encode_ns(1);
    record_binary_encode_ns(2);
    record_text_encode_ns(3);
    record_text_batch_build_ns(4);
    record_publish_enqueue_wait_ns(5);
    record_write_ns(6);
    record_send_await_ns(7);
    record_sub_read_wait_ns(8);
    record_sub_read_await_ns(9);
    record_sub_queue_wait_ns(10);
    record_sub_decode_ns(11);
    record_sub_dispatch_ns(12);
    record_sub_consumer_gap_ns(13);
    record_sub_poll_gap_ns(131);
    record_sub_time_in_queue_ns(132);
    record_sub_runtime_gap_ns(133);
    record_sub_delivery_chan_wait_ns(134);
    record_e2e_latency_ns(14);
    record_ack_read_wait_ns(15);
    record_ack_decode_ns(16);
    record_cache_encode_ns(16);
    record_cache_open_stream_ns(17);
    record_cache_write_ns(18);
    record_cache_finish_ns(19);
    record_cache_read_wait_ns(20);
    record_cache_read_drain_ns(21);
    record_cache_decode_ns(22);
    record_cache_validate_ns(23);
    assert!(take_samples().is_none());
    assert!(take_cache_samples().is_none());
}

#[test]
#[serial]
#[cfg(feature = "telemetry")]
fn collector_records_and_samples() {
    reset_collector_for_tests();
    enable_collection(2);
    assert!(should_sample());
    assert!(!should_sample());
    set_enabled(false);
    assert!(!should_sample());
    set_enabled(true);
    assert!(should_sample());
    record_encode_ns(100);
    record_binary_encode_ns(200);
    record_text_encode_ns(300);
    record_text_batch_build_ns(400);
    record_publish_enqueue_wait_ns(500);
    record_write_ns(600);
    record_send_await_ns(700);
    record_sub_read_wait_ns(800);
    record_sub_read_await_ns(900);
    record_sub_queue_wait_ns(1000);
    record_sub_decode_ns(1100);
    record_sub_dispatch_ns(1200);
    record_sub_consumer_gap_ns(1300);
    record_sub_poll_gap_ns(1310);
    record_sub_time_in_queue_ns(1320);
    record_sub_runtime_gap_ns(1330);
    record_sub_delivery_chan_wait_ns(1340);
    record_e2e_latency_ns(1400);
    record_ack_read_wait_ns(1500);
    record_ack_decode_ns(1600);
    record_cache_encode_ns(1600);
    record_cache_open_stream_ns(1700);
    record_cache_write_ns(1800);
    record_cache_finish_ns(1900);
    record_cache_read_wait_ns(2000);
    record_cache_read_drain_ns(2100);
    record_cache_decode_ns(2200);
    record_cache_validate_ns(2300);
    let samples = take_samples().expect("samples");
    assert!(samples.0.contains(&500));
    assert!(samples.1.contains(&100));
    assert!(samples.2.contains(&200));
    assert!(samples.3.contains(&300));
    assert!(samples.4.contains(&400));
    assert!(samples.5.contains(&600));
    assert!(samples.6.contains(&700));
    assert!(samples.7.contains(&800));
    assert!(samples.8.contains(&900));
    assert!(samples.9.contains(&1000));
    assert!(samples.10.contains(&1100));
    assert!(samples.11.contains(&1200));
    assert!(samples.12.contains(&1300));
    assert!(samples.13.contains(&1310));
    assert!(samples.14.contains(&1320));
    assert!(samples.15.contains(&1330));
    assert!(samples.16.contains(&1340));
    assert!(samples.17.contains(&1400));
    assert!(samples.18.contains(&1500));
    assert!(samples.19.contains(&1600));
    let cache_samples = take_cache_samples().expect("cache samples");
    assert!(cache_samples.0.contains(&1600));
    assert!(cache_samples.1.contains(&1700));
    assert!(cache_samples.2.contains(&1800));
    assert!(cache_samples.3.contains(&1900));
    assert!(cache_samples.4.contains(&2000));
    assert!(cache_samples.5.contains(&2100));
    assert!(cache_samples.6.contains(&2200));
    assert!(cache_samples.7.contains(&2300));
}
