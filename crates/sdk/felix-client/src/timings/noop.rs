//! The timings API without the `telemetry` feature: nothing is recorded.

use super::{ClientCacheTimingSamples, ClientTimingSamples};

pub fn enable_collection(_sample_every: usize) {}

pub fn set_enabled(_enabled: bool) {}

pub fn should_sample() -> bool {
    false
}

pub fn record_encode_ns(_value: u64) {}
pub fn record_binary_encode_ns(_value: u64) {}
pub fn record_text_encode_ns(_value: u64) {}
pub fn record_text_batch_build_ns(_value: u64) {}
pub fn record_publish_enqueue_wait_ns(_value: u64) {}
pub fn record_write_ns(_value: u64) {}
pub fn record_send_await_ns(_value: u64) {}
pub fn record_sub_read_wait_ns(_value: u64) {}
pub fn record_sub_read_await_ns(_value: u64) {}
pub fn record_sub_queue_wait_ns(_value: u64) {}
pub fn record_sub_decode_ns(_value: u64) {}
pub fn record_sub_dispatch_ns(_value: u64) {}
pub fn record_sub_consumer_gap_ns(_value: u64) {}
pub fn record_sub_poll_gap_ns(_value: u64) {}
pub fn record_sub_time_in_queue_ns(_value: u64) {}
pub fn record_sub_runtime_gap_ns(_value: u64) {}
pub fn record_sub_delivery_chan_wait_ns(_value: u64) {}
pub fn record_e2e_latency_ns(_value: u64) {}
pub fn record_ack_read_wait_ns(_value: u64) {}
pub fn record_ack_decode_ns(_value: u64) {}
pub fn record_cache_encode_ns(_value: u64) {}
pub fn record_cache_open_stream_ns(_value: u64) {}
pub fn record_cache_write_ns(_value: u64) {}
pub fn record_cache_finish_ns(_value: u64) {}
pub fn record_cache_read_wait_ns(_value: u64) {}
pub fn record_cache_read_drain_ns(_value: u64) {}
pub fn record_cache_decode_ns(_value: u64) {}
pub fn record_cache_validate_ns(_value: u64) {}

pub fn take_samples() -> Option<ClientTimingSamples> {
    None
}

pub fn take_cache_samples() -> Option<ClientCacheTimingSamples> {
    None
}
