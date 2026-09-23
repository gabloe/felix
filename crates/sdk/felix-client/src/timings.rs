#[cfg(feature = "telemetry")]
mod telemetry {
    include!("timings_telemetry.rs");
}

#[cfg(not(feature = "telemetry"))]
mod telemetry {
    pub type ClientTimingSamples = (
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
    );

    pub type ClientCacheTimingSamples = (
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
    );

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
}

pub use telemetry::*;

#[cfg(test)]
mod tests {
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
}
