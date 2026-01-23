#[cfg(feature = "telemetry")]
mod telemetry {
    include!("timings_telemetry.rs");
}

#[cfg(not(feature = "telemetry"))]
mod telemetry {
    pub type BrokerTimingSamples = (
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
        Vec<u64>,
    );

    pub type BrokerCacheTimingSamples = (
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

    pub fn record_decode_ns(_value: u64) {}
    pub fn record_fanout_ns(_value: u64) {}
    pub fn record_ack_write_ns(_value: u64) {}
    pub fn record_quic_write_ns(_value: u64) {}
    pub fn record_sub_queue_wait_ns(_value: u64) {}
    pub fn record_sub_write_ns(_value: u64) {}
    pub fn record_sub_delivery_ns(_value: u64) {}

    pub fn record_cache_encode_ns(_value: u64) {}
    pub fn record_cache_write_ns(_value: u64) {}
    pub fn record_cache_read_ns(_value: u64) {}
    pub fn record_cache_decode_ns(_value: u64) {}
    pub fn record_cache_insert_ns(_value: u64) {}
    pub fn record_cache_lookup_ns(_value: u64) {}

    pub fn take_samples() -> Option<BrokerTimingSamples> {
        None
    }

    pub fn take_cache_samples() -> Option<BrokerCacheTimingSamples> {
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
    fn should_sample_returns_false() {
        assert!(!should_sample());
    }

    #[test]
    fn record_functions_do_not_panic() {
        record_decode_ns(100);
        record_fanout_ns(200);
        record_ack_write_ns(300);
        record_quic_write_ns(400);
        record_sub_queue_wait_ns(500);
        record_sub_write_ns(600);
        record_sub_delivery_ns(700);
        record_cache_encode_ns(800);
        record_cache_write_ns(900);
        record_cache_read_ns(1000);
        record_cache_decode_ns(1100);
        record_cache_insert_ns(1200);
        record_cache_lookup_ns(1300);
    }

    #[test]
    fn take_samples_returns_none() {
        assert!(take_samples().is_none());
    }

    #[test]
    fn take_cache_samples_returns_none() {
        assert!(take_cache_samples().is_none());
    }
}
