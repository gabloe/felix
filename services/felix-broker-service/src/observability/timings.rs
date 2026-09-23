//! Opt-in per-stage latency sampling for the hot paths, for demos and perf
//! work rather than production metrics.
//!
//! With the `telemetry` feature the collector records samples once it is
//! enabled. Without it every function here is a no-op and the `take_*`
//! functions return `None`, so call sites need no feature gates.

#[cfg(feature = "telemetry")]
mod collector;

#[cfg(not(feature = "telemetry"))]
mod collector {
    pub type BrokerTimingSamples = (
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
    pub fn record_sub_prefix_ns(_value: u64) {}
    pub fn record_sub_write_ns(_value: u64) {}
    pub fn record_sub_write_await_ns(_value: u64) {}
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

pub use collector::*;

#[cfg(test)]
mod tests;
