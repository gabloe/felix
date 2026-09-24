//! The collector's API with nothing behind it, so callers need no `cfg` of
//! their own when `telemetry` is off.

pub type BrokerPublishSamples = (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>);

pub fn enable_collection(_sample_every: usize) {}

pub fn set_enabled(_enabled: bool) {}

pub fn should_sample() -> bool {
    false
}

pub fn record_lookup_ns(_value: u64) {}
pub fn record_append_ns(_value: u64) {}
pub fn record_fanout_ns(_value: u64) {}
pub fn record_enqueue_ns(_value: u64) {}
pub fn record_send_ns(_value: u64) {}

pub fn take_samples() -> Option<BrokerPublishSamples> {
    None
}
