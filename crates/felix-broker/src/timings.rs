#[cfg(feature = "telemetry")]
mod telemetry {
    include!("timings_telemetry.rs");
}

#[cfg(not(feature = "telemetry"))]
mod telemetry {
    pub type BrokerPublishSamples = (Vec<u64>, Vec<u64>, Vec<u64>);

    pub fn enable_collection(_sample_every: usize) {}

    pub fn set_enabled(_enabled: bool) {}

    pub fn should_sample() -> bool {
        false
    }

    pub fn record_lookup_ns(_value: u64) {}
    pub fn record_append_ns(_value: u64) {}
    pub fn record_send_ns(_value: u64) {}

    pub fn take_samples() -> Option<BrokerPublishSamples> {
        None
    }
}

pub use telemetry::*;
