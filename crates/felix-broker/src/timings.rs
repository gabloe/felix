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
        record_lookup_ns(100);
        record_append_ns(200);
        record_send_ns(300);
    }

    #[test]
    fn take_samples_returns_none() {
        assert!(take_samples().is_none());
    }
}
