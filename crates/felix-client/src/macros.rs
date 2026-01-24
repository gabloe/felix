// Metric macro wrappers and lightweight sampling helpers.
#[cfg(not(feature = "telemetry"))]
use std::time::Instant;

#[cfg(feature = "telemetry")]
use std::time::Instant;

#[cfg(feature = "telemetry")]
#[allow(unused_macros)]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        metrics::counter!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
#[allow(unused_macros)]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        $crate::macros::NoopCounter
    };
}

#[cfg(feature = "telemetry")]
#[allow(unused_macros)]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
#[allow(unused_macros)]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        $crate::macros::NoopHistogram
    };
}

#[cfg(feature = "telemetry")]
#[allow(unused_macros)]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        metrics::gauge!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
#[allow(unused_macros)]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        $crate::macros::NoopGauge
    };
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
pub(crate) struct NoopCounter;

#[cfg(not(feature = "telemetry"))]
impl NoopCounter {
    pub(crate) fn increment(&self, _value: u64) {}
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
pub(crate) struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
impl NoopHistogram {
    #[allow(dead_code)]
    pub(crate) fn record(&self, _value: f64) {}
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
pub(crate) struct NoopGauge;

#[cfg(not(feature = "telemetry"))]
impl NoopGauge {
    pub(crate) fn set(&self, _value: f64) {}
}

#[cfg(feature = "telemetry")]
#[inline]
pub(crate) fn t_should_sample() -> bool {
    crate::timings::should_sample()
}

#[cfg(not(feature = "telemetry"))]
#[inline]
pub(crate) fn t_should_sample() -> bool {
    false
}

#[cfg(feature = "telemetry")]
#[inline]
pub(crate) fn t_now_if(sample: bool) -> Option<Instant> {
    sample.then(Instant::now)
}

#[cfg(not(feature = "telemetry"))]
#[inline]
pub(crate) fn t_now_if(_sample: bool) -> Option<Instant> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
