//! Metric macros that become `metrics` calls with the `telemetry` feature and
//! no-ops without it, so call sites need no `cfg` of their own.

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
        $crate::telemetry::macros::NoopCounter
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
        $crate::telemetry::macros::NoopHistogram
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
        $crate::telemetry::macros::NoopGauge
    };
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
pub(crate) struct NoopCounter;

#[cfg(not(feature = "telemetry"))]
impl NoopCounter {
    pub(crate) fn increment(&self, _value: u64) {}
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
#[allow(dead_code)]
pub(crate) struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
impl NoopHistogram {
    pub(crate) fn record(&self, _value: f64) {}
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
pub(crate) struct NoopGauge;

#[cfg(not(feature = "telemetry"))]
impl NoopGauge {
    pub(crate) fn set(&self, _value: f64) {}
}
