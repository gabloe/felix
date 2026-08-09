// Telemetry shims for the broker hot path.
//
// Under `--features telemetry` these forward to the `metrics` crate; otherwise
// they compile down to no-ops so the publish path pays nothing for sampling.

use std::time::Instant;

#[cfg(feature = "telemetry")]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        $crate::telemetry::NoopHistogram
    };
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
pub(crate) struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
impl NoopHistogram {
    pub(crate) fn record(&self, _value: f64) {}
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
