//! Client-side measurement: the `t_*` metric macros, frame counters, and the
//! sampling helpers hot paths call before timing anything.
//!
//! Almost all of it compiles to nothing without the `telemetry` feature. The
//! exception is [`publishes_forwarded`], which is always on because it answers
//! a question worth asking of any build.

// First, and `#[macro_use]`, so the macros are in scope for the rest of the
// crate. See lib.rs.
#[macro_use]
pub(crate) mod macros;
mod bench_ts;
mod decode_log;
mod frame_counters;

pub use frame_counters::{
    FrameCountersSnapshot, frame_counters_snapshot, publishes_forwarded, reset_frame_counters,
};

pub(crate) use bench_ts::{
    maybe_append_publish_ts, maybe_append_publish_ts_batch, record_e2e_latency,
};
pub(crate) use decode_log::log_decode_error;
#[cfg(feature = "telemetry")]
pub(crate) use frame_counters::frame_counters;
pub(crate) use frame_counters::record_publish_forwarded;

use std::time::Instant;

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
mod tests;
