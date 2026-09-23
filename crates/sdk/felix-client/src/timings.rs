//! Sampled per-operation timings, for benchmarks and latency investigations.
//!
//! [`enable_collection`] turns sampling on, every `sample_every`th operation
//! records how long each stage took, and [`take_samples`] and
//! [`take_cache_samples`] drain what was collected. Without the `telemetry`
//! feature every function here is a no-op and the takes return `None`, so
//! callers need no `cfg` of their own.

#[cfg(feature = "telemetry")]
mod collector;
#[cfg(not(feature = "telemetry"))]
mod noop;

#[cfg(feature = "telemetry")]
pub use collector::*;
#[cfg(not(feature = "telemetry"))]
pub use noop::*;

/// Publish and subscribe samples, in nanoseconds, one `Vec` per stage:
/// publish enqueue wait, encode, binary encode, text encode, text batch
/// build, write, send await, subscriber read wait, read await, queue wait,
/// decode, dispatch, consumer gap, poll gap, time in queue, runtime gap,
/// delivery channel wait, end-to-end latency, ack read wait, ack decode.
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

/// Cache samples, in nanoseconds, one `Vec` per stage: encode, open
/// stream, write, finish, read wait, read drain, decode, validate.
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

#[cfg(test)]
mod tests;
