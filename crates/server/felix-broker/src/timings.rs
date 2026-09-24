//! Per-stage timings of the publish path, sampled for benchmarks.
//!
//! With `--features telemetry` samples are collected in process-global
//! vectors; without it every function here is a no-op, so the publish path
//! pays nothing.

#[cfg(feature = "telemetry")]
mod collector;
#[cfg(not(feature = "telemetry"))]
mod noop;

#[cfg(feature = "telemetry")]
pub use collector::*;
#[cfg(not(feature = "telemetry"))]
pub use noop::*;

#[cfg(test)]
mod tests;
