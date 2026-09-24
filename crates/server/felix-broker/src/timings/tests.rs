//! These tests drive the process-global timing collector. With the
//! `telemetry` feature on these are the real functions rather than the no-op
//! stubs: `enable_collection` resets `sample_every` and *clears every sample
//! vector*, and the record functions push into those same vectors.
//!
//! Unserialised they race the `#[serial]` tests in `timings/collector/tests.rs`,
//! wiping samples between a test recording them and reading them back, which
//! surfaces as `take_samples_clears_collected_data` asserting `0 == 1`.
//! `serial_test` only serialises against other `#[serial]` tests, so both
//! sides have to opt in — marking one side of a race is no protection.
//!
//! The window is narrow, so this shows up under coverage instrumentation
//! (slow enough to widen it) far more often than in a plain test run.

use serial_test::serial;

use super::*;

#[test]
#[serial]
fn enable_collection_does_not_panic() {
    enable_collection(10);
}

#[test]
#[serial]
fn set_enabled_does_not_panic() {
    set_enabled(true);
    set_enabled(false);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn should_sample_returns_false() {
    assert!(!should_sample());
}

#[test]
#[serial]
fn record_functions_do_not_panic() {
    record_lookup_ns(100);
    record_append_ns(200);
    record_fanout_ns(250);
    record_enqueue_ns(275);
    record_send_ns(300);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn take_samples_returns_none() {
    assert!(take_samples().is_none());
}
