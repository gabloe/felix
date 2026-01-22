// Telemetry and metrics helpers for the QUIC transport adapter.
use felix_wire::Frame;
use std::time::Instant;

#[cfg(feature = "telemetry")]
use super::{DECODE_ERROR_LOG_LIMIT, DECODE_ERROR_LOGS};
#[cfg(feature = "telemetry")]
use crate::timings;
#[cfg(feature = "telemetry")]
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
#[cfg(feature = "telemetry")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "telemetry")]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        metrics::counter!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        $crate::transport::quic::telemetry::NoopCounter
    };
}

#[cfg(feature = "telemetry")]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        $crate::transport::quic::telemetry::NoopHistogram
    };
}

#[cfg(feature = "telemetry")]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        metrics::gauge!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        $crate::transport::quic::telemetry::NoopGauge
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
pub(crate) struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
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

#[cfg(feature = "telemetry")]
#[inline]
pub(crate) fn t_should_sample() -> bool {
    timings::should_sample()
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

#[cfg(feature = "telemetry")]
pub(crate) type TelemetryInstant = Instant;

#[cfg(not(feature = "telemetry"))]
pub(crate) type TelemetryInstant = u64;

#[cfg(feature = "telemetry")]
#[inline]
pub(crate) fn t_instant_now() -> TelemetryInstant {
    Instant::now()
}

#[cfg(not(feature = "telemetry"))]
#[inline]
pub(crate) fn t_instant_now() -> TelemetryInstant {
    0
}

#[inline]
pub(crate) fn t_consume_instant(_instant: TelemetryInstant) {}

#[cfg(feature = "telemetry")]
#[derive(Default)]
pub(crate) struct FrameCounters {
    pub(crate) frames_in_ok: AtomicU64,
    pub(crate) frames_in_err: AtomicU64,
    pub(crate) frames_out_ok: AtomicU64,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) pub_frames_in_ok: AtomicU64,
    pub(crate) pub_frames_in_err: AtomicU64,
    pub(crate) pub_items_in_ok: AtomicU64,
    pub(crate) pub_items_in_err: AtomicU64,
    pub(crate) pub_batches_in_ok: AtomicU64,
    pub(crate) pub_batches_in_err: AtomicU64,
    pub(crate) ack_frames_out_ok: AtomicU64,
    pub(crate) ack_items_out_ok: AtomicU64,
    pub(crate) sub_frames_out_ok: AtomicU64,
    pub(crate) sub_items_out_ok: AtomicU64,
    pub(crate) sub_batches_out_ok: AtomicU64,
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Default)]
struct FrameCounters;

#[derive(Debug, Clone)]
pub struct FrameCountersSnapshot {
    pub frames_in_ok: u64,
    pub frames_in_err: u64,
    pub frames_out_ok: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub pub_frames_in_ok: u64,
    pub pub_frames_in_err: u64,
    pub pub_items_in_ok: u64,
    pub pub_items_in_err: u64,
    pub pub_batches_in_ok: u64,
    pub pub_batches_in_err: u64,
    pub ack_frames_out_ok: u64,
    pub ack_items_out_ok: u64,
    pub sub_frames_out_ok: u64,
    pub sub_items_out_ok: u64,
    pub sub_batches_out_ok: u64,
}

#[cfg(feature = "telemetry")]
static FRAME_COUNTERS: OnceLock<Arc<FrameCounters>> = OnceLock::new();

#[cfg(feature = "telemetry")]
pub(crate) fn frame_counters() -> Arc<FrameCounters> {
    FRAME_COUNTERS
        .get_or_init(|| Arc::new(FrameCounters::default()))
        .clone()
}

#[cfg(feature = "telemetry")]
pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    let counters = frame_counters();
    FrameCountersSnapshot {
        frames_in_ok: counters.frames_in_ok.load(Ordering::Relaxed),
        frames_in_err: counters.frames_in_err.load(Ordering::Relaxed),
        frames_out_ok: counters.frames_out_ok.load(Ordering::Relaxed),
        bytes_in: counters.bytes_in.load(Ordering::Relaxed),
        bytes_out: counters.bytes_out.load(Ordering::Relaxed),
        pub_frames_in_ok: counters.pub_frames_in_ok.load(Ordering::Relaxed),
        pub_frames_in_err: counters.pub_frames_in_err.load(Ordering::Relaxed),
        pub_items_in_ok: counters.pub_items_in_ok.load(Ordering::Relaxed),
        pub_items_in_err: counters.pub_items_in_err.load(Ordering::Relaxed),
        pub_batches_in_ok: counters.pub_batches_in_ok.load(Ordering::Relaxed),
        pub_batches_in_err: counters.pub_batches_in_err.load(Ordering::Relaxed),
        ack_frames_out_ok: counters.ack_frames_out_ok.load(Ordering::Relaxed),
        ack_items_out_ok: counters.ack_items_out_ok.load(Ordering::Relaxed),
        sub_frames_out_ok: counters.sub_frames_out_ok.load(Ordering::Relaxed),
        sub_items_out_ok: counters.sub_items_out_ok.load(Ordering::Relaxed),
        sub_batches_out_ok: counters.sub_batches_out_ok.load(Ordering::Relaxed),
    }
}

#[cfg(not(feature = "telemetry"))]
pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    FrameCountersSnapshot {
        frames_in_ok: 0,
        frames_in_err: 0,
        frames_out_ok: 0,
        bytes_in: 0,
        bytes_out: 0,
        pub_frames_in_ok: 0,
        pub_frames_in_err: 0,
        pub_items_in_ok: 0,
        pub_items_in_err: 0,
        pub_batches_in_ok: 0,
        pub_batches_in_err: 0,
        ack_frames_out_ok: 0,
        ack_items_out_ok: 0,
        sub_frames_out_ok: 0,
        sub_items_out_ok: 0,
        sub_batches_out_ok: 0,
    }
}

#[cfg(feature = "telemetry")]
pub fn reset_frame_counters() {
    let counters = frame_counters();
    counters.frames_in_ok.store(0, Ordering::Relaxed);
    counters.frames_in_err.store(0, Ordering::Relaxed);
    counters.frames_out_ok.store(0, Ordering::Relaxed);
    counters.bytes_in.store(0, Ordering::Relaxed);
    counters.bytes_out.store(0, Ordering::Relaxed);
    counters.pub_frames_in_ok.store(0, Ordering::Relaxed);
    counters.pub_frames_in_err.store(0, Ordering::Relaxed);
    counters.pub_items_in_ok.store(0, Ordering::Relaxed);
    counters.pub_items_in_err.store(0, Ordering::Relaxed);
    counters.pub_batches_in_ok.store(0, Ordering::Relaxed);
    counters.pub_batches_in_err.store(0, Ordering::Relaxed);
    counters.ack_frames_out_ok.store(0, Ordering::Relaxed);
    counters.ack_items_out_ok.store(0, Ordering::Relaxed);
    counters.sub_frames_out_ok.store(0, Ordering::Relaxed);
    counters.sub_items_out_ok.store(0, Ordering::Relaxed);
    counters.sub_batches_out_ok.store(0, Ordering::Relaxed);
}

#[cfg(not(feature = "telemetry"))]
pub fn reset_frame_counters() {}

#[cfg(feature = "telemetry")]
pub(crate) fn log_decode_error(context: &str, err: &anyhow::Error, frame: &Frame) {
    let count = DECODE_ERROR_LOGS.fetch_add(1, Ordering::Relaxed);
    if count >= DECODE_ERROR_LOG_LIMIT {
        return;
    }
    let preview_len = frame.payload.len().min(64);
    let preview = &frame.payload[..preview_len];
    let hex = preview
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let printable = preview
        .iter()
        .map(|b| {
            let c = *b as char;
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '.'
            }
        })
        .collect::<String>();
    tracing::warn!(
        error = %err,
        context,
        frame_len = frame.header.length,
        payload_len = frame.payload.len(),
        preview_hex = %hex,
        preview_printable = %printable,
        "frame decode error"
    );
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn log_decode_error(_context: &str, _err: &anyhow::Error, _frame: &Frame) {}

pub(crate) use t_counter;
pub(crate) use t_histogram;
