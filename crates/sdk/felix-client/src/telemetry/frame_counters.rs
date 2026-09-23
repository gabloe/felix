//! Frame and byte counters for the client's streams.

#[cfg(feature = "telemetry")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "telemetry")]
static FRAME_COUNTERS: std::sync::OnceLock<FrameCounters> = std::sync::OnceLock::new();

/// Publishes the broker forwarded, as reported on their acks.
///
/// **Always on**, unlike the frame counters, which are behind `telemetry`. One
/// relaxed increment per *forwarded* publish -- not per publish -- and it is the
/// answer to "am I paying the forwarding tax", which is a question worth being
/// able to ask of a build that was not compiled for measurement. A client that
/// cannot answer it is exactly the blindness the hint was added to remove
/// (#536).
///
/// Non-zero means this client is publishing to a broker that does not own the
/// shard, and each of those records is decrypted, re-encrypted and decrypted
/// again on the way -- roughly half the throughput per core. Zero means either
/// the connections are landing on the owners, or the broker predates the hint.
static PUBLISHES_FORWARDED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Totals of the client's frame and byte counters at one moment.
///
/// All zero unless the `telemetry` feature is on.
#[derive(Debug, Clone)]
pub struct FrameCountersSnapshot {
    pub frames_in_ok: u64,
    pub frames_in_err: u64,
    pub frames_out_ok: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub pub_frames_out_ok: u64,
    pub pub_frames_out_err: u64,
    pub sub_frames_in_ok: u64,
    pub ack_frames_in_ok: u64,
    pub pub_items_out_ok: u64,
    pub pub_items_out_err: u64,
    pub pub_batches_out_ok: u64,
    pub pub_batches_out_err: u64,
    pub sub_items_in_ok: u64,
    pub sub_batches_in_ok: u64,
    pub ack_items_in_ok: u64,
    pub binary_encode_reallocs: u64,
    pub text_encode_reallocs: u64,
}

#[cfg(feature = "telemetry")]
#[derive(Default)]
pub(crate) struct FrameCounters {
    pub(crate) frames_in_ok: AtomicU64,
    pub(crate) frames_in_err: AtomicU64,
    pub(crate) frames_out_ok: AtomicU64,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) pub_frames_out_ok: AtomicU64,
    pub(crate) pub_frames_out_err: AtomicU64,
    pub(crate) pub_items_out_ok: AtomicU64,
    pub(crate) pub_items_out_err: AtomicU64,
    pub(crate) pub_batches_out_ok: AtomicU64,
    pub(crate) pub_batches_out_err: AtomicU64,
    pub(crate) sub_frames_in_ok: AtomicU64,
    pub(crate) sub_items_in_ok: AtomicU64,
    pub(crate) sub_batches_in_ok: AtomicU64,
    pub(crate) ack_frames_in_ok: AtomicU64,
    pub(crate) ack_items_in_ok: AtomicU64,
    pub(crate) binary_encode_reallocs: AtomicU64,
    pub(crate) text_encode_reallocs: AtomicU64,
}

/// The frame and byte counters so far. All zero without the `telemetry` feature.
pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        FrameCountersSnapshot {
            frames_in_ok: counters.frames_in_ok.load(Ordering::Relaxed),
            frames_in_err: counters.frames_in_err.load(Ordering::Relaxed),
            frames_out_ok: counters.frames_out_ok.load(Ordering::Relaxed),
            bytes_in: counters.bytes_in.load(Ordering::Relaxed),
            bytes_out: counters.bytes_out.load(Ordering::Relaxed),
            pub_frames_out_ok: counters.pub_frames_out_ok.load(Ordering::Relaxed),
            pub_frames_out_err: counters.pub_frames_out_err.load(Ordering::Relaxed),
            sub_frames_in_ok: counters.sub_frames_in_ok.load(Ordering::Relaxed),
            ack_frames_in_ok: counters.ack_frames_in_ok.load(Ordering::Relaxed),
            pub_items_out_ok: counters.pub_items_out_ok.load(Ordering::Relaxed),
            pub_items_out_err: counters.pub_items_out_err.load(Ordering::Relaxed),
            pub_batches_out_ok: counters.pub_batches_out_ok.load(Ordering::Relaxed),
            pub_batches_out_err: counters.pub_batches_out_err.load(Ordering::Relaxed),
            sub_items_in_ok: counters.sub_items_in_ok.load(Ordering::Relaxed),
            sub_batches_in_ok: counters.sub_batches_in_ok.load(Ordering::Relaxed),
            ack_items_in_ok: counters.ack_items_in_ok.load(Ordering::Relaxed),
            binary_encode_reallocs: counters.binary_encode_reallocs.load(Ordering::Relaxed),
            text_encode_reallocs: counters.text_encode_reallocs.load(Ordering::Relaxed),
        }
    }
    #[cfg(not(feature = "telemetry"))]
    {
        FrameCountersSnapshot {
            frames_in_ok: 0,
            frames_in_err: 0,
            frames_out_ok: 0,
            bytes_in: 0,
            bytes_out: 0,
            pub_frames_out_ok: 0,
            pub_frames_out_err: 0,
            sub_frames_in_ok: 0,
            ack_frames_in_ok: 0,
            pub_items_out_ok: 0,
            pub_items_out_err: 0,
            pub_batches_out_ok: 0,
            pub_batches_out_err: 0,
            sub_items_in_ok: 0,
            sub_batches_in_ok: 0,
            ack_items_in_ok: 0,
            binary_encode_reallocs: 0,
            text_encode_reallocs: 0,
        }
    }
}

/// Zero every frame and byte counter.
pub fn reset_frame_counters() {
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.store(0, Ordering::Relaxed);
        counters.frames_in_err.store(0, Ordering::Relaxed);
        counters.frames_out_ok.store(0, Ordering::Relaxed);
        counters.bytes_in.store(0, Ordering::Relaxed);
        counters.bytes_out.store(0, Ordering::Relaxed);
        counters.pub_frames_out_ok.store(0, Ordering::Relaxed);
        counters.pub_frames_out_err.store(0, Ordering::Relaxed);
        counters.pub_items_out_ok.store(0, Ordering::Relaxed);
        counters.pub_items_out_err.store(0, Ordering::Relaxed);
        counters.pub_batches_out_ok.store(0, Ordering::Relaxed);
        counters.pub_batches_out_err.store(0, Ordering::Relaxed);
        counters.sub_frames_in_ok.store(0, Ordering::Relaxed);
        counters.sub_items_in_ok.store(0, Ordering::Relaxed);
        counters.sub_batches_in_ok.store(0, Ordering::Relaxed);
        counters.ack_frames_in_ok.store(0, Ordering::Relaxed);
        counters.ack_items_in_ok.store(0, Ordering::Relaxed);
        counters.binary_encode_reallocs.store(0, Ordering::Relaxed);
        counters.text_encode_reallocs.store(0, Ordering::Relaxed);
    }
}

/// How many publishes this process has had forwarded since it started.
pub fn publishes_forwarded() -> u64 {
    PUBLISHES_FORWARDED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(feature = "telemetry")]
pub(crate) fn frame_counters() -> &'static FrameCounters {
    FRAME_COUNTERS.get_or_init(FrameCounters::default)
}

pub(crate) fn record_publish_forwarded() {
    PUBLISHES_FORWARDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
