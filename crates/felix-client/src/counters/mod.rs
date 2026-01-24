// Frame counters and telemetry snapshots for the client.
#[cfg(feature = "telemetry")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::{AtomicU64, Ordering};

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
static FRAME_COUNTERS: std::sync::OnceLock<FrameCounters> = std::sync::OnceLock::new();
#[cfg(feature = "telemetry")]
pub(crate) static DECODE_ERROR_LOGS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "telemetry")]
pub(crate) fn frame_counters() -> &'static FrameCounters {
    FRAME_COUNTERS.get_or_init(FrameCounters::default)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_counters_snapshot_returns_values() {
        let snapshot = frame_counters_snapshot();
        // Just verify it returns a snapshot (all zeros in non-telemetry mode)
        assert_eq!(snapshot.frames_in_ok, 0);
        assert_eq!(snapshot.frames_in_err, 0);
        assert_eq!(snapshot.frames_out_ok, 0);
    }

    #[test]
    fn reset_frame_counters_does_not_panic() {
        reset_frame_counters();
        // Just ensure it doesn't panic
    }

    #[test]
    fn frame_counters_snapshot_has_all_fields() {
        let snapshot = frame_counters_snapshot();
        // Verify all fields are accessible
        let _ = snapshot.frames_in_ok;
        let _ = snapshot.frames_in_err;
        let _ = snapshot.frames_out_ok;
        let _ = snapshot.bytes_in;
        let _ = snapshot.bytes_out;
        let _ = snapshot.pub_frames_out_ok;
        let _ = snapshot.pub_frames_out_err;
        let _ = snapshot.sub_frames_in_ok;
        let _ = snapshot.ack_frames_in_ok;
        let _ = snapshot.pub_items_out_ok;
        let _ = snapshot.pub_items_out_err;
        let _ = snapshot.pub_batches_out_ok;
        let _ = snapshot.pub_batches_out_err;
        let _ = snapshot.sub_items_in_ok;
        let _ = snapshot.sub_batches_in_ok;
        let _ = snapshot.ack_items_in_ok;
        let _ = snapshot.binary_encode_reallocs;
        let _ = snapshot.text_encode_reallocs;
    }
}
