// Entry point and shared constants for the broker QUIC transport adapter.
//! QUIC transport adapter for the broker.

mod codec;
mod conn;
mod errors;
mod streams;
#[macro_use]
mod telemetry;

pub mod handlers {
    pub mod publish;
    pub mod subscribe;
}

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

pub(crate) const PUBLISH_QUEUE_DEPTH: usize = 1024;
pub(crate) const ACK_QUEUE_DEPTH: usize = 2048;
pub(crate) const ACK_WAITERS_MAX: usize = 1024;
pub(crate) const ACK_HI_WATER: usize = ACK_QUEUE_DEPTH * 3 / 4;
pub(crate) const ACK_LO_WATER: usize = ACK_QUEUE_DEPTH / 2;
pub(crate) const ACK_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
pub(crate) const ACK_TIMEOUT_WINDOW: Duration = Duration::from_millis(200);
pub(crate) const ACK_TIMEOUT_THRESHOLD: u32 = 3;
pub(crate) const DEFAULT_EVENT_QUEUE_DEPTH: usize = 1024;
pub(crate) const STREAM_CACHE_TTL: Duration = Duration::from_secs(2);
pub(crate) const EVENT_SINGLE_BINARY_MIN_BYTES_DEFAULT: usize = 512;
pub(crate) const EVENT_SINGLE_BINARY_ENV: &str = "FELIX_BINARY_SINGLE_EVENT";
pub(crate) const EVENT_SINGLE_BINARY_MIN_BYTES_ENV: &str = "FELIX_BINARY_SINGLE_EVENT_MIN_BYTES";
pub(crate) static SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) static GLOBAL_INGRESS_DEPTH: AtomicUsize = AtomicUsize::new(0);
pub(crate) static GLOBAL_ACK_DEPTH: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
pub(crate) static DECODE_ERROR_LOGS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
pub(crate) const DECODE_ERROR_LOG_LIMIT: usize = 20;

pub use codec::{read_message_limited, write_message};
pub use conn::serve;
pub use telemetry::{FrameCountersSnapshot, frame_counters_snapshot, reset_frame_counters};
