//! FrameSource: an abstraction over "read the next felix-wire Frame".
//!
//! Why this exists
//! ---------------
//! The QUIC stream loops (control + uni) want to be testable without standing up real
//! QUIC sockets. In production, we read frames from `quinn::RecvStream`. In unit tests,
//! we want deterministic inputs (a fixed sequence of frames), plus the ability to force
//! specific edge cases (EOF, decode errors, delays for cancel/timeout paths).
//!
//! This module provides a small trait (`FrameSource`) that exposes a single operation:
//! `next_frame(...) -> Result<Option<Frame>>`.
//!
//! Semantics
//! ---------
//! - `Ok(Some(frame))` => a full frame was read and is ready for decoding.
//! - `Ok(None)`        => graceful close / EOF (peer finished the stream cleanly).
//! - `Err(e)`          => transport error or frame-format violation (e.g., oversize).
//!
//! Performance notes
//! -----------------
//! We pass a caller-owned `BytesMut` scratch buffer that is reused across reads. This
//! avoids allocating a new buffer for every frame and is important for tail-latency.
//!
//! The trait returns a boxed future (`Pin<Box<dyn Future + Send>>`) to keep the trait
//! object-safe and to avoid introducing `async_trait` (which would add an extra layer
//! and can complicate lifetimes). The box allocation is not on a per-frame hot path in
//! production if this trait is used behind static dispatch; even when dynamically
//! dispatched (tests), it's acceptable.

use anyhow::Result;
use bytes::BytesMut;
use felix_wire::Frame;
use quinn::RecvStream;
use std::future::Future;
use std::pin::Pin;

use crate::transport::quic::codec::read_frame_limited_into;

/// Read one frame from an underlying transport.
///
/// Callers provide:
/// - `max_frame_bytes`: a hard cap to defend against oversized frames.
/// - `scratch`: a reusable buffer that the underlying codec can fill/extend.
///
/// Returning `None` is a *normal* end-of-stream signal and is interpreted by the
/// calling loop as a graceful close.
pub(super) trait FrameSource {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>>;
}

/// Production implementation.
///
/// This wires the trait directly to the Quinn receive stream and enforces the maximum
/// frame size using the shared codec helper.
impl FrameSource for RecvStream {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        // `read_frame_limited_into` reuses `scratch` and returns `Ok(None)` on EOF.
        Box::pin(read_frame_limited_into(self, max_frame_bytes, scratch))
    }
}

// ---- Test helpers -----------------------------------------------------------

/// Deterministic frame source backed by a queue.
///
/// Useful for:
/// - feeding a known sequence of frames into the control/uni loops
/// - injecting `Err(...)` to exercise error-handling branches
/// - returning `Ok(None)` to simulate a graceful close
#[cfg(test)]
pub(super) struct TestFrameSource {
    pub(super) frames: std::collections::VecDeque<Result<Option<Frame>>>,
}

#[cfg(test)]
impl TestFrameSource {
    pub(super) fn new(frames: Vec<Result<Option<Frame>>>) -> Self {
        Self {
            frames: frames.into(),
        }
    }
}

#[cfg(test)]
impl FrameSource for TestFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        // If the queue is empty, behave like EOF: `Ok(None)`.
        Box::pin(async move { self.frames.pop_front().unwrap_or_else(|| Ok(None)) })
    }
}

/// Frame source that returns EOF after a delay.
///
/// Useful for exercising:
/// - cancellation paths where the read is pending
/// - drain/timeout logic in stream shutdown
#[cfg(test)]
pub(super) struct DelayFrameSource {
    pub(super) delay: std::time::Duration,
}

#[cfg(test)]
impl FrameSource for DelayFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Ok(None)
        })
    }
}
