// FrameSource abstracts the "next frame" read so control/uni loops can be tested without QUIC sockets.
use anyhow::Result;
use bytes::BytesMut;
use felix_wire::Frame;
use quinn::RecvStream;
use std::future::Future;
use std::pin::Pin;

use crate::transport::quic::codec::read_frame_limited_into;

// Read one frame from an underlying transport. Returning None signals graceful stream close.
pub(super) trait FrameSource {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>>;
}

// Production implementation: read frames from a Quinn RecvStream with size limits enforced.
impl FrameSource for RecvStream {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(read_frame_limited_into(self, max_frame_bytes, scratch))
    }
}

// Test helper: deterministic frame source backed by a queue.
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
        Box::pin(async move { self.frames.pop_front().unwrap_or_else(|| Ok(None)) })
    }
}

// Test helper: returns None after a short delay to exercise timeout/cancel paths.
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
