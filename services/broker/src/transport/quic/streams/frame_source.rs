use anyhow::Result;
use bytes::BytesMut;
use felix_wire::Frame;
use quinn::RecvStream;
use std::future::Future;
use std::pin::Pin;

use crate::transport::quic::codec::read_frame_limited_into;

pub(super) trait FrameSource {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>>;
}

impl FrameSource for RecvStream {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(read_frame_limited_into(self, max_frame_bytes, scratch))
    }
}

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
