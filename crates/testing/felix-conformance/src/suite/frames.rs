//! Reading frames off an event stream and unpacking the events in them.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use felix_wire::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, Frame, FrameHeader, Message,
};
use quinn::{ReadExactError, RecvStream};

/// The one read [`read_frame`] needs, so a test can stand in for a quinn stream.
pub(crate) trait FrameReader {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>>;
}

impl FrameReader for RecvStream {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>> {
        Box::pin(RecvStream::read_exact(self, buf))
    }
}

pub(crate) async fn read_frame<R: FrameReader + ?Sized>(recv: &mut R) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

pub(crate) fn handle_event_frame(
    expected_id: u64,
    frame: Frame,
    pending: &mut VecDeque<Vec<u8>>,
    allow_hello: bool,
) -> Result<()> {
    if frame.header.flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
        if allow_hello {
            return Err(anyhow!("missing event stream hello for subscription"));
        }
        let batch = felix_wire::binary::decode_shared_event_batch(&frame)
            .context("decode shared binary event batch")?;
        for payload in batch.payloads {
            pending.push_back(payload.to_vec());
        }
        return Ok(());
    }
    if frame.header.flags & FLAG_BINARY_EVENT_BATCH != 0 {
        if allow_hello {
            return Err(anyhow!("missing event stream hello for subscription"));
        }
        let batch =
            felix_wire::binary::decode_event_batch(&frame).context("decode binary event batch")?;
        if expected_id != batch.subscription_id {
            return Err(anyhow!(
                "subscription id mismatch: expected {expected_id} got {}",
                batch.subscription_id
            ));
        }
        for payload in batch.payloads {
            pending.push_back(payload.to_vec());
        }
        return Ok(());
    }

    let message = Message::decode(frame).context("decode event message")?;
    match message {
        Message::EventStreamHello { subscription_id } => {
            if !allow_hello {
                return Err(anyhow!("unexpected hello after subscription start"));
            }
            if expected_id != subscription_id {
                return Err(anyhow!(
                    "subscription id mismatch: expected {expected_id} got {subscription_id}"
                ));
            }
        }
        other => return Err(anyhow!("unexpected message: {other:?}")),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
