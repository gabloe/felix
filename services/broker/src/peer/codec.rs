//! Reading and writing internal frames on a QUIC stream.
//!
//! Every read is bounded by the length the header declares, which is itself
//! bounded by [`felix_wire::internal::MAX_BODY_BYTES`] before a buffer is sized
//! from it. A peer is authenticated, not assumed correct.
use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};
use felix_wire::internal::{InternalHeader, InternalMessage, MAX_BODY_BYTES};
use quinn::{RecvStream, SendStream};

/// Write one complete frame.
///
/// A single `write_all` per frame is what lets several requests share a stream:
/// the caller holds the only handle to this `SendStream`, so frames cannot
/// interleave.
pub async fn write_frame(send: &mut SendStream, message: &InternalMessage) -> Result<()> {
    let frame = message.encode().context("encode internal frame")?;
    send.write_all(&frame)
        .await
        .context("write internal frame")?;
    Ok(())
}

/// Read one complete frame, or `None` at a clean end of stream.
///
/// `None` is only returned when the stream ended *between* frames. A stream that
/// ends part-way through one is an error: the peer stopped mid-message, and
/// treating that as a clean close would silently drop a request.
pub async fn read_frame(recv: &mut RecvStream) -> Result<Option<InternalMessage>> {
    let mut head = [0u8; InternalHeader::LEN];
    match recv.read_exact(&mut head).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(err) => return Err(err).context("read internal header"),
    }

    let header =
        InternalHeader::decode(&Bytes::copy_from_slice(&head)).context("decode internal header")?;
    if header.length > MAX_BODY_BYTES {
        bail!("internal frame declares {} bytes", header.length);
    }

    let mut frame = BytesMut::with_capacity(InternalHeader::LEN + header.length as usize);
    frame.extend_from_slice(&head);
    frame.resize(InternalHeader::LEN + header.length as usize, 0);
    recv.read_exact(&mut frame[InternalHeader::LEN..])
        .await
        .context("read internal body")?;

    Ok(Some(
        InternalMessage::decode(frame.freeze()).context("decode internal frame")?,
    ))
}
