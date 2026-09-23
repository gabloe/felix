//! Acked binary publish: a publish batch the broker owes an answer for.
//!
//! The unacked binary publish frame has no room for a request id, which is why
//! acked publishes were stuck on the JSON control encoding. An acked frame sets
//! FLAG_BINARY_PUBLISH_ACKED alongside FLAG_BINARY_PUBLISH_BATCH and prefixes the
//! ordinary publish-batch body with:
//!
//! ```text
//! u64 request_id
//! u8  ack_mode      (1 = PerMessage, 2 = PerBatch)
//! ```
//!
//! The prefix goes first so a decoder can read the correlation id without parsing
//! the rest of the frame — that is what lets the broker answer with an error
//! carrying the right request_id even when the body turns out to be malformed.
//!
//! AckMode::None is deliberately not representable here: an unacked publish uses
//! the plain FLAG_BINARY_PUBLISH_BATCH encoding with no prefix at all, so there is
//! exactly one encoding per mode rather than two ways to say "no ack".

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

use super::publish::{
    PublishBatch, decode_publish_batch, encode_publish_batch_keyed, publish_flags,
};
use crate::client::flags::{
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_IDEMPOTENT,
    FLAG_BINARY_PUBLISH_KEYED,
};
use crate::client::frame::{Frame, FrameHeader};
use crate::client::message::AckMode;
use crate::error::{Error, Result};

const ACK_MODE_PER_MESSAGE: u8 = 1;
const ACK_MODE_PER_BATCH: u8 = 2;
// u64 request_id + u8 ack_mode.
const ACKED_PREFIX_LEN: usize = 9;

fn ack_mode_to_wire(ack: AckMode) -> Result<u8> {
    match ack {
        AckMode::PerMessage => Ok(ACK_MODE_PER_MESSAGE),
        AckMode::PerBatch => Ok(ACK_MODE_PER_BATCH),
        // Callers must route AckMode::None to `encode_publish_batch`.
        AckMode::None => Err(Error::Deserialize(SerdeError::custom(
            "AckMode::None has no acked binary encoding",
        ))),
    }
}

fn ack_mode_from_wire(byte: u8) -> Result<AckMode> {
    match byte {
        ACK_MODE_PER_MESSAGE => Ok(AckMode::PerMessage),
        ACK_MODE_PER_BATCH => Ok(AckMode::PerBatch),
        _ => Err(Error::Deserialize(SerdeError::custom("invalid ack mode"))),
    }
}

/// A binary publish batch that asked to be acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedPublishBatch {
    pub request_id: u64,
    pub ack: AckMode,
    /// Present when the frame carried `FLAG_BINARY_PUBLISH_IDEMPOTENT`.
    pub producer: Option<ProducerSequence>,
    pub batch: PublishBatch,
}

/// The producer id and sequence an idempotent batch is appended under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSequence {
    pub producer_id: u64,
    pub sequence: u64,
}

// u64 producer_id + u64 sequence, after the acked prefix.
const PRODUCER_PREFIX_LEN: usize = 16;

/// Encode an acked publish batch into a full framed buffer (header included).
pub fn encode_acked_publish_batch_bytes(
    request_id: u64,
    ack: AckMode,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_publish_batch_bytes_keyed(
        request_id, ack, None, tenant_id, namespace, stream, payloads,
    )
}

/// The keyed form of [`encode_acked_publish_batch_bytes`].
///
/// The key prefix sits *after* the correlation prefix, so
/// [`peek_acked_publish_prefix`] still reads the request id at offset 0 whether
/// or not a key follows.
pub fn encode_acked_publish_batch_bytes_keyed(
    request_id: u64,
    ack: AckMode,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_inner(
        request_id, ack, None, key, tenant_id, namespace, stream, payloads,
    )
}

/// Encode a batch under an idempotent producer's sequence. Always acked per
/// batch; the producer prefix follows the acked prefix and precedes any key.
#[allow(clippy::too_many_arguments)]
pub fn encode_idempotent_publish_batch_bytes(
    request_id: u64,
    producer: ProducerSequence,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_inner(
        request_id,
        AckMode::PerBatch,
        Some(producer),
        key,
        tenant_id,
        namespace,
        stream,
        payloads,
    )
}

#[allow(clippy::too_many_arguments)]
fn encode_acked_inner(
    request_id: u64,
    ack: AckMode,
    producer: Option<ProducerSequence>,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    let ack_byte = ack_mode_to_wire(ack)?;
    // Reuse the unacked body encoder rather than duplicating its bounds checks,
    // then splice the prefixes in front and restate the header.
    let body = encode_publish_batch_keyed(key, tenant_id, namespace, stream, payloads)?.payload;
    let prefix_len = ACKED_PREFIX_LEN + producer.map_or(0, |_| PRODUCER_PREFIX_LEN);
    let payload_len = prefix_len
        .checked_add(body.len())
        .ok_or(Error::FrameTooLarge)?;
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut flags = publish_flags(key) | FLAG_BINARY_PUBLISH_ACKED;
    if producer.is_some() {
        flags |= FLAG_BINARY_PUBLISH_IDEMPOTENT;
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    buf.put_u64(request_id);
    buf.put_u8(ack_byte);
    if let Some(producer) = producer {
        buf.put_u64(producer.producer_id);
        buf.put_u64(producer.sequence);
    }
    buf.extend_from_slice(&body);
    Ok(buf.freeze())
}

/// Read only the correlation prefix, without decoding the batch body.
///
/// The broker needs this to answer a malformed acked publish with a
/// `PublishError` the client can actually match to its pending request. Without
/// it a body-level decode failure would leave the client blocked until timeout.
pub fn peek_acked_publish_prefix(frame: &Frame) -> Result<(u64, AckMode)> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < ACKED_PREFIX_LEN {
        return Err(Error::Incomplete);
    }
    let request_id = buf.get_u64();
    let ack = ack_mode_from_wire(buf.get_u8())?;
    Ok((request_id, ack))
}

/// Decode an acked binary publish batch frame.
pub fn decode_acked_publish_batch(frame: &Frame) -> Result<AckedPublishBatch> {
    let (request_id, ack) = peek_acked_publish_prefix(frame)?;
    let mut prefix_len = ACKED_PREFIX_LEN;
    let producer = if frame.header.flags & FLAG_BINARY_PUBLISH_IDEMPOTENT != 0 {
        let mut buf = frame.payload.slice(ACKED_PREFIX_LEN..);
        if buf.remaining() < PRODUCER_PREFIX_LEN {
            return Err(Error::Incomplete);
        }
        prefix_len += PRODUCER_PREFIX_LEN;
        Some(ProducerSequence {
            producer_id: buf.get_u64(),
            sequence: buf.get_u64(),
        })
    } else {
        None
    };
    // Re-frame the remainder as a plain publish batch so both encodings share one
    // body parser, and with it one set of bounds checks.
    let body = Frame {
        header: FrameHeader::new(
            // Carry the keyed bit across: it is what tells the body parser a key
            // prefix comes before the tenant id.
            FLAG_BINARY_PUBLISH_BATCH | (frame.header.flags & FLAG_BINARY_PUBLISH_KEYED),
            (frame.payload.len() - prefix_len) as u32,
        ),
        payload: frame.payload.slice(prefix_len..),
    };
    Ok(AckedPublishBatch {
        request_id,
        ack,
        producer,
        batch: decode_publish_batch(&body)?,
    })
}

#[cfg(test)]
mod tests;
