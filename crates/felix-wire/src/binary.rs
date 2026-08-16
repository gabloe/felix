// Binary batch codec for publish and event batches.
//
// These frames carry untrusted, attacker-controllable payload counts, so every
// decode path bounds the count against the remaining buffer before allocating.

use crate::error::{Error, Result};
use crate::frame::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_EVENT_BATCH_OFFSETS, Frame,
    FrameHeader,
};
use crate::message::AckMode;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

// Every payload in a batch is encoded as a 4-byte length prefix followed by its
// bytes, so a frame can never carry more payloads than it has 4-byte groups left.
const PAYLOAD_LEN_PREFIX: usize = 4;

// Bound an attacker-declared payload count against what the frame can actually
// hold, before it ever reaches `Vec::with_capacity`. The count is read straight
// off the wire, so without this a ~20-byte frame declaring `u32::MAX` payloads
// reserves 95-127 GiB of address space. Overcommit means one such frame usually
// succeeds, but roughly a thousand concurrent ones exhaust the address space and
// the failing allocation calls `handle_alloc_error`, which aborts the process
// instead of unwinding — so it is not contained by per-task panic recovery. A
// memory cgroup or strict overcommit, as in a typical container deployment,
// brings that threshold far lower. The loop below still validates each payload
// individually; this only stops the count itself from being trusted.
fn checked_payload_count(count: usize, remaining: usize) -> Result<usize> {
    if count > remaining / PAYLOAD_LEN_PREFIX {
        return Err(Error::Incomplete);
    }
    Ok(count)
}

// Parsed representation of a binary publish batch frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatch {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payloads: Vec<Vec<u8>>,
}

// Encode a publish batch into a binary frame (payload only).
pub fn encode_publish_batch(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Frame> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len =
        2usize + tenant_bytes.len() + 2 + namespace_bytes.len() + 2 + stream_bytes.len() + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut buf = BytesMut::with_capacity(payload_len);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        buf.extend_from_slice(payload);
    }
    Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeStats {
    pub reallocs: u64,
}

// Encode a full binary publish frame, including header and payload.
pub fn encode_publish_batch_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    let (bytes, _stats) =
        encode_publish_batch_bytes_with_stats(tenant_id, namespace, stream, payloads)?;
    Ok(bytes)
}

pub fn encode_publish_batch_bytes_from_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Result<Bytes> {
    let (bytes, _stats) =
        encode_publish_batch_bytes_with_stats_from_bytes(tenant_id, namespace, stream, payloads)?;
    Ok(bytes)
}

// Encode a full binary publish frame, including header and payload, and return stats.
pub fn encode_publish_batch_bytes_with_stats(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<(Bytes, EncodeStats)> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len =
        2usize + tenant_bytes.len() + 2 + namespace_bytes.len() + 2 + stream_bytes.len() + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    let mut reallocs = 0u64;
    let mut cap = buf.capacity();
    let header = FrameHeader::new(FLAG_BINARY_PUBLISH_BATCH, payload_len as u32);
    header.encode(&mut buf);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        buf.extend_from_slice(payload);
        let next_cap = buf.capacity();
        if next_cap != cap {
            reallocs += 1;
            cap = next_cap;
        }
    }
    Ok((buf.freeze(), EncodeStats { reallocs }))
}

pub fn encode_publish_batch_bytes_with_stats_from_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Result<(Bytes, EncodeStats)> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len =
        2usize + tenant_bytes.len() + 2 + namespace_bytes.len() + 2 + stream_bytes.len() + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    let mut reallocs = 0u64;
    let mut cap = buf.capacity();
    let header = FrameHeader::new(FLAG_BINARY_PUBLISH_BATCH, payload_len as u32);
    header.encode(&mut buf);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        let start = buf.len();
        buf.resize(start + len as usize, 0);
        buf[start..].copy_from_slice(payload);
        if buf.capacity() != cap {
            reallocs = reallocs.saturating_add(1);
            cap = buf.capacity();
        }
    }
    Ok((buf.freeze(), EncodeStats { reallocs }))
}

// Decode a binary publish batch frame into its structured form.
pub fn decode_publish_batch(frame: &Frame) -> Result<PublishBatch> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    let tenant_len = buf.get_u16() as usize;
    if buf.remaining() < tenant_len + 2 {
        return Err(Error::Incomplete);
    }
    let tenant_bytes = buf.copy_to_bytes(tenant_len);
    let tenant_id = String::from_utf8(tenant_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid tenant id")))?;
    let namespace_len = buf.get_u16() as usize;
    if buf.remaining() < namespace_len + 2 {
        return Err(Error::Incomplete);
    }
    let namespace_bytes = buf.copy_to_bytes(namespace_len);
    let namespace = String::from_utf8(namespace_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid namespace")))?;
    let stream_len = buf.get_u16() as usize;
    if buf.remaining() < stream_len + 4 {
        return Err(Error::Incomplete);
    }
    let stream_bytes = buf.copy_to_bytes(stream_len);
    let stream = String::from_utf8(stream_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid stream name")))?;
    let count = checked_payload_count(buf.get_u32() as usize, buf.remaining())?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(Error::Incomplete);
        }
        let bytes = buf.copy_to_bytes(len);
        payloads.push(bytes.to_vec());
    }
    Ok(PublishBatch {
        tenant_id,
        namespace,
        stream,
        payloads,
    })
}

// --- Acked publish batch -------------------------------------------------
//
// The unacked binary publish frame has no room for a request id, which is why
// acked publishes were stuck on the JSON control encoding. An acked frame sets
// FLAG_BINARY_PUBLISH_ACKED alongside FLAG_BINARY_PUBLISH_BATCH and prefixes the
// ordinary publish-batch body with:
//
//   u64 request_id
//   u8  ack_mode      (1 = PerMessage, 2 = PerBatch)
//
// The prefix goes first so a decoder can read the correlation id without parsing
// the rest of the frame — that is what lets the broker answer with an error
// carrying the right request_id even when the body turns out to be malformed.
//
// AckMode::None is deliberately not representable here: an unacked publish uses
// the plain FLAG_BINARY_PUBLISH_BATCH encoding with no prefix at all, so there is
// exactly one encoding per mode rather than two ways to say "no ack".
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
    pub batch: PublishBatch,
}

/// Encode an acked publish batch into a full framed buffer (header included).
pub fn encode_acked_publish_batch_bytes(
    request_id: u64,
    ack: AckMode,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    let ack_byte = ack_mode_to_wire(ack)?;
    // Reuse the unacked body encoder rather than duplicating its bounds checks,
    // then splice the prefix in front and restate the header with both flags.
    let body = encode_publish_batch(tenant_id, namespace, stream, payloads)?.payload;
    let payload_len = ACKED_PREFIX_LEN
        .checked_add(body.len())
        .ok_or(Error::FrameTooLarge)?;
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        payload_len as u32,
    )
    .encode(&mut buf);
    buf.put_u64(request_id);
    buf.put_u8(ack_byte);
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
    // Re-frame the remainder as a plain publish batch so both encodings share one
    // body parser, and with it one set of bounds checks.
    let body = Frame {
        header: FrameHeader::new(
            FLAG_BINARY_PUBLISH_BATCH,
            (frame.payload.len() - ACKED_PREFIX_LEN) as u32,
        ),
        payload: frame.payload.slice(ACKED_PREFIX_LEN..),
    };
    Ok(AckedPublishBatch {
        request_id,
        ack,
        batch: decode_publish_batch(&body)?,
    })
}

// --- Publish ack response ------------------------------------------------
//
//   u8  status        (0 = ok, 1 = error)
//   u64 request_id
//   u16 message_len   (0 when status = ok)
//   u8[message_len] message
const ACK_STATUS_OK: u8 = 0;
const ACK_STATUS_ERROR: u8 = 1;

/// Broker → client acknowledgement for an acked binary publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub request_id: u64,
    /// `None` on success, `Some(message)` when the publish failed.
    pub error: Option<String>,
}

/// Encode a publish ack into a full framed buffer (header included).
pub fn encode_publish_ack_bytes(request_id: u64, error: Option<&str>) -> Result<Bytes> {
    let message = error.unwrap_or("");
    let message_bytes = message.as_bytes();
    let message_len = u16::try_from(message_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let payload_len = 1 + 8 + 2 + message_bytes.len();
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(FLAG_BINARY_PUBLISH_ACK, payload_len as u32).encode(&mut buf);
    buf.put_u8(if error.is_some() {
        ACK_STATUS_ERROR
    } else {
        ACK_STATUS_OK
    });
    buf.put_u64(request_id);
    buf.put_u16(message_len);
    buf.extend_from_slice(message_bytes);
    Ok(buf.freeze())
}

/// Decode a publish ack frame.
pub fn decode_publish_ack(frame: &Frame) -> Result<PublishAck> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < 11 {
        return Err(Error::Incomplete);
    }
    let status = buf.get_u8();
    let request_id = buf.get_u64();
    let message_len = buf.get_u16() as usize;
    if buf.remaining() < message_len {
        return Err(Error::Incomplete);
    }
    let message_bytes = buf.copy_to_bytes(message_len);
    let error = match status {
        ACK_STATUS_OK => None,
        ACK_STATUS_ERROR => Some(
            String::from_utf8(message_bytes.to_vec())
                .map_err(|_| Error::Deserialize(SerdeError::custom("invalid ack message")))?,
        ),
        _ => return Err(Error::Deserialize(SerdeError::custom("invalid ack status"))),
    };
    Ok(PublishAck { request_id, error })
}

// Parsed representation of a binary event batch frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    pub subscription_id: u64,
    pub payloads: Vec<Bytes>,
    /// Offset of `payloads[0]`, when the sender negotiated
    /// `FLAG_EVENT_BATCH_OFFSETS`. Payload `i` is at `base_offset + i`.
    pub base_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEventBatch {
    pub payloads: Vec<Bytes>,
    /// Offset of `payloads[0]`, when negotiated. See [`EventBatch::base_offset`].
    pub base_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct EncodedEventBatchParts {
    frame_len: usize,
    segments: Vec<Bytes>,
}

impl EncodedEventBatchParts {
    pub fn frame_len(&self) -> usize {
        self.frame_len
    }

    pub fn segments(&self) -> &[Bytes] {
        &self.segments
    }

    pub fn into_segments(self) -> Vec<Bytes> {
        self.segments
    }
}

// Encode binary event batch into a full framed payload.
pub fn encode_event_batch_bytes(subscription_id: u64, payloads: &[Bytes]) -> Result<Bytes> {
    let parts = encode_event_batch_parts(subscription_id, payloads)?;
    let mut buf = BytesMut::with_capacity(parts.frame_len());
    for segment in parts.segments() {
        buf.extend_from_slice(segment.as_ref());
    }
    Ok(buf.freeze())
}

/// Encode an event batch carrying the offset of its first payload.
///
/// Layout is the plain batch with a `u64 base_offset` inserted after the
/// subscription id, and the frame flagged `FLAG_EVENT_BATCH_OFFSETS` so a
/// decoder knows to expect it. Only sent to a peer that negotiated the bit; a
/// peer that did not gets [`encode_event_batch_bytes`] and is unable to tell
/// the difference.
pub fn encode_event_batch_bytes_with_offset(
    subscription_id: u64,
    payloads: &[Bytes],
    base_offset: u64,
) -> Result<Bytes> {
    let mut payload_len = 8usize + 8 + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(
        FLAG_BINARY_EVENT_BATCH | FLAG_EVENT_BATCH_OFFSETS,
        payload_len as u32,
    )
    .encode(&mut buf);
    buf.extend_from_slice(&subscription_id.to_be_bytes());
    buf.extend_from_slice(&base_offset.to_be_bytes());
    buf.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
    }
    Ok(buf.freeze())
}

/// Encode a shared (encode-once, fan-out-to-many) batch carrying its base offset.
///
/// Offsets belong to the stream rather than the subscriber, so one encoding
/// still serves every subscriber that negotiated the bit -- which is what keeps
/// this off the per-subscriber cost model.
pub fn encode_shared_event_batch_bytes_with_offset(
    payloads: &[Bytes],
    base_offset: u64,
) -> Result<Bytes> {
    let mut payload_len = 8usize + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(
        FLAG_BINARY_EVENT_BATCH_SHARED | FLAG_EVENT_BATCH_OFFSETS,
        payload_len as u32,
    )
    .encode(&mut buf);
    buf.extend_from_slice(&base_offset.to_be_bytes());
    buf.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
    }
    Ok(buf.freeze())
}

pub fn encode_event_batch_parts(
    subscription_id: u64,
    payloads: &[Bytes],
) -> Result<EncodedEventBatchParts> {
    let mut payload_len = 8usize + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    // Segment 0 is the frame header + fixed event-batch prefix. Remaining segments
    // alternate between [payload_len_prefix, payload_bytes] so writers can stream
    // payload Bytes directly without copying into a contiguous buffer.
    let mut frame_prefix = [0u8; FrameHeader::LEN + 12];
    let header = FrameHeader::new(FLAG_BINARY_EVENT_BATCH, payload_len as u32);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    frame_prefix[..FrameHeader::LEN].copy_from_slice(&header_bytes);
    frame_prefix[FrameHeader::LEN..FrameHeader::LEN + 8]
        .copy_from_slice(&subscription_id.to_be_bytes());
    frame_prefix[FrameHeader::LEN + 8..FrameHeader::LEN + 12]
        .copy_from_slice(&(payloads.len() as u32).to_be_bytes());

    let mut segments = Vec::with_capacity(1 + (payloads.len() * 2));
    segments.push(Bytes::copy_from_slice(&frame_prefix));
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        segments.push(Bytes::copy_from_slice(&len.to_be_bytes()));
        segments.push(payload.clone());
    }

    Ok(EncodedEventBatchParts {
        frame_len: FrameHeader::LEN + payload_len,
        segments,
    })
}

pub fn encode_shared_event_batch_bytes(payloads: &[Bytes]) -> Result<Bytes> {
    let mut payload_len = 4usize;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(FLAG_BINARY_EVENT_BATCH_SHARED, payload_len as u32).encode(&mut buf);
    buf.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
    }
    Ok(buf.freeze())
}

// Decode binary event batch frame into its structured form.
pub fn decode_event_batch(frame: &Frame) -> Result<EventBatch> {
    let mut buf = frame.payload.clone();
    let has_offsets = frame.header.flags & FLAG_EVENT_BATCH_OFFSETS != 0;
    let fixed = if has_offsets { 20 } else { 12 };
    if buf.remaining() < fixed {
        return Err(Error::Incomplete);
    }
    let subscription_id = buf.get_u64();
    let base_offset = has_offsets.then(|| buf.get_u64());
    let count = checked_payload_count(buf.get_u32() as usize, buf.remaining())?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(Error::Incomplete);
        }
        let bytes = buf.copy_to_bytes(len);
        payloads.push(bytes);
    }
    Ok(EventBatch {
        subscription_id,
        payloads,
        base_offset,
    })
}

pub fn decode_shared_event_batch(frame: &Frame) -> Result<SharedEventBatch> {
    let mut buf = frame.payload.clone();
    let has_offsets = frame.header.flags & FLAG_EVENT_BATCH_OFFSETS != 0;
    let fixed = if has_offsets { 12 } else { 4 };
    if buf.remaining() < fixed {
        return Err(Error::Incomplete);
    }
    let base_offset = has_offsets.then(|| buf.get_u64());
    let count = checked_payload_count(buf.get_u32() as usize, buf.remaining())?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(Error::Incomplete);
        }
        payloads.push(buf.copy_to_bytes(len));
    }
    Ok(SharedEventBatch {
        payloads,
        base_offset,
    })
}
