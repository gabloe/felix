// Binary batch codec for publish and event batches.
//
// These frames carry untrusted, attacker-controllable payload counts, so every
// decode path bounds the count against the remaining buffer before allocating.

use crate::error::{Error, Result};
use crate::frame::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_BATCH, Frame,
    FrameHeader,
};
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

// Parsed representation of a binary event batch frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    pub subscription_id: u64,
    pub payloads: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEventBatch {
    pub payloads: Vec<Bytes>,
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
    if buf.remaining() < 12 {
        return Err(Error::Incomplete);
    }
    let subscription_id = buf.get_u64();
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
    })
}

pub fn decode_shared_event_batch(frame: &Frame) -> Result<SharedEventBatch> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < 4 {
        return Err(Error::Incomplete);
    }
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
    Ok(SharedEventBatch { payloads })
}
