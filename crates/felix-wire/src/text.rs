// Hand-rolled zero-copy JSON writer for the publish-batch hot path.
// Avoids serde_json's intermediate allocations by computing the exact encoded
// length up front and writing directly into the caller's buffer.

use crate::error::{Error, Result};
use crate::message::AckMode;
use base64::Engine;
use bytes::{BufMut, BytesMut};

#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeStats {
    pub reallocs: u64,
}

const PUBLISH_BATCH_PREFIX: &str = "{\"type\":\"publish_batch\",\"tenant_id\":\"";
const PUBLISH_BATCH_NAMESPACE: &str = "\",\"namespace\":\"";
const PUBLISH_BATCH_STREAM: &str = "\",\"stream\":\"";
const PUBLISH_BATCH_PAYLOADS: &str = "\",\"payloads\":[";
const REQUEST_ID_PREFIX: &str = "\",\"request_id\":";
const ACK_PREFIX: &str = ",\"ack\":\"";

pub fn publish_batch_json_len(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
    request_id: Option<u64>,
    ack: Option<AckMode>,
) -> Result<usize> {
    let mut len = 0usize;
    len = len
        .checked_add(PUBLISH_BATCH_PREFIX.len())
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(escaped_len(tenant_id))
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(PUBLISH_BATCH_NAMESPACE.len())
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(escaped_len(namespace))
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(PUBLISH_BATCH_STREAM.len())
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(escaped_len(stream))
        .ok_or(Error::FrameTooLarge)?;
    len = len
        .checked_add(PUBLISH_BATCH_PAYLOADS.len())
        .ok_or(Error::FrameTooLarge)?;
    for (idx, payload) in payloads.iter().enumerate() {
        let encoded_len = base64_len(payload.len())?;
        let item_len = 2usize
            .checked_add(encoded_len)
            .and_then(|v| if idx == 0 { Some(v) } else { v.checked_add(1) })
            .ok_or(Error::FrameTooLarge)?;
        len = len.checked_add(item_len).ok_or(Error::FrameTooLarge)?;
    }
    len = len.checked_add(1).ok_or(Error::FrameTooLarge)?; // closing ]
    if let Some(request_id) = request_id {
        len = len
            .checked_add(REQUEST_ID_PREFIX.len())
            .ok_or(Error::FrameTooLarge)?;
        len = len
            .checked_add(decimal_len(request_id))
            .ok_or(Error::FrameTooLarge)?;
    }
    if let Some(ack) = ack {
        let ack_str = ack_str(ack);
        len = len
            .checked_add(ACK_PREFIX.len())
            .ok_or(Error::FrameTooLarge)?;
        len = len
            .checked_add(ack_str.len() + 1)
            .ok_or(Error::FrameTooLarge)?;
    }
    len = len.checked_add(1).ok_or(Error::FrameTooLarge)?; // closing }
    if len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    Ok(len)
}

pub fn write_publish_batch_json(
    buf: &mut BytesMut,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
    request_id: Option<u64>,
    ack: Option<AckMode>,
) -> Result<EncodeStats> {
    let mut stats = EncodeStats::default();
    let mut cap = buf.capacity();
    let check_realloc = |buf: &BytesMut, stats: &mut EncodeStats, cap: &mut usize| {
        let next = buf.capacity();
        if next != *cap {
            stats.reallocs += 1;
            *cap = next;
        }
    };

    buf.extend_from_slice(PUBLISH_BATCH_PREFIX.as_bytes());
    check_realloc(buf, &mut stats, &mut cap);
    write_json_str(buf, tenant_id);
    check_realloc(buf, &mut stats, &mut cap);
    buf.extend_from_slice(PUBLISH_BATCH_NAMESPACE.as_bytes());
    write_json_str(buf, namespace);
    buf.extend_from_slice(PUBLISH_BATCH_STREAM.as_bytes());
    write_json_str(buf, stream);
    buf.extend_from_slice(PUBLISH_BATCH_PAYLOADS.as_bytes());
    for (idx, payload) in payloads.iter().enumerate() {
        if idx > 0 {
            buf.put_u8(b',');
        }
        buf.put_u8(b'"');
        let encoded_len = base64_len(payload.len())?;
        let start = buf.len();
        buf.resize(start + encoded_len, 0);
        let written = base64::engine::general_purpose::STANDARD
            .encode_slice(payload, &mut buf[start..])
            .expect("base64 encode slice");
        debug_assert_eq!(written, encoded_len);
        buf.put_u8(b'"');
        check_realloc(buf, &mut stats, &mut cap);
    }
    buf.put_u8(b']');
    if let Some(request_id) = request_id {
        buf.extend_from_slice(REQUEST_ID_PREFIX.as_bytes());
        write_decimal(buf, request_id);
    }
    if let Some(ack) = ack {
        buf.extend_from_slice(ACK_PREFIX.as_bytes());
        buf.extend_from_slice(ack_str(ack).as_bytes());
        buf.put_u8(b'"');
    }
    buf.put_u8(b'}');
    check_realloc(buf, &mut stats, &mut cap);
    Ok(stats)
}

fn ack_str(ack: AckMode) -> &'static str {
    match ack {
        AckMode::None => "none",
        AckMode::PerMessage => "per_message",
        AckMode::PerBatch => "per_batch",
    }
}

fn escaped_len(value: &str) -> usize {
    value.bytes().fold(0usize, |len, byte| {
        len + match byte {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' => 2,
            0x00..=0x1F => 6,
            _ => 1,
        }
    })
}

fn write_json_str(buf: &mut BytesMut, value: &str) {
    for byte in value.bytes() {
        match byte {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            0x00..=0x1F => {
                buf.extend_from_slice(b"\\u00");
                let hi = byte >> 4;
                let lo = byte & 0x0F;
                buf.put_u8(hex_digit(hi));
                buf.put_u8(hex_digit(lo));
            }
            _ => buf.put_u8(byte),
        }
    }
}

fn hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        10..=15 => b'a' + (value - 10),
        _ => b'0',
    }
}

fn base64_len(len: usize) -> Result<usize> {
    let chunks = len.checked_add(2).ok_or(Error::FrameTooLarge)? / 3;
    chunks.checked_mul(4).ok_or(Error::FrameTooLarge)
}

fn decimal_len(mut value: u64) -> usize {
    if value == 0 {
        return 1;
    }
    let mut len = 0usize;
    while value > 0 {
        value /= 10;
        len += 1;
    }
    len
}

fn write_decimal(buf: &mut BytesMut, mut value: u64) {
    let mut scratch = [0u8; 20];
    let mut idx = scratch.len();
    if value == 0 {
        buf.put_u8(b'0');
        return;
    }
    while value > 0 {
        idx -= 1;
        scratch[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    buf.extend_from_slice(&scratch[idx..]);
}
