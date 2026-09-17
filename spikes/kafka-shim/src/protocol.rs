//! Just enough of the Kafka wire protocol to be read by a real client.
//!
//! Hand-rolled rather than pulled from a crate, because the question the spike
//! answers is *how much work is this*, and a dependency that already did the
//! work would answer a different one.
use bytes::{Buf, BufMut, BytesMut};

/// CRC-32C. The v2 record batch uses Castagnoli, not the IEEE polynomial
/// `crc32fast` implements — a client that computes the other one rejects every
/// batch, which is the kind of detail that decides whether a shim is a weekend
/// or a month.
pub fn crc32c(bytes: &[u8]) -> u32 {
    const ALGO: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    ALGO.checksum(bytes)
}

/// The API keys this spike answers. Everything else is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    ApiVersions = 18,
}

impl ApiKey {
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            18 => Some(Self::ApiVersions),
            _ => None,
        }
    }
}

/// A request header, v1: the shape every request this spike answers arrives in.
#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

pub fn get_nullable_string(buf: &mut impl Buf) -> Option<String> {
    let len = buf.get_i16();
    if len < 0 {
        return None;
    }
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

pub fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_i16(value.len() as i16);
    buf.put_slice(value.as_bytes());
}

pub fn parse_request_header(buf: &mut impl Buf) -> RequestHeader {
    RequestHeader {
        api_key: buf.get_i16(),
        api_version: buf.get_i16(),
        correlation_id: buf.get_i32(),
        client_id: get_nullable_string(buf),
    }
}

/// A varint, zigzag-encoded, as the record format uses.
pub fn put_varint(buf: &mut BytesMut, value: i32) {
    put_varlong(buf, value as i64);
}

pub fn put_varlong(buf: &mut BytesMut, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    while zigzag >= 0x80 {
        buf.put_u8((zigzag as u8) | 0x80);
        zigzag >>= 7;
    }
    buf.put_u8(zigzag as u8);
}

/// One record inside a batch.
pub struct Record {
    pub offset_delta: i32,
    pub timestamp_delta: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

/// Encode a v2 record batch.
///
/// The layout is exact and clients check it: the CRC covers everything *after*
/// the CRC field, `batch_length` counts everything after itself, and the
/// per-record length is a varint of the bytes that follow it.
pub fn encode_record_batch(base_offset: i64, first_timestamp: i64, records: &[Record]) -> BytesMut {
    let mut body = BytesMut::new();
    // From `attributes` onward — this is what the CRC covers.
    body.put_i16(0); // attributes: no compression, create time
    body.put_i32(records.last().map_or(0, |r| r.offset_delta));
    body.put_i64(first_timestamp);
    body.put_i64(first_timestamp + records.last().map_or(0, |r| r.timestamp_delta));
    body.put_i64(-1); // producerId: not idempotent
    body.put_i16(-1); // producerEpoch
    body.put_i32(-1); // baseSequence
    body.put_i32(records.len() as i32);
    for record in records {
        let mut encoded = BytesMut::new();
        encoded.put_i8(0); // attributes
        put_varlong(&mut encoded, record.timestamp_delta);
        put_varint(&mut encoded, record.offset_delta);
        match &record.key {
            Some(key) => {
                put_varint(&mut encoded, key.len() as i32);
                encoded.put_slice(key);
            }
            None => put_varint(&mut encoded, -1),
        }
        put_varint(&mut encoded, record.value.len() as i32);
        encoded.put_slice(&record.value);
        put_varint(&mut encoded, 0); // headers
        put_varint(&mut body, encoded.len() as i32);
        body.put_slice(&encoded);
    }

    let mut batch = BytesMut::new();
    batch.put_i64(base_offset);
    // batch_length counts every byte after it: the 4 CRC bytes, the 4 before
    // them (partition_leader_epoch + magic), and the body.
    batch.put_i32((4 + 1 + 4 + body.len()) as i32);
    batch.put_i32(-1); // partition_leader_epoch
    batch.put_i8(2); // magic
    batch.put_u32(crc32c(&body));
    batch.put_slice(&body);
    batch
}
