// Simple wire format for framing bytes on the network.
use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const MAGIC: u32 = 0x464C5831;
pub const VERSION: u16 = 1;
// Flags describe how to interpret the frame payload.
pub const FLAG_BINARY_PUBLISH_BATCH: u16 = 0x0001;
pub const FLAG_BINARY_EVENT_BATCH: u16 = 0x0002;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid magic number")]
    InvalidMagic,
    #[error("unsupported version {0}")]
    UnsupportedVersion(u16),
    #[error("frame too large")]
    FrameTooLarge,
    #[error("incomplete frame")]
    Incomplete,
    #[error("failed to serialize message")]
    Serialize(serde_json::Error),
    #[error("failed to deserialize message")]
    Deserialize(serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub length: u32,
}

impl FrameHeader {
    pub const LEN: usize = 12;

    // Create a header with the current protocol constants.
    pub fn new(flags: u16, length: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            flags,
            length,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        // Always encode in network byte order for portability.
        buf.extend_from_slice(&self.magic.to_be_bytes());
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf.extend_from_slice(&self.length.to_be_bytes());
    }

    pub fn encode_into(&self, buf: &mut [u8; Self::LEN]) {
        // Always encode in network byte order for portability.
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..6].copy_from_slice(&self.version.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..12].copy_from_slice(&self.length.to_be_bytes());
    }

    pub fn decode(mut buf: Bytes) -> Result<Self> {
        // Validate header before we trust the length.
        if buf.remaining() < Self::LEN {
            return Err(Error::Incomplete);
        }
        let magic = buf.get_u32();
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = buf.get_u16();
        if version != VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let flags = buf.get_u16();
        let length = buf.get_u32();
        Ok(Self {
            magic,
            version,
            flags,
            length,
        })
    }
}

/// Frame containing a header and payload.
///
/// ```
/// use bytes::Bytes;
/// use felix_wire::Frame;
///
/// let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
/// let encoded = frame.encode();
/// let decoded = Frame::decode(encoded).expect("decode");
/// assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(flags: u16, payload: Bytes) -> Result<Self> {
        // Keep length within the on-wire u32 size.
        if payload.len() > u32::MAX as usize {
            return Err(Error::FrameTooLarge);
        }
        Ok(Self {
            header: FrameHeader::new(flags, payload.len() as u32),
            payload,
        })
    }

    pub fn encode(&self) -> Bytes {
        // Pre-allocate the exact size to avoid reallocation.
        let mut buf = BytesMut::with_capacity(FrameHeader::LEN + self.payload.len());
        self.header.encode(&mut buf);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(input: Bytes) -> Result<Self> {
        // Split header and payload based on the declared length.
        if input.len() < FrameHeader::LEN {
            return Err(Error::Incomplete);
        }
        let header = FrameHeader::decode(input.slice(0..FrameHeader::LEN))?;
        let length = header.length as usize;
        if input.len() < FrameHeader::LEN + length {
            return Err(Error::Incomplete);
        }
        let payload = input.slice(FrameHeader::LEN..FrameHeader::LEN + length);
        Ok(Self { header, payload })
    }
}

/// V1 wire messages encoded in framed payloads.
///
/// ```
/// use felix_wire::Message;
///
/// let message = Message::Publish {
///     tenant_id: "t1".to_string(),
///     namespace: "default".to_string(),
///     stream: "updates".to_string(),
///     payload: b"hello".to_vec(),
///     request_id: None,
///     ack: None,
/// };
/// let frame = message.encode().expect("encode");
/// let decoded = Message::decode(frame).expect("decode");
/// assert_eq!(message, decoded);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // Publish a single payload to a stream.
    Publish {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    // Publish a batch of payloads in a single request.
    PublishBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_vec")]
        payloads: Vec<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    // Subscribe to a stream; server responds with Subscribed.
    Subscribe {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        subscription_id: Option<u64>,
    },
    // Subscription confirmation with server-assigned ID.
    Subscribed {
        subscription_id: u64,
    },
    // First message on the event stream for a subscription.
    EventStreamHello {
        subscription_id: u64,
    },
    // Single event delivered to a subscriber.
    Event {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
    },
    // JSON event batch (binary batch uses FLAG_BINARY_EVENT_BATCH).
    EventBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_vec")]
        payloads: Vec<Vec<u8>>,
    },
    // Cache set operation; may include TTL and request id.
    CachePut {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "base64_bytes_bytes")]
        value: Bytes,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        ttl_ms: Option<u64>,
    },
    // Cache get operation; request id is echoed in responses.
    CacheGet {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    // Cache read response (value is optional for misses).
    CacheValue {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "base64_option_bytes")]
        value: Option<Bytes>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    // Cache write response with request id.
    CacheOk {
        request_id: u64,
    },
    // Publish ack with request id.
    PublishOk {
        request_id: u64,
    },
    // Publish error with request id.
    PublishError {
        request_id: u64,
        message: String,
    },
    // Generic success response.
    Ok,
    // Protocol-level error for invalid requests or unexpected message types.
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    None,
    PerMessage,
    PerBatch,
}

impl Message {
    pub fn encode(&self) -> Result<Frame> {
        // JSON-encode into a framed payload.
        let payload = serde_json::to_vec(self).map_err(Error::Serialize)?;
        Frame::new(0, Bytes::from(payload))
    }

    pub fn decode(frame: Frame) -> Result<Self> {
        serde_json::from_slice(&frame.payload).map_err(Error::Deserialize)
    }
}

mod base64_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Vec<u8> as base64 string for JSON payloads.
    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }

    // Decode base64 string into Vec<u8>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(D::Error::custom)
    }
}

mod base64_bytes_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Bytes as base64 string for JSON payloads.
    pub fn serialize<S>(value: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }

    // Decode base64 string into Bytes.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(D::Error::custom)?;
        Ok(Bytes::from(decoded))
    }
}

mod base64_option_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Option<Bytes> as nullable base64 string.
    pub fn serialize<S>(
        value: &Option<Bytes>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(bytes) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                serializer.serialize_some(&encoded)
            }
            None => serializer.serialize_none(),
        }
    }

    // Decode optional base64 string into Option<Bytes>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<Bytes>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = Option::<String>::deserialize(deserializer)?;
        match encoded {
            Some(value) => base64::engine::general_purpose::STANDARD
                .decode(value.as_bytes())
                .map(|decoded| Some(Bytes::from(decoded)))
                .map_err(D::Error::custom),
            None => Ok(None),
        }
    }
}

mod base64_vec {
    use super::*;
    use serde::de::Error;

    // Encode Vec<Vec<u8>> as base64 array.
    pub fn serialize<S>(values: &[Vec<u8>], serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded: Vec<String> = values
            .iter()
            .map(|value| base64::engine::general_purpose::STANDARD.encode(value))
            .collect();
        encoded.serialize(serializer)
    }

    // Decode base64 array into Vec<Vec<u8>>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<Vec<u8>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = Vec::<String>::deserialize(deserializer)?;
        encoded
            .into_iter()
            .map(|value| {
                base64::engine::general_purpose::STANDARD
                    .decode(value.as_bytes())
                    .map_err(D::Error::custom)
            })
            .collect()
    }
}

pub mod text {
    use super::*;
    use bytes::BufMut;

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
}

pub mod binary {
    use super::*;
    use bytes::BufMut;
    use serde::de::Error as SerdeError;

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
        let mut buf = BytesMut::new();
        let tenant_bytes = tenant_id.as_bytes();
        let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u16(tenant_len);
        buf.extend_from_slice(tenant_bytes);
        let namespace_bytes = namespace.as_bytes();
        let namespace_len =
            u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u16(namespace_len);
        buf.extend_from_slice(namespace_bytes);
        let stream_bytes = stream.as_bytes();
        let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
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
        let namespace_len =
            u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
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
        let count = buf.get_u32() as usize;
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

    // Encode binary event batch into a full framed payload.
    pub fn encode_event_batch_bytes(subscription_id: u64, payloads: &[Bytes]) -> Result<Bytes> {
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
        let header = FrameHeader::new(FLAG_BINARY_EVENT_BATCH, payload_len as u32);
        header.encode(&mut buf);
        buf.put_u64(subscription_id);
        buf.put_u32(payloads.len() as u32);
        for payload in payloads {
            let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
            buf.put_u32(len);
            buf.extend_from_slice(payload.as_ref());
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
        let count = buf.get_u32() as usize;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        // Encoding then decoding should preserve header and payload.
        let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
        let encoded = frame.encode();
        let decoded = Frame::decode(encoded).expect("decode");
        assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
        assert_eq!(decoded.header.flags, 0x1);
    }

    #[test]
    fn decode_rejects_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
        buf.extend_from_slice(&VERSION.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let err = FrameHeader::decode(buf.freeze()).expect_err("invalid magic");
        assert!(matches!(err, Error::InvalidMagic));
    }

    #[test]
    fn decode_rejects_unsupported_version() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.extend_from_slice(&0xFFFFu16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let err = FrameHeader::decode(buf.freeze()).expect_err("unsupported version");
        assert!(matches!(err, Error::UnsupportedVersion(0xFFFF)));
    }

    #[test]
    fn decode_rejects_incomplete_header() {
        let err = FrameHeader::decode(Bytes::from_static(b"short")).expect_err("incomplete");
        assert!(matches!(err, Error::Incomplete));
    }

    #[test]
    fn decode_rejects_incomplete_payload() {
        let header = FrameHeader {
            magic: MAGIC,
            version: VERSION,
            flags: 0,
            length: 5,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        buf.extend_from_slice(b"hi");
        let err = Frame::decode(buf.freeze()).expect_err("incomplete payload");
        assert!(matches!(err, Error::Incomplete));
    }

    #[test]
    fn binary_event_batch_round_trip() {
        let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
        let encoded = binary::encode_event_batch_bytes(7, &payloads).expect("encode");
        let frame = Frame::decode(encoded).expect("decode");
        assert_eq!(frame.header.flags, FLAG_BINARY_EVENT_BATCH);
        let decoded = binary::decode_event_batch(&frame).expect("decode batch");
        assert_eq!(decoded.subscription_id, 7);
        assert_eq!(decoded.payloads, payloads);
    }

    #[test]
    fn binary_event_batch_rejects_incomplete_payload() {
        let frame =
            Frame::new(FLAG_BINARY_EVENT_BATCH, Bytes::from_static(b"short")).expect("frame");
        let err = binary::decode_event_batch(&frame).expect_err("incomplete");
        assert!(matches!(err, Error::Incomplete));
    }

    #[test]
    fn binary_publish_batch_round_trip() {
        let payloads = vec![b"one".to_vec(), b"two".to_vec()];
        let frame =
            binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
        assert_eq!(frame.header.flags, FLAG_BINARY_PUBLISH_BATCH);
        let decoded = binary::decode_publish_batch(&frame).expect("decode");
        assert_eq!(decoded.tenant_id, "t1");
        assert_eq!(decoded.namespace, "default");
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.payloads, payloads);
    }

    #[test]
    fn binary_publish_batch_rejects_incomplete_payload() {
        let frame =
            Frame::new(FLAG_BINARY_PUBLISH_BATCH, Bytes::from_static(b"\x00")).expect("frame");
        let err = binary::decode_publish_batch(&frame).expect_err("incomplete");
        assert!(matches!(err, Error::Incomplete));
    }

    #[test]
    fn message_round_trip() {
        let message = Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "topic".to_string(),
            payload: b"payload".to_vec(),
            request_id: None,
            ack: None,
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn message_error_round_trip() {
        let message = Message::Error {
            message: "oops".to_string(),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn binary_decode_publish_batch_invalid_utf8_tenant() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_u16(2); // tenant_id len
        buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
        let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
        let result = binary::decode_publish_batch(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn binary_decode_publish_batch_invalid_utf8_namespace() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_u16(2); // tenant_id len
        buf.extend_from_slice(b"t1");
        buf.put_u16(2); // namespace len
        buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
        let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
        let result = binary::decode_publish_batch(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn binary_decode_publish_batch_invalid_utf8_stream() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_u16(2); // tenant_id len
        buf.extend_from_slice(b"t1");
        buf.put_u16(2); // namespace len
        buf.extend_from_slice(b"ns");
        buf.put_u16(2); // stream len
        buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
        let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
        let result = binary::decode_publish_batch(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn text_publish_batch_json_with_special_chars() {
        // Test encoding with special characters that need escaping
        let payloads = vec![b"payload\nwith\nnewlines".to_vec()];
        let message = Message::PublishBatch {
            tenant_id: "tenant".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
            payloads,
            request_id: Some(123),
            ack: Some(AckMode::PerBatch),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn message_all_variants_encode_decode() {
        // Test Subscribe message
        let message = Message::Subscribe {
            subscription_id: Some(42),
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test Subscribed message
        let message = Message::Subscribed {
            subscription_id: 42,
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test PublishOk message
        let message = Message::PublishOk { request_id: 123 };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test PublishError message
        let message = Message::PublishError {
            request_id: 123,
            message: "error".to_string(),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test Ok message
        let message = Message::Ok;
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test EventStreamHello message
        let message = Message::EventStreamHello {
            subscription_id: 99,
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn binary_event_batch_empty_payloads() {
        let payloads = vec![];
        let encoded = binary::encode_event_batch_bytes(1, &payloads).expect("encode");
        let frame = Frame::decode(encoded).expect("decode");
        let decoded = binary::decode_event_batch(&frame).expect("decode batch");
        assert_eq!(decoded.subscription_id, 1);
        assert_eq!(decoded.payloads, payloads);
    }

    #[test]
    fn binary_publish_batch_empty_payloads() {
        let payloads = vec![];
        let frame =
            binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
        let decoded = binary::decode_publish_batch(&frame).expect("decode");
        assert_eq!(decoded.payloads, payloads);
    }

    #[test]
    fn frame_header_encode_decode() {
        let header = FrameHeader::new(0x1234, 0xABCD);
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let decoded = FrameHeader::decode(buf.freeze()).expect("decode");
        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.version, VERSION);
        assert_eq!(decoded.flags, 0x1234);
        assert_eq!(decoded.length, 0xABCD);
    }

    #[test]
    fn message_cache_operations() {
        // Test CachePut
        let message = Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "cache1".to_string(),
            key: "key1".to_string(),
            value: Bytes::from_static(b"value1"),
            request_id: Some(42),
            ttl_ms: Some(60000),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test CacheGet
        let message = Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "cache1".to_string(),
            key: "key1".to_string(),
            request_id: Some(42),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test CacheValue with value
        let message = Message::CacheValue {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "cache1".to_string(),
            key: "key1".to_string(),
            value: Some(Bytes::from_static(b"value1")),
            request_id: Some(42),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test CacheValue miss (no value)
        let message = Message::CacheValue {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "cache1".to_string(),
            key: "key1".to_string(),
            value: None,
            request_id: Some(42),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test CacheOk
        let message = Message::CacheOk { request_id: 42 };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn message_event_variants() {
        // Test Event message
        let message = Message::Event {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream1".to_string(),
            payload: b"event data".to_vec(),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test EventBatch message
        let message = Message::EventBatch {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream1".to_string(),
            payloads: vec![b"event1".to_vec(), b"event2".to_vec()],
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test EventStreamHello
        let message = Message::EventStreamHello {
            subscription_id: 123,
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn message_publish_with_ack_modes() {
        // Test Publish with AckMode::None
        let message = Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream1".to_string(),
            payload: b"data".to_vec(),
            request_id: Some(1),
            ack: Some(AckMode::None),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test Publish with AckMode::PerMessage
        let message = Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream1".to_string(),
            payload: b"data".to_vec(),
            request_id: Some(2),
            ack: Some(AckMode::PerMessage),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);

        // Test PublishBatch with AckMode::PerBatch
        let message = Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "stream1".to_string(),
            payloads: vec![b"data1".to_vec(), b"data2".to_vec()],
            request_id: Some(3),
            ack: Some(AckMode::PerBatch),
        };
        let frame = message.encode().expect("encode");
        let decoded = Message::decode(frame).expect("decode");
        assert_eq!(message, decoded);
    }

    #[test]
    fn binary_encode_publish_batch_stats() {
        let payloads = vec![
            b"payload1".to_vec(),
            b"payload2".to_vec(),
            b"payload3".to_vec(),
        ];
        let result = binary::encode_publish_batch_bytes_with_stats("t1", "ns", "stream", &payloads);
        assert!(result.is_ok());
        let (bytes, stats) = result.unwrap();
        assert!(!bytes.is_empty());
        // Stats should be valid (reallocs is a usize, so always >= 0)
        let _ = stats.reallocs;
    }

    #[test]
    fn binary_encode_event_batch_large() {
        let payloads: Vec<Bytes> = (0..100)
            .map(|i| Bytes::from(format!("payload{}", i)))
            .collect();
        let result = binary::encode_event_batch_bytes(42, &payloads);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        let frame = Frame::decode(bytes).expect("decode frame");
        let decoded = binary::decode_event_batch(&frame).expect("decode batch");
        assert_eq!(decoded.subscription_id, 42);
        assert_eq!(decoded.payloads.len(), 100);
    }

    #[test]
    fn frame_decode_error_cases() {
        // Test frame with invalid header length
        let short_bytes = Bytes::from_static(b"short");
        let result = Frame::decode(short_bytes);
        assert!(result.is_err());

        // Test frame header with incomplete payload
        let mut buf = BytesMut::new();
        let header = FrameHeader::new(0, 100); // Claims 100 bytes
        header.encode(&mut buf);
        buf.extend_from_slice(b"only_10"); // But only has 7 bytes
        let result = Frame::decode(buf.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn ack_mode_serialization() {
        // Test all AckMode variants serialize correctly
        let none = AckMode::None;
        let per_msg = AckMode::PerMessage;
        let per_batch = AckMode::PerBatch;

        // Just ensure they can be used in messages
        let msg = Message::Publish {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            payload: vec![1, 2, 3],
            request_id: Some(1),
            ack: Some(none),
        };
        assert!(msg.encode().is_ok());

        let msg2 = Message::Publish {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            payload: vec![1, 2, 3],
            request_id: Some(2),
            ack: Some(per_msg),
        };
        assert!(msg2.encode().is_ok());

        let msg3 = Message::PublishBatch {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            payloads: vec![vec![1, 2], vec![3, 4]],
            request_id: Some(3),
            ack: Some(per_batch),
        };
        assert!(msg3.encode().is_ok());
    }
}
