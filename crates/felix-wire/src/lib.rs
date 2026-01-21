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

    // Encode a full binary publish frame, including header and payload.
    pub fn encode_publish_batch_bytes(
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> Result<Bytes> {
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
        }
        Ok(buf.freeze())
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
}
