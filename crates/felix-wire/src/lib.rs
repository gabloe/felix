// Simple wire format for framing bytes on the network.
use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const MAGIC: u32 = 0x464C5831;
pub const VERSION: u16 = 1;
pub const FLAG_BINARY_PUBLISH_BATCH: u16 = 0x0001;

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
///     ack: None,
/// };
/// let frame = message.encode().expect("encode");
/// let decoded = Message::decode(frame).expect("decode");
/// assert_eq!(message, decoded);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Publish {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    PublishBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_vec")]
        payloads: Vec<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    Subscribe {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    Event {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
    },
    CachePut {
        key: String,
        #[serde(with = "base64_bytes")]
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    },
    CacheGet {
        key: String,
    },
    CacheValue {
        key: String,
        #[serde(with = "base64_option")]
        value: Option<Vec<u8>>,
    },
    Ok,
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

    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }

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

mod base64_option {
    use super::*;
    use serde::de::Error;

    pub fn serialize<S>(
        value: &Option<Vec<u8>>,
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

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<Vec<u8>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = Option::<String>::deserialize(deserializer)?;
        match encoded {
            Some(value) => base64::engine::general_purpose::STANDARD
                .decode(value.as_bytes())
                .map(Some)
                .map_err(D::Error::custom),
            None => Ok(None),
        }
    }
}

mod base64_vec {
    use super::*;
    use serde::de::Error;

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

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PublishBatch {
        pub tenant_id: String,
        pub namespace: String,
        pub stream: String,
        pub payloads: Vec<Vec<u8>>,
    }

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
    fn message_round_trip() {
        let message = Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "topic".to_string(),
            payload: b"payload".to_vec(),
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
