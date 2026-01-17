// Simple wire format for framing bytes on the network.
use bytes::{Buf, Bytes, BytesMut};

pub const MAGIC: u32 = 0x464C5831;
pub const VERSION: u16 = 1;

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
}
