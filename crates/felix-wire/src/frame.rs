// Frame header layout and framing primitives for the v1 wire protocol.

use crate::error::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};

pub const MAGIC: u32 = 0x464C5831;
pub const VERSION: u16 = 1;
// Flags describe how to interpret the frame payload.
pub const FLAG_BINARY_PUBLISH_BATCH: u16 = 0x0001;
pub const FLAG_BINARY_EVENT_BATCH: u16 = 0x0002;
pub const FLAG_BINARY_EVENT_BATCH_SHARED: u16 = 0x0004;
/// Modifier on `FLAG_BINARY_PUBLISH_BATCH`: the payload is prefixed with a
/// `request_id` and an ack mode, and the broker owes the client an ack frame.
/// Never meaningful on its own — see [`crate::binary::decode_acked_publish_batch`].
pub const FLAG_BINARY_PUBLISH_ACKED: u16 = 0x0008;
/// Broker → client acknowledgement of an acked publish, replacing the JSON
/// `PublishOk`/`PublishError` messages on the binary path.
pub const FLAG_BINARY_PUBLISH_ACK: u16 = 0x0010;
/// Modifier on either event-batch flag: the payload carries a `base_offset`
/// before its payload count, giving the log offset of the batch's first event.
///
/// A batch's offsets are contiguous, so one `u64` per *batch* is enough — a
/// client derives each event's offset by adding its index. That is what keeps
/// this off the per-event cost model, and it is why offsets can ride the shared
/// encode-once batch at all: the offsets belong to the stream, not to the
/// subscriber, so one encoding still serves every subscriber that negotiated
/// the bit.
pub const FLAG_EVENT_BATCH_OFFSETS: u16 = 0x0020;

/// Every flag bit this version understands.
///
/// Frames carrying bits outside this mask are rejected rather than parsed with
/// the unknown bits ignored. That distinction matters: flag bits here change how
/// the *payload* is laid out, so silently ignoring one means confidently
/// misparsing the body. `FLAG_BINARY_PUBLISH_ACKED` is exactly that case — an
/// older broker that masked it off would read the new `request_id` prefix as a
/// `tenant_len` and produce garbage instead of an error. Rejecting unknown bits
/// cannot help those older brokers, but it means the next extension fails loudly
/// on this version instead of repeating the same trap.
pub const KNOWN_FLAGS: u16 = FLAG_BINARY_PUBLISH_BATCH
    | FLAG_BINARY_EVENT_BATCH
    | FLAG_BINARY_EVENT_BATCH_SHARED
    | FLAG_BINARY_PUBLISH_ACKED
    | FLAG_BINARY_PUBLISH_ACK
    | FLAG_EVENT_BATCH_OFFSETS;

/// The flag bits that existed before capability negotiation.
///
/// This is what a peer must be assumed to support when it does not advertise a
/// mask: brokers predating negotiation answer `Auth` with a plain `Ok`, and the
/// only safe reading of that silence is "the original three bits and nothing
/// more". Deliberately frozen — new bits must never be added here, or clients
/// will start assuming support that old brokers do not have.
pub const ORIGINAL_V1_FLAGS: u16 =
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_EVENT_BATCH | FLAG_BINARY_EVENT_BATCH_SHARED;

/// True if `flags` contains any bit this version does not define.
pub fn has_unknown_flags(flags: u16) -> bool {
    flags & !KNOWN_FLAGS != 0
}

/// True if `peer_flags` advertises support for every bit in `required`.
pub fn supports(peer_flags: u16, required: u16) -> bool {
    peer_flags & required == required
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
