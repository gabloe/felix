//! On-disk encoding for durable log segments.
//!
//! The format is deliberately independent of `felix-wire`: network framing is
//! allowed to change shape for latency reasons, while bytes already on disk must
//! stay readable by every later build. What the two share is the convention —
//! big-endian integers, a magic/version prefix, an explicit length — not code.
//!
//! The authoritative description lives in `docs/storage-format.md`; this module
//! is the implementation of that document and the two must move together.

use bytes::Bytes;

use crate::log::Offset;
use crate::{Corruption, CorruptionKind};

/// `"FLSG"` — **F**e**L**ix **S**e**g**ment. Identifies a segment data file.
///
/// Deliberately distinct from `felix-wire`'s frame magic `"FLX1"`: storage bytes
/// and network bytes are separate formats with separate versioning, and either
/// turning up where the other belongs should be rejected on the first four bytes
/// rather than misparsed.
pub const SEGMENT_MAGIC: u32 = 0x464C_5347;
/// `"FLSI"` — **F**e**L**ix **S**egment **I**ndex. Identifies a sparse index file.
pub const INDEX_MAGIC: u32 = 0x464C_5349;
/// Version of both the segment and index layouts described here.
///
/// v2 added a record header checksum. v1 segments are rejected on open with
/// `CorruptionKind::SegmentVersion`, naming the version found — the format was
/// only ever written by unreleased builds, so the migration path is to discard
/// the data directory rather than to carry a second decoder. Failing loudly is
/// the point: a v1 record read as v2 would misparse every field.
pub const FORMAT_VERSION: u16 = 2;

/// Bytes occupied by a segment file header.
pub const SEGMENT_HEADER_LEN: u64 = 32;
/// Bytes occupied by a record header, excluding its payload.
pub const RECORD_HEADER_LEN: u64 = 28;
/// Bytes occupied by an index file header.
pub const INDEX_HEADER_LEN: u64 = 24;
/// Bytes occupied by a single sparse index entry.
pub const INDEX_ENTRY_LEN: u64 = 16;

/// Ceiling on a single record's payload, enforced before any allocation is made
/// on behalf of a length field read from disk. A corrupt `payload_len` is
/// otherwise a request to allocate up to 4 GiB.
pub const MAX_PAYLOAD_BYTES: u32 = 64 * 1024 * 1024;

/// Decode results carry a [`Corruption`] rather than an I/O error: these
/// functions operate on bytes already in memory.
pub type DecodeResult<T> = std::result::Result<T, Corruption>;

/// The fixed prefix of a segment data file.
///
/// ```text
///  0   4  magic              u32  "FLSG"
///  4   2  version            u16
///  6   2  flags              u16
///  8   8  base_offset        u64  logical offset of the segment's first record
/// 16   8  created_at_micros  u64
/// 24   4  header_crc         u32  crc32 over bytes 0..24
/// 28   4  reserved           u32  must be zero
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub base_offset: Offset,
    pub created_at_micros: u64,
    pub flags: u16,
}

impl SegmentHeader {
    pub fn new(base_offset: Offset, created_at_micros: u64) -> Self {
        Self {
            base_offset,
            created_at_micros,
            flags: 0,
        }
    }

    pub fn encode(&self) -> [u8; SEGMENT_HEADER_LEN as usize] {
        let mut buf = [0u8; SEGMENT_HEADER_LEN as usize];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..16].copy_from_slice(&self.base_offset.to_be_bytes());
        buf[16..24].copy_from_slice(&self.created_at_micros.to_be_bytes());
        let checksum = crc32(&[&buf[0..24]]);
        buf[24..28].copy_from_slice(&checksum.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < SEGMENT_HEADER_LEN {
            return Err(truncated(SEGMENT_HEADER_LEN, buf.len() as u64));
        }
        let magic = read_u32(buf, 0);
        if magic != SEGMENT_MAGIC {
            return Err(Corruption::new(CorruptionKind::SegmentMagic {
                found: magic,
            }));
        }
        let version = read_u16(buf, 4);
        if version != FORMAT_VERSION {
            return Err(Corruption::new(CorruptionKind::SegmentVersion {
                found: version,
            }));
        }
        let flags = read_u16(buf, 6);
        if flags != 0 {
            return Err(Corruption::new(CorruptionKind::SegmentFlags {
                found: flags,
            }));
        }
        let expected = read_u32(buf, 24);
        let found = crc32(&[&buf[0..24]]);
        if expected != found {
            return Err(Corruption::new(CorruptionKind::SegmentHeaderChecksum {
                expected,
                found,
            }));
        }
        Ok(Self {
            base_offset: read_u64(buf, 8),
            created_at_micros: read_u64(buf, 16),
            flags,
        })
    }
}

/// The fixed-size prefix of a record.
///
/// ```text
///  0   4  payload_len        u32
///  4   8  offset             u64  logical offset of this record
/// 12   8  timestamp_micros   u64
/// 20   4  checksum           u32  crc32 over bytes 0..20 followed by the payload
/// 24   n  payload
/// ```
///
/// `payload_len` sits first and is covered by the checksum, so a reader can walk
/// to the next record with `position + RECORD_HEADER_LEN + payload_len` without
/// looking at payload bytes at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub payload_len: u32,
    pub offset: Offset,
    pub timestamp_micros: u64,
    /// CRC-32 over bytes `0..20`. Validated before `payload_len` is used for
    /// anything, which is what lets recovery tell an unfinished write from a
    /// rotted length field.
    pub header_crc: u32,
    pub checksum: u32,
}

impl RecordHeader {
    /// Total bytes this record occupies on disk, header included.
    pub fn encoded_len(&self) -> u64 {
        RECORD_HEADER_LEN + u64::from(self.payload_len)
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < RECORD_HEADER_LEN {
            return Err(truncated(RECORD_HEADER_LEN, buf.len() as u64));
        }
        // The header checksum is verified first, before any field is used.
        // Everything downstream — the payload length, the offset, how far to
        // step to the next record — is only meaningful once the header is known
        // to be intact.
        let header_crc = read_u32(buf, 20);
        let found = crc32(&[&buf[0..20]]);
        if header_crc != found {
            return Err(Corruption::new(CorruptionKind::RecordHeaderChecksum {
                expected: header_crc,
                found,
            }));
        }
        let payload_len = read_u32(buf, 0);
        if payload_len > MAX_PAYLOAD_BYTES {
            return Err(Corruption::new(CorruptionKind::RecordTooLarge {
                payload_len,
                limit: MAX_PAYLOAD_BYTES,
            }));
        }
        Ok(Self {
            payload_len,
            offset: read_u64(buf, 4),
            timestamp_micros: read_u64(buf, 12),
            header_crc,
            checksum: read_u32(buf, 24),
        })
    }
}

/// Serialize one record into `out`, returning the bytes appended.
///
/// Callers batch many of these into a single buffer so that one `write_all`
/// covers a whole append.
pub fn encode_record(
    out: &mut Vec<u8>,
    offset: Offset,
    timestamp_micros: u64,
    payload: &[u8],
) -> u64 {
    debug_assert!(payload.len() <= MAX_PAYLOAD_BYTES as usize);
    let start = out.len();
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(&timestamp_micros.to_be_bytes());
    // Header checksum first, so a reader can trust `payload_len` without having
    // read the payload it describes.
    let header_crc = crc32(&[&out[start..start + 20]]);
    out.extend_from_slice(&header_crc.to_be_bytes());
    let checksum = crc32(&[&out[start..start + 24], payload]);
    out.extend_from_slice(&checksum.to_be_bytes());
    out.extend_from_slice(payload);
    (out.len() - start) as u64
}

/// A decoded record together with the bytes it consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub header: RecordHeader,
    pub payload: Bytes,
}

/// Decode the record at the front of `buf`, verifying its checksum.
///
/// Returns the record and the number of bytes consumed. A `Truncated` error
/// means "ask for more bytes"; every other error means the bytes present are
/// not a valid record.
pub fn decode_record(buf: &[u8]) -> DecodeResult<(DecodedRecord, u64)> {
    let header = RecordHeader::decode(buf)?;
    let total = header.encoded_len();
    if (buf.len() as u64) < total {
        return Err(truncated(total, buf.len() as u64));
    }
    let payload = &buf[RECORD_HEADER_LEN as usize..total as usize];
    let found = crc32(&[&buf[0..24], payload]);
    if found != header.checksum {
        return Err(Corruption::new(CorruptionKind::RecordChecksum {
            expected: header.checksum,
            found,
        }));
    }
    Ok((
        DecodedRecord {
            header,
            payload: Bytes::copy_from_slice(payload),
        },
        total,
    ))
}

/// Header of a sparse index file.
///
/// ```text
///  0   4  magic         u32  "FLSI"
///  4   2  version       u16
///  6   2  flags         u16
///  8   8  base_offset   u64  must match the segment it describes
/// 16   8  reserved      u64
/// ```
///
/// Index files carry no checksums. They are a pure accelerator: every entry is
/// verified against the segment on use, and a missing or stale index is rebuilt
/// from the segment rather than trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHeader {
    pub base_offset: Offset,
}

impl IndexHeader {
    pub fn encode(&self) -> [u8; INDEX_HEADER_LEN as usize] {
        let mut buf = [0u8; INDEX_HEADER_LEN as usize];
        buf[0..4].copy_from_slice(&INDEX_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        buf[8..16].copy_from_slice(&self.base_offset.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < INDEX_HEADER_LEN {
            return Err(truncated(INDEX_HEADER_LEN, buf.len() as u64));
        }
        let magic = read_u32(buf, 0);
        if magic != INDEX_MAGIC {
            return Err(Corruption::new(CorruptionKind::IndexMagic { found: magic }));
        }
        let version = read_u16(buf, 4);
        if version != FORMAT_VERSION {
            return Err(Corruption::new(CorruptionKind::IndexVersion {
                found: version,
            }));
        }
        Ok(Self {
            base_offset: read_u64(buf, 8),
        })
    }
}

/// One sparse index entry: `offset` starts a record at byte `position`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: Offset,
    pub position: u64,
}

impl IndexEntry {
    pub fn encode(&self) -> [u8; INDEX_ENTRY_LEN as usize] {
        let mut buf = [0u8; INDEX_ENTRY_LEN as usize];
        buf[0..8].copy_from_slice(&self.offset.to_be_bytes());
        buf[8..16].copy_from_slice(&self.position.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < INDEX_ENTRY_LEN {
            return Err(truncated(INDEX_ENTRY_LEN, buf.len() as u64));
        }
        Ok(Self {
            offset: read_u64(buf, 0),
            position: read_u64(buf, 8),
        })
    }
}

/// Verify that `found` continues the sequence at `expected`.
pub fn check_offset_continuity(expected: Offset, found: Offset) -> DecodeResult<()> {
    if expected == found {
        Ok(())
    } else {
        Err(Corruption::new(CorruptionKind::OffsetOutOfOrder {
            expected,
            found,
        }))
    }
}

pub(crate) fn crc32(parts: &[&[u8]]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

fn truncated(needed: u64, available: u64) -> Corruption {
    Corruption::new(CorruptionKind::Truncated { needed, available })
}

fn read_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([buf[at], buf[at + 1]])
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]])
}

fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_be_bytes([
        buf[at],
        buf[at + 1],
        buf[at + 2],
        buf[at + 3],
        buf[at + 4],
        buf[at + 5],
        buf[at + 6],
        buf[at + 7],
    ])
}

#[cfg(test)]
mod tests;
