// On-disk encoding for durable log segments.
//
// The format is deliberately independent of `felix-wire`: network framing is
// allowed to change shape for latency reasons, while bytes already on disk must
// stay readable by every later build. What the two share is the convention —
// big-endian integers, a magic/version prefix, an explicit length — not code.
//
// The authoritative description lives in `docs/storage-format.md`; this module
// is the implementation of that document and the two must move together.

use bytes::Bytes;
use std::fmt;

use crate::log::{Offset, SegmentId};

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

/// Where a corruption was found, for operator-facing diagnostics.
///
/// Recovery reports the shard and segment it was reading; the low-level decoder
/// only knows byte positions, so it leaves those `None` and lets the caller fill
/// them in with [`Corruption::in_segment`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CorruptionSite {
    pub shard: Option<String>,
    pub segment: Option<SegmentId>,
    /// Byte position within the segment file where decoding failed.
    pub position: Option<u64>,
}

/// A typed decode failure. Every variant names the specific invariant that was
/// violated so a failure is actionable without a hex dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptionKind {
    SegmentMagic {
        found: u32,
    },
    SegmentVersion {
        found: u16,
    },
    /// Unknown flag bits are rejected rather than masked off: a bit we do not
    /// understand may change how the records behind it are laid out.
    SegmentFlags {
        found: u16,
    },
    SegmentHeaderChecksum {
        expected: u32,
        found: u32,
    },
    IndexMagic {
        found: u32,
    },
    IndexVersion {
        found: u16,
    },
    /// A header was cut short. Distinct from a record checksum failure because a
    /// truncated *tail* is the expected outcome of a crash mid-append, and
    /// recovery repairs it instead of refusing to start.
    Truncated {
        needed: u64,
        available: u64,
    },
    RecordChecksum {
        expected: u32,
        found: u32,
    },
    /// The record header did not verify against its own checksum, so
    /// `payload_len` cannot be trusted.
    ///
    /// This is what makes a torn write distinguishable from bit rot. A header
    /// that verifies means the length is real, so a payload short of it was
    /// provably never finished; a header that does not verify could be a
    /// complete, acknowledged record whose length field rotted, and recovery
    /// must not guess.
    RecordHeaderChecksum {
        expected: u32,
        found: u32,
    },
    RecordTooLarge {
        payload_len: u32,
        limit: u32,
    },
    /// Offsets must ascend by exactly one across a segment; a gap means a record
    /// was lost or the file was spliced.
    OffsetOutOfOrder {
        expected: Offset,
        found: Offset,
    },
}

impl fmt::Display for CorruptionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionKind::SegmentMagic { found } => {
                write!(f, "bad segment magic {found:#010x}")
            }
            CorruptionKind::SegmentVersion { found } => {
                write!(f, "unsupported segment version {found}")
            }
            CorruptionKind::SegmentFlags { found } => {
                write!(f, "unknown segment flags {found:#06x}")
            }
            CorruptionKind::SegmentHeaderChecksum { expected, found } => write!(
                f,
                "segment header checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::IndexMagic { found } => write!(f, "bad index magic {found:#010x}"),
            CorruptionKind::IndexVersion { found } => {
                write!(f, "unsupported index version {found}")
            }
            CorruptionKind::Truncated { needed, available } => {
                write!(f, "truncated: needed {needed} bytes, {available} available")
            }
            CorruptionKind::RecordChecksum { expected, found } => write!(
                f,
                "record checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::RecordHeaderChecksum { expected, found } => write!(
                f,
                "record header checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::RecordTooLarge { payload_len, limit } => {
                write!(f, "record payload {payload_len} exceeds limit {limit}")
            }
            CorruptionKind::OffsetOutOfOrder { expected, found } => {
                write!(
                    f,
                    "offset out of order (expected {expected}, found {found})"
                )
            }
        }
    }
}

/// A [`CorruptionKind`] plus the location it was found at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Corruption {
    pub kind: CorruptionKind,
    pub site: CorruptionSite,
}

impl Corruption {
    pub fn new(kind: CorruptionKind) -> Self {
        Self {
            kind,
            site: CorruptionSite::default(),
        }
    }

    /// Attach the shard/segment the decoder had no way to know about.
    pub fn in_segment(mut self, shard: impl fmt::Display, segment: SegmentId) -> Self {
        self.site.shard = Some(shard.to_string());
        self.site.segment = Some(segment);
        self
    }

    /// Attach the byte position, unless a nested call already recorded a more
    /// specific one.
    pub fn at_position(mut self, position: u64) -> Self {
        self.site.position.get_or_insert(position);
        self
    }

    /// True when the failure is consistent with a write that was interrupted
    /// part-way, which is what recovery is allowed to truncate.
    pub fn is_truncation(&self) -> bool {
        matches!(self.kind, CorruptionKind::Truncated { .. })
    }
}

impl fmt::Display for Corruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        let mut sep = " (";
        if let Some(shard) = &self.site.shard {
            write!(f, "{sep}shard={shard}")?;
            sep = ", ";
        }
        if let Some(segment) = self.site.segment {
            write!(f, "{sep}segment={segment}")?;
            sep = ", ";
        }
        if let Some(position) = self.site.position {
            write!(f, "{sep}position={position}")?;
            sep = ", ";
        }
        if sep == ", " {
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// Decode results carry a [`Corruption`] rather than an I/O error: these
/// functions operate on bytes already in memory.
pub type DecodeResult<T> = std::result::Result<T, Corruption>;

fn truncated(needed: u64, available: u64) -> Corruption {
    Corruption::new(CorruptionKind::Truncated { needed, available })
}

fn crc32(parts: &[&[u8]]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_header_round_trip() {
        let header = SegmentHeader::new(42, 1_700_000_000_000_000);
        let decoded = SegmentHeader::decode(&header.encode()).expect("decode");
        assert_eq!(decoded, header);
    }

    #[test]
    fn segment_header_is_a_stable_golden_vector() {
        // Pinned bytes: a change here is a format change and must bump the
        // version and update docs/storage-format.md.
        let bytes = SegmentHeader::new(1, 2).encode();
        assert_eq!(
            bytes,
            [
                0x46, 0x4C, 0x53, 0x47, // magic "FLSG"
                0x00, 0x02, // version
                0x00, 0x00, // flags
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // base_offset
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // created_at_micros
                0x7A, 0x09, 0xE5, 0xB1, // header crc32
                0x00, 0x00, 0x00, 0x00, // reserved
            ]
        );
    }

    #[test]
    fn record_is_a_stable_golden_vector() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 7, 9, b"hi");
        assert_eq!(
            buf,
            vec![
                0x00, 0x00, 0x00, 0x02, // payload_len
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // timestamp
                0xC6, 0x54, 0xDE, 0x27, // header crc32 (bytes 0..20)
                0x24, 0x02, 0x15, 0x2C, // checksum (bytes 0..24 + payload)
                b'h', b'i',
            ]
        );
    }

    #[test]
    fn record_round_trips_every_field() {
        let mut buf = Vec::new();
        let written = encode_record(&mut buf, 9, 1234, b"payload");
        let (decoded, consumed) = decode_record(&buf).expect("decode");
        assert_eq!(written, consumed);
        assert_eq!(consumed, buf.len() as u64);
        assert_eq!(decoded.header.offset, 9);
        assert_eq!(decoded.header.timestamp_micros, 1234);
        assert_eq!(decoded.header.payload_len, 7);
        assert_eq!(decoded.payload, Bytes::from_static(b"payload"));
    }

    #[test]
    fn zero_length_payload_round_trips() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 0, 0, b"");
        let (decoded, consumed) = decode_record(&buf).expect("decode");
        assert_eq!(consumed, RECORD_HEADER_LEN);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn records_decode_back_to_back() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 0, 1, b"a");
        encode_record(&mut buf, 1, 2, b"bb");
        let (first, consumed) = decode_record(&buf).expect("first");
        assert_eq!(first.header.offset, 0);
        let (second, _) = decode_record(&buf[consumed as usize..]).expect("second");
        assert_eq!(second.header.offset, 1);
        assert_eq!(second.payload, Bytes::from_static(b"bb"));
    }

    #[test]
    fn truncation_at_every_boundary_reports_truncated() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 3, 4, b"abcd");
        for cut in 0..buf.len() {
            let err = decode_record(&buf[..cut]).expect_err("short buffer");
            assert!(err.is_truncation(), "cut {cut} gave {err}");
        }
        assert!(decode_record(&buf).is_ok());
    }

    #[test]
    fn payload_corruption_fails_the_checksum() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 0, 0, b"payload");
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        let err = decode_record(&buf).expect_err("corrupt payload");
        assert!(matches!(err.kind, CorruptionKind::RecordChecksum { .. }));
        assert!(!err.is_truncation());
    }

    #[test]
    fn header_corruption_fails_the_header_checksum() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 0, 0, b"payload");
        // Flip a bit in the timestamp. The header checksum covers it, so this
        // is caught without reading the payload at all.
        buf[12] ^= 0x01;
        let err = decode_record(&buf).expect_err("corrupt header");
        assert!(matches!(
            err.kind,
            CorruptionKind::RecordHeaderChecksum { .. }
        ));
    }

    #[test]
    fn a_corrupt_length_is_caught_by_the_header_checksum() {
        let mut buf = Vec::new();
        encode_record(&mut buf, 4, 5, b"payload");
        // Damage only the length field. Before the header checksum this was
        // indistinguishable from an unfinished write: the claimed extent ran
        // past the data, so recovery truncated an acknowledged record. Now the
        // header itself reports the damage.
        buf[0..4].copy_from_slice(&9_999u32.to_be_bytes());
        let err = decode_record(&buf).expect_err("corrupt length");
        assert!(
            matches!(err.kind, CorruptionKind::RecordHeaderChecksum { .. }),
            "expected a header checksum failure, got {err}"
        );
    }

    #[test]
    fn oversized_length_is_rejected_before_allocating() {
        // A header whose length is impossible but whose checksum is valid: the
        // shape that would reach an allocation if the bound were not checked.
        let mut buf = vec![0u8; RECORD_HEADER_LEN as usize];
        buf[0..4].copy_from_slice(&u32::MAX.to_be_bytes());
        let header_crc = crc32(&[&buf[0..20]]);
        buf[20..24].copy_from_slice(&header_crc.to_be_bytes());

        let err = decode_record(&buf).expect_err("oversized");
        assert!(
            matches!(
                err.kind,
                CorruptionKind::RecordTooLarge {
                    payload_len: u32::MAX,
                    limit: MAX_PAYLOAD_BYTES,
                }
            ),
            "got {err}"
        );
    }

    #[test]
    fn a_garbage_header_is_rejected_before_its_length_is_believed() {
        // Random bytes almost never carry a valid header checksum, so the
        // length they happen to encode is never acted on.
        let buf = vec![0u8; RECORD_HEADER_LEN as usize];
        let err = decode_record(&buf).expect_err("garbage");
        assert!(matches!(
            err.kind,
            CorruptionKind::RecordHeaderChecksum { .. }
        ));
    }

    #[test]
    fn max_size_payload_round_trips() {
        let payload = vec![0xA5u8; MAX_PAYLOAD_BYTES as usize];
        let mut buf = Vec::new();
        encode_record(&mut buf, 0, 0, &payload);
        let (decoded, _) = decode_record(&buf).expect("decode");
        assert_eq!(decoded.header.payload_len, MAX_PAYLOAD_BYTES);
        assert_eq!(decoded.payload.len(), payload.len());
    }

    #[test]
    fn a_header_alone_locates_the_next_record() {
        let mut buf = Vec::new();
        let written = encode_record(&mut buf, 0, 0, b"some payload");
        // Only the header bytes are available, yet the next position is known
        // without ever touching the payload.
        let header = RecordHeader::decode(&buf[..RECORD_HEADER_LEN as usize]).expect("header");
        assert_eq!(header.encoded_len(), written);
    }

    #[test]
    fn segment_header_rejects_bad_magic_version_and_flags() {
        let good = SegmentHeader::new(0, 0).encode();

        let mut bad = good;
        bad[0] ^= 0xFF;
        assert!(matches!(
            SegmentHeader::decode(&bad).expect_err("magic").kind,
            CorruptionKind::SegmentMagic { .. }
        ));

        let mut bad = good;
        bad[4..6].copy_from_slice(&99u16.to_be_bytes());
        assert!(matches!(
            SegmentHeader::decode(&bad).expect_err("version").kind,
            CorruptionKind::SegmentVersion { found: 99 }
        ));

        let mut bad = good;
        bad[6..8].copy_from_slice(&1u16.to_be_bytes());
        assert!(matches!(
            SegmentHeader::decode(&bad).expect_err("flags").kind,
            CorruptionKind::SegmentFlags { found: 1 }
        ));

        let mut bad = good;
        bad[8] ^= 0xFF;
        assert!(matches!(
            SegmentHeader::decode(&bad).expect_err("checksum").kind,
            CorruptionKind::SegmentHeaderChecksum { .. }
        ));
    }

    #[test]
    fn segment_header_truncation_is_reported_as_truncation() {
        let good = SegmentHeader::new(0, 0).encode();
        for cut in 0..good.len() {
            assert!(
                SegmentHeader::decode(&good[..cut])
                    .expect_err("short")
                    .is_truncation()
            );
        }
    }

    #[test]
    fn index_header_and_entry_round_trip() {
        let header = IndexHeader { base_offset: 17 };
        assert_eq!(IndexHeader::decode(&header.encode()).expect("hdr"), header);

        let entry = IndexEntry {
            offset: 5,
            position: 64,
        };
        assert_eq!(IndexEntry::decode(&entry.encode()).expect("entry"), entry);
    }

    #[test]
    fn index_header_rejects_bad_magic_and_version() {
        let good = IndexHeader { base_offset: 0 }.encode();
        let mut bad = good;
        bad[0] ^= 0xFF;
        assert!(matches!(
            IndexHeader::decode(&bad).expect_err("magic").kind,
            CorruptionKind::IndexMagic { .. }
        ));
        let mut bad = good;
        bad[4..6].copy_from_slice(&7u16.to_be_bytes());
        assert!(matches!(
            IndexHeader::decode(&bad).expect_err("version").kind,
            CorruptionKind::IndexVersion { found: 7 }
        ));
    }

    #[test]
    fn offset_continuity_is_checked() {
        assert!(check_offset_continuity(4, 4).is_ok());
        let err = check_offset_continuity(4, 6).expect_err("gap");
        assert!(matches!(
            err.kind,
            CorruptionKind::OffsetOutOfOrder {
                expected: 4,
                found: 6
            }
        ));
    }

    #[test]
    fn corruption_display_includes_the_site() {
        let err = Corruption::new(CorruptionKind::RecordChecksum {
            expected: 1,
            found: 2,
        })
        .in_segment("t/ns/s/0", 3)
        .at_position(128);
        let rendered = err.to_string();
        assert!(rendered.contains("shard=t/ns/s/0"), "{rendered}");
        assert!(rendered.contains("segment=3"), "{rendered}");
        assert!(rendered.contains("position=128"), "{rendered}");
    }

    #[test]
    fn at_position_keeps_the_innermost_site() {
        let err = Corruption::new(CorruptionKind::Truncated {
            needed: 8,
            available: 2,
        })
        .at_position(10)
        .at_position(999);
        assert_eq!(err.site.position, Some(10));
    }

    #[test]
    fn corruption_display_without_a_site_has_no_parentheses() {
        let err = Corruption::new(CorruptionKind::IndexVersion { found: 2 });
        assert_eq!(err.to_string(), "unsupported index version 2");
    }
}
