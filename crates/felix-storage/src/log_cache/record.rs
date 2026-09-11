//! The on-disk shape of one cache write.
//!
//! Durable, so it is explicit and versioned rather than whatever a serialiser
//! happened to emit. See `docs/cache-on-log.md` for the layout and the
//! reasoning; the short version is that a reader years from now has only these
//! bytes, and a misread record is worse than an unreadable one.
use bytes::{BufMut, Bytes, BytesMut};

use crate::segment::{Corruption, CorruptionKind};

/// The only version this build writes, and the only one it reads.
pub const VERSION: u8 = 1;

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

const OP_AT: usize = 1;
const EXPIRES_AT: usize = 2;
const KEY_LEN_AT: usize = 10;
/// Version, op, expiry, key length. Every record carries all four.
pub const HEADER_LEN: usize = 14;

/// What one record says happened to one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheOp {
    /// This key now has this value, until `expires_at_millis` unless it is zero.
    Put {
        key: String,
        value: Bytes,
        /// Absolute Unix milliseconds; zero means it never expires.
        ///
        /// Absolute rather than a duration, because a record read back after a
        /// restart has to mean what it meant when it was written. A duration
        /// would silently restart its life on every recovery, so an entry with
        /// a one-hour TTL would outlive any number of restarts an hour apart.
        expires_at_millis: u64,
    },
    /// This key is gone.
    ///
    /// A tombstone has to be written rather than the record simply dropped: the
    /// log is the history, and an absence cannot be appended.
    Delete { key: String },
}

impl CacheOp {
    pub fn key(&self) -> &str {
        match self {
            CacheOp::Put { key, .. } | CacheOp::Delete { key } => key,
        }
    }

    pub fn encode(&self) -> Bytes {
        let (op, key, value, expires_at_millis) = match self {
            CacheOp::Put {
                key,
                value,
                expires_at_millis,
            } => (OP_PUT, key, Some(value), *expires_at_millis),
            CacheOp::Delete { key } => (OP_DELETE, key, None, 0),
        };
        let value_len = value.map_or(0, |value| value.len());
        let mut buf = BytesMut::with_capacity(HEADER_LEN + key.len() + value_len);
        buf.put_u8(VERSION);
        buf.put_u8(op);
        buf.put_u64_le(expires_at_millis);
        buf.put_u32_le(key.len() as u32);
        buf.put_slice(key.as_bytes());
        if let Some(value) = value {
            buf.put_slice(value);
        }
        buf.freeze()
    }

    /// Decode one record read back from the log.
    ///
    /// Every failure here means the bytes are not what this build can read, and
    /// none of them are recoverable by guessing — so all of them are errors
    /// rather than a best effort at the remainder.
    pub fn decode(payload: &Bytes) -> Result<Self, Corruption> {
        if payload.len() < HEADER_LEN {
            return Err(Corruption::new(CorruptionKind::CacheRecordTooShort {
                len: payload.len(),
                header_len: HEADER_LEN,
            }));
        }
        let version = payload[0];
        if version != VERSION {
            return Err(Corruption::new(CorruptionKind::CacheRecordVersion {
                found: version,
                expected: VERSION,
            }));
        }
        let expires_at_millis = u64::from_le_bytes(
            payload[EXPIRES_AT..EXPIRES_AT + 8]
                .try_into()
                .expect("eight bytes, length checked above"),
        );
        let key_len = u32::from_le_bytes(
            payload[KEY_LEN_AT..KEY_LEN_AT + 4]
                .try_into()
                .expect("four bytes, length checked above"),
        ) as usize;
        let available = payload.len() - HEADER_LEN;
        if key_len > available {
            return Err(Corruption::new(CorruptionKind::CacheRecordKeyLength {
                claimed: key_len,
                available,
            }));
        }
        let key_end = HEADER_LEN + key_len;
        let key = std::str::from_utf8(&payload[HEADER_LEN..key_end])
            .map_err(|_| Corruption::new(CorruptionKind::CacheRecordKeyNotUtf8))?
            .to_string();

        match payload[OP_AT] {
            OP_PUT => Ok(CacheOp::Put {
                key,
                // The rest of the record. The segment framing already delimits
                // it, so the value needs no length of its own.
                value: payload.slice(key_end..),
                expires_at_millis,
            }),
            OP_DELETE => Ok(CacheOp::Delete { key }),
            found => Err(Corruption::new(CorruptionKind::CacheRecordOp { found })),
        }
    }
}

#[cfg(test)]
#[path = "record_tests.rs"]
mod tests;
