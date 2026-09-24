//! The header every measured payload carries: a sequence number and this
//! process's monotonic nanos at publish time.

use std::time::Instant;

/// Bytes at the front of every payload that the header takes.
const HEADER: usize = 16;

/// A payload of `bytes` bytes (never less than the header) stamped with
/// `seq` and the time since `epoch`.
pub(super) fn payload(seq: u64, epoch: Instant, bytes: usize) -> Vec<u8> {
    let mut body = vec![0u8; HEADER.max(HEADER + bytes.saturating_sub(HEADER))];
    // The header rides *inside* the requested payload size when it fits, so a
    // "256-byte" case moves 256 bytes; only sizes below the header grow.
    let len = bytes.max(HEADER);
    body.truncate(len);
    body[0..8].copy_from_slice(&seq.to_be_bytes());
    body[8..16].copy_from_slice(&(epoch.elapsed().as_nanos() as u64).to_be_bytes());
    body
}

/// The `(seq, nanos since epoch)` a payload was stamped with, or `None` if it
/// is too short to carry them.
pub(super) fn read_header(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() < HEADER {
        return None;
    }
    let seq = u64::from_be_bytes(payload[0..8].try_into().ok()?);
    let t0 = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    Some((seq, t0))
}
