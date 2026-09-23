//! Deterministic record payloads.
//!
//! Crash tests need to answer "is record 4,271 the record the writer said it
//! wrote, or a byte-identical neighbour?" — so payloads carry their own offset
//! and a checksum-friendly filler derived from it. Any surviving record can then
//! be validated against nothing but its position in the log, with no side table
//! to keep in sync.

/// Smallest payload that still carries a full offset marker: `felix-record-`
/// plus a fixed-width offset plus the trailing separator.
///
/// The offset is padded to 20 digits so that `u64::MAX` fits without widening
/// the marker — a variable-width marker could not be parsed back from a prefix.
const OFFSET_DIGITS: usize = 20;
const MARKER_BYTES: usize = 13 + OFFSET_DIGITS + 1;

/// Build the payload for `offset`, padded to `len` bytes.
///
/// Layout: `felix-record-<offset:020>-` followed by a repeating byte pattern
/// seeded from the offset.
pub(crate) fn payload_for(offset: u64, len: usize) -> Vec<u8> {
    let marker = format!(
        "felix-record-{offset:0OFFSET_DIGITS$}-",
        OFFSET_DIGITS = OFFSET_DIGITS
    );
    let len = len.max(marker.len());
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(marker.as_bytes());
    // A pattern rather than zeros: zero-filled payloads would survive several
    // classes of corruption unnoticed.
    let seed = (offset % 251) as u8;
    while out.len() < len {
        out.push(seed.wrapping_add(out.len() as u8));
    }
    out
}

/// Check that `payload` is exactly what `payload_for(offset, len)` produces.
pub(crate) fn matches(offset: u64, len: usize, payload: &[u8]) -> bool {
    payload_for(offset, len) == payload
}

/// Recover the offset a payload claims, or `None` if it is not one of ours.
pub(crate) fn claimed_offset(payload: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(payload.get(..MARKER_BYTES)?).ok()?;
    text.strip_prefix("felix-record-")?
        .trim_end_matches('-')
        .parse()
        .ok()
}

#[cfg(test)]
mod tests;
