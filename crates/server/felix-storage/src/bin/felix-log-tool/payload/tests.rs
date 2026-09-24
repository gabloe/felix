use super::*;

#[test]
fn a_payload_is_padded_to_the_requested_length() {
    assert_eq!(payload_for(0, 128).len(), 128);
    assert_eq!(payload_for(u64::MAX, 512).len(), 512);
}

#[test]
fn a_short_request_still_carries_the_full_marker() {
    // The marker cannot be truncated or the offset would be unrecoverable.
    let payload = payload_for(7, 1);
    assert_eq!(payload.len(), MARKER_BYTES);
    assert_eq!(claimed_offset(&payload), Some(7));
}

#[test]
fn payloads_round_trip_through_their_offset() {
    for offset in [0u64, 1, 42, 1_000_000, u64::MAX] {
        let payload = payload_for(offset, 64);
        assert_eq!(claimed_offset(&payload), Some(offset));
        assert!(matches(offset, 64, &payload));
    }
}

#[test]
fn payloads_are_deterministic() {
    assert_eq!(payload_for(9, 100), payload_for(9, 100));
}

#[test]
fn neighbouring_offsets_produce_different_bytes() {
    assert_ne!(payload_for(9, 64), payload_for(10, 64));
}

#[test]
fn a_single_flipped_byte_fails_the_check() {
    let mut payload = payload_for(5, 64);
    let last = payload.len() - 1;
    payload[last] ^= 0xFF;
    assert!(!matches(5, 64, &payload));
}

#[test]
fn foreign_payloads_claim_no_offset() {
    assert_eq!(claimed_offset(b"hello"), None);
    assert_eq!(claimed_offset(&[0xFFu8; 64]), None);
    assert_eq!(claimed_offset(b""), None);
}
