//! Record decoding against arbitrary bytes.
//!
//! Once M5 lands, segment bytes arrive over the network from a peer, so the
//! decoder is an attack surface: it must never panic, never allocate on the
//! strength of an unvalidated length, and never report a record it did not
//! actually verify.

#![no_main]

use libfuzzer_sys::fuzz_target;

use felix_storage::segment::format::{RECORD_HEADER_LEN, decode_record, encode_record};

fuzz_target!(|data: &[u8]| {
    // Property 1: arbitrary bytes decode or error, but never panic or hang.
    if let Ok((decoded, consumed)) = decode_record(data) {
        // Property 2: a successful decode reports exactly the bytes it used, so
        // a caller walking a segment cannot be walked off a cliff.
        assert_eq!(consumed, RECORD_HEADER_LEN + decoded.payload.len() as u64);
        assert!(consumed as usize <= data.len());

        // Property 3: re-encoding what was decoded reproduces the same bytes.
        // A decoder that accepted a record it could not have written would mean
        // two byte sequences map to one record, which breaks checksum-based
        // replication comparisons.
        let mut re_encoded = Vec::new();
        encode_record(
            &mut re_encoded,
            decoded.header.offset,
            decoded.header.timestamp_micros,
            &decoded.payload,
        );
        assert_eq!(re_encoded.as_slice(), &data[..consumed as usize]);
    }
});
