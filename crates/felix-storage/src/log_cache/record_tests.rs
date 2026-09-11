//! The durable record format.
//!
//! Worth testing byte-for-byte: these bytes outlive the process that wrote
//! them, and a format that only round-trips within one build has proven
//! nothing about the build that reads it back.
use super::*;

#[test]
fn a_put_round_trips() {
    let op = CacheOp::Put {
        key: "session:42".to_string(),
        value: Bytes::from_static(b"hello"),
        expires_at_millis: 1_700_000_000_000,
    };
    assert_eq!(CacheOp::decode(&op.encode()).expect("decode"), op);
}

#[test]
fn a_delete_round_trips() {
    let op = CacheOp::Delete {
        key: "session:42".to_string(),
    };
    assert_eq!(CacheOp::decode(&op.encode()).expect("decode"), op);
}

/// An empty value is a value, not an absence. Only a tombstone means absent,
/// and the two must not collapse into each other.
#[test]
fn an_empty_value_is_not_a_delete() {
    let op = CacheOp::Put {
        key: "k".to_string(),
        value: Bytes::new(),
        expires_at_millis: 0,
    };
    let decoded = CacheOp::decode(&op.encode()).expect("decode");
    assert_eq!(decoded, op);
    assert!(matches!(decoded, CacheOp::Put { .. }));
}

#[test]
fn a_key_may_hold_any_utf8() {
    let op = CacheOp::Put {
        key: "ключ/例え:🔑".to_string(),
        value: Bytes::from_static(b"v"),
        expires_at_millis: 0,
    };
    assert_eq!(CacheOp::decode(&op.encode()).expect("decode"), op);
}

/// **The layout is pinned, not just self-consistent.** A round-trip test alone
/// would let both sides move together and still call it a pass; a build reading
/// bytes written by another build would then find a plausible wrong answer.
#[test]
fn the_encoding_is_the_documented_layout() {
    let encoded = CacheOp::Put {
        key: "ab".to_string(),
        value: Bytes::from_static(b"xyz"),
        expires_at_millis: 1,
    }
    .encode();

    assert_eq!(encoded[0], VERSION, "version leads the record");
    assert_eq!(encoded[1], 0, "0 is a put");
    assert_eq!(
        &encoded[2..10],
        &1u64.to_le_bytes(),
        "expiry is little-endian absolute milliseconds"
    );
    assert_eq!(
        &encoded[10..14],
        &2u32.to_le_bytes(),
        "key length is little-endian"
    );
    assert_eq!(&encoded[14..16], b"ab");
    assert_eq!(
        &encoded[16..],
        b"xyz",
        "the value is the rest of the record"
    );
}

/// A record from a later build is refused, not read with this build's layout.
/// Guessing would produce a plausible wrong value where an error is honest.
#[test]
fn a_future_version_is_refused() {
    let mut encoded = CacheOp::Put {
        key: "k".to_string(),
        value: Bytes::from_static(b"v"),
        expires_at_millis: 0,
    }
    .encode()
    .to_vec();
    encoded[0] = VERSION + 1;

    let err = CacheOp::decode(&Bytes::from(encoded)).expect_err("a later version is unreadable");
    assert!(
        err.to_string().contains("not readable by this build"),
        "unhelpful message: {err}"
    );
}

#[test]
fn an_unknown_op_is_refused() {
    let mut encoded = CacheOp::Put {
        key: "k".to_string(),
        value: Bytes::from_static(b"v"),
        expires_at_millis: 0,
    }
    .encode()
    .to_vec();
    encoded[1] = 9;
    assert!(CacheOp::decode(&Bytes::from(encoded)).is_err());
}

#[test]
fn a_record_shorter_than_its_header_is_refused() {
    assert!(CacheOp::decode(&Bytes::from_static(b"short")).is_err());
}

/// A key length larger than the record is refused rather than clamped: it means
/// the bytes are not what they claim, and truncating to fit would invent a key.
#[test]
fn a_key_longer_than_the_record_is_refused() {
    let mut encoded = CacheOp::Put {
        key: "ab".to_string(),
        value: Bytes::new(),
        expires_at_millis: 0,
    }
    .encode()
    .to_vec();
    encoded[10..14].copy_from_slice(&9999u32.to_le_bytes());
    assert!(CacheOp::decode(&Bytes::from(encoded)).is_err());
}

#[test]
fn a_key_that_is_not_utf8_is_refused() {
    let mut encoded = Vec::new();
    encoded.push(VERSION);
    encoded.push(0);
    encoded.extend_from_slice(&0u64.to_le_bytes());
    encoded.extend_from_slice(&2u32.to_le_bytes());
    encoded.extend_from_slice(&[0xff, 0xfe]);
    assert!(CacheOp::decode(&Bytes::from(encoded)).is_err());
}
