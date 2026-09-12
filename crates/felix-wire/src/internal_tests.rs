//! Round-trips, byte-exact golden vectors, and malformed input.
//!
//! The malformed cases matter most. A peer is authenticated, not assumed
//! correct: every one of these must be an error, and none may panic.
use super::*;

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        generation: 7,
    }
}

fn forward() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 42,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")],
    })
}

fn every_message() -> Vec<InternalMessage> {
    vec![
        forward(),
        InternalMessage::ForwardPublishOk(ForwardPublishOk {
            correlation_id: 42,
            first_offset: 100,
            last_offset: 101,
        }),
        InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: 42,
            code: ErrorCode::StaleRoute,
            detail: "generation 7 is ahead of mine".to_string(),
        }),
        InternalMessage::NotLeader(NotLeader {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
            advertise_addr: "10.0.0.5:7000".to_string(),
            generation: 9,
        }),
        InternalMessage::Hello(Hello {
            correlation_id: 42,
            node_id: "broker-a".to_string(),
        }),
        InternalMessage::HelloOk(HelloOk {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
        }),
        replicate(),
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 42,
            durable_offset: 102,
        }),
        InternalMessage::ReplicateError(ReplicateError {
            correlation_id: 42,
            code: ErrorCode::LogGap,
            expected_offset: 100,
            detail: "batch starts at 105, expected 100".to_string(),
        }),
        InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 42,
            shard: shard(),
            op: CacheOpKind::Put,
            key: "session:abc".to_string(),
            value: Bytes::from_static(b"payload"),
            ttl_ms: 30_000,
        }),
        InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 42,
            shard: shard(),
            op: CacheOpKind::Get,
            key: "session:abc".to_string(),
            value: Bytes::new(),
            ttl_ms: 0,
        }),
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id: 42,
            value: Some(Bytes::from_static(b"payload")),
        }),
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id: 42,
            value: None,
        }),
        InternalMessage::ForwardCacheError(ForwardCacheError {
            correlation_id: 42,
            code: ErrorCode::Unavailable,
            detail: "cache scope not found".to_string(),
        }),
    ]
}

fn replicate() -> InternalMessage {
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
    InternalMessage::ReplicateRecords(ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 100,
        checksum: 0x0102_0304,
        payloads,
    })
}

#[test]
fn every_message_round_trips() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        let decoded = InternalMessage::decode(encoded).expect("decode");
        assert_eq!(decoded, message);
    }
}

#[test]
fn every_message_carries_a_correlation_id() {
    for message in every_message() {
        assert_eq!(
            message.correlation_id(),
            42,
            "a response with no correlation could never be matched or discarded",
        );
    }
}

/// The whole point of a distinct magic: a client frame must not decode here,
/// and an internal frame must not decode as a client frame.
#[test]
fn client_and_internal_frames_do_not_decode_as_each_other() {
    let internal = forward().encode().expect("encode");
    assert!(
        matches!(
            crate::Frame::decode(internal.clone()),
            Err(Error::InvalidMagic)
        ),
        "an internal frame must not parse as a client frame",
    );

    let client = crate::Frame::new(0x1, Bytes::from_static(b"hello"))
        .expect("frame")
        .encode();
    assert!(
        matches!(InternalMessage::decode(client), Err(Error::InvalidMagic)),
        "a client frame must not parse as an internal frame",
    );
}

/// Byte-exact, so a layout change has to be deliberate. Every field is at a
/// known offset; a reordering or a width change fails here rather than in a
/// cluster.
#[test]
fn forward_publish_matches_its_golden_vector() {
    let encoded = forward().encode().expect("encode");
    let expected: Vec<u8> = [
        // header: magic "FLXI", version 1, kind 1, length
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x01][..],
        &[0x00, 0x00, 0x00, 0x3A][..],
        // correlation_id 42
        &[0, 0, 0, 0, 0, 0, 0, 42][..],
        // "t1", "ns", "orders"
        &[0, 0, 0, 2][..],
        b"t1",
        &[0, 0, 0, 2][..],
        b"ns",
        &[0, 0, 0, 6][..],
        b"orders",
        // shard 3, generation 7, ack on-commit
        &[0, 0, 0, 3][..],
        &[0, 0, 0, 0, 0, 0, 0, 7][..],
        &[2][..],
        // two payloads
        &[0, 0, 0, 2][..],
        &[0, 0, 0, 1][..],
        b"a",
        &[0, 0, 0, 2][..],
        b"bb",
    ]
    .concat();
    assert_eq!(encoded.as_ref(), expected.as_slice());
}

#[test]
fn not_leader_matches_its_golden_vector() {
    let message = InternalMessage::NotLeader(NotLeader {
        correlation_id: 1,
        node_id: "b".to_string(),
        advertise_addr: "h:1".to_string(),
        generation: 2,
    });
    let expected: Vec<u8> = [
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x04][..],
        &[0x00, 0x00, 0x00, 0x1C][..],
        &[0, 0, 0, 0, 0, 0, 0, 1][..],
        &[0, 0, 0, 1][..],
        b"b",
        &[0, 0, 0, 3][..],
        b"h:1",
        &[0, 0, 0, 0, 0, 0, 0, 2][..],
    ]
    .concat();
    assert_eq!(
        message.encode().expect("encode").as_ref(),
        expected.as_slice()
    );
}

/// An unknown kind is rejected rather than skipped: the kind selects how to
/// read the body, so ignoring one means confidently misparsing it.
#[test]
fn an_unknown_kind_is_rejected() {
    let mut frame = BytesMut::new();
    frame.put_u32(INTERNAL_MAGIC);
    frame.put_u16(INTERNAL_VERSION);
    frame.put_u16(999);
    frame.put_u32(0);
    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::UnsupportedInternalKind(999)),
    ));
}

#[test]
fn an_unknown_version_is_rejected() {
    let mut frame = BytesMut::new();
    frame.put_u32(INTERNAL_MAGIC);
    frame.put_u16(INTERNAL_VERSION + 1);
    frame.put_u16(Kind::ForwardPublishOk as u16);
    frame.put_u32(24);
    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::UnsupportedVersion(_)),
    ));
}

/// The allocation trap the client binary path documents: a tiny body declaring
/// an enormous payload count must be rejected before `with_capacity` sees it.
#[test]
fn a_declared_payload_count_is_bounded_by_the_body() {
    let mut body = BytesMut::new();
    body.put_u64(1);
    for value in ["t1", "ns", "orders"] {
        body.put_u32(value.len() as u32);
        body.extend_from_slice(value.as_bytes());
    }
    body.put_u32(0);
    body.put_u64(0);
    body.put_u8(0);
    body.put_u32(u32::MAX); // "there are four billion payloads after this"

    let mut frame = BytesMut::new();
    InternalHeader {
        kind: Kind::ForwardPublish,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(&body);

    assert!(
        matches!(
            InternalMessage::decode(frame.freeze()),
            Err(Error::Incomplete)
        ),
        "a declared count must be checked against the bytes that remain",
    );
}

/// Truncation at every byte must be an error, never a panic. This is the test
/// that says a hostile or buggy peer cannot take a broker down.
#[test]
fn truncation_at_every_byte_is_an_error_not_a_panic() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        for cut in 0..encoded.len() {
            let truncated = encoded.slice(0..cut);
            assert!(
                InternalMessage::decode(truncated).is_err(),
                "{:?} truncated to {cut} bytes must not decode",
                message.kind(),
            );
        }
    }
}

/// Every single-byte corruption must either decode to something well formed or
/// error. Neither may panic.
#[test]
fn single_byte_corruption_never_panics() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        for index in 0..encoded.len() {
            for bit in 0..8u32 {
                let mut corrupted = encoded.to_vec();
                corrupted[index] ^= 1 << bit;
                let _ = InternalMessage::decode(Bytes::from(corrupted));
            }
        }
    }
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut encoded = forward().encode().expect("encode").to_vec();
    encoded.push(0);
    // The header's length no longer matches the body, which is the first thing
    // checked.
    assert!(InternalMessage::decode(Bytes::from(encoded)).is_err());
}

#[test]
fn a_non_utf8_identifier_is_rejected() {
    let mut body = BytesMut::new();
    body.put_u64(1);
    body.put_u32(2);
    body.extend_from_slice(&[0xff, 0xfe]);

    let mut frame = BytesMut::new();
    InternalHeader {
        kind: Kind::NotLeader,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(&body);

    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::InvalidUtf8),
    ));
}

#[test]
fn an_over_long_identifier_is_refused_on_encode() {
    let message = InternalMessage::NotLeader(NotLeader {
        correlation_id: 1,
        node_id: "n".repeat(MAX_IDENT_BYTES + 1),
        advertise_addr: "h:1".to_string(),
        generation: 1,
    });
    assert!(matches!(message.encode(), Err(Error::FrameTooLarge)));
}

#[test]
fn an_over_large_batch_is_refused_on_encode() {
    let message = InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 1,
        shard: shard(),
        ack: AckMode::None,
        payloads: vec![Bytes::new(); MAX_BATCH_PAYLOADS + 1],
    });
    assert!(matches!(message.encode(), Err(Error::FrameTooLarge)));
}

#[test]
fn an_empty_batch_round_trips() {
    let message = InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 5,
        shard: shard(),
        ack: AckMode::None,
        payloads: Vec::new(),
    });
    let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn unknown_enum_values_are_rejected() {
    assert!(Kind::from_u16(0).is_err());
    // One past the highest kind: an unknown kind must be rejected rather than
    // skipped, because the kind is what selects how to read the body.
    assert!(Kind::from_u16(14).is_err());
    assert!(ErrorCode::from_u16(0).is_err());
    assert!(ErrorCode::from_u16(999).is_err());
    assert!(AckMode::from_u8(9).is_err());
    assert!(CacheOpKind::from_u8(0).is_err());
    assert!(CacheOpKind::from_u8(4).is_err());
}

/// Retryability is a property of the code, so a requester does not have to
/// re-derive it from a message string.
#[test]
fn error_codes_say_whether_to_retry() {
    for code in [
        ErrorCode::StaleRoute,
        ErrorCode::Unavailable,
        ErrorCode::Overload,
    ] {
        assert!(code.is_retryable(), "{code:?}");
    }
    for code in [
        ErrorCode::Unauthorized,
        ErrorCode::ProtocolVersion,
        ErrorCode::Malformed,
        ErrorCode::StorageFailed,
    ] {
        assert!(!code.is_retryable(), "{code:?}");
    }
}

#[test]
fn replicate_records_matches_its_golden_vector() {
    let encoded = replicate().encode().expect("encode");
    let expected: Vec<u8> = [
        // header: magic "FLXI", version 1, kind 7, length
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x07][..],
        &[0x00, 0x00, 0x00, 0x49][..],
        // correlation_id 42
        &[0, 0, 0, 0, 0, 0, 0, 42][..],
        // "t1", "ns", "orders"
        &[0, 0, 0, 2][..],
        b"t1",
        &[0, 0, 0, 2][..],
        b"ns",
        &[0, 0, 0, 6][..],
        b"orders",
        // shard 3, generation 7
        &[0, 0, 0, 3][..],
        &[0, 0, 0, 0, 0, 0, 0, 7][..],
        // first_offset 100, checksum 0x01020304
        &[0, 0, 0, 0, 0, 0, 0, 100][..],
        &[0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04][..],
        // two payloads
        &[0, 0, 0, 2][..],
        &[0, 0, 0, 1][..],
        b"a",
        &[0, 0, 0, 2][..],
        b"bb",
    ]
    .concat();
    assert_eq!(encoded.as_ref(), expected.as_slice());
}

/// **The checksum covers each payload's length as well as its bytes.** Without
/// the length, a batch resplit in transit hashes the same as the original, and
/// a resplit batch is a different set of records — exactly the divergence the
/// checksum exists to catch.
#[test]
fn the_batch_checksum_separates_a_different_split_of_the_same_bytes() {
    let one = batch_checksum(&[Bytes::from_static(b"ab"), Bytes::from_static(b"c")]);
    let other = batch_checksum(&[Bytes::from_static(b"a"), Bytes::from_static(b"bc")]);

    assert_ne!(one, other);
}

#[test]
fn the_batch_checksum_is_stable_and_order_sensitive() {
    let batch = [Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
    let reversed = [Bytes::from_static(b"bb"), Bytes::from_static(b"a")];

    assert_eq!(batch_checksum(&batch), batch_checksum(&batch));
    assert_ne!(batch_checksum(&batch), batch_checksum(&reversed));
    assert_eq!(batch_checksum(&[]), batch_checksum(&[]));
}

/// The divergence codes say what a leader may do next, and getting these
/// backwards is how a diverged follower gets hammered or a lagging one gets
/// abandoned.
#[test]
fn a_gap_is_retryable_and_a_conflict_is_not() {
    assert!(
        ErrorCode::LogGap.is_retryable(),
        "a follower that named the offset it wants was told not to retry",
    );
    assert!(
        !ErrorCode::LogConflict.is_retryable(),
        "diverged logs do not converge by retrying",
    );
    assert!(
        !ErrorCode::FencedEpoch.is_retryable(),
        "the fence does not lift",
    );
}

/// Every code survives the wire as itself. A code that decoded as a neighbour
/// would turn "stop, we have diverged" into "try again".
#[test]
fn every_error_code_round_trips() {
    for code in [
        ErrorCode::StaleRoute,
        ErrorCode::Unavailable,
        ErrorCode::Unauthorized,
        ErrorCode::Overload,
        ErrorCode::ProtocolVersion,
        ErrorCode::Malformed,
        ErrorCode::StorageFailed,
        ErrorCode::LogGap,
        ErrorCode::LogConflict,
        ErrorCode::FencedEpoch,
    ] {
        assert_eq!(ErrorCode::from_u16(code as u16).expect("known"), code);
    }
}

/// The kinds already on the wire keep their discriminants. A peer mid-upgrade
/// decodes by number, so renumbering one silently reinterprets every frame of
/// that kind.
#[test]
fn the_existing_kind_discriminants_are_unchanged() {
    for (value, kind) in [
        (1, Kind::ForwardPublish),
        (2, Kind::ForwardPublishOk),
        (3, Kind::ForwardPublishError),
        (4, Kind::NotLeader),
        (5, Kind::Hello),
        (6, Kind::HelloOk),
        (7, Kind::ReplicateRecords),
        (8, Kind::ReplicateOk),
        (9, Kind::ReplicateError),
        (10, Kind::ReplicateBootstrap),
        (11, Kind::ForwardCacheOp),
        (12, Kind::ForwardCacheOk),
        (13, Kind::ForwardCacheError),
    ] {
        assert_eq!(Kind::from_u16(value).expect("known"), kind);
        assert_eq!(kind as u16, value);
    }
}

/// A miss and a stored empty value are different answers, and the presence byte
/// is the only thing separating them. Encode them as the same bytes and a
/// cached empty value reads back as "not there" forever.
#[test]
fn an_empty_cached_value_is_not_a_miss() {
    let empty = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: Some(Bytes::new()),
    });
    let miss = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: None,
    });

    let empty_bytes = empty.encode().expect("encode");
    let miss_bytes = miss.encode().expect("encode");
    assert_ne!(empty_bytes, miss_bytes);

    assert_eq!(InternalMessage::decode(empty_bytes).expect("decode"), empty);
    assert_eq!(InternalMessage::decode(miss_bytes).expect("decode"), miss);
}

/// The presence byte has two meanings and no third. A peer sending anything
/// else is not a peer this can guess for.
#[test]
fn an_unknown_cache_value_presence_byte_is_refused() {
    let ok = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: None,
    });
    let mut bytes = ok.encode().expect("encode").to_vec();
    let last = bytes.len() - 1;
    bytes[last] = 2;

    assert!(InternalMessage::decode(Bytes::from(bytes)).is_err());
}

#[test]
fn an_unknown_cache_operation_is_refused() {
    let op = InternalMessage::ForwardCacheOp(ForwardCacheOp {
        correlation_id: 42,
        shard: shard(),
        op: CacheOpKind::Get,
        key: "k".to_string(),
        value: Bytes::new(),
        ttl_ms: 0,
    });
    let bytes = op.encode().expect("encode").to_vec();
    let position = bytes
        .windows(1)
        .position(|w| w == [CacheOpKind::Get as u8])
        .expect("the op byte is in there somewhere");
    let mut broken = bytes;
    broken[position] = 9;

    assert!(InternalMessage::decode(Bytes::from(broken)).is_err());
}
