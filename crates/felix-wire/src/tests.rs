// Unit tests for the wire crate: framing, message JSON round-trips, the text
// publish-batch writer, and the binary batch codec (including limit enforcement).

use crate::binary;
use crate::error::Error;
use crate::frame::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED,
    FLAG_EVENT_BATCH_OFFSETS, Frame, FrameHeader, KNOWN_FLAGS, MAGIC, VERSION, has_unknown_flags,
};
use crate::message::{AckMode, Message};
use bytes::{BufMut, Bytes, BytesMut};

#[test]
fn round_trip() {
    // Encoding then decoding should preserve header and payload.
    let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
    let encoded = frame.encode();
    let decoded = Frame::decode(encoded).expect("decode");
    assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
    assert_eq!(decoded.header.flags, 0x1);
}

#[test]
fn decode_rejects_invalid_magic() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
    buf.extend_from_slice(&VERSION.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    let err = FrameHeader::decode(buf.freeze()).expect_err("invalid magic");
    assert!(matches!(err, Error::InvalidMagic));
}

#[test]
fn decode_rejects_unsupported_version() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.extend_from_slice(&0xFFFFu16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    let err = FrameHeader::decode(buf.freeze()).expect_err("unsupported version");
    assert!(matches!(err, Error::UnsupportedVersion(0xFFFF)));
}

#[test]
fn decode_rejects_incomplete_header() {
    let err = FrameHeader::decode(Bytes::from_static(b"short")).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn decode_rejects_incomplete_payload() {
    let header = FrameHeader {
        magic: MAGIC,
        version: VERSION,
        flags: 0,
        length: 5,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(b"hi");
    let err = Frame::decode(buf.freeze()).expect_err("incomplete payload");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_event_batch_round_trip() {
    let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    let encoded = binary::encode_event_batch_bytes(7, &payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    assert_eq!(frame.header.flags, FLAG_BINARY_EVENT_BATCH);
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 7);
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn shared_binary_event_batch_round_trip() {
    let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    let encoded = binary::encode_shared_event_batch_bytes(&payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    assert_eq!(frame.header.flags, FLAG_BINARY_EVENT_BATCH_SHARED);
    let decoded = binary::decode_shared_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn binary_event_batch_rejects_incomplete_payload() {
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, Bytes::from_static(b"short")).expect("frame");
    let err = binary::decode_event_batch(&frame).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_event_batch_parts_match_full_encoding_single() {
    let payloads = vec![Bytes::from_static(b"hello world")];
    let encoded = binary::encode_event_batch_bytes(42, &payloads).expect("encode");
    let parts = binary::encode_event_batch_parts(42, &payloads).expect("parts");

    let mut flattened = BytesMut::with_capacity(parts.frame_len());
    for segment in parts.segments() {
        flattened.extend_from_slice(segment.as_ref());
    }

    assert_eq!(flattened.freeze(), encoded);
}

#[test]
fn binary_event_batch_parts_match_full_encoding_multi_payload() {
    fn payload(seed: u8, len: usize) -> Bytes {
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            out.push((i as u8).wrapping_mul(31) ^ seed);
        }
        Bytes::from(out)
    }

    for case in 0..16u8 {
        let payloads = vec![
            payload(case, (case as usize) * 7),
            payload(case.wrapping_add(3), 17 + case as usize),
            payload(case.wrapping_add(9), 257 + (case as usize * 13)),
        ];
        let encoded =
            binary::encode_event_batch_bytes(10_000 + case as u64, &payloads).expect("encode");
        let parts =
            binary::encode_event_batch_parts(10_000 + case as u64, &payloads).expect("parts");

        let mut flattened = BytesMut::with_capacity(parts.frame_len());
        for segment in parts.segments() {
            flattened.extend_from_slice(segment.as_ref());
        }
        assert_eq!(flattened.freeze(), encoded);
    }
}

#[test]
fn binary_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let frame = binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    assert_eq!(frame.header.flags, FLAG_BINARY_PUBLISH_BATCH);
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.tenant_id, "t1");
    assert_eq!(decoded.namespace, "default");
    assert_eq!(decoded.stream, "orders");
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn keyed_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let frame =
        binary::encode_publish_batch_keyed(Some(b"orders-7"), "t1", "default", "orders", &payloads)
            .expect("encode");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED
    );
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.key.as_deref(), Some(b"orders-7".as_slice()));
    assert_eq!(decoded.tenant_id, "t1");
    assert_eq!(decoded.namespace, "default");
    assert_eq!(decoded.stream, "orders");
    assert_eq!(decoded.payloads, payloads);
}

// An empty key is a key: it hashes to a shard like any other, which is not what
// an unkeyed publish does.
#[test]
fn an_empty_key_is_not_an_absent_key() {
    let payloads = vec![b"one".to_vec()];
    let keyed = binary::encode_publish_batch_keyed(Some(b""), "t1", "default", "orders", &payloads)
        .expect("encode");
    assert_eq!(
        binary::decode_publish_batch(&keyed).expect("decode").key,
        Some(Bytes::new())
    );
    let unkeyed =
        binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    assert_eq!(
        binary::decode_publish_batch(&unkeyed).expect("decode").key,
        None
    );
}

// The keyed bit changes where the body starts, so a decoder that ignored it
// would read the key length as a tenant length. This is the frame that catches
// that: the same bytes, read both ways.
#[test]
fn the_keyed_bit_is_what_shifts_the_body() {
    let payloads = vec![b"one".to_vec()];
    let keyed =
        binary::encode_publish_batch_keyed(Some(b"k1"), "t1", "default", "orders", &payloads)
            .expect("encode");
    let as_unkeyed = Frame::new(FLAG_BINARY_PUBLISH_BATCH, keyed.payload.clone()).expect("frame");
    let misread = binary::decode_publish_batch(&as_unkeyed);
    assert!(
        misread.is_err() || misread.expect("decoded").tenant_id != "t1",
        "the key prefix must not parse as a tenant id"
    );
}

#[test]
fn keyed_publish_batch_rejects_truncated_key() {
    // A key length of four with only two bytes behind it.
    let mut buf = BytesMut::new();
    buf.put_u16(4);
    buf.extend_from_slice(b"ab");
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame).expect_err("truncated key"),
        Error::Incomplete
    ));
}

#[test]
fn acked_keyed_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec()];
    let bytes = binary::encode_acked_publish_batch_bytes_keyed(
        42,
        AckMode::PerBatch,
        Some(b"orders-7"),
        "t1",
        "default",
        "orders",
        &payloads,
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED | FLAG_BINARY_PUBLISH_KEYED
    );
    // The correlation prefix still reads at offset 0 with a key behind it.
    let (request_id, ack) = binary::peek_acked_publish_prefix(&frame).expect("peek");
    assert_eq!(request_id, 42);
    assert_eq!(ack, AckMode::PerBatch);
    let decoded = binary::decode_acked_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.batch.key.as_deref(), Some(b"orders-7".as_slice()));
    assert_eq!(decoded.batch.payloads, payloads);
}

#[test]
fn binary_publish_batch_rejects_incomplete_payload() {
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, Bytes::from_static(b"\x00")).expect("frame");
    let err = binary::decode_publish_batch(&frame).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn acked_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    for ack in [AckMode::PerMessage, AckMode::PerBatch] {
        let bytes =
            binary::encode_acked_publish_batch_bytes(42, ack, "t1", "default", "orders", &payloads)
                .expect("encode");
        let frame = Frame::decode(bytes).expect("frame");
        // Both bits are set: the acked frame is a publish batch that also owes an ack.
        assert_eq!(
            frame.header.flags,
            FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED
        );
        let decoded = binary::decode_acked_publish_batch(&frame).expect("decode");
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.ack, ack);
        assert_eq!(decoded.batch.tenant_id, "t1");
        assert_eq!(decoded.batch.namespace, "default");
        assert_eq!(decoded.batch.stream, "orders");
        assert_eq!(decoded.batch.payloads, payloads);
    }
}

// The prefix must be readable on its own, because that is what lets the broker
// answer a corrupt body with an error the client can still correlate.
#[test]
fn acked_publish_prefix_readable_without_valid_body() {
    let mut buf = BytesMut::new();
    buf.put_u64(7);
    buf.put_u8(2); // PerBatch
    buf.extend_from_slice(b"\xff\xff garbage body");
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    let (request_id, ack) = binary::peek_acked_publish_prefix(&frame).expect("peek");
    assert_eq!(request_id, 7);
    assert_eq!(ack, AckMode::PerBatch);
    // The body is still garbage, so the full decode must fail.
    assert!(binary::decode_acked_publish_batch(&frame).is_err());
}

#[test]
fn acked_publish_batch_rejects_truncated_prefix() {
    // Eight bytes: a full request_id but no ack-mode byte.
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x00"),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_acked_publish_batch(&frame).expect_err("truncated"),
        Error::Incomplete
    ));
}

#[test]
fn acked_publish_batch_rejects_invalid_ack_mode() {
    let mut buf = BytesMut::new();
    buf.put_u64(1);
    buf.put_u8(0); // AckMode::None is not representable in this encoding
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(binary::decode_acked_publish_batch(&frame).is_err());
}

#[test]
fn acked_publish_batch_rejects_none_ack_mode_on_encode() {
    // AckMode::None must go through `encode_publish_batch` instead, so there is
    // exactly one wire encoding per mode.
    assert!(
        binary::encode_acked_publish_batch_bytes(
            1,
            AckMode::None,
            "t1",
            "default",
            "orders",
            &[b"x".to_vec()],
        )
        .is_err()
    );
}

// The body parser is shared with the unacked path, so the payload-count bound
// must still apply once the prefix has been stripped.
#[test]
fn acked_publish_batch_rejects_oversized_payload_count() {
    let mut buf = BytesMut::new();
    buf.put_u64(1);
    buf.put_u8(1);
    buf.put_u16(2);
    buf.extend_from_slice(b"t1");
    buf.put_u16(2);
    buf.extend_from_slice(b"ns");
    buf.put_u16(2);
    buf.extend_from_slice(b"st");
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_acked_publish_batch(&frame).expect_err("oversized count"),
        Error::Incomplete
    ));
}

#[test]
fn publish_ack_round_trip_ok_and_error() {
    let bytes = binary::encode_publish_ack_bytes(9, None).expect("encode ok");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(frame.header.flags, FLAG_BINARY_PUBLISH_ACK);
    let decoded = binary::decode_publish_ack(&frame).expect("decode");
    assert_eq!(decoded.request_id, 9);
    assert_eq!(decoded.error, None);

    let bytes = binary::encode_publish_ack_bytes(10, Some("stream full")).expect("encode err");
    let frame = Frame::decode(bytes).expect("frame");
    let decoded = binary::decode_publish_ack(&frame).expect("decode");
    assert_eq!(decoded.request_id, 10);
    assert_eq!(decoded.error.as_deref(), Some("stream full"));
}

#[test]
fn publish_ack_rejects_truncated_and_invalid_status() {
    let frame =
        Frame::new(FLAG_BINARY_PUBLISH_ACK, Bytes::from_static(b"\x00\x00")).expect("frame");
    assert!(matches!(
        binary::decode_publish_ack(&frame).expect_err("truncated"),
        Error::Incomplete
    ));

    let mut buf = BytesMut::new();
    buf.put_u8(7); // neither ok (0) nor error (1)
    buf.put_u64(1);
    buf.put_u16(0);
    let frame = Frame::new(FLAG_BINARY_PUBLISH_ACK, buf.freeze()).expect("frame");
    assert!(binary::decode_publish_ack(&frame).is_err());
}

// A declared message length longer than the frame must not be trusted.
#[test]
fn publish_ack_rejects_oversized_message_len() {
    let mut buf = BytesMut::new();
    buf.put_u8(1);
    buf.put_u64(1);
    buf.put_u16(u16::MAX); // no message bytes follow
    let frame = Frame::new(FLAG_BINARY_PUBLISH_ACK, buf.freeze()).expect("frame");
    assert!(matches!(
        binary::decode_publish_ack(&frame).expect_err("oversized message len"),
        Error::Incomplete
    ));
}

#[test]
fn unknown_flags_are_detected() {
    assert!(!has_unknown_flags(FLAG_BINARY_PUBLISH_BATCH));
    assert!(!has_unknown_flags(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED
    ));
    assert!(!has_unknown_flags(0));
    assert!(!has_unknown_flags(FLAG_EVENT_BATCH_OFFSETS));

    // The lowest undefined bit must be rejected rather than masked off.
    // Derived from `KNOWN_FLAGS` rather than written as a literal: this
    // assertion was `0x0020` until that bit was defined, at which point it
    // quietly became a claim about a *known* flag and failed. Computing it
    // keeps the test about the property instead of about one bit.
    let first_undefined = (0..16u16)
        .map(|bit| 1u16 << bit)
        .find(|bit| KNOWN_FLAGS & bit == 0)
        .expect("a free flag bit");
    assert!(has_unknown_flags(first_undefined));
    assert!(has_unknown_flags(FLAG_BINARY_PUBLISH_BATCH | 0x8000));
}

// The publish-batch JSON is written by hand rather than by serde, so nothing but
// a test keeps it in step with the decoder. It drifted: the `request_id` prefix
// carried a leading `"` that belongs after a string field, not after the
// `payloads` array, so every acked JSON batch serialised to `]","request_id"` and
// failed to deserialise. Cover every combination of the optional fields.
#[test]
fn text_publish_batch_json_round_trips_through_decoder() {
    let payloads = vec![b"payload".to_vec(), b"second".to_vec()];
    let cases = [
        (None, None),
        (Some(1u64), None),
        (None, Some(AckMode::PerBatch)),
        (Some(7), Some(AckMode::PerMessage)),
        (Some(u64::MAX), Some(AckMode::PerBatch)),
    ];
    for (request_id, ack) in cases {
        let mut buf = BytesMut::new();
        crate::text::write_publish_batch_json(
            &mut buf, "t1", "default", "updates", &payloads, None, request_id, ack,
        )
        .expect("write");

        // The declared length must match what was actually written, or callers
        // that pre-reserve using it will mis-size the frame.
        let declared = crate::text::publish_batch_json_len(
            "t1", "default", "updates", &payloads, None, request_id, ack,
        )
        .expect("len");
        assert_eq!(
            declared,
            buf.len(),
            "length mismatch for {request_id:?}/{ack:?}"
        );

        let frame = Frame::new(0, buf.freeze()).expect("frame");
        match Message::decode(frame).expect("decode") {
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads: decoded,
                request_id: decoded_id,
                ack: decoded_ack,
                key: None,
            } => {
                assert_eq!(tenant_id, "t1");
                assert_eq!(namespace, "default");
                assert_eq!(stream, "updates");
                assert_eq!(decoded, payloads);
                assert_eq!(decoded_id, request_id);
                assert_eq!(decoded_ack, ack);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}

#[test]
fn message_round_trip() {
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "topic".to_string(),
        payload: b"payload".to_vec(),
        request_id: None,
        ack: None,
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_error_round_trip() {
    let message = Message::Error {
        message: "oops".to_string(),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_tenant() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_namespace() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_stream() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(b"ns");
    buf.put_u16(2); // stream len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

// A declared payload count is attacker-controlled and was previously passed
// straight to `Vec::with_capacity`. These frames are ~20 bytes but claim
// `u32::MAX` payloads; before the bound each reserved 95-127 GiB of address
// space, and enough concurrent ones turn that into an abort. See
// `checked_payload_count`.
#[test]
fn binary_decode_publish_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(b"ns");
    buf.put_u16(2); // stream len
    buf.extend_from_slice(b"st");
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let err = binary::decode_publish_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_decode_event_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u64(1); // subscription id
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, buf.freeze()).expect("frame");
    let err = binary::decode_event_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_decode_shared_event_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH_SHARED, buf.freeze()).expect("frame");
    let err = binary::decode_shared_event_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

// The bound must reject only counts the frame cannot back, never a legitimate
// batch sitting exactly at the limit.
#[test]
fn binary_decode_event_batch_accepts_maximum_supportable_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u64(7);
    buf.put_u32(3); // three zero-length payloads: 3 * 4 bytes of prefix follow
    for _ in 0..3 {
        buf.put_u32(0);
    }
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, buf.freeze()).expect("frame");
    let batch = binary::decode_event_batch(&frame).expect("decode");
    assert_eq!(batch.subscription_id, 7);
    assert_eq!(batch.payloads.len(), 3);
}

#[test]
fn text_publish_batch_json_with_special_chars() {
    // Test encoding with special characters that need escaping
    let payloads = vec![b"payload\nwith\nnewlines".to_vec()];
    let message = Message::PublishBatch {
        tenant_id: "tenant".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        payloads,
        request_id: Some(123),
        ack: Some(AckMode::PerBatch),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_all_variants_encode_decode() {
    // Test Subscribe message
    let message = Message::Subscribe {
        start: None,
        subscription_id: Some(42),
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        shard: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Subscribed message
    let message = Message::Subscribed {
        subscription_id: 42,
        start_offset: None,
        live_offset: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishOk message
    let message = Message::PublishOk { request_id: 123 };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishError message
    let message = Message::PublishError {
        request_id: 123,
        message: "error".to_string(),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Ok message
    let message = Message::Ok;
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventStreamHello message
    let message = Message::EventStreamHello {
        subscription_id: 99,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn binary_event_batch_empty_payloads() {
    let payloads = vec![];
    let encoded = binary::encode_event_batch_bytes(1, &payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 1);
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn binary_publish_batch_empty_payloads() {
    let payloads = vec![];
    let frame = binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn frame_header_encode_decode() {
    let header = FrameHeader::new(0x1234, 0xABCD);
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    let decoded = FrameHeader::decode(buf.freeze()).expect("decode");
    assert_eq!(decoded.magic, MAGIC);
    assert_eq!(decoded.version, VERSION);
    assert_eq!(decoded.flags, 0x1234);
    assert_eq!(decoded.length, 0xABCD);
}

#[test]
fn message_cache_operations() {
    // Test CachePut
    let message = Message::CachePut {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: Bytes::from_static(b"value1"),
        request_id: Some(42),
        ttl_ms: Some(60000),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheGet
    let message = Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test the consumer-group messages
    let message = Message::GroupPoll {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 3,
        group: "workers".to_string(),
        max_records: 32,
        wait_ms: 5_000,
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    let message = Message::GroupRecords {
        records: vec![
            crate::GroupRecord {
                offset: 7,
                payload: Bytes::from_static(b"one"),
                attempts: 1,
            },
            crate::GroupRecord {
                offset: 9,
                payload: Bytes::new(),
                attempts: 3,
            },
        ],
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    // An empty batch is an answer, not an error: nothing was available.
    let message = Message::GroupRecords {
        records: Vec::new(),
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    for message in [
        Message::GroupAck {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 3,
            group: "workers".to_string(),
            offset: 11,
            request_id: 42,
        },
        Message::GroupNack {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 3,
            group: "workers".to_string(),
            offset: 11,
            request_id: 42,
        },
    ] {
        let frame = message.clone().encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }

    // Ack and nack differ on the wire, or a hand-back would finish the record.
    let ack = Message::GroupAck {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        offset: 1,
        request_id: 1,
    };
    let nack = Message::GroupNack {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        offset: 1,
        request_id: 1,
    };
    assert_ne!(
        ack.encode().expect("encode"),
        nack.encode().expect("encode"),
    );

    // A poll from a client that predates long-polling asks for no wait, which
    // is the behaviour every broker had before it.
    let legacy = r#"{"type":"group_poll","tenant_id":"t1","namespace":"ns",
        "stream":"jobs","shard":0,"group":"g","max_records":1,"request_id":1}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy poll") {
        Message::GroupPoll { wait_ms, .. } => assert_eq!(wait_ms, 0),
        other => panic!("expected a group poll, got {other:?}"),
    }

    // A record delivered by a broker that does not report attempts reads as
    // unknown rather than as a first attempt.
    let legacy = r#"{"offset":4,"payload":"YWJj"}"#;
    let record: crate::GroupRecord = serde_json::from_str(legacy).expect("legacy record");
    assert_eq!(record.attempts, 0);

    // Test CacheDelete
    let message = Message::CacheDelete {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheValue with value
    let message = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: Some(Bytes::from_static(b"value1")),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheValue miss (no value)
    let message = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: None,
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheOk
    let message = Message::CacheOk { request_id: 42 };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_event_variants() {
    // Test Event message
    let message = Message::Event {
        offset: None,
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"event data".to_vec(),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventBatch message
    let message = Message::EventBatch {
        base_offset: None,
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payloads: vec![b"event1".to_vec(), b"event2".to_vec()],
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventStreamHello
    let message = Message::EventStreamHello {
        subscription_id: 123,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_publish_with_ack_modes() {
    // Test Publish with AckMode::None
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"data".to_vec(),
        request_id: Some(1),
        ack: Some(AckMode::None),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Publish with AckMode::PerMessage
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"data".to_vec(),
        request_id: Some(2),
        ack: Some(AckMode::PerMessage),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishBatch with AckMode::PerBatch
    let message = Message::PublishBatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payloads: vec![b"data1".to_vec(), b"data2".to_vec()],
        request_id: Some(3),
        ack: Some(AckMode::PerBatch),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn binary_encode_publish_batch_stats() {
    let payloads = vec![
        b"payload1".to_vec(),
        b"payload2".to_vec(),
        b"payload3".to_vec(),
    ];
    let result = binary::encode_publish_batch_bytes_with_stats("t1", "ns", "stream", &payloads);
    assert!(result.is_ok());
    let (bytes, stats) = result.unwrap();
    assert!(!bytes.is_empty());
    // Stats should be valid (reallocs is a usize, so always >= 0)
    let _ = stats.reallocs;
}

#[test]
fn binary_encode_event_batch_large() {
    let payloads: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("payload{}", i)))
        .collect();
    let result = binary::encode_event_batch_bytes(42, &payloads);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let frame = Frame::decode(bytes).expect("decode frame");
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 42);
    assert_eq!(decoded.payloads.len(), 100);
}

#[test]
fn frame_decode_error_cases() {
    // Test frame with invalid header length
    let short_bytes = Bytes::from_static(b"short");
    let result = Frame::decode(short_bytes);
    assert!(result.is_err());

    // Test frame header with incomplete payload
    let mut buf = BytesMut::new();
    let header = FrameHeader::new(0, 100); // Claims 100 bytes
    header.encode(&mut buf);
    buf.extend_from_slice(b"only_10"); // But only has 7 bytes
    let result = Frame::decode(buf.freeze());
    assert!(result.is_err());
}

#[test]
fn ack_mode_serialization() {
    // Test all AckMode variants serialize correctly
    let none = AckMode::None;
    let per_msg = AckMode::PerMessage;
    let per_batch = AckMode::PerBatch;

    // Just ensure they can be used in messages
    let msg = Message::Publish {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payload: vec![1, 2, 3],
        request_id: Some(1),
        ack: Some(none),
        key: None,
    };
    assert!(msg.encode().is_ok());

    let msg2 = Message::Publish {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payload: vec![1, 2, 3],
        request_id: Some(2),
        ack: Some(per_msg),
        key: None,
    };
    assert!(msg2.encode().is_ok());

    let msg3 = Message::PublishBatch {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payloads: vec![vec![1, 2], vec![3, 4]],
        request_id: Some(3),
        ack: Some(per_batch),
        key: None,
    };
    assert!(msg3.encode().is_ok());
}

/// **An `AuthOk` from a broker that predates features decodes.** Absent is not
/// zero-by-accident: it has to mean "implements none", because a client that
/// read silence as support would send a message the broker's control loop
/// treats as a fatal protocol error, costing the connection.
#[test]
fn an_auth_ok_without_features_reads_as_supporting_none() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"auth_ok","server_flags":7}"#),
    )
    .expect("frame");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(
        decoded,
        Message::AuthOk {
            server_flags: 7,
            server_features: None,
            listener_ports: None,
        }
    );
}

/// **A broker advertising no features encodes the same bytes it always did.**
/// An old client parses this, and a new one reads it as supporting nothing.
#[test]
fn an_auth_ok_advertising_nothing_omits_the_field() {
    let encoded = Message::AuthOk {
        server_flags: 7,
        server_features: None,
        listener_ports: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&encoded.payload).expect("utf8");
    assert!(
        !json.contains("server_features"),
        "an absent feature set must not appear on the wire: {json}"
    );
    assert!(
        !json.contains("listener_ports"),
        "a single-listener broker must not mention listener_ports: {json}"
    );
}

/// **A broker with one listener is byte-identical to one that predates the
/// field.** The default is a single listener, so this is the common case: an
/// old client must see exactly the frame it has always seen.
#[test]
fn a_single_listener_auth_ok_is_unchanged_on_the_wire() {
    let before = Message::AuthOk {
        server_flags: 7,
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&before.payload).expect("utf8");
    assert!(!json.contains("listener_ports"), "{json}");
}

#[test]
fn an_auth_ok_carries_the_listener_ports_it_binds() {
    let message = Message::AuthOk {
        server_flags: felix_wire_flags(),
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: Some(vec![5000, 5001, 5002, 5003]),
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn an_auth_ok_carries_the_features_it_advertises() {
    let message = Message::AuthOk {
        server_flags: felix_wire_flags(),
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: None,
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn a_topology_exchange_round_trips() {
    let request = Message::Topology;
    assert_eq!(
        Message::decode(request.encode().expect("encode")).expect("decode"),
        request
    );

    let view = Message::TopologyView {
        brokers: vec![
            crate::BrokerEndpoint {
                node_id: "broker-a".to_string(),
                addr: "127.0.0.1:5000".to_string(),
            },
            crate::BrokerEndpoint {
                node_id: "broker-b".to_string(),
                addr: "127.0.0.1:5010".to_string(),
            },
        ],
    };
    assert_eq!(
        Message::decode(view.encode().expect("encode")).expect("decode"),
        view
    );
}

/// A cluster with nothing to report says so, rather than failing.
#[test]
fn an_empty_topology_round_trips() {
    let view = Message::TopologyView { brokers: vec![] };
    assert_eq!(
        Message::decode(view.encode().expect("encode")).expect("decode"),
        view
    );
}

#[test]
fn a_feature_bit_is_only_supported_when_advertised() {
    assert!(crate::supports_feature(
        crate::FEATURE_TOPOLOGY,
        crate::FEATURE_TOPOLOGY
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_TOPOLOGY));
}

fn felix_wire_flags() -> u16 {
    crate::KNOWN_FLAGS
}

/// **An `Auth` from a client that predates features decodes.** The mirror of
/// the broker-side case: absent has to mean "implements none", or a broker
/// would send a message the client cannot decode and cost the connection.
#[test]
fn an_auth_without_features_reads_as_supporting_none() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"auth","tenant_id":"t1","token":"x"}"#),
    )
    .expect("frame");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(
        decoded,
        Message::Auth {
            tenant_id: "t1".to_string(),
            token: "x".to_string(),
            client_flags: None,
            client_features: None,
        }
    );
}

/// A client advertising nothing sends the bytes it always did, so a broker that
/// predates features parses it unchanged.
#[test]
fn an_auth_advertising_nothing_omits_the_field() {
    let encoded = Message::Auth {
        tenant_id: "t1".to_string(),
        token: "x".to_string(),
        client_flags: None,
        client_features: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&encoded.payload).expect("utf8");
    assert!(
        !json.contains("client_features"),
        "an absent feature set must not appear on the wire: {json}"
    );
}

#[test]
fn a_not_leader_round_trips() {
    let message = Message::NotLeader {
        node_id: "broker-b".to_string(),
        addr: Some("10.0.0.5:5000".to_string()),
        generation: 7,
    };
    assert_eq!(
        Message::decode(message.encode().expect("encode")).expect("decode"),
        message
    );
}

/// **A redirect with no address still names the owner.** "Not here, and here is
/// who has it" is more use than "not here", and a client that knows that broker
/// from discovery can act on the name alone.
#[test]
fn a_not_leader_without_an_address_round_trips() {
    let message = Message::NotLeader {
        node_id: "broker-b".to_string(),
        addr: None,
        generation: 7,
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);

    let json = String::from_utf8(message.encode().expect("encode").payload.to_vec()).expect("utf8");
    assert!(
        !json.contains("addr"),
        "an absent address must not appear: {json}"
    );
}

/// The two feature bits are distinct, and neither implies the other.
#[test]
fn the_feature_bits_do_not_overlap() {
    assert_ne!(crate::FEATURE_TOPOLOGY, crate::FEATURE_REDIRECT);
    assert!(!crate::supports_feature(
        crate::FEATURE_TOPOLOGY,
        crate::FEATURE_REDIRECT
    ));
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_REDIRECT
    ));
}

/// **The hand-written batch encoder carries the routing key.** It, not serde, is
/// what the client's writer task uses, so a key omitted here is a key that never
/// reaches the broker — and the record lands on shard 0 with nothing to show for
/// it. Exactly the kind of silent divergence a fast path invites.
#[test]
fn the_fast_batch_encoder_carries_the_routing_key() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let key = b"customer-42".to_vec();

    let mut buf = BytesMut::new();
    crate::text::write_publish_batch_json(
        &mut buf,
        "t1",
        "default",
        "updates",
        &payloads,
        Some(&key),
        Some(7),
        Some(AckMode::PerMessage),
    )
    .expect("write");

    let declared = crate::text::publish_batch_json_len(
        "t1",
        "default",
        "updates",
        &payloads,
        Some(&key),
        Some(7),
        Some(AckMode::PerMessage),
    )
    .expect("len");
    assert_eq!(
        declared,
        buf.len(),
        "a declared length that ignores the key mis-sizes the frame",
    );

    let decoded = Message::decode(Frame::new(0, buf.freeze()).expect("frame")).expect("decode");
    match decoded {
        Message::PublishBatch { key: decoded, .. } => {
            assert_eq!(decoded.as_deref(), Some(&key[..]), "the key was dropped");
        }
        other => panic!("expected a publish batch, got {other:?}"),
    }
}

/// A batch with no key encodes the bytes it always did, so an old broker parses
/// a new client's unkeyed publish unchanged.
#[test]
fn an_unkeyed_batch_omits_the_field() {
    let payloads = vec![b"one".to_vec()];
    let mut buf = BytesMut::new();
    crate::text::write_publish_batch_json(
        &mut buf, "t1", "default", "updates", &payloads, None, None, None,
    )
    .expect("write");
    let json = std::str::from_utf8(&buf).expect("utf8");
    assert!(
        !json.contains("key"),
        "an absent key must not appear: {json}"
    );
}

/// Feature bits say a request *exists*; frame flags select a payload layout.
/// They are separate number spaces, and a new feature must not disturb either
/// the frozen v1 flag set or the bits already handed out.
#[test]
fn cache_delete_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_DELETE & (crate::FEATURE_TOPOLOGY | crate::FEATURE_REDIRECT),
        0,
        "the new bit overlaps one already in use",
    );
    assert_eq!(
        crate::FEATURE_CONSUMER_GROUP
            & (crate::FEATURE_TOPOLOGY | crate::FEATURE_REDIRECT | crate::FEATURE_CACHE_DELETE),
        0,
        "the consumer-group bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_DELETE
    ));
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CONSUMER_GROUP
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_CONSUMER_GROUP));
    assert_eq!(
        crate::FEATURE_GROUP_DEAD_LETTERS
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP),
        0,
        "the dead-letter bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_GROUP_DEAD_LETTERS
    ));
    // Serving groups does not imply serving dead letters: a broker built before
    // these requests existed advertises the first bit and not the second.
    assert!(!crate::supports_feature(
        crate::FEATURE_CONSUMER_GROUP,
        crate::FEATURE_GROUP_DEAD_LETTERS
    ));
    // Silence from a peer that predates negotiation must not be read as support.
    assert!(!crate::supports_feature(0, crate::FEATURE_CACHE_DELETE));
}

/// The watch messages survive an encode/decode round trip, and the optional
/// fields default the way an older peer's silence must be read.
#[test]
fn cache_watch_messages_round_trip() {
    let watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        key: Some("user:42".to_string()),
        prefix: None,
        shard: None,
        from_offset: Some(7),
        retained: false,
        subscription_id: None,
    };
    let decoded = Message::decode(watch.encode().expect("encode")).expect("decode");
    assert_eq!(watch, decoded);

    let prefix_watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        key: None,
        prefix: Some("user:".to_string()),
        shard: Some(3),
        from_offset: None,
        retained: false,
        subscription_id: Some(9),
    };
    let decoded = Message::decode(prefix_watch.encode().expect("encode")).expect("decode");
    assert_eq!(prefix_watch, decoded);

    let started = Message::CacheWatchStarted {
        subscription_id: 9,
        resume_offset: 12,
        resnapshot: true,
        retained_count: None,
    };
    let decoded = Message::decode(started.encode().expect("encode")).expect("decode");
    assert_eq!(started, decoded);

    let put = Message::CacheEvent {
        key: "user:42".to_string(),
        value: Some(Bytes::from_static(b"online")),
        offset: 12,
        expires_at_millis: 1_700_000_000_000,
    };
    let decoded = Message::decode(put.encode().expect("encode")).expect("decode");
    assert_eq!(put, decoded);

    // A delete carries no value, and the absent field must not appear on the
    // wire at all -- an old JSON reader sees exactly the fields it knows.
    let delete = Message::CacheEvent {
        key: "user:42".to_string(),
        value: None,
        offset: 13,
        expires_at_millis: 0,
    };
    let frame = delete.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("value"),
        "absent value must be omitted: {json}"
    );
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(delete, decoded);

    let lagged = Message::CacheWatchLagged { resume_from: 40 };
    let decoded = Message::decode(lagged.encode().expect("encode")).expect("decode");
    assert_eq!(lagged, decoded);

    // A `resnapshot` the sender omitted reads as false: a watch that did not
    // ask to resume was never resnapshotted.
    let legacy = r#"{"type":"cache_watch_started","subscription_id":1,"resume_offset":0}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy started") {
        Message::CacheWatchStarted { resnapshot, .. } => assert!(!resnapshot),
        other => panic!("expected cache_watch_started, got {other:?}"),
    }
}

/// The watch feature is a new bit: disjoint from every bit already handed out,
/// absent from a silent peer, and never implied by the other cache features.
#[test]
fn cache_watch_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_WATCH
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS),
        0,
        "the watch bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_WATCH
    ));
    // Deleting does not imply watching: a broker built before watches exist
    // advertises the delete bit and not this one.
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_DELETE,
        crate::FEATURE_CACHE_WATCH
    ));
    // Silence from a peer that predates negotiation must not be read as support.
    assert!(!crate::supports_feature(0, crate::FEATURE_CACHE_WATCH));
}

/// Retained delivery rides the existing watch messages as optional fields, so
/// a watch that does not use it stays byte-identical to one that predates it.
#[test]
fn retained_watch_fields_round_trip_and_default_off_the_wire() {
    let watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "presence".to_string(),
        key: None,
        prefix: Some("user:".to_string()),
        shard: None,
        from_offset: None,
        retained: true,
        subscription_id: None,
    };
    let decoded = Message::decode(watch.encode().expect("encode")).expect("decode");
    assert_eq!(watch, decoded);

    // An unretained watch must not carry the field at all: an old broker sees
    // exactly the frame an old client would have sent.
    let plain = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "presence".to_string(),
        key: Some("k".to_string()),
        prefix: None,
        shard: None,
        from_offset: None,
        retained: false,
        subscription_id: None,
    };
    let frame = plain.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("retained"),
        "an unset flag must stay off the wire: {json}"
    );

    // A frame that predates the field reads as unretained.
    let legacy = r#"{"type":"cache_watch","tenant_id":"t1","namespace":"ns",
        "cache":"presence","key":"k"}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy watch") {
        Message::CacheWatch { retained, .. } => assert!(!retained),
        other => panic!("expected cache_watch, got {other:?}"),
    }

    // `Some(0)` is the "no retained value" signal, distinct from absent.
    let started = Message::CacheWatchStarted {
        subscription_id: 3,
        resume_offset: 8,
        resnapshot: false,
        retained_count: Some(0),
    };
    let decoded = Message::decode(started.encode().expect("encode")).expect("decode");
    assert_eq!(started, decoded);
    let legacy = r#"{"type":"cache_watch_started","subscription_id":1,"resume_offset":0}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy started") {
        Message::CacheWatchStarted { retained_count, .. } => assert_eq!(retained_count, None),
        other => panic!("expected cache_watch_started, got {other:?}"),
    }
}

/// The retained bit is new, disjoint, and never implied by the watch bit: a
/// broker built when the watch bit meant live-and-resume only must not be
/// asked for retained delivery it would silently not perform.
#[test]
fn cache_watch_retained_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_WATCH_RETAINED
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS
                | crate::FEATURE_CACHE_WATCH),
        0,
        "the retained bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_WATCH,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
    assert!(!crate::supports_feature(
        0,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
}

/// The counter messages round trip, and their answers keep never-written and
/// zero apart.
#[test]
fn counter_messages_round_trip() {
    let add = Message::CounterAdd {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "metrics".to_string(),
        key: "page-views".to_string(),
        delta: -3,
        request_id: 9,
    };
    let decoded = Message::decode(add.encode().expect("encode")).expect("decode");
    assert_eq!(add, decoded);

    let get = Message::CounterGet {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "metrics".to_string(),
        key: "page-views".to_string(),
        request_id: 10,
    };
    let decoded = Message::decode(get.encode().expect("encode")).expect("decode");
    assert_eq!(get, decoded);

    // A sum of zero is a value; a counter never written has none, and the
    // absent field stays off the wire entirely.
    let zero = Message::CounterValue {
        value: Some(0),
        request_id: 10,
    };
    let decoded = Message::decode(zero.encode().expect("encode")).expect("decode");
    assert_eq!(zero, decoded);
    let missing = Message::CounterValue {
        value: None,
        request_id: 10,
    };
    let frame = missing.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("\"value\":"),
        "absent must be omitted: {json}"
    );
    assert_eq!(Message::decode(frame).expect("decode"), missing);
}

/// The counters bit is new and disjoint, absent from silence, and not implied
/// by any cache feature.
#[test]
fn counters_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_COUNTERS
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS
                | crate::FEATURE_CACHE_WATCH
                | crate::FEATURE_CACHE_WATCH_RETAINED),
        0,
        "the counters bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_COUNTERS
    ));
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_DELETE | crate::FEATURE_CACHE_WATCH,
        crate::FEATURE_COUNTERS
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_COUNTERS));
}

/// Idempotent producers: a feature bit, two requests, and a typed refusal.
mod idempotent_producer {
    use crate::{Message, PublishRefusalReason};

    /// The bit is new, disjoint, and absent from silence.
    #[test]
    fn idempotent_producer_is_a_new_feature_bit_and_disturbs_nothing() {
        let older = crate::FEATURE_TOPOLOGY
            | crate::FEATURE_REDIRECT
            | crate::FEATURE_CACHE_DELETE
            | crate::FEATURE_CONSUMER_GROUP
            | crate::FEATURE_GROUP_DEAD_LETTERS
            | crate::FEATURE_STREAM_SHARDS
            | crate::FEATURE_CACHE_WATCH
            | crate::FEATURE_CACHE_WATCH_RETAINED
            | crate::FEATURE_COUNTERS;
        assert_eq!(crate::FEATURE_IDEMPOTENT_PRODUCER & older, 0);
        assert!(crate::supports_feature(
            crate::KNOWN_FEATURES,
            crate::FEATURE_IDEMPOTENT_PRODUCER
        ));
        assert!(!crate::supports_feature(
            older,
            crate::FEATURE_IDEMPOTENT_PRODUCER
        ));
        assert!(!crate::supports_feature(
            0,
            crate::FEATURE_IDEMPOTENT_PRODUCER
        ));
    }

    #[test]
    fn producer_init_round_trips() {
        for message in [
            Message::ProducerInit { request_id: 7 },
            Message::ProducerInitOk {
                request_id: 7,
                producer_id: 0xdead_beef_cafe_f00d,
            },
        ] {
            let frame = message.encode().expect("encode");
            assert_eq!(Message::decode(frame).expect("decode"), message);
        }
    }

    /// The producer id and sequence are not optional: a batch without them is
    /// an ordinary `publish_batch`, and the broker must never guess which.
    #[test]
    fn publish_idempotent_round_trips_and_names_its_producer() {
        let message = Message::PublishIdempotent {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            key: Some(bytes::Bytes::from_static(b"k")),
            request_id: 3,
            producer_id: 42,
            sequence: 9,
        };
        let frame = message.encode().expect("encode");
        let json = std::str::from_utf8(&frame.payload).expect("utf8");
        assert!(json.contains("\"type\":\"publish_idempotent\""), "{json}");
        assert!(json.contains("\"producer_id\":42"), "{json}");
        assert!(json.contains("\"sequence\":9"), "{json}");
        assert_eq!(Message::decode(frame).expect("decode"), message);

        let without_them = "{\"type\":\"publish_idempotent\",\"tenant_id\":\"t1\",\
             \"namespace\":\"ns\",\"stream\":\"orders\",\"payloads\":[],\"request_id\":1}";
        assert!(
            serde_json::from_str::<Message>(without_them).is_err(),
            "a producer publish without a producer decoded"
        );
    }

    /// A key is optional and omitted when absent, like every other publish.
    #[test]
    fn publish_idempotent_omits_an_absent_key() {
        let message = Message::PublishIdempotent {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            payloads: vec![],
            key: None,
            request_id: 3,
            producer_id: 1,
            sequence: 0,
        };
        let frame = message.encode().expect("encode");
        let json = std::str::from_utf8(&frame.payload).expect("utf8");
        assert!(!json.contains("\"key\""), "{json}");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }

    /// Every reason survives the wire, and the one carrying data carries it.
    #[test]
    fn every_refusal_reason_round_trips() {
        for reason in [
            PublishRefusalReason::SequenceGap { expected: 12 },
            PublishRefusalReason::UnknownProducer,
            PublishRefusalReason::SequenceExpired,
            PublishRefusalReason::NotLeader {
                node_id: "broker-b".to_string(),
                addr: Some("10.0.0.2:5000".to_string()),
            },
            PublishRefusalReason::NotLeader {
                node_id: "broker-c".to_string(),
                addr: None,
            },
        ] {
            let message = Message::PublishRefused {
                request_id: 5,
                reason: reason.clone(),
                message: "why".to_string(),
            };
            let frame = message.encode().expect("encode");
            assert_eq!(Message::decode(frame).expect("decode"), message);
        }
        let gap = serde_json::to_string(&PublishRefusalReason::SequenceGap { expected: 12 })
            .expect("json");
        assert_eq!(gap, "{\"sequence_gap\":{\"expected\":12}}");
        let plain = serde_json::to_string(&PublishRefusalReason::UnknownProducer).expect("json");
        assert_eq!(plain, "\"unknown_producer\"");
    }
}

/// An ack with no owner is byte-identical to one from before the hint existed.
///
/// The compatibility claim the whole design rests on: a broker that has nothing
/// to hint, or a client that did not ask, exchange exactly the bytes they
/// always did. If this ever differs, every peer predating `0x0080` breaks on an
/// acknowledgement for a publish that succeeded.
#[test]
fn an_ack_without_an_owner_is_unchanged() {
    for error in [None, Some("stream not found")] {
        let plain = crate::binary::encode_publish_ack_bytes(42, error).expect("plain");
        let owned = crate::binary::encode_publish_ack_bytes_owned(42, error, None).expect("owned");
        assert_eq!(
            plain, owned,
            "the no-owner encoding drifted from the original"
        );
    }
}

#[test]
fn a_forwarded_ack_round_trips_its_owner() {
    let owner = crate::binary::PublishOwner {
        node_id: "broker-2".to_string(),
        addr: Some("10.0.0.2:5000".to_string()),
        generation: 7,
    };
    let bytes =
        crate::binary::encode_publish_ack_bytes_owned(9, None, Some(&owner)).expect("encode");
    let frame = crate::Frame::decode(bytes).expect("frame");
    // The flag is the signal that forwarding happened, so it has to be set.
    assert_ne!(frame.header.flags & crate::FLAG_BINARY_PUBLISH_ACK_OWNER, 0);
    let ack = crate::binary::decode_publish_ack(&frame).expect("decode");
    assert_eq!(ack.request_id, 9);
    assert_eq!(ack.error, None);
    assert_eq!(ack.forwarded_to, Some(owner));
}

/// An owner whose client address the cluster has not published.
///
/// The same gap `NotLeader` has: the client learns *who* owns the shard but has
/// nowhere to send to, so it keeps publishing where it is. Empty on the wire
/// decodes to `None` rather than to an empty string, because there is no such
/// address and a caller must not try to parse one.
#[test]
fn an_owner_without_a_published_address_decodes_as_absent() {
    let owner = crate::binary::PublishOwner {
        node_id: "broker-3".to_string(),
        addr: None,
        generation: 1,
    };
    let bytes =
        crate::binary::encode_publish_ack_bytes_owned(1, None, Some(&owner)).expect("encode");
    let frame = crate::Frame::decode(bytes).expect("frame");
    let ack = crate::binary::decode_publish_ack(&frame).expect("decode");
    let decoded = ack.forwarded_to.expect("owner");
    assert_eq!(decoded.node_id, "broker-3");
    assert_eq!(decoded.addr, None);
}

/// A truncated owner is an error, not a half-read one.
#[test]
fn a_truncated_owner_is_refused() {
    let owner = crate::binary::PublishOwner {
        node_id: "broker-2".to_string(),
        addr: Some("10.0.0.2:5000".to_string()),
        generation: 7,
    };
    let bytes =
        crate::binary::encode_publish_ack_bytes_owned(9, None, Some(&owner)).expect("encode");
    // Every prefix that still parses as a frame must fail to decode rather than
    // inventing an owner from whatever bytes happen to be there.
    for cut in 1..12 {
        let short = bytes.slice(..bytes.len() - cut);
        if let Ok(frame) = crate::Frame::decode(short) {
            assert!(
                crate::binary::decode_publish_ack(&frame).is_err(),
                "a truncated owner decoded instead of erroring (cut {cut})"
            );
        }
    }
}
