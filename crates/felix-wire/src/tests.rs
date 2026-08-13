// Unit tests for the wire crate: framing, message JSON round-trips, the text
// publish-batch writer, and the binary batch codec (including limit enforcement).

use crate::binary;
use crate::error::Error;
use crate::frame::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, Frame, FrameHeader, MAGIC, VERSION,
    has_unknown_flags,
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
    // The first undefined bit must be rejected rather than masked off.
    assert!(has_unknown_flags(0x0020));
    assert!(has_unknown_flags(FLAG_BINARY_PUBLISH_BATCH | 0x8000));
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
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_all_variants_encode_decode() {
    // Test Subscribe message
    let message = Message::Subscribe {
        subscription_id: Some(42),
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Subscribed message
    let message = Message::Subscribed {
        subscription_id: 42,
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
    };
    assert!(msg.encode().is_ok());

    let msg2 = Message::Publish {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payload: vec![1, 2, 3],
        request_id: Some(2),
        ack: Some(per_msg),
    };
    assert!(msg2.encode().is_ok());

    let msg3 = Message::PublishBatch {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payloads: vec![vec![1, 2], vec![3, 4]],
        request_id: Some(3),
        ack: Some(per_batch),
    };
    assert!(msg3.encode().is_ok());
}
