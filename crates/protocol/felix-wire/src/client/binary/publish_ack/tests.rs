use bytes::{BufMut, Bytes, BytesMut};

use crate::error::Error;
use crate::{FLAG_BINARY_PUBLISH_ACK, Frame, binary};

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

#[test]
fn a_failed_ack_carries_its_code_under_the_flag() {
    use crate::{ErrorCode, FLAG_BINARY_PUBLISH_ACK_CODE, RetryClass};

    let bytes = binary::encode_publish_ack_bytes_coded(
        11,
        Some("did not reach a majority"),
        Some((&ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown)),
        None,
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_CODE
    );
    let decoded = binary::decode_publish_ack(&frame).expect("decode");
    assert_eq!(decoded.error.as_deref(), Some("did not reach a majority"));
    assert_eq!(
        decoded.code,
        Some((ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown))
    );
}

/// Without a code the frame is the one an older broker sends, and a success
/// never carries one even if asked to.
#[test]
fn an_ack_without_a_code_is_unchanged() {
    use crate::{ErrorCode, RetryClass};

    let old = binary::encode_publish_ack_bytes(12, Some("stream full")).expect("old");
    let new =
        binary::encode_publish_ack_bytes_coded(12, Some("stream full"), None, None).expect("new");
    assert_eq!(old, new);

    let ok = binary::encode_publish_ack_bytes(13, None).expect("ok");
    let coded_ok = binary::encode_publish_ack_bytes_coded(
        13,
        None,
        Some((&ErrorCode::Internal, RetryClass::Fatal)),
        None,
    )
    .expect("coded ok");
    assert_eq!(ok, coded_ok);
}

/// A code number this version does not know still decodes, keeping its class.
#[test]
fn an_unknown_binary_code_keeps_its_retry_class() {
    use crate::{ErrorCode, FLAG_BINARY_PUBLISH_ACK_CODE, RetryClass};

    let mut buf = BytesMut::new();
    buf.put_u8(1);
    buf.put_u64(14);
    buf.put_u16(4);
    buf.extend_from_slice(b"nope");
    buf.put_u16(999);
    buf.put_u8(RetryClass::Retry.to_u8());
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_CODE,
        buf.freeze(),
    )
    .expect("frame");
    let decoded = binary::decode_publish_ack(&frame).expect("decode");
    assert_eq!(
        decoded.code,
        Some((
            ErrorCode::Unknown("code_999".to_string()),
            RetryClass::Retry
        ))
    );
}
