use crate::{AckMode, Message};

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

/// Without join offsets `subscribed` is the frame it always was, and an old
/// broker's frame decodes with none.
#[test]
fn subscribed_without_join_offsets_is_unchanged() {
    let plain = Message::Subscribed {
        subscription_id: 42,
        start_offset: None,
        live_offset: None,
    };
    assert_eq!(
        serde_json::to_string(&plain).expect("serialize"),
        r#"{"type":"subscribed","subscription_id":42}"#
    );
    let legacy: Message =
        serde_json::from_str(r#"{"type":"subscribed","subscription_id":42}"#).expect("decode");
    assert_eq!(legacy, plain);

    let joined = Message::Subscribed {
        subscription_id: 42,
        start_offset: Some(10),
        live_offset: Some(25),
    };
    let frame = joined.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), joined);
}
