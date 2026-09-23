use bytes::Bytes;
use felix_wire::Message;

use super::*;

#[test]
fn parse_subscribe_response_variants() {
    assert_eq!(
        parse_subscribe_response(Some(Message::Subscribed {
            subscription_id: 7,
            start_offset: None,
            live_offset: None,
        }))
        .expect("ok"),
        7
    );
    assert!(
        parse_subscribe_response(Some(Message::Error {
            message: "nope".into()
        }))
        .is_err()
    );
    assert!(parse_subscribe_response(None).is_err());
}

#[test]
fn ensure_publish_ok_variants() {
    ensure_publish_ok(Some(Message::PublishOk { request_id: 9 }), 9).expect("ok");
    assert!(ensure_publish_ok(Some(Message::PublishOk { request_id: 8 }), 9).is_err());
    assert!(
        ensure_publish_ok(
            Some(Message::Error {
                message: "no".into()
            }),
            9
        )
        .is_err()
    );
    assert!(ensure_publish_ok(None, 9).is_err());
}

#[test]
fn ensure_ok_response_variants() {
    ensure_ok_response(Some(Message::Ok), "cache put").expect("ok");
    assert!(
        ensure_ok_response(
            Some(Message::Error {
                message: "no".into()
            }),
            "cache put"
        )
        .is_err()
    );
    assert!(ensure_ok_response(None, "cache put").is_err());
}

#[test]
fn parse_cache_get_response_variants() {
    let value = Bytes::from_static(b"value");
    assert_eq!(
        parse_cache_get_response(Some(Message::CacheValue {
            tenant_id: "t1".into(),
            namespace: "default".into(),
            cache: "primary".into(),
            key: "k".into(),
            value: Some(value.clone()),
            request_id: None
        }))
        .expect("ok"),
        Some(value)
    );
    assert!(parse_cache_get_response(Some(Message::Ok)).is_err());
    assert!(parse_cache_get_response(None).is_err());
}

#[test]
fn ensure_event_order_variants() {
    ensure_event_order(&[b"alpha".to_vec(), b"beta".to_vec()]).expect("ok");
    assert!(ensure_event_order(&[b"beta".to_vec(), b"alpha".to_vec()]).is_err());
}

#[test]
fn ensure_cache_value_variants() {
    let expected = Bytes::from_static(b"value");
    ensure_cache_value(Some(expected.clone()), expected.clone(), "cache get").expect("ok");
    assert!(ensure_cache_value(None, expected.clone(), "cache get").is_err());
    assert!(ensure_cache_value(Some(Bytes::from_static(b"nope")), expected, "cache get").is_err());
}

#[test]
fn ensure_cache_expired_variants() {
    ensure_cache_expired(None, "expired").expect("ok");
    assert!(ensure_cache_expired(Some(Bytes::from_static(b"value")), "expired").is_err());
}

#[test]
fn ensure_client_event_variants() {
    ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"alpha")).expect("ok");
    assert!(
        ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")).is_err()
    );
}
