use bytes::Bytes;
use felix_storage::EphemeralCache;

use crate::error::BrokerError;
use crate::{Broker, Cursor, StreamMetadata};

#[tokio::test]
async fn cursor_replays_log_then_streams_new_events() {
    let broker = Broker::new(EphemeralCache::new().into());

    broker.register_tenant("t1").await.expect("tenant");

    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    let (backlog, mut sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, cursor)
        .await
        .expect("subscribe");
    assert_eq!(
        backlog,
        vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")]
    );
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"three"))
        .await
        .expect("publish");
    assert_eq!(
        sub.recv().await.expect("recv"),
        Bytes::from_static(b"three")
    );
}

#[tokio::test]
async fn subscribe_drop_unregisters_subscriber() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");

    let stream_state = broker
        .get_stream_state("t1", "default", "orders", 0)
        .await
        .expect("stream state");
    assert_eq!(stream_state.subscriber_count(), 0);

    let sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    assert_eq!(stream_state.subscriber_count(), 1);
    drop(sub);
    assert_eq!(stream_state.subscriber_count(), 0);
}

#[tokio::test]
async fn subscribe_to_nonexistent_stream_errors() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let err = broker
        .subscribe("t1", "default", "missing", 0)
        .await
        .expect_err("stream");
    assert!(matches!(err, BrokerError::StreamNotFound { .. }));
}

#[tokio::test]
async fn cursor_methods() {
    let cursor = Cursor { next_seq: 42 };
    assert_eq!(cursor.next_seq(), 42);
}
