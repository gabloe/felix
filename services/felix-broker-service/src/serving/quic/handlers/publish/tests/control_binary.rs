//! Binary publish batches on the control stream.

use super::*;

#[tokio::test]
async fn handle_binary_publish_batch_control_requires_auth() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        None,
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("auth required");
    assert!(err.to_string().contains("auth required"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_tenant_mismatch() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("other", &["stream.publish:stream:tenant/ns/stream"]);
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("tenant mismatch");
    assert!(err.to_string().contains("tenant mismatch"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_forbidden() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.subscribe:stream:tenant/ns/stream"]);
    let err = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await
    .expect_err("forbidden");
    assert!(err.to_string().contains("forbidden"));
}

#[tokio::test]
async fn handle_binary_publish_batch_control_missing_stream_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, mut rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
    let recv = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await;
    assert!(recv.is_err(), "publish should not be enqueued");
}

#[tokio::test]
async fn handle_binary_publish_batch_control_enqueue_dropped_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn handle_binary_publish_batch_control_enqueue_error_is_ok() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_control(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
        false,
        &watch::channel(false).0,
    )
    .await;
    assert!(result.is_ok());
}
