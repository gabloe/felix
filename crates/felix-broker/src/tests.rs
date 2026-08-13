// Unit tests for the broker: fanout, queue policy, replay, subscription lifecycle,
// and the tenant/namespace/stream/cache registries.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::broker::{Broker, CacheMetadata, StreamMetadata};
use crate::config::SubQueuePolicy;
use crate::error::BrokerError;
use crate::stream_state::{Cursor, StreamState};

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    // Basic pub/sub flow with a single subscriber.
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
    let mut sub = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"hello"))
        .await
        .expect("publish");
    let msg = sub.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn publish_without_subscribers_returns_zero() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "empty", StreamMetadata::default())
        .await
        .expect("register");
    let delivered = broker
        .publish("t1", "default", "empty", Bytes::from_static(b"payload"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
}

#[tokio::test]
async fn stream_delivers_in_order_to_single_subscriber() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "ordered", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "ordered")
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "ordered", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "ordered", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
}

#[test]
fn append_batch_keeps_monotonic_sequences_and_trims_once() {
    let stream = StreamState::new(1, 8, SubQueuePolicy::DropNew, None);
    let first = vec![
        Bytes::from_static(b"a"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"c"),
        Bytes::from_static(b"d"),
        Bytes::from_static(b"e"),
    ];
    stream.append_batch(&first, 3);
    let second = vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")];
    stream.append_batch(&second, 3);

    let state = stream.log_state.lock();
    let seqs = state.log.iter().map(|entry| entry.seq).collect::<Vec<_>>();
    let payloads = state
        .log
        .iter()
        .map(|entry| entry.payload.clone())
        .collect::<Vec<_>>();

    assert_eq!(state.next_seq, 7);
    assert_eq!(seqs, vec![4, 5, 6]);
    assert_eq!(
        payloads,
        vec![
            Bytes::from_static(b"e"),
            Bytes::from_static(b"f"),
            Bytes::from_static(b"g")
        ]
    );
}

#[tokio::test]
async fn slow_subscriber_drops_messages_without_blocking_publish() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "laggy", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "laggy")
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "laggy", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    let delivered = broker
        .publish("t1", "default", "laggy", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn block_policy_backpressures_publish_when_queue_is_full() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::Block);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "blocky", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "blocky")
        .await
        .expect("subscribe");

    broker
        .publish("t1", "default", "blocky", Bytes::from_static(b"one"))
        .await
        .expect("publish");

    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        broker.publish("t1", "default", "blocky", Bytes::from_static(b"two")),
    )
    .await;
    assert!(blocked.is_err(), "publish should block on full queue");
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    let sent = broker
        .publish("t1", "default", "blocky", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(sent, 1);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
}

#[tokio::test]
async fn drop_old_policy_is_emulated_as_drop_new() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::DropOld);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "drop_old", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "drop_old")
        .await
        .expect("subscribe");

    broker
        .publish("t1", "default", "drop_old", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    let delivered = broker
        .publish("t1", "default", "drop_old", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn small_queue_does_not_grow_unbounded_under_burst() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(2)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::DropNew);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "bounded", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "bounded")
        .await
        .expect("subscribe");

    for i in 0..100 {
        let payload = Bytes::from(format!("msg-{i}"));
        let _ = broker
            .publish("t1", "default", "bounded", payload)
            .await
            .expect("publish");
    }

    // Queue is capped at 2; only the earliest buffered items are still available.
    let _ = sub.recv().await.expect("recv");
    let _ = sub.recv().await.expect("recv");
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn multiple_subscribers_receive_payload() {
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
    let mut sub_a = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    let mut sub_b = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"fanout"))
        .await
        .expect("publish");
    assert_eq!(
        sub_a.recv().await.expect("recv"),
        Bytes::from_static(b"fanout")
    );
    assert_eq!(
        sub_b.recv().await.expect("recv"),
        Bytes::from_static(b"fanout")
    );
}

#[tokio::test]
async fn publish_batch_shares_one_encoded_frame_across_subscribers() {
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
    let (mut rx_a, _guard_a) = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe a")
        .into_parts();
    let (mut rx_b, _guard_b) = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe b")
        .into_parts();
    let payloads = [Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    broker
        .publish_batch("t1", "default", "orders", &payloads)
        .await
        .expect("publish");

    let envelope_a = rx_a.recv().await.expect("recv a");
    let envelope_b = rx_b.recv().await.expect("recv b");
    let frame_a = envelope_a.shared_event_frame().expect("encode a");
    let frame_b = envelope_b.shared_event_frame().expect("encode b");

    assert_eq!(frame_a, frame_b);
    assert_eq!(frame_a.as_ptr(), frame_b.as_ptr());
}

#[tokio::test]
async fn queue_depth_returns_to_zero_after_receive_and_receiver_drop() {
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
        .get_stream_state("t1", "default", "orders")
        .await
        .expect("stream state");
    let (mut receiver, _guard) = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe")
        .into_parts();
    let payloads = [Bytes::from_static(b"one"), Bytes::from_static(b"two")];

    broker
        .publish_batch("t1", "default", "orders", &payloads)
        .await
        .expect("publish");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 2);
    receiver.recv().await.expect("receive");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 0);

    broker
        .publish_batch("t1", "default", "orders", &payloads)
        .await
        .expect("publish");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 2);
    drop(receiver);
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 0);
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
        .get_stream_state("t1", "default", "orders")
        .await
        .expect("stream state");
    assert_eq!(stream_state.subscriber_count(), 0);

    let sub = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    assert_eq!(stream_state.subscriber_count(), 1);
    drop(sub);
    assert_eq!(stream_state.subscriber_count(), 0);
}

#[tokio::test]
#[ignore = "microbenchmark: run explicitly for perf validation"]
async fn perf_hot_path_payload_4096_fanout_1_batch_64_binary() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(16_384)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");

    let mut sub = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    let iterations = 200usize;
    let payloads: Vec<Bytes> = (0..64).map(|_| Bytes::from(vec![0xAB; 4096])).collect();
    let expected = iterations * payloads.len();

    let drain = tokio::spawn(async move {
        for _ in 0..expected {
            let _ = sub.recv().await;
        }
    });

    let stream_state = broker
        .get_stream_state("t1", "default", "orders")
        .await
        .expect("stream_state");

    let mut snapshot_ns = 0u128;
    let mut publish_ns = 0u128;
    let mut encode_ns = 0u128;
    let mut write_ns = 0u128;

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = stream_state.subscriber_snapshot();
        snapshot_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        broker
            .publish_batch("t1", "default", "orders", &payloads)
            .await
            .expect("publish");
        publish_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        let frame = felix_wire::binary::encode_event_batch_bytes(1, &payloads)
            .expect("encode binary event batch");
        encode_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        let mut io_buf = Vec::with_capacity(frame.len());
        io_buf.extend_from_slice(frame.as_ref());
        write_ns += start.elapsed().as_nanos();
    }

    drain.await.expect("drain");

    println!(
        "perf payload=4096 fanout=1 batch=64 binary=true iterations={} snapshot_avg_us={:.2} publish_avg_us={:.2} encode_avg_us={:.2} write_avg_us={:.2}",
        iterations,
        snapshot_ns as f64 / iterations as f64 / 1_000.0,
        publish_ns as f64 / iterations as f64 / 1_000.0,
        encode_ns as f64 / iterations as f64 / 1_000.0,
        write_ns as f64 / iterations as f64 / 1_000.0,
    );
}

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
        .cursor_tail("t1", "default", "orders")
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
        .subscribe_with_cursor("t1", "default", "orders", cursor)
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

#[test]
fn zero_capacity_is_rejected() {
    let broker = Broker::new(EphemeralCache::new().into());
    let err = broker.with_topic_capacity(0).expect_err("capacity");
    assert!(matches!(err, BrokerError::CapacityTooLarge));
}

#[tokio::test]
async fn existence_checks_reflect_registrations() {
    let broker = Broker::new(EphemeralCache::new().into());
    assert!(!broker.namespace_exists("t1", "default").await);
    assert!(!broker.cache_exists("t1", "default", "primary").await);
    assert!(!broker.stream_exists("t1", "default", "orders").await);

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_cache("t1", "default", "primary", CacheMetadata)
        .await
        .expect("cache");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");

    assert!(broker.namespace_exists("t1", "default").await);
    assert!(broker.cache_exists("t1", "default", "primary").await);
    assert!(broker.stream_exists("t1", "default", "orders").await);
}

#[tokio::test]
async fn register_namespace_requires_tenant() {
    let broker = Broker::new(EphemeralCache::new().into());
    let err = broker
        .register_namespace("missing", "default")
        .await
        .expect_err("tenant");
    assert!(matches!(err, BrokerError::TenantNotFound(_)));
}

#[tokio::test]
async fn register_stream_requires_namespace() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    let err = broker
        .register_stream("t1", "missing", "orders", StreamMetadata::default())
        .await
        .expect_err("namespace");
    assert!(matches!(err, BrokerError::NamespaceNotFound { .. }));
}

#[tokio::test]
async fn publish_to_nonexistent_stream_errors() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let err = broker
        .publish("t1", "default", "missing", Bytes::from_static(b"data"))
        .await
        .expect_err("stream");
    assert!(matches!(err, BrokerError::StreamNotFound { .. }));
}

#[tokio::test]
async fn resolved_stream_handle_publishes_without_name_lookup() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    let mut sub = broker
        .subscribe("t1", "default", "orders")
        .await
        .expect("subscribe");
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders")
        .await
        .expect("handle");

    broker
        .publish_batch_to_handle(&handle, &[Bytes::from_static(b"handled")])
        .await
        .expect("publish");
    assert_eq!(
        sub.recv().await.expect("delivery"),
        Bytes::from_static(b"handled")
    );
}

#[tokio::test]
async fn removed_stream_invalidates_resolved_handle() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders")
        .await
        .expect("handle");
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");

    let err = broker
        .publish_batch_to_handle(&handle, &[Bytes::from_static(b"stale")])
        .await
        .expect_err("stale handle");
    assert!(matches!(err, BrokerError::StreamHandleInactive(id) if id == handle.id()));
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
        .subscribe("t1", "default", "missing")
        .await
        .expect_err("stream");
    assert!(matches!(err, BrokerError::StreamNotFound { .. }));
}

#[tokio::test]
async fn remove_tenant_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    let removed = broker.remove_tenant("t1").await.expect("remove");
    assert!(removed);
    // Removing again returns false
    let removed_again = broker.remove_tenant("t1").await.expect("remove");
    assert!(!removed_again);
}

#[tokio::test]
async fn remove_namespace_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    assert!(broker.namespace_exists("t1", "default").await);
    let removed = broker
        .remove_namespace("t1", "default")
        .await
        .expect("remove");
    assert!(removed);
    assert!(!broker.namespace_exists("t1", "default").await);
}

#[tokio::test]
async fn remove_stream_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    assert!(broker.stream_exists("t1", "default", "orders").await);
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    assert!(!broker.stream_exists("t1", "default", "orders").await);
}

#[tokio::test]
async fn cursor_methods() {
    let cursor = Cursor { next_seq: 42 };
    assert_eq!(cursor.next_seq(), 42);
}

#[tokio::test]
async fn broker_error_display() {
    let err = BrokerError::CapacityTooLarge;
    assert!(err.to_string().contains("capacity"));

    let err = BrokerError::CursorTooOld {
        oldest: 10,
        requested: 5,
    };
    assert!(err.to_string().contains("10"));
    assert!(err.to_string().contains("5"));

    let err = BrokerError::TenantNotFound("t1".to_string());
    assert!(err.to_string().contains("t1"));

    let err = BrokerError::NamespaceNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));

    let err = BrokerError::StreamNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
        stream: "s1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));
    assert!(err.to_string().contains("s1"));
}
