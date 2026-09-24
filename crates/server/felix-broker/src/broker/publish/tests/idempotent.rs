//! Idempotent publishes: one producer, one sequence, one append.

use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;

use crate::error::BrokerError;
use crate::{Broker, StreamHandle, StreamMetadata};

async fn broker_with_stream() -> (Broker, StreamHandle) {
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
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    (broker, handle)
}

/// **A re-sent batch is answered from memory and appended nothing.** The
/// subscriber sees it once, and the second answer says it was a duplicate
/// of the first.
#[tokio::test]
async fn a_re_sent_batch_lands_once() {
    let (broker, handle) = broker_with_stream().await;
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");

    let first = broker
        .publish_batch_idempotent(&handle, 7, 0, &[Bytes::from_static(b"a")])
        .await
        .expect("first");
    let again = broker
        .publish_batch_idempotent(&handle, 7, 0, &[Bytes::from_static(b"a")])
        .await
        .expect("re-send");
    assert!(!first.duplicate);
    assert!(again.duplicate, "the re-send was appended as new");
    assert_eq!(again.outcome, first.outcome);

    assert_eq!(sub.recv().await.expect("one"), Bytes::from_static(b"a"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "the re-send was delivered"
    );

    let next = broker
        .publish_batch_idempotent(&handle, 7, 1, &[Bytes::from_static(b"b")])
        .await
        .expect("next");
    assert!(!next.duplicate);
    assert_eq!(sub.recv().await.expect("two"), Bytes::from_static(b"b"));
}

/// A gap, an unknown producer, and an expired sequence are each refused
/// with their own error, and nothing is appended for any of them.
#[tokio::test]
async fn refusals_append_nothing() {
    let (broker, handle) = broker_with_stream().await;
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    broker
        .publish_batch_idempotent(&handle, 7, 0, &[Bytes::from_static(b"a")])
        .await
        .expect("first");
    sub.recv().await.expect("one");

    let gap = broker
        .publish_batch_idempotent(&handle, 7, 3, &[Bytes::from_static(b"skip")])
        .await;
    assert!(
        matches!(gap, Err(BrokerError::SequenceGap { expected: 1 })),
        "{gap:?}"
    );

    let unknown = broker
        .publish_batch_idempotent(&handle, 8, 2, &[Bytes::from_static(b"who")])
        .await;
    assert!(
        matches!(
            unknown,
            Err(BrokerError::UnknownProducer { producer_id: 8 })
        ),
        "{unknown:?}"
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "a refused batch was delivered"
    );
}

/// Two producers are independent: each has its own sequence, and a batch
/// from one does not move the other's.
#[tokio::test]
async fn producers_do_not_share_a_sequence() {
    let (broker, handle) = broker_with_stream().await;
    broker
        .publish_batch_idempotent(&handle, 1, 0, &[Bytes::from_static(b"a")])
        .await
        .expect("producer 1");
    broker
        .publish_batch_idempotent(&handle, 2, 0, &[Bytes::from_static(b"b")])
        .await
        .expect("producer 2 begins at zero too");
    let outcome = broker
        .publish_batch_idempotent(&handle, 1, 1, &[Bytes::from_static(b"c")])
        .await
        .expect("producer 1 continues");
    assert!(!outcome.duplicate);
}

/// Ids are the broker's, distinct, and never the zero a caller might
/// read as "no producer".
#[test]
fn producer_ids_are_distinct_and_never_zero() {
    let broker = Broker::new(EphemeralCache::new().into());
    let ids: std::collections::HashSet<u64> = (0..1000).map(|_| broker.new_producer_id()).collect();
    assert_eq!(ids.len(), 1000, "a producer id repeated");
    assert!(!ids.contains(&0));
}

/// Two re-sends of one sequence racing each other still append once: the
/// producer's turn serialises them.
#[tokio::test]
async fn racing_re_sends_append_once() {
    let (broker, handle) = broker_with_stream().await;
    let broker = std::sync::Arc::new(broker);
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let broker = std::sync::Arc::clone(&broker);
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            broker
                .publish_batch_idempotent(&handle, 9, 0, &[Bytes::from_static(b"once")])
                .await
                .expect("publish")
        }));
    }
    let mut appended = 0;
    for task in tasks {
        if !task.await.expect("task").duplicate {
            appended += 1;
        }
    }
    assert_eq!(appended, 1, "more than one re-send was appended");
    sub.recv().await.expect("one");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "a second copy was delivered"
    );
}
