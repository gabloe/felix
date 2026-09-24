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

mod durable {
    //! On a durable stream the sequences come from the log, so a broker that
    //! did not take the original -- restarted, or a replica shipped the
    //! records -- answers a re-send the same way.

    use bytes::Bytes;
    use felix_storage::EphemeralCache;
    use felix_storage::log::{FsyncMode, LogConfig};

    use crate::durable::StreamLog;
    use crate::error::BrokerError;
    use crate::{Broker, DurableStorage, StreamHandle, StreamMetadata};

    async fn open(dir: &std::path::Path) -> (Broker, StreamHandle, DurableStorage) {
        let config = LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let storage = DurableStorage::open(dir, config).expect("storage");
        let broker =
            Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                StreamMetadata {
                    durable: true,
                    ..Default::default()
                },
            )
            .await
            .expect("register");
        let handle = broker
            .resolve_stream_handle("t1", "default", "orders", 0)
            .await
            .expect("handle");
        (broker, handle, storage)
    }

    fn log(handle: &StreamHandle) -> &StreamLog {
        handle.state.durable.as_ref().expect("durable")
    }

    fn bytes(values: &[&'static str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|v| Bytes::from_static(v.as_bytes()))
            .collect()
    }

    async fn stored(handle: &StreamHandle) -> Vec<Bytes> {
        log(handle)
            .read_from(0, usize::MAX)
            .await
            .expect("read")
            .into_iter()
            .map(|record| record.payload)
            .collect()
    }

    /// Ship `leader`'s log from `from` to `follower` the way replication does,
    /// stopping after `count` records.
    async fn ship(
        leader: &StreamHandle,
        broker: &Broker,
        follower: &StreamHandle,
        from: u64,
        count: usize,
    ) {
        let records: Vec<_> = log(leader)
            .read_from(from, usize::MAX)
            .await
            .expect("read")
            .into_iter()
            .take(count)
            .collect();
        let payloads: Vec<Bytes> = records.iter().map(|r| r.payload.clone()).collect();
        let marks: Vec<_> = records
            .iter()
            .map(|r| crate::replication::mark_to_wire(r.mark))
            .collect();
        let checksum = felix_wire::internal::batch_checksum(&payloads, &marks);
        let applied = crate::replication::apply(log(follower), from, checksum, &payloads, &marks)
            .await
            .expect("apply")
            .expect("in order");
        broker
            .adopt_replicated("t1", "default", "orders", 0, applied.durable_offset)
            .await
            .expect("adopt");
    }

    #[tokio::test]
    async fn a_restarted_leader_answers_a_re_send_from_its_log() {
        let dir = tempfile::tempdir().expect("dir");
        let first = {
            let (broker, handle, storage) = open(dir.path()).await;
            broker
                .publish_batch_idempotent(&handle, 7, 0, &bytes(&["a"]))
                .await
                .expect("0");
            let first = broker
                .publish_batch_idempotent(&handle, 7, 1, &bytes(&["b", "c"]))
                .await
                .expect("1");
            storage.shutdown().await.expect("shutdown");
            first
        };

        let (broker, handle, _storage) = open(dir.path()).await;
        let again = broker
            .publish_batch_idempotent(&handle, 7, 1, &bytes(&["b", "c"]))
            .await
            .expect("re-send");
        assert!(again.duplicate, "the re-send was appended again");
        assert_eq!(again.outcome.offsets, first.outcome.offsets);

        let next = broker
            .publish_batch_idempotent(&handle, 7, 2, &bytes(&["d"]))
            .await
            .expect("next");
        assert!(!next.duplicate);
        assert_eq!(stored(&handle).await, bytes(&["a", "b", "c", "d"]));
    }

    /// A promoted replica, or a move's destination, holds only what it was
    /// shipped, and that is enough.
    #[tokio::test]
    async fn a_replica_answers_a_re_send_of_what_it_was_shipped() {
        let leader_dir = tempfile::tempdir().expect("dir");
        let follower_dir = tempfile::tempdir().expect("dir");
        let (leader, on_leader, _l) = open(leader_dir.path()).await;
        let (follower, on_follower, _f) = open(follower_dir.path()).await;

        for (sequence, batch) in [["a", "b"], ["c", "d"]].iter().enumerate() {
            leader
                .publish_batch_idempotent(&on_leader, 7, sequence as u64, &bytes(batch))
                .await
                .expect("publish");
        }
        ship(&on_leader, &follower, &on_follower, 0, usize::MAX).await;

        let again = follower
            .publish_batch_idempotent(&on_follower, 7, 1, &bytes(&["c", "d"]))
            .await
            .expect("re-send");
        assert!(again.duplicate, "the replica appended the re-send");
        assert_eq!(again.outcome.offsets, Some((2, 3)));
        let next = follower
            .publish_batch_idempotent(&on_follower, 7, 2, &bytes(&["e"]))
            .await
            .expect("the producer carries on");
        assert!(!next.duplicate);
        assert_eq!(
            stored(&on_follower).await,
            bytes(&["a", "b", "c", "d", "e"])
        );

        let err = follower
            .publish_batch_idempotent(&on_follower, 8, 3, &bytes(&["x"]))
            .await
            .expect_err("a producer the log never saw may not start mid-way");
        assert!(matches!(
            err,
            BrokerError::UnknownProducer { producer_id: 8 }
        ));
    }

    /// A replica promoted with only the start of a batch -- its leader died
    /// mid-shipment -- finishes the batch on the re-send instead of writing
    /// the start a second time.
    #[tokio::test]
    async fn a_batch_cut_short_is_finished_not_repeated() {
        let leader_dir = tempfile::tempdir().expect("dir");
        let follower_dir = tempfile::tempdir().expect("dir");
        let (leader, on_leader, _l) = open(leader_dir.path()).await;
        let (follower, on_follower, _f) = open(follower_dir.path()).await;

        leader
            .publish_batch_idempotent(&on_leader, 7, 0, &bytes(&["x", "y", "z"]))
            .await
            .expect("publish");
        ship(&on_leader, &follower, &on_follower, 0, 1).await;

        let resent = follower
            .publish_batch_idempotent(&on_follower, 7, 0, &bytes(&["x", "y", "z"]))
            .await
            .expect("re-send");
        assert!(!resent.duplicate, "part of it was new");
        assert_eq!(resent.outcome.offsets, Some((0, 2)));
        assert_eq!(stored(&on_follower).await, bytes(&["x", "y", "z"]));

        let again = follower
            .publish_batch_idempotent(&on_follower, 7, 0, &bytes(&["x", "y", "z"]))
            .await
            .expect("re-send");
        assert!(again.duplicate);
    }
}
