//! Replay and the handover to live delivery.
//!
//! This is the seam a subscriber cannot see: history from disk, then whatever
//! queued while that was being written, then live. It has to be contiguous, and
//! it has to be contiguous even though the queue in the middle is allowed to
//! drop. The sink here records what a subscriber would have received, so the
//! offsets can be read back and checked.
use bytes::Bytes;
use felix_broker::{Broker, HistoryRange, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::TempDir;

use super::*;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const SUBSCRIPTION: u64 = 7;

/// Records the frames a subscriber would have received.
#[derive(Default)]
struct Recorder {
    frames: Vec<Bytes>,
}

impl EventSink for Recorder {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.frames.push(Bytes::copy_from_slice(bytes));
        Ok(())
    }
}

impl Recorder {
    /// Every offset delivered, in the order it was written.
    fn offsets(&self) -> Vec<u64> {
        let mut offsets = Vec::new();
        for frame in &self.frames {
            let frame = felix_wire::Frame::decode(frame.clone()).expect("decode the frame");
            let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode the batch");
            let base = batch.base_offset.expect("offsets were negotiated");
            for index in 0..batch.payloads.len() {
                offsets.push(base + index as u64);
            }
        }
        offsets
    }

    /// Every payload delivered, in order.
    fn payloads(&self) -> Vec<Bytes> {
        let mut payloads = Vec::new();
        for frame in &self.frames {
            let frame = felix_wire::Frame::decode(frame.clone()).expect("decode the frame");
            let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode the batch");
            payloads.extend(batch.payloads);
        }
        payloads
    }

    fn batches(&self) -> usize {
        self.frames.len()
    }
}

/// A broker with a durable stream, so history can be paged from disk.
async fn durable_broker() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (Arc::new(broker), dir)
}

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

async fn publish(broker: &Broker, values: &[&str]) {
    for value in values {
        broker
            .publish(TENANT, NAMESPACE, STREAM, payload(value))
            .await
            .expect("publish");
    }
}

#[allow(clippy::too_many_arguments)]
async fn replay(
    sink: &mut Recorder,
    broker: &Arc<Broker>,
    history: Option<HistoryRange>,
    backlog: Vec<(u64, Bytes)>,
    backlog_start: u64,
    subscription: &mut felix_broker::Subscription,
    max_events: usize,
) -> Result<()> {
    write_replay(
        sink,
        broker,
        TENANT,
        NAMESPACE,
        STREAM,
        0,
        SUBSCRIPTION,
        history,
        backlog,
        backlog_start,
        subscription,
        max_events,
        1024 * 1024,
        true,
    )
    .await
}

/// History is paged from disk in order, and the offsets on the wire are the log
/// offsets — a subscriber uses them to tell a gap from a pause.
#[tokio::test]
async fn history_is_replayed_from_disk_in_order() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b", "c", "d"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 4,
        }),
        Vec::new(),
        4,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.offsets(), vec![0, 1, 2, 3]);
    assert_eq!(
        sink.payloads(),
        vec![payload("a"), payload("b"), payload("c"), payload("d")],
    );
}

/// The range is half-open: `until_offset` is where live delivery picks up, so
/// replaying it would deliver that record twice.
#[tokio::test]
async fn the_history_range_stops_before_its_end() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b", "c", "d"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 1,
            until_offset: 3,
        }),
        Vec::new(),
        3,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.offsets(), vec![1, 2]);
}

/// History runs before the backlog, and the two are contiguous.
#[tokio::test]
async fn history_is_followed_by_the_backlog() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 2,
        }),
        vec![(2, payload("c")), (3, payload("d"))],
        4,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.offsets(), vec![0, 1, 2, 3]);
    assert_eq!(sink.payloads().last(), Some(&payload("d")));
}

/// **A publish that lands during replay is delivered, not lost.** The live
/// subscription is registered before replay starts, so records queue on it the
/// whole time replay is writing; the catch-up pass is what carries them across.
#[tokio::test]
async fn a_publish_that_arrives_during_replay_is_carried_across() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");

    // Queued on the live subscription after it was registered, which is exactly
    // where a publish concurrent with replay ends up.
    publish(&broker, &["c", "d"]).await;

    let mut sink = Recorder::default();
    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 2,
        }),
        Vec::new(),
        2,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(
        sink.offsets(),
        vec![0, 1, 2, 3],
        "a publish concurrent with replay was dropped at the handover",
    );
}

/// **A gap the subscriber queue dropped is filled from disk.** The queue is a
/// shortcut; the log is the authority. Without this, a long replay silently
/// loses whatever the bounded queue evicted while it ran.
#[tokio::test]
async fn a_gap_left_by_the_queue_is_filled_from_disk() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b", "c", "d", "e"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");

    // Only the tail is queued: offsets 2 and 3 are on disk and nowhere else,
    // which is what an eviction looks like from here.
    publish(&broker, &["f"]).await;

    let mut sink = Recorder::default();
    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 2,
        }),
        Vec::new(),
        2,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(
        sink.offsets(),
        vec![0, 1, 2, 3, 4, 5],
        "the records the queue never held were not paged back in",
    );
}

/// A queued record that history already covered is not sent again. The two
/// sources overlap by construction, and a duplicate is as visible to a client
/// as a gap.
#[tokio::test]
async fn a_queued_record_already_covered_by_history_is_not_repeated() {
    let (broker, _dir) = durable_broker().await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    publish(&broker, &["a", "b", "c"]).await;

    let mut sink = Recorder::default();
    replay(
        &mut sink,
        &broker,
        // History covers everything already queued.
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 3,
        }),
        Vec::new(),
        3,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.offsets(), vec![0, 1, 2]);
}

/// Nothing to replay writes nothing, rather than an empty batch a subscriber
/// would have to interpret.
#[tokio::test]
async fn an_empty_replay_writes_nothing() {
    let (broker, _dir) = durable_broker().await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        None,
        Vec::new(),
        0,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.batches(), 0);
}

/// The batch limit is respected while the offsets stay contiguous across the
/// batches it produces.
#[tokio::test]
async fn replay_is_split_into_batches_without_losing_the_run() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a", "b", "c", "d", "e", "f"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 0,
            until_offset: 6,
        }),
        Vec::new(),
        6,
        &mut subscription,
        2,
    )
    .await
    .expect("replay");

    assert_eq!(sink.batches(), 3, "the batch limit was not applied");
    assert_eq!(sink.offsets(), vec![0, 1, 2, 3, 4, 5]);
}

/// A backlog handed in without any history is delivered as it stands.
#[tokio::test]
async fn a_backlog_without_history_is_delivered() {
    let (broker, _dir) = durable_broker().await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        None,
        vec![(0, payload("a")), (1, payload("b"))],
        2,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.offsets(), vec![0, 1]);
}

/// A history range that starts past the end of the log stops rather than
/// spinning on empty reads.
#[tokio::test]
async fn a_history_range_past_the_end_of_the_log_stops() {
    let (broker, _dir) = durable_broker().await;
    publish(&broker, &["a"]).await;
    let mut subscription = broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let mut sink = Recorder::default();

    replay(
        &mut sink,
        &broker,
        Some(HistoryRange {
            from_offset: 50,
            until_offset: 60,
        }),
        Vec::new(),
        60,
        &mut subscription,
        64,
    )
    .await
    .expect("replay");

    assert_eq!(sink.batches(), 0);
}

/// The batch builder, on its own: a run is kept together, and a break in the
/// offsets closes the batch — a batch carries one base offset, so a gap inside
/// one would misplace everything after it.
mod batching {
    use super::*;

    #[test]
    fn a_break_in_the_offsets_closes_the_batch() {
        let mut batch = ReplayBatch::new(64, 1024);
        assert!(batch.push(0, payload("a")).is_none());
        assert!(batch.push(1, payload("b")).is_none());

        let closed = batch.push(9, payload("c")).expect("the run broke");

        assert_eq!(
            closed.iter().map(|(offset, _)| *offset).collect::<Vec<_>>(),
            vec![0, 1],
        );
        assert_eq!(
            batch
                .take()
                .expect("the new run")
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            vec![9],
        );
    }

    #[test]
    fn the_byte_limit_closes_a_batch() {
        let mut batch = ReplayBatch::new(64, 4);
        assert!(batch.push(0, payload("aaa")).is_none());

        let closed = batch.push(1, payload("bbb")).expect("over the byte limit");

        assert_eq!(closed.len(), 1);
    }

    /// A single record larger than the limit still goes: the alternative is a
    /// record that can never be delivered.
    #[test]
    fn a_record_larger_than_the_limit_is_still_delivered() {
        let mut batch = ReplayBatch::new(64, 1);
        assert!(batch.push(0, payload("a much larger payload")).is_none());
        assert_eq!(batch.take().expect("the record").len(), 1);
    }

    #[test]
    fn an_empty_batch_has_nothing_to_take() {
        let mut batch = ReplayBatch::new(64, 1024);
        assert!(batch.take().is_none());
    }
}
