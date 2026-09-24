//! A shard being drained reports once its write fence is quiet.

use super::*;
use crate::shards::lifecycle::fence::ShardFence;

/// One pass over a draining shard with one follower and no reporter.
async fn drain_pass(
    follower: &AcceptingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    fence: &ShardFence,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once_with(
        follower,
        broker,
        router,
        fence,
        &QuorumMarks::new(),
        None,
        cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
}

/// **A draining shard reports drained only once its fence is closed with no
/// write inside.** A write that entered before the close holds the report
/// back however still the tail looks; once it has landed and left, the next
/// pass ships it and reports drained against a tail that includes it.
#[tokio::test]
async fn a_draining_shard_reports_drained_once_its_fence_is_quiet() {
    let (broker, _dir) = leader_with(3).await;
    let router = draining_router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let fence = ShardFence::default();
    let shard = watch_key(&key());
    let mut cursors = HashMap::new();

    fence.open(&shard, 4);
    let in_flight = fence.enter(&shard, 4).expect("open");
    fence.close(&shard);

    // The tail is not moving, and that is not enough: a write is inside.
    for _ in 0..3 {
        let pass = drain_pass(&follower, &broker, &router, &fence, &mut cursors).await;
        assert!(
            pass.reports.iter().all(|report| !report.drained),
            "drained with a write in flight: {:?}",
            pass.reports
        );
    }

    let log = broker
        .durable_storage()
        .expect("durable")
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    log.append(&[Bytes::from("late")]).await.expect("append");
    drop(in_flight);

    let pass = drain_pass(&follower, &broker, &router, &fence, &mut cursors).await;
    let report = pass
        .reports
        .into_iter()
        .find(|report| report.drained)
        .expect("a quiet fence should report drained");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(
        follower.batches().iter().map(|(_, _, n)| n).sum::<usize>(),
        4,
        "the write that was in flight was shipped to the successor",
    );
}

/// A shard whose fence was never opened here has nothing in flight, so the
/// first pass ships the tail and reports drained in one go.
#[tokio::test]
async fn a_draining_shard_never_served_here_drains_on_its_first_pass() {
    let (broker, _dir) = leader_with(3).await;
    let router = draining_router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();

    let pass = drain_pass(
        &follower,
        &broker,
        &router,
        &ShardFence::default(),
        &mut HashMap::new(),
    )
    .await;
    let report = pass
        .reports
        .into_iter()
        .find(|report| report.drained)
        .expect("drained");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
}

/// A follower that stores every log it is sent, but refuses the auxiliary
/// ones — group cursors, dead letters, counters — while `refusing` is set,
/// to `only` when that names a node and to everyone otherwise.
#[derive(Default)]
struct AuxRefusingFollower {
    refusing: std::sync::atomic::AtomicBool,
    only: Option<&'static str>,
    aux_stored: std::sync::atomic::AtomicUsize,
}

impl PeerRequester for AuxRefusingFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        use std::sync::atomic::Ordering::SeqCst;
        let (batch, aux) = match message {
            InternalMessage::ReplicateRecords(batch) => (batch, false),
            InternalMessage::ReplicateGroupRecords(batch)
            | InternalMessage::ReplicateDeadLetterRecords(batch)
            | InternalMessage::ReplicateCounterRecords(batch) => (batch, true),
            other => panic!("unexpected message {other:?}"),
        };
        if aux {
            if self.refusing.load(SeqCst) && self.only.is_none_or(|only| only == node_id) {
                return Err(PeerError::Unavailable {
                    node_id: node_id.to_string(),
                    detail: "refusing auxiliary logs".to_string(),
                });
            }
            self.aux_stored.fetch_add(batch.payloads.len(), SeqCst);
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// Cursors for every log a pass ships, kept across passes.
#[derive(Default)]
struct AllCursors {
    main: HashMap<ShardKey, ShardCursors>,
    group: HashMap<ShardKey, ShardCursors>,
    dead_letters: HashMap<ShardKey, ShardCursors>,
    counters: HashMap<ShardKey, ShardCursors>,
}

/// One pass over a draining shard whose fence was never opened here, so only
/// the logs themselves can hold the drained report back.
async fn aux_pass(
    follower: &AuxRefusingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut AllCursors,
) -> Vec<crate::replication::reporter::ShardReport> {
    replicate_once_with(
        follower,
        broker,
        router,
        &ShardFence::default(),
        &QuorumMarks::new(),
        None,
        &mut cursors.main,
        &mut cursors.group,
        &mut cursors.dead_letters,
        &mut cursors.counters,
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
    .reports
}

/// A broker leading one record on the shard, which a group has dead-lettered.
async fn broker_with_a_dead_letter(dir: &std::path::Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir.join("streams"), config.clone()).expect("storage");
    storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open")
        .append(&[Bytes::from("v0")])
        .await
        .expect("append");
    let dead_letters = Arc::new(
        felix_broker::DeadLetters::open(dir.join("dead-letters"), config.clone())
            .expect("dead letters"),
    );
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.join("groups"), config).expect("groups"),
                ),
                Arc::clone(&dead_letters),
                std::time::Duration::from_secs(30),
                3,
            ),
    );
    dead_letters
        .record(
            &felix_broker::GroupKey {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
                shard: 0,
                group: "workers".to_string(),
            },
            0,
        )
        .await
        .expect("dead letter");
    broker
}

/// **A draining shard is not drained while a dead letter is not on the
/// successor.** The main log is level and the fence is quiet, but the
/// control plane cuts over on the drained report alone, and a dead letter left
/// behind is a record the new leader's group silently skips.
#[tokio::test]
async fn a_draining_shard_withholds_drained_until_its_dead_letters_are_shipped() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with_a_dead_letter(dir.path()).await;

    let router = draining_router(LOCAL, &["broker-b"], 4);
    let follower = AuxRefusingFollower::default();
    follower.refusing.store(true, SeqCst);
    let mut cursors = AllCursors::default();

    for _ in 0..3 {
        let reports = aux_pass(&follower, &broker, &router, &mut cursors).await;
        assert!(
            reports.iter().all(|report| !report.drained),
            "drained with the dead letter not on the successor: {reports:?}",
        );
    }

    follower.refusing.store(false, SeqCst);
    let reports = aux_pass(&follower, &broker, &router, &mut cursors).await;
    assert!(
        reports.iter().any(|report| report.drained),
        "a successor holding every log should see the shard drained: {reports:?}",
    );
    assert!(
        follower.aux_stored.load(SeqCst) > 0,
        "the dead letter was shipped"
    );
}

/// **The same for a counter add on a cache shard.** An acknowledged add that
/// is not on the successor when the shard cuts over disappears from the sum.
#[tokio::test]
async fn a_draining_cache_withholds_drained_until_its_counters_are_shipped() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let counters_store = Arc::new(
        felix_storage::CounterStore::open(dir.path().join("counters"), config.clone())
            .expect("counters"),
    );
    let broker = Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.path().join("caches"), config.clone())
                .expect("cache"),
        ))
        .with_durable_storage(
            DurableStorage::open(dir.path().join("streams"), config).expect("storage"),
        )
        .with_counters(Arc::clone(&counters_store)),
    );
    counters_store
        .add(TENANT, NAMESPACE, STREAM, 0, "hits", 5)
        .await
        .expect("add");

    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    router.publish(
        RoutingTable::build_with(
            [felix_router::Placed {
                key: cache,
                leader: LOCAL.to_string(),
                replicas: vec!["broker-b".to_string()],
                generation: 4,
                draining: true,
                successor: Some("broker-b".to_string()),
            }],
            &nodes,
        ),
        &nodes,
    );
    let follower = AuxRefusingFollower::default();
    follower.refusing.store(true, SeqCst);
    let mut cursors = AllCursors::default();

    for _ in 0..3 {
        let reports = aux_pass(&follower, &broker, &router, &mut cursors).await;
        assert!(
            reports.iter().all(|report| !report.drained),
            "drained with the counter add not on the successor: {reports:?}",
        );
    }

    follower.refusing.store(false, SeqCst);
    let reports = aux_pass(&follower, &broker, &router, &mut cursors).await;
    assert!(
        reports.iter().any(|report| report.drained),
        "a successor holding every log should see the cache drained: {reports:?}",
    );
}

/// **Only the successor gates the report.** Another replica still missing a
/// dead letter does not hold the move, but it is not offered as a leader
/// either: it is left out of `caught_up`.
#[tokio::test]
async fn a_lagging_replica_other_than_the_successor_is_left_out_not_waited_for() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with_a_dead_letter(dir.path()).await;
    // broker-b is the successor.
    let router = draining_router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AuxRefusingFollower {
        only: Some("broker-c"),
        ..AuxRefusingFollower::default()
    };
    follower.refusing.store(true, SeqCst);
    let mut cursors = AllCursors::default();

    let reports = aux_pass(&follower, &broker, &router, &mut cursors).await;
    let report = reports
        .iter()
        .find(|report| report.drained)
        .unwrap_or_else(|| panic!("the successor holds everything: {reports:?}"));
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
}
