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
