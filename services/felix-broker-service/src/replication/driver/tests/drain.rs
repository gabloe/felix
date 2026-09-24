//! A shard being drained reports when its tail stops moving.

use super::*;

/// One pass over a draining shard with one follower and no reporter.
async fn drain_pass(
    follower: &AcceptingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once(
        follower,
        broker,
        router,
        &QuorumMarks::new(),
        None,
        cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await
}

/// **A draining shard reports drained only once its log has stopped growing.**
/// The first passes ship the tail and say nothing; once the tail has held
/// still with nothing in flight, the report carries the flag and the
/// successor caught up against that tail. A record landing in between starts
/// the wait again.
#[tokio::test]
async fn a_draining_shard_reports_drained_once_its_tail_holds_still() {
    let (broker, _dir) = leader_with(3).await;
    let router = draining_router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let mut cursors = HashMap::new();

    // The catch-up pass, then the settling passes.
    let first = drain_pass(&follower, &broker, &router, &mut cursors).await;
    assert!(
        first.reports.iter().all(|report| !report.drained),
        "the first pass is the catch-up: {:?}",
        first.reports
    );
    let mut drained = None;
    for _ in 0..DRAIN_SETTLE_PASSES + 1 {
        let out = drain_pass(&follower, &broker, &router, &mut cursors).await;
        if let Some(report) = out.reports.into_iter().find(|report| report.drained) {
            drained = Some(report);
            break;
        }
    }
    let report = drained.expect("a settled draining shard never reported drained");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);

    // An append after the fence -- a publish admitted just before it -- is
    // shipped, and the settle wait starts over.
    let log = broker
        .durable_storage()
        .expect("durable")
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    log.append(&[Bytes::from("late")]).await.expect("append");
    let after_append = drain_pass(&follower, &broker, &router, &mut cursors).await;
    assert!(
        after_append.reports.iter().all(|report| !report.drained),
        "a late record must reset the drain: {:?}",
        after_append.reports
    );
    assert_eq!(
        follower.batches().iter().map(|(_, _, n)| n).sum::<usize>(),
        4,
        "the late record was shipped to the successor",
    );
}
