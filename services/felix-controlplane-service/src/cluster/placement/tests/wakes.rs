//! Running placement on demand.
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::store::ControlPlaneStore;
use crate::test_support::{one_shard_cluster, shard_zero};

/// A drained leader is cut over as soon as a pass is asked for, not at the
/// next tick of an interval that here is an hour away.
#[tokio::test]
async fn a_requested_pass_runs_without_waiting_for_the_interval() {
    let (store, _keys) = one_shard_cluster().await;
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
        })
        .await
        .expect("fenced move");

    let wakes = Arc::new(PlacementWakes::default());
    let written = wakes.watch_assignments();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = spawn_reconciler(
        Arc::clone(&store) as _,
        Default::default(),
        MovePolicy::default(),
        Duration::from_secs(3600),
        crate::raft::LeadershipGate::Always,
        Arc::clone(&wakes),
        shutdown.clone(),
    );
    // The interval's first tick is immediate; let that pass find nothing to do.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        fenced,
        "nothing to do until the leader reports drained",
    );

    store
        .record_replica_report(crate::model::ReplicaReport {
            key: shard_zero(),
            generation: fenced.generation,
            caught_up: ["broker-y".to_string()].into_iter().collect(),
            offsets: Default::default(),
            reported_at_millis: store.now_millis().await.expect("clock"),
            drained: true,
        })
        .await
        .expect("report");
    wakes.request_pass();

    let cut_over = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = store
                .get_shard_assignment(&shard_zero())
                .await
                .expect("get");
            if current.leader == "broker-y" {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the requested pass cut the shard over");
    assert_eq!(cut_over.state, ShardState::Assigning);
    assert!(
        written.has_changed().expect("sender alive"),
        "the write wakes this instance's long-polls",
    );

    shutdown.cancel();
    task.await.expect("reconciler");
}
