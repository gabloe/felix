//! Which reports wake placement, and that a woken pass acts on them.
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::advances_move;
use crate::model::{ShardAssignment, ShardState};
use crate::store::ControlPlaneStore;
use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

fn assignment(state: ShardState, successor: Option<&str>) -> ShardAssignment {
    ShardAssignment {
        key: shard_zero(),
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 4,
        state,
        successor: successor.map(str::to_string),
    }
}

#[test]
fn only_the_report_a_move_waits_for_wakes_placement() {
    let y = ["broker-y".to_string()];
    let staged = assignment(ShardState::Active, Some("broker-y"));
    let fenced = assignment(ShardState::Draining, Some("broker-y"));
    let settled = assignment(ShardState::Active, None);

    assert!(advances_move(&staged, 4, false, &y), "successor caught up");
    assert!(advances_move(&fenced, 4, true, &y), "leader drained");

    assert!(!advances_move(&staged, 4, false, &[]), "still catching up");
    assert!(!advances_move(&fenced, 4, false, &y), "fenced, not drained");
    assert!(
        !advances_move(&settled, 4, false, &y),
        "no move in progress"
    );
    assert!(
        !advances_move(&fenced, 3, true, &y),
        "drained at an old generation"
    );
}

/// End to end on one instance: the leader reports drained over the API and
/// placement cuts over without waiting out its interval.
#[tokio::test]
async fn a_drained_report_cuts_over_without_waiting_for_the_interval() {
    let (store, keys) = one_shard_cluster().await;
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

    let state = app_state_ready(Arc::clone(&store) as _);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let reconciler = crate::cluster::placement::spawn_reconciler(
        Arc::clone(&store) as _,
        Default::default(),
        Default::default(),
        Duration::from_secs(3600),
        crate::raft::LeadershipGate::Always,
        Arc::clone(&state.placement_wakes),
        shutdown.clone(),
    );
    // Past the interval's immediate first tick.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let report = serde_json::json!({
        "incarnation": 0,
        "shards": [{
            "tenant_id": "t1",
            "namespace": "ns",
            "stream": "orders",
            "shard": 0,
            "generation": fenced.generation,
            "caught_up": ["broker-y"],
            "replica_offsets": [],
            "drained": true,
        }],
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/nodes/broker-x/replica-status")
        .header(
            "authorization",
            format!("Bearer {}", token(&keys, &["node.manage:cluster:*"])),
        )
        .header("content-type", "application/json")
        .body(Body::from(report.to_string()))
        .expect("request");
    let response = crate::api::build_router(state)
        .oneshot(request)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    tokio::time::timeout(Duration::from_secs(5), async {
        while store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get")
            .leader
            != "broker-y"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the report woke placement, which cut over");

    shutdown.cancel();
    reconciler.await.expect("reconciler");
}
