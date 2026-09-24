//! End-to-end against a real store: a three-node cluster with a three-shard
//! stream.
use super::*;
use crate::model::{Namespace, NodeCapacity as Cap, Tenant};
use crate::store::memory::InMemoryStore;
use crate::store::{ControlPlaneStore, StoreConfig};

async fn cluster(node_ids: &[&str]) -> InMemoryStore {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    });
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "NS".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(stream("orders", 3))
        .await
        .expect("stream");

    for (i, id) in node_ids.iter().enumerate() {
        let mut n = node(id, NodeLifecycle::Live, None);
        n.spec.advertise_addr = format!("10.0.0.4:{}", 7600 + i);
        n.spec.capacity = Cap::default();
        store.register_node(n).await.expect("node");
    }
    store
}

#[tokio::test]
async fn three_nodes_and_three_shards_each_get_one_owner() {
    let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;

    let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
    assert_eq!(outcome.placed, 3);
    assert_eq!(outcome.unplaceable, 0);
    assert_eq!(outcome.failed, 0);

    let assignments = store.list_shard_assignments().await.expect("list");
    assert_eq!(assignments.len(), 3);
    assert!(assignments.iter().all(|a| a.state == ShardState::Assigning));
    assert!(assignments.iter().all(|a| a.generation == 0));
}

/// A pass over an already-placed cluster must write nothing, or a timer
/// would churn the rows and flood the changefeed.
#[tokio::test]
async fn a_second_pass_writes_nothing() {
    let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
    reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
    let after_first = store
        .shard_assignment_snapshot()
        .await
        .expect("snap")
        .next_seq;

    for _ in 0..5 {
        let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
        assert_eq!(outcome.placed, 0);
        assert_eq!(outcome.kept, 3);
    }

    assert_eq!(
        store
            .shard_assignment_snapshot()
            .await
            .expect("snap")
            .next_seq,
        after_first,
        "no change should have been published",
    );
}

/// Losing a broker re-places only its shards, and the generation moves so a
/// stale report from the old leader can be rejected.
#[tokio::test]
async fn a_lost_node_has_its_shards_replaced() {
    let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
    reconcile_once(&store, &Default::default(), MovePolicy::default()).await;

    let before = store.list_shard_assignments().await.expect("list");
    let victim = before[0].leader.clone();
    let lost = before.iter().filter(|a| a.leader == victim).count();
    store
        .set_node_lifecycle(&victim, NodeLifecycle::Down)
        .await
        .expect("down");

    let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
    assert_eq!(outcome.placed, lost);
    assert_eq!(outcome.kept, 3 - lost);

    let after = store.list_shard_assignments().await.expect("list");
    assert!(
        after.iter().all(|a| a.leader != victim),
        "nothing may remain on a node that is down",
    );
    for moved in after
        .iter()
        .filter(|a| before.iter().any(|b| b.key == a.key && b.leader == victim))
    {
        assert_eq!(moved.generation, 1, "a re-placement moves the generation");
    }
}

#[tokio::test]
async fn an_empty_cluster_places_nothing_and_says_so() {
    let store = cluster(&[]).await;
    let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
    assert_eq!(outcome.placed, 0);
    assert_eq!(outcome.unplaceable, 3);
    assert!(
        store
            .list_shard_assignments()
            .await
            .expect("list")
            .is_empty()
    );
}

fn shard_zero() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

async fn report(store: &InMemoryStore, generation: u64, caught_up: &[&str], drained: bool) {
    store
        .record_replica_report(crate::model::ReplicaReport {
            key: shard_zero(),
            generation,
            caught_up: caught_up.iter().map(|id| id.to_string()).collect(),
            offsets: Default::default(),
            reported_at_millis: store.now_millis().await.expect("clock"),
            drained,
        })
        .await
        .expect("report");
}

/// Two instances run placement over one store. One plans a fence from a
/// snapshot, then stalls; the other fences and cuts over. The stalled plan
/// must not land: it would hand the shard back to the old leader after the
/// new one may already have acknowledged writes the old one never saw.
#[tokio::test]
async fn a_fence_planned_before_a_cut_over_is_not_written_after_it() {
    let store = cluster(&["broker-x", "broker-y"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(stream("orders", 1))
        .await
        .expect("stream");
    let staged = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Active,
            successor: Some("broker-y".to_string()),
        })
        .await
        .expect("staged move");
    report(&store, staged.generation, &["broker-y"], false).await;

    let liveness = Default::default();
    let stale = super::super::reconciler::plan_pass(&store, &liveness, MovePolicy::default())
        .await
        .expect("plan");
    assert!(
        stale
            .plan()
            .moves()
            .any(|(_, step, _)| *step == MoveStep::Fence),
        "the stalled instance plans a fence",
    );

    let fenced = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(fenced.moved, 1);
    let draining = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(draining.state, ShardState::Draining);
    report(&store, draining.generation, &["broker-y"], true).await;
    let cut = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(cut.moved, 1);
    let cut_over = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(cut_over.leader, "broker-y");

    let late = super::super::reconciler::apply_pass(&store, &stale).await;
    assert_eq!(late.moved, 0);
    assert_eq!(late.conflicts, 1);
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        cut_over,
        "the cut-over stands",
    );
}

/// Two instances placing the same new shard: the second finds it placed.
#[tokio::test]
async fn a_placement_planned_against_no_assignment_does_not_overwrite_one() {
    let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
    let liveness = Default::default();
    let first = super::super::reconciler::plan_pass(&store, &liveness, MovePolicy::default())
        .await
        .expect("plan");
    let second = super::super::reconciler::plan_pass(&store, &liveness, MovePolicy::default())
        .await
        .expect("plan");

    assert_eq!(
        super::super::reconciler::apply_pass(&store, &first)
            .await
            .placed,
        3
    );
    let placed = store.list_shard_assignments().await.expect("list");
    let late = super::super::reconciler::apply_pass(&store, &second).await;
    assert_eq!(late.placed, 0);
    assert_eq!(late.conflicts, 3);
    assert_eq!(store.list_shard_assignments().await.expect("list"), placed);
}
