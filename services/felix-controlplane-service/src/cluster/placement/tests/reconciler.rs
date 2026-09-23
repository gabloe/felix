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
