//! Node store behaviour that both backends must satisfy.
//!
//! One body, two callers: the memory store runs it in `memory::tests`, Postgres
//! in `postgres_tests`. Parity is the point -- a rule that holds only in memory
//! is a rule the deployed system does not have.
use super::{ControlPlaneStore, StoreError};
use crate::model::{Node, NodeCapacity, NodeLifecycle, NodePatchRequest, NodeSpec, NodeStatus};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) fn node(node_id: &str, port: u16) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.4:{port}"),
            region: "us-west-2".to_string(),
            labels: BTreeMap::from([("rack".to_string(), "a1".to_string())]),
            capacity: NodeCapacity {
                max_shards: Some(64),
                weight: 2,
            },
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1_700_000_000_000,
            registered_at_millis: 1_699_000_000_000,
            incarnation: 0,
        },
    }
}

/// Run every node contract case against `store`.
///
/// Takes a store the caller has already emptied of nodes.
pub(crate) async fn run_node_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    register_is_readable(store).await;
    register_rejects_an_address_another_node_holds(store).await;
    reregistering_preserves_identity_and_bumps_incarnation(store).await;
    an_invalid_node_is_rejected(store).await;
    patch_updates_the_spec_and_leaves_observed_status_alone(store).await;
    patch_rejects_an_unsupported_transition(store).await;
    a_heartbeat_never_moves_backwards(store).await;
    a_heartbeat_publishes_no_change(store).await;
    set_lifecycle_is_idempotent(store).await;
    missing_nodes_report_not_found(store).await;
    delete_removes_and_publishes(store).await;
    a_snapshot_and_the_changes_after_it_lose_nothing(store).await;
    changes_are_ordered_and_monotonic(store).await;
}

/// Run the cases that need to share the store across tasks.
///
/// Separate from [`run_node_contract`] only because these need an owned handle.
pub(crate) async fn run_node_concurrency_contract(store: Arc<dyn ControlPlaneStore>) {
    a_snapshot_taken_under_concurrent_writes_loses_nothing(store).await;
}

/// The reason `node_changes.seq` is allocated from a locked row rather than a
/// sequence.
///
/// A sequence hands out numbers in request order, not commit order, so a
/// snapshot taken while writer 4 is still in flight and writer 5 has committed
/// reads `next_seq = 6` and never sees 4. Reconstructing from that snapshot
/// then comes up short, which is what this asserts cannot happen.
async fn a_snapshot_taken_under_concurrent_writes_loses_nothing(store: Arc<dyn ControlPlaneStore>) {
    const WRITERS: u16 = 24;

    clear(store.as_ref()).await;

    let writers: Vec<_> = (0..WRITERS)
        .map(|i| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .register_node(node(&format!("broker-{i:03}"), 7200 + i))
                    .await
                    .expect("register");
            })
        })
        .collect();

    // Taken while the writers are still going, which is the whole point.
    let mut snapshots = Vec::new();
    for _ in 0..WRITERS {
        snapshots.push(store.node_snapshot().await.expect("snapshot"));
        tokio::task::yield_now().await;
    }

    for writer in writers {
        writer.await.expect("writer");
    }

    let final_state: BTreeMap<String, Node> = store
        .list_nodes()
        .await
        .expect("list")
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect();
    assert_eq!(final_state.len(), WRITERS as usize);

    for snapshot in snapshots {
        let since = snapshot.next_seq;
        let mut reconstructed: BTreeMap<String, Node> = snapshot
            .items
            .into_iter()
            .map(|n| (n.node_id.clone(), n))
            .collect();
        for change in store.node_changes(since).await.expect("changes").items {
            match change.node {
                Some(node) => {
                    reconstructed.insert(change.node_id, node);
                }
                None => {
                    reconstructed.remove(&change.node_id);
                }
            }
        }
        assert_eq!(
            reconstructed.keys().collect::<Vec<_>>(),
            final_state.keys().collect::<Vec<_>>(),
            "a snapshot at next_seq={since} plus the changes after it lost a node",
        );
    }
}

async fn clear(store: &dyn ControlPlaneStore) {
    for node in store.list_nodes().await.expect("list") {
        store.delete_node(&node.node_id).await.expect("delete");
    }
}

async fn register_is_readable(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let stored = store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");
    assert_eq!(stored.node_id, "broker-a");
    assert_eq!(stored.status.incarnation, 0);

    let fetched = store.get_node("broker-a").await.expect("get");
    assert_eq!(fetched, stored);
    assert_eq!(store.list_nodes().await.expect("list"), vec![stored]);
}

async fn register_rejects_an_address_another_node_holds(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");

    let mut clash = node("broker-b", 7001);
    clash.spec.advertise_addr = node("broker-a", 7001).spec.advertise_addr;
    let err = store
        .register_node(clash)
        .await
        .expect_err("should conflict");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    // The clash must not have half-registered the second node.
    assert!(store.get_node("broker-b").await.is_err());
}

async fn reregistering_preserves_identity_and_bumps_incarnation(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let first = store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");

    let mut restarted = node("broker-a", 7001);
    restarted.spec.region = "eu-central-1".to_string();
    // A restarting broker does not know when its identity was first seen.
    restarted.status.registered_at_millis = 0;
    restarted.status.last_heartbeat_at_millis = first.status.last_heartbeat_at_millis + 5_000;

    let second = store.register_node(restarted).await.expect("re-register");
    assert_eq!(second.status.incarnation, 1);
    assert_eq!(
        second.status.registered_at_millis, first.status.registered_at_millis,
        "the identity keeps its original registration time",
    );
    assert_eq!(second.spec.region, "eu-central-1");
}

async fn an_invalid_node_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut bad = node("broker-a", 7001);
    bad.spec.advertise_addr = "not-an-address".to_string();
    let err = store.register_node(bad).await.expect_err("should reject");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(store.list_nodes().await.expect("list").is_empty());
}

async fn patch_updates_the_spec_and_leaves_observed_status_alone(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let before = store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");

    let after = store
        .patch_node(
            "broker-a",
            NodePatchRequest {
                region: Some("eu-central-1".to_string()),
                ..NodePatchRequest::default()
            },
        )
        .await
        .expect("patch");

    assert_eq!(after.spec.region, "eu-central-1");
    assert_eq!(after.status, before.status);
}

async fn patch_rejects_an_unsupported_transition(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("set down");

    let err = store
        .patch_node(
            "broker-a",
            NodePatchRequest {
                lifecycle: Some(NodeLifecycle::Draining),
                ..NodePatchRequest::default()
            },
        )
        .await
        .expect_err("should reject");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let unchanged = store.get_node("broker-a").await.expect("get");
    assert_eq!(unchanged.status.lifecycle, NodeLifecycle::Down);
}

async fn a_heartbeat_never_moves_backwards(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let registered = store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");
    let at = registered.status.last_heartbeat_at_millis;

    store
        .record_node_heartbeat("broker-a", at + 1_000)
        .await
        .expect("beat");
    store
        .record_node_heartbeat("broker-a", at - 1_000)
        .await
        .expect("late beat");

    let seen = store.get_node("broker-a").await.expect("get");
    assert_eq!(seen.status.last_heartbeat_at_millis, at + 1_000);
}

async fn a_heartbeat_publishes_no_change(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");
    let before = store.node_snapshot().await.expect("snapshot").next_seq;

    store
        .record_node_heartbeat("broker-a", 1_800_000_000_000)
        .await
        .expect("beat");

    let after = store.node_snapshot().await.expect("snapshot").next_seq;
    assert_eq!(
        before, after,
        "a heartbeat would evict real membership changes from the window",
    );
}

async fn set_lifecycle_is_idempotent(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");

    let moved = store
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("set down");
    assert!(moved.is_some());
    let after_first = store.node_snapshot().await.expect("snapshot").next_seq;

    let repeat = store
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("set down again");
    assert!(repeat.is_none(), "a repeated sweep should publish nothing");
    assert_eq!(
        after_first,
        store.node_snapshot().await.expect("snapshot").next_seq
    );
}

async fn missing_nodes_report_not_found(store: &dyn ControlPlaneStore) {
    clear(store).await;
    assert!(matches!(
        store.get_node("absent").await.expect_err("get"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store.delete_node("absent").await.expect_err("delete"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store
            .record_node_heartbeat("absent", 1)
            .await
            .expect_err("beat"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store
            .patch_node("absent", NodePatchRequest::default())
            .await
            .expect_err("patch"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store
            .set_node_lifecycle("absent", NodeLifecycle::Down)
            .await
            .expect_err("lifecycle"),
        StoreError::NotFound(_)
    ));
}

async fn delete_removes_and_publishes(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");
    let since = store.node_snapshot().await.expect("snapshot").next_seq;

    store.delete_node("broker-a").await.expect("delete");

    assert!(store.get_node("broker-a").await.is_err());
    let changes = store.node_changes(since).await.expect("changes");
    assert_eq!(changes.items.len(), 1);
    assert_eq!(changes.items[0].node_id, "broker-a");
    assert!(
        changes.items[0].node.is_none(),
        "a deregistration carries no body",
    );
}

/// The bootstrap contract: take a snapshot, poll from its `next_seq`, and the
/// two together describe every node exactly once.
async fn a_snapshot_and_the_changes_after_it_lose_nothing(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .register_node(node("broker-a", 7001))
        .await
        .expect("register");

    let snapshot = store.node_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.items.len(), 1);

    store
        .register_node(node("broker-b", 7002))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-b", NodeLifecycle::Draining)
        .await
        .expect("drain");

    let changes = store
        .node_changes(snapshot.next_seq)
        .await
        .expect("changes");
    assert!(
        changes.items.iter().all(|c| c.seq >= snapshot.next_seq),
        "a change already in the snapshot must not be replayed",
    );
    assert!(
        changes.items.iter().any(|c| c.node_id == "broker-b"),
        "a change committed after the snapshot must be visible from its next_seq",
    );

    // Applying the snapshot then the changes reproduces the store exactly.
    let mut reconstructed: BTreeMap<String, Node> = snapshot
        .items
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect();
    for change in changes.items {
        match change.node {
            Some(node) => {
                reconstructed.insert(change.node_id, node);
            }
            None => {
                reconstructed.remove(&change.node_id);
            }
        }
    }
    let actual: BTreeMap<String, Node> = store
        .list_nodes()
        .await
        .expect("list")
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect();
    assert_eq!(reconstructed, actual);
}

async fn changes_are_ordered_and_monotonic(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let since = store.node_snapshot().await.expect("snapshot").next_seq;

    for i in 0..5u16 {
        store
            .register_node(node(&format!("broker-{i}"), 7100 + i))
            .await
            .expect("register");
    }

    let changes = store.node_changes(since).await.expect("changes");
    assert_eq!(changes.items.len(), 5);
    for pair in changes.items.windows(2) {
        assert!(
            pair[0].seq < pair[1].seq,
            "seq must be strictly increasing: {} then {}",
            pair[0].seq,
            pair[1].seq,
        );
    }
    assert!(
        changes.next_seq > changes.items.last().expect("last").seq,
        "next_seq must be past the last change returned",
    );
}
