//! Shard assignment behaviour both backends must satisfy.
//!
//! One body, two callers, for the same reason as `node_contract`: a rule that
//! holds only in memory is a rule the deployed system does not have.
use std::sync::Arc;

use super::{ControlPlaneStore, StoreError};
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, ShardAssignment,
    ShardKey, ShardKind, ShardState, Stream, StreamKind, Tenant,
};
use crate::store::node_contract::node;

const TENANT: &str = "shard-t";
const NAMESPACE: &str = "shard-ns";
const STREAM: &str = "orders";
const SHARDS: u32 = 4;
/// Deliberately the stream's name, and deliberately a different shard count:
/// the two must be distinguished by kind and nothing else.
const CACHE: &str = "orders";
const CACHE_SHARDS: u32 = 2;
const _: () = assert!(
    SHARDS > CACHE_SHARDS,
    "the cache must have fewer shards than the stream, or the bound test proves nothing",
);

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

fn assignment(shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        // Deliberately non-zero: the store owns this, and every test that checks
        // a generation is checking the store ignored what the caller sent.
        generation: 999,
        state: ShardState::Assigning,
    }
}

/// Build the tenant, namespace, stream, and two nodes every case needs.
async fn seed(store: &dyn ControlPlaneStore) {
    let _ = store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Shards".to_string(),
        })
        .await;
    let _ = store
        .create_namespace(Namespace {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            display_name: "Shards".to_string(),
        })
        .await;
    let _ = store
        .create_stream(Stream {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            kind: StreamKind::Stream,
            shards: SHARDS,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
        })
        .await;

    let _ = store
        .create_cache(Cache {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            cache: CACHE.to_string(),
            display_name: "Orders cache".to_string(),
            shards: CACHE_SHARDS,
            replication_factor: 1,
        })
        .await;

    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        let _ = store.register_node(node(id, 7500 + i as u16)).await;
    }
}

async fn clear(store: &dyn ControlPlaneStore) {
    for assignment in store.list_shard_assignments().await.expect("list") {
        store
            .delete_shard_assignment(&assignment.key)
            .await
            .expect("delete");
    }
}

pub(crate) async fn run_shard_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    seed(store).await;

    an_assignment_is_readable(store).await;
    the_store_owns_the_generation(store).await;
    one_assignment_per_shard(store).await;
    a_shard_outside_the_stream_is_rejected(store).await;
    an_unknown_stream_is_rejected(store).await;
    an_unknown_node_is_rejected(store).await;
    an_unsupported_state_transition_is_rejected(store).await;
    assignments_can_be_listed_by_leader(store).await;
    missing_assignments_report_not_found(store).await;
    deleting_leaves_the_shard_unowned(store).await;
    a_node_leading_a_shard_cannot_be_deleted(store).await;
    a_snapshot_and_the_changes_after_it_lose_nothing(store).await;
    a_cache_shard_and_a_stream_shard_of_the_same_name_coexist(store).await;
    a_cache_shard_is_bounded_by_the_cache_not_the_stream(store).await;
    an_unknown_cache_is_rejected(store).await;
}

fn cache_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard,
        kind: ShardKind::Cache,
    }
}

fn cache_assignment(shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: cache_key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 999,
        state: ShardState::Assigning,
    }
}

/// The collision the kind exists to prevent. Without it these two writes are
/// the same primary key, and the second silently takes the first's ownership --
/// so a client would be routed to a broker holding the wrong log entirely.
async fn a_cache_shard_and_a_stream_shard_of_the_same_name_coexist(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("stream shard");
    store
        .put_shard_assignment(cache_assignment(0, "broker-y"))
        .await
        .expect("cache shard");

    let stream_shard = store.get_shard_assignment(&key(0)).await.expect("stream");
    let cache_shard = store
        .get_shard_assignment(&cache_key(0))
        .await
        .expect("cache");

    assert_eq!(stream_shard.leader, "broker-x");
    assert_eq!(cache_shard.leader, "broker-y");
    assert_eq!(store.list_shard_assignments().await.expect("list").len(), 2);
}

/// The shard bound comes from whichever of the two the key names. Shard 3 is
/// inside the stream and outside the cache, so resolving the bound against the
/// wrong one would let this through.
async fn a_cache_shard_is_bounded_by_the_cache_not_the_stream(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(SHARDS - 1, "broker-x"))
        .await
        .expect("the stream has this shard");

    let err = store
        .put_shard_assignment(cache_assignment(SHARDS - 1, "broker-x"))
        .await
        .expect_err("the cache does not");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
}

async fn an_unknown_cache_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut orphan = cache_assignment(0, "broker-x");
    orphan.key.stream = "no-such-cache".to_string();
    let err = store
        .put_shard_assignment(orphan)
        .await
        .expect_err("unknown cache");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

/// Cases needing an owned handle to share across tasks.
pub(crate) async fn run_shard_concurrency_contract(store: Arc<dyn ControlPlaneStore>) {
    seed(store.as_ref()).await;
    concurrent_writes_to_one_shard_serialise(store).await;
}

/// Two placement passes can write the same shard at once. Whatever order they
/// land in, the shard must end with one assignment, and the generation must
/// count every write rather than two writers both reading generation 0 and both
/// storing 1 -- which is how a broker gets told its current view is stale.
async fn concurrent_writes_to_one_shard_serialise(store: Arc<dyn ControlPlaneStore>) {
    const WRITERS: u64 = 8;

    clear(store.as_ref()).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("initial");

    let writers: Vec<_> = (0..WRITERS)
        .map(|i| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                let leader = if i % 2 == 0 { "broker-x" } else { "broker-y" };
                store
                    .put_shard_assignment(assignment(0, leader))
                    .await
                    .expect("concurrent put")
                    .generation
            })
        })
        .collect();

    let mut generations = Vec::new();
    for writer in writers {
        generations.push(writer.await.expect("writer"));
    }
    generations.sort_unstable();

    assert_eq!(
        store.list_shard_assignments().await.expect("list").len(),
        1,
        "concurrent writes must not create a second assignment",
    );
    assert_eq!(
        generations,
        (1..=WRITERS).collect::<Vec<_>>(),
        "every write must take a distinct, consecutive generation",
    );
    assert_eq!(
        store
            .get_shard_assignment(&key(0))
            .await
            .expect("get")
            .generation,
        WRITERS,
        "the stored generation must be the last one handed out",
    );
}

async fn an_assignment_is_readable(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let stored = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    assert_eq!(stored.key, key(0));
    assert_eq!(stored.leader, "broker-x");
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        stored
    );
    assert_eq!(
        store.list_shard_assignments().await.expect("list"),
        vec![stored]
    );
}

/// A broker reports against the generation it read. If a caller could choose
/// one, a stale report could be made to look current.
async fn the_store_owns_the_generation(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let first = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    assert_eq!(first.generation, 0, "the caller's 999 must be ignored");

    let second = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put again");
    assert_eq!(second.generation, 1, "every write moves the generation on");
}

/// The invariant placement depends on: writing a shard replaces its assignment
/// rather than adding a second one.
async fn one_assignment_per_shard(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(1, "broker-x"))
        .await
        .expect("put");
    let mut moved = assignment(1, "broker-y");
    moved.state = ShardState::Assigning;
    store.put_shard_assignment(moved).await.expect("reassign");

    let all = store.list_shard_assignments().await.expect("list");
    assert_eq!(all.len(), 1, "one assignment per shard");
    assert_eq!(all[0].leader, "broker-y");
}

async fn a_shard_outside_the_stream_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let err = store
        .put_shard_assignment(assignment(SHARDS, "broker-x"))
        .await
        .expect_err("out of bounds");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(
        store
            .list_shard_assignments()
            .await
            .expect("list")
            .is_empty()
    );
}

async fn an_unknown_stream_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut orphan = assignment(0, "broker-x");
    orphan.key.stream = "no-such-stream".to_string();
    let err = store
        .put_shard_assignment(orphan)
        .await
        .expect_err("unknown stream");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

/// Checked in the store rather than by a foreign key, because the node
/// reference deliberately has none.
async fn an_unknown_node_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let err = store
        .put_shard_assignment(assignment(0, "broker-ghost"))
        .await
        .expect_err("unknown leader");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");

    let mut with_ghost_replica = assignment(0, "broker-x");
    with_ghost_replica.replicas = vec!["broker-ghost".to_string()];
    let err = store
        .put_shard_assignment(with_ghost_replica)
        .await
        .expect_err("unknown replica");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

async fn an_unsupported_state_transition_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assigning");

    // Assigning -> Draining is not a move: nothing is serving yet.
    let mut draining = assignment(0, "broker-x");
    draining.state = ShardState::Draining;
    let err = store
        .put_shard_assignment(draining)
        .await
        .expect_err("bad transition");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let unchanged = store.get_shard_assignment(&key(0)).await.expect("get");
    assert_eq!(unchanged.state, ShardState::Assigning);
    assert_eq!(unchanged.generation, 0, "a rejected write moves nothing");
}

/// The question placement asks when a node fails or is drained.
async fn assignments_can_be_listed_by_leader(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    store
        .put_shard_assignment(assignment(1, "broker-y"))
        .await
        .expect("put");
    store
        .put_shard_assignment(assignment(2, "broker-x"))
        .await
        .expect("put");

    let owned = store
        .list_shard_assignments_for_node("broker-x")
        .await
        .expect("by node");
    assert_eq!(
        owned.iter().map(|a| a.key.shard).collect::<Vec<_>>(),
        vec![0, 2],
    );
    assert!(
        store
            .list_shard_assignments_for_node("broker-ghost")
            .await
            .expect("by node")
            .is_empty()
    );
}

async fn missing_assignments_report_not_found(store: &dyn ControlPlaneStore) {
    clear(store).await;
    assert!(matches!(
        store.get_shard_assignment(&key(3)).await.expect_err("get"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store
            .delete_shard_assignment(&key(3))
            .await
            .expect_err("delete"),
        StoreError::NotFound(_)
    ));
}

async fn deleting_leaves_the_shard_unowned(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    let since = store
        .shard_assignment_snapshot()
        .await
        .expect("snapshot")
        .next_seq;

    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");

    assert!(store.get_shard_assignment(&key(0)).await.is_err());
    let changes = store
        .shard_assignment_changes(since)
        .await
        .expect("changes");
    assert_eq!(changes.items.len(), 1);
    assert!(
        changes.items[0].assignment.is_none(),
        "an unassignment carries no body",
    );
}

/// Cascading would delete the only record of where that shard's data lives.
async fn a_node_leading_a_shard_cannot_be_deleted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");

    let err = store
        .delete_node("broker-x")
        .await
        .expect_err("should refuse");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(
        store.get_node("broker-x").await.is_ok(),
        "node must survive"
    );

    // Reassigning frees it.
    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");
    assert!(store.delete_node("broker-x").await.is_ok());
    store
        .register_node(node("broker-x", 7500))
        .await
        .expect("re-register for later cases");
}

async fn a_snapshot_and_the_changes_after_it_lose_nothing(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");

    let snapshot = store.shard_assignment_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.items.len(), 1);

    store
        .put_shard_assignment(assignment(1, "broker-y"))
        .await
        .expect("put");
    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");

    let mut reconstructed: std::collections::BTreeMap<(String, u32), ShardAssignment> = snapshot
        .items
        .into_iter()
        .map(|a| ((a.key.stream.clone(), a.key.shard), a))
        .collect();
    for change in store
        .shard_assignment_changes(snapshot.next_seq)
        .await
        .expect("changes")
        .items
    {
        let id = (change.key.stream.clone(), change.key.shard);
        match change.assignment {
            Some(assignment) => {
                reconstructed.insert(id, assignment);
            }
            None => {
                reconstructed.remove(&id);
            }
        }
    }

    let actual: std::collections::BTreeMap<(String, u32), ShardAssignment> = store
        .list_shard_assignments()
        .await
        .expect("list")
        .into_iter()
        .map(|a| ((a.key.stream.clone(), a.key.shard), a))
        .collect();
    assert_eq!(reconstructed, actual);
}

/// Seeding exposed for the restart test, which needs the catalog in place
/// across two store handles.
///
/// Gated to match its only caller: `postgres_tests` is `pg-tests`-only, so
/// without that feature this has no users and is dead code.
#[cfg(feature = "pg-tests")]
pub(crate) async fn seed_for_restart(store: &dyn ControlPlaneStore) {
    seed(store).await;
}

/// Write one assignment and hand it back, for the restart test to compare.
///
/// `pg-tests`-only for the same reason as [`seed_for_restart`].
#[cfg(feature = "pg-tests")]
pub(crate) async fn assign_for_restart(store: &dyn ControlPlaneStore) -> ShardAssignment {
    clear(store).await;
    let first = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    let mut active = assignment(0, "broker-x");
    active.state = ShardState::Active;
    let _ = first;
    store.put_shard_assignment(active).await.expect("activate")
}
