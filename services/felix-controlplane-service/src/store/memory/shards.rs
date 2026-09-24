//! Shard assignments, and what leaders report about their replicas.
use super::InMemoryStore;
use crate::model::{
    CacheKey, ReplicaReport, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp,
    ShardKey, ShardKind, StreamKey,
};
use crate::store::{ChangeSet, Snapshot, StoreError, StoreResult};

pub(super) async fn put_shard_assignment(
    store: &InMemoryStore,
    assignment: ShardAssignment,
) -> StoreResult<ShardAssignment> {
    assignment.validate().map_err(invalid_shard)?;

    // The stream or cache bounds the shard number, and it has to exist at
    // all. Which of the two is decided by the key's kind, not by looking in
    // both: a cache and a stream may share a name, and falling back from one
    // to the other would let a shard of the wrong thing validate.
    let shards = match assignment.key.kind {
        ShardKind::Stream => store
            .streams
            .read()
            .await
            .get(&StreamKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                stream: assignment.key.stream.clone(),
            })
            .map(|stream| stream.shards)
            .ok_or_else(|| StoreError::NotFound("stream".into()))?,
        ShardKind::Cache => store
            .caches
            .read()
            .await
            .get(&CacheKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                cache: assignment.key.stream.clone(),
            })
            .map(|cache| cache.shards)
            .ok_or_else(|| StoreError::NotFound("cache".into()))?,
    };
    assignment.validate_within(shards).map_err(invalid_shard)?;

    // Checked here rather than by a foreign key: the node reference has none
    // deliberately, so that deleting a node cannot cascade an assignment away.
    {
        let nodes = store.nodes.read().await;
        for node_id in assignment.nodes() {
            if !nodes.records.contains_key(node_id) {
                return Err(StoreError::NotFound(format!("node {node_id}")));
            }
        }
    }

    let mut state = store.shards.write().await;
    let (op, generation) = match state.records.get(&assignment.key) {
        Some(existing) => {
            if !existing.state.can_transition_to(assignment.state) {
                return Err(invalid_shard(
                    crate::model::ShardValidationError::UnsupportedTransition {
                        from: existing.state,
                        to: assignment.state,
                    },
                ));
            }
            (
                ShardAssignmentChangeOp::Updated,
                existing.generation.saturating_add(1),
            )
        }
        None => (ShardAssignmentChangeOp::Assigned, 0),
    };

    // Store-owned, so a caller cannot pin a generation and make its own
    // stale report look current.
    let stored = ShardAssignment {
        generation,
        ..assignment
    };
    state.records.insert(stored.key.clone(), stored.clone());
    state.record(op, &stored.key, Some(stored.clone()));
    metrics::counter!("felix_shard_assignment_changes_total", "op" => match op {
        ShardAssignmentChangeOp::Assigned => "assigned",
        ShardAssignmentChangeOp::Updated => "updated",
        ShardAssignmentChangeOp::Unassigned => "unassigned",
    })
    .increment(1);
    Ok(stored)
}

pub(super) async fn get_shard_assignment(
    store: &InMemoryStore,
    key: &ShardKey,
) -> StoreResult<ShardAssignment> {
    store
        .shards
        .read()
        .await
        .records
        .get(key)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
}

pub(super) async fn list_shard_assignments(
    store: &InMemoryStore,
) -> StoreResult<Vec<ShardAssignment>> {
    let mut items: Vec<ShardAssignment> = store
        .shards
        .read()
        .await
        .records
        .values()
        .cloned()
        .collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(items)
}

pub(super) async fn list_shard_assignments_for_node(
    store: &InMemoryStore,
    node_id: &str,
) -> StoreResult<Vec<ShardAssignment>> {
    let mut items: Vec<ShardAssignment> = store
        .shards
        .read()
        .await
        .records
        .values()
        .filter(|assignment| assignment.leader == node_id)
        .cloned()
        .collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(items)
}

pub(super) async fn delete_shard_assignment(
    store: &InMemoryStore,
    key: &ShardKey,
) -> StoreResult<()> {
    let mut state = store.shards.write().await;
    if state.records.remove(key).is_none() {
        return Err(StoreError::NotFound("shard assignment".into()));
    }
    state.record(ShardAssignmentChangeOp::Unassigned, key, None);
    // Taken under the assignment lock, so a report cannot slip in between.
    store.replica_reports.write().await.remove(key);
    metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned").increment(1);
    Ok(())
}

pub(super) async fn shard_assignment_snapshot(
    store: &InMemoryStore,
) -> StoreResult<Snapshot<ShardAssignment>> {
    let state = store.shards.read().await;
    let mut items: Vec<ShardAssignment> = state.records.values().cloned().collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(Snapshot {
        items,
        next_seq: state.changes.next_seq,
    })
}

pub(super) async fn shard_assignment_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
    let state = store.shards.read().await;
    let items = state
        .changes
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .take(store.limit())
        .cloned()
        .collect();
    Ok(ChangeSet {
        items,
        next_seq: state.changes.next_seq,
    })
}

pub(super) async fn record_replica_report(
    store: &InMemoryStore,
    report: ReplicaReport,
) -> StoreResult<()> {
    // Held across the existence check so a concurrent delete either sees
    // the report and removes it, or runs after this and finds nothing.
    let shards = store.shards.read().await;
    if !shards.records.contains_key(&report.key) {
        return Err(StoreError::NotFound("shard assignment".into()));
    }
    let mut reports = store.replica_reports.write().await;
    if let Some(held) = reports.get(&report.key)
        && held.generation > report.generation
    {
        return Ok(());
    }
    reports.insert(report.key.clone(), report);
    Ok(())
}

pub(super) async fn list_replica_reports(store: &InMemoryStore) -> StoreResult<Vec<ReplicaReport>> {
    let mut reports: Vec<ReplicaReport> = store
        .replica_reports
        .read()
        .await
        .values()
        .cloned()
        .collect();
    reports.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(reports)
}

fn invalid_shard(err: crate::model::ShardValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

/// Sort key giving a stable stream-then-shard order.
fn shard_order(key: &ShardKey) -> (&str, &str, &str, u32) {
    (&key.tenant_id, &key.namespace, &key.stream, key.shard)
}
