//! Shard assignments, and what leaders report about their replicas.
use anyhow::anyhow;
use sqlx::FromRow;

use super::{PostgresStore, begin_consistent_read};
use crate::model::{
    ReplicaReport, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp, ShardKey,
    ShardKind, ShardState, ShardValidationError,
};
use crate::store::{ChangeSet, Snapshot, StoreError, StoreResult};

#[derive(Debug, Clone, FromRow)]
struct DbShardAssignment {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    leader: String,
    replicas: serde_json::Value,
    generation: i64,
    state: String,
    successor: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct ShardAssignmentChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, FromRow)]
struct DbReplicaReport {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    generation: i64,
    caught_up: serde_json::Value,
    offsets: serde_json::Value,
    reported_at_millis: i64,
    drained: bool,
}

pub(super) async fn put_shard_assignment(
    store: &PostgresStore,
    assignment: ShardAssignment,
) -> StoreResult<ShardAssignment> {
    assignment.validate().map_err(invalid_shard)?;
    let mut tx = store.pool.begin().await?;

    // The shard bound comes from whichever of the two the key names, chosen
    // by the kind rather than by trying one and falling back to the other:
    // a cache and a stream may share a name, and a fallback would let a
    // shard of the wrong one validate against the other's count.
    let (table, missing) = match assignment.key.kind {
        ShardKind::Stream => (
            "SELECT shards FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3",
            "stream",
        ),
        ShardKind::Cache => (
            "SELECT shards FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3",
            "cache",
        ),
    };
    let shards: Option<i32> = sqlx::query_scalar(table)
        .bind(&assignment.key.tenant_id)
        .bind(&assignment.key.namespace)
        .bind(&assignment.key.stream)
        .fetch_optional(&mut *tx)
        .await?;
    let shards = shards.ok_or_else(|| StoreError::NotFound(missing.into()))? as u32;
    assignment.validate_within(shards).map_err(invalid_shard)?;

    // Checked here rather than by a foreign key: the node reference has none
    // deliberately, so deleting a node cannot cascade an assignment away.
    for node_id in assignment.nodes() {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)")
                .bind(node_id)
                .fetch_one(&mut *tx)
                .await?;
        if !exists {
            return Err(StoreError::NotFound(format!("node {node_id}")));
        }
    }

    // `FOR UPDATE` so a concurrent write to the same shard waits rather than
    // reading the row this transaction is about to replace.
    let existing = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor
               FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5
               FOR UPDATE"#,
    )
    .bind(&assignment.key.tenant_id)
    .bind(&assignment.key.namespace)
    .bind(&assignment.key.stream)
    .bind(assignment.key.shard as i32)
    .bind(assignment.key.kind.as_str())
    .fetch_optional(&mut *tx)
    .await?
    .map(shard_from_db)
    .transpose()?;

    let (op, generation) = match &existing {
        Some(existing) => {
            if !existing.state.can_transition_to(assignment.state) {
                return Err(invalid_shard(ShardValidationError::UnsupportedTransition {
                    from: existing.state,
                    to: assignment.state,
                }));
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

    sqlx::query(
        r#"INSERT INTO shard_assignments (tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO UPDATE SET
                 leader = EXCLUDED.leader,
                 replicas = EXCLUDED.replicas,
                 generation = EXCLUDED.generation,
                 state = EXCLUDED.state,
                 successor = EXCLUDED.successor,
                 updated_at = now()"#,
    )
    .bind(&stored.key.tenant_id)
    .bind(&stored.key.namespace)
    .bind(&stored.key.stream)
    .bind(stored.key.shard as i32)
    .bind(stored.key.kind.as_str())
    .bind(&stored.leader)
    .bind(serde_json::to_value(&stored.replicas)?)
    .bind(stored.generation as i64)
    .bind(shard_state_to_str(stored.state))
    .bind(&stored.successor)
    .execute(&mut *tx)
    .await?;

    record_shard_change(&mut tx, op, &stored.key, Some(&stored)).await?;
    tx.commit().await?;
    metrics::counter!("felix_shard_assignment_changes_total", "op" => shard_op_to_str(op))
        .increment(1);
    Ok(stored)
}

pub(super) async fn get_shard_assignment(
    store: &PostgresStore,
    key: &ShardKey,
) -> StoreResult<ShardAssignment> {
    sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor
               FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .fetch_optional(&store.pool)
    .await?
    .map(shard_from_db)
    .transpose()?
    .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
}

pub(super) async fn list_shard_assignments(
    store: &PostgresStore,
) -> StoreResult<Vec<ShardAssignment>> {
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(shard_from_db).collect()
}

pub(super) async fn list_shard_assignments_for_node(
    store: &PostgresStore,
    node_id: &str,
) -> StoreResult<Vec<ShardAssignment>> {
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor
               FROM shard_assignments WHERE leader = $1
               ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .bind(node_id)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(shard_from_db).collect()
}

pub(super) async fn delete_shard_assignment(
    store: &PostgresStore,
    key: &ShardKey,
) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;
    let deleted = sqlx::query(
        r#"DELETE FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .execute(&mut *tx)
    .await?;
    if deleted.rows_affected() == 0 {
        return Err(StoreError::NotFound("shard assignment".into()));
    }
    record_shard_change(&mut tx, ShardAssignmentChangeOp::Unassigned, key, None).await?;
    tx.commit().await?;
    metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned").increment(1);
    Ok(())
}

pub(super) async fn shard_assignment_snapshot(
    store: &PostgresStore,
) -> StoreResult<Snapshot<ShardAssignment>> {
    // REPEATABLE READ for the same reason as `node_snapshot`: under READ
    // COMMITTED the two reads below see different snapshots, so an
    // assignment committed between them is missing from `items` while
    // already counted in `next_seq`.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    let items = rows
        .into_iter()
        .map(shard_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn shard_assignment_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, ShardAssignmentChangeRow>(
        r#"SELECT seq, op, tenant_id, namespace, stream, shard, kind, payload
               FROM shard_assignment_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&mut *tx)
    .await?;
    let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(ShardAssignmentChange {
            seq: row.seq as u64,
            op: parse_shard_op(&row.op)?,
            key: ShardKey {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                stream: row.stream,
                shard: row.shard as u32,
                kind: parse_shard_kind(&row.kind)?,
            },
            assignment: row.payload.map(serde_json::from_value).transpose()?,
        });
    }
    Ok(ChangeSet { items, next_seq })
}

pub(super) async fn record_replica_report(
    store: &PostgresStore,
    report: ReplicaReport,
) -> StoreResult<()> {
    // The upsert keeps the newer generation: an older leader's report is
    // dropped by the WHERE, which is a no-op rather than an error. The
    // foreign key is what answers "no assignment" -- and what removes the
    // report when the assignment goes.
    let result = sqlx::query(
        r#"INSERT INTO replica_reports
                   (tenant_id, namespace, kind, stream, shard, generation, caught_up, offsets,
                    reported_at_millis, drained)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO UPDATE
               SET generation = EXCLUDED.generation,
                   caught_up = EXCLUDED.caught_up,
                   offsets = EXCLUDED.offsets,
                   reported_at_millis = EXCLUDED.reported_at_millis,
                   drained = EXCLUDED.drained
               WHERE EXCLUDED.generation >= replica_reports.generation"#,
    )
    .bind(&report.key.tenant_id)
    .bind(&report.key.namespace)
    .bind(shard_kind_to_str(report.key.kind))
    .bind(&report.key.stream)
    .bind(report.key.shard as i32)
    .bind(report.generation as i64)
    .bind(serde_json::to_value(&report.caught_up).expect("a set of strings serializes"))
    .bind(serde_json::to_value(&report.offsets).expect("a map of integers serializes"))
    .bind(report.reported_at_millis as i64)
    .bind(report.drained)
    .execute(&store.pool)
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(err)) if err.is_foreign_key_violation() => {
            Err(StoreError::NotFound("shard assignment".into()))
        }
        Err(err) => Err(err.into()),
    }
}

pub(super) async fn list_replica_reports(store: &PostgresStore) -> StoreResult<Vec<ReplicaReport>> {
    sqlx::query_as::<_, DbReplicaReport>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, generation, caught_up, offsets,
                      reported_at_millis, drained
               FROM replica_reports
               ORDER BY tenant_id, namespace, kind, stream, shard"#,
    )
    .fetch_all(&store.pool)
    .await?
    .into_iter()
    .map(replica_report_from_db)
    .collect()
}

fn shard_from_db(row: DbShardAssignment) -> StoreResult<ShardAssignment> {
    Ok(ShardAssignment {
        key: ShardKey {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            stream: row.stream,
            shard: row.shard as u32,
            kind: parse_shard_kind(&row.kind)?,
        },
        leader: row.leader,
        replicas: serde_json::from_value(row.replicas)?,
        generation: row.generation as u64,
        state: parse_shard_state(&row.state)?,
        successor: row.successor,
    })
}

fn replica_report_from_db(row: DbReplicaReport) -> StoreResult<ReplicaReport> {
    Ok(ReplicaReport {
        key: ShardKey {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            stream: row.stream,
            shard: row.shard as u32,
            kind: parse_shard_kind(&row.kind)?,
        },
        generation: row.generation as u64,
        caught_up: serde_json::from_value(row.caught_up)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode caught_up: {err}")))?,
        offsets: serde_json::from_value(row.offsets)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode offsets: {err}")))?,
        reported_at_millis: row.reported_at_millis as u64,
        drained: row.drained,
    })
}

/// Append a shard change, taking its `seq` from the locked counter.
///
/// Same construction as `record_node_change`: the row lock makes seq order equal
/// commit order, which is what lets a consumer resume at `next_seq` without
/// skipping a change. See 0006_shard_assignments.sql.
async fn record_shard_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: ShardAssignmentChangeOp,
    key: &ShardKey,
    assignment: Option<&ShardAssignment>,
) -> StoreResult<()> {
    let seq = sqlx::query_scalar::<_, i64>(
        "UPDATE shard_assignment_change_seq SET next_seq = next_seq + 1 RETURNING next_seq - 1",
    )
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query(
        r#"INSERT INTO shard_assignment_changes (seq, op, tenant_id, namespace, stream, shard, kind, payload)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
    )
    .bind(seq)
    .bind(shard_op_to_str(op))
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .bind(assignment.map(serde_json::to_value).transpose()?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn shard_state_to_str(state: ShardState) -> &'static str {
    match state {
        ShardState::Assigning => "assigning",
        ShardState::Active => "active",
        ShardState::Draining => "draining",
    }
}

fn parse_shard_state(value: &str) -> StoreResult<ShardState> {
    match value {
        "assigning" => Ok(ShardState::Assigning),
        "active" => Ok(ShardState::Active),
        "draining" => Ok(ShardState::Draining),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard state: {other}"
        ))),
    }
}

fn shard_kind_to_str(kind: ShardKind) -> &'static str {
    match kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    }
}

fn parse_shard_kind(value: &str) -> StoreResult<ShardKind> {
    match value {
        "stream" => Ok(ShardKind::Stream),
        "cache" => Ok(ShardKind::Cache),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard kind: {other}"
        ))),
    }
}

fn shard_op_to_str(op: ShardAssignmentChangeOp) -> &'static str {
    match op {
        ShardAssignmentChangeOp::Assigned => "assigned",
        ShardAssignmentChangeOp::Updated => "updated",
        ShardAssignmentChangeOp::Unassigned => "unassigned",
    }
}

fn parse_shard_op(value: &str) -> StoreResult<ShardAssignmentChangeOp> {
    match value {
        "assigned" => Ok(ShardAssignmentChangeOp::Assigned),
        "updated" => Ok(ShardAssignmentChangeOp::Updated),
        "unassigned" => Ok(ShardAssignmentChangeOp::Unassigned),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard change op: {other}"
        ))),
    }
}

fn invalid_shard(err: ShardValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}
