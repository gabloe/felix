//! Postgres-backed implementation of the control-plane store.
//!
//! # What this module is
//! This module implements the `ControlPlaneStore` trait using Postgres (via `sqlx`) as a durable,
//! shared backing store for *control-plane metadata* (tenants, namespaces, streams, caches) used by
//! Felix components to discover and validate configuration/state.
//!
//! # What this module is NOT
//! This store is **not** the data-plane persistence layer (it does not store events, cache payloads,
//! broker logs, or durable queues). It stores *metadata* that other services use to decide how to
//! route, authorize, or configure data-plane behavior.
//!
//! # Data model: authoritative state + append-only change logs
//! We persist state in two complementary forms:
//!
//! 1) **Authoritative tables** (`tenants`, `namespaces`, `streams`, `caches`)
//!    - These tables represent the *current* state.
//!    - Reads such as `list_*()` and `get_*()` use these tables.
//!
//! 2) **Append-only change tables** (`tenant_changes`, `namespace_changes`, `stream_changes`, `cache_changes`)
//!    - These tables represent an ordered stream of changes for incremental sync.
//!    - Each row has a monotonically increasing `seq` (per change table) assigned by Postgres.
//!    - Each row includes an `op` (Created/Updated/Deleted), entity key columns, and an optional JSON payload.
//!      Deletions often store `NULL` payloads, but some delete paths include the prior object as a “tombstone”
//!      for richer downstream caches.
//!
//! This design allows clients to either:
//! - bootstrap from a snapshot of the current state (fast, consistent picture), then
//! - poll for changes since a checkpoint `seq` (cheap incremental updates).
//!
//! # Snapshot + changes contract
//! Callers generally follow this pattern:
//! 1) Call `*_snapshot()` once:
//!    - returns `items` (full state) and `next_seq` (the next change sequence to request)
//! 2) Periodically call `*_changes(since = next_seq)`:
//!    - returns a page of changes `items` with `seq >= since` and a new `next_seq` checkpoint
//! 3) Repeat step 2 using the returned `next_seq`.
//!
//! Important details:
//! - `seq` is monotonic **within each change table**, not globally across entity types.
//! - `since` is interpreted as **inclusive** (`seq >= since`).
//! - `next_seq` is computed as `(MAX(seq) + 1)` (or `0` if empty), meaning “the next sequence number
//!   that has not been observed yet”. If you request `since = next_seq`, you are asking for changes
//!   that occur strictly after your last observed `seq`.
//!
//! # Consistency / atomicity
//! Mutation operations (`create_*`, `patch_*`, `delete_*`) are implemented as transactions that
//! update the authoritative table *and* append a corresponding change-log row in the same commit.
//! This avoids inconsistent states such as “object exists but no change was emitted”.
//!
//! # Operational notes
//! - Migrations are executed at startup via `sqlx::migrate!("./migrations")` to ensure the schema is
//!   present and compatible before serving API requests.
//! - Durability semantics come from your Postgres deployment (WAL + fsync settings, replication,
//!   backups, disk durability). This code assumes Postgres is configured for real durability.
//! - Connection pooling/timeouts are explicitly configured because hanging forever on DB failures is
//!   unacceptable for production control-plane services.
//!
//! # Retention of change logs (optional)
//! This module can spawn a best-effort retention task that bounds each change table to the most
//! recent `N` rows. This prevents unbounded growth, but it reduces the “catch-up window” for clients.
//! If a client falls behind beyond the retained window, it must re-bootstrap via `*_snapshot()`.
//!
//! Retention is intentionally best-effort and operationally conservative: failures do not crash the
//! service; the task simply retries on the next tick.
//!
//! # Security notes
//! - Use a least-privilege DB role (only the required CRUD and migration permissions).
//! - Prefer TLS to Postgres in production.
//! - Avoid dynamic SQL. This module uses dynamic SQL only for retention deletes and only with a
//!   fixed allowlist of table names defined in code.
use super::{ChangeSet, ControlPlaneStore, Snapshot, StoreConfig, StoreError, StoreResult};
use crate::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, Namespace, NamespaceChange,
    NamespaceChangeOp, NamespaceKey, RetentionPolicy, Stream, StreamChange, StreamChangeOp,
    StreamKey, StreamKind, StreamPatchRequest, Tenant, TenantChange, TenantChangeOp,
    config::PostgresConfig,
};
use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{FromRow, PgPool};
use std::str::FromStr;
use std::time::Duration;

/// Durable control-plane store backed by Postgres.
///
/// ## Concurrency and pooling
/// `PostgresStore` is safe to share across request handlers. Each API call issues one or more SQL
/// statements through a `sqlx::PgPool`. `sqlx` manages concurrent access by leasing connections from
/// the pool; pool sizing is therefore a key tuning knob.
///
/// ## Durability semantics
/// Writes are durable to the extent your Postgres deployment is durable (WAL/fsync, storage, etc.).
/// This module does not implement its own durability; it relies on Postgres as the source of truth.
///
/// ## Consistency model
/// Mutations are transactional: authoritative state and the corresponding change-log entry are
/// written in the same transaction, providing atomicity for “snapshot + incremental changes” consumers.
pub struct PostgresStore {
    pool: PgPool,
    config: StoreConfig,
}

/// Row shape for the `streams` authoritative table.
///
/// This is a direct mapping of the SQL schema into Rust types via `sqlx::FromRow`.
/// We keep these DB-facing structs separate from domain types (`Stream`, etc.) to:
/// - isolate schema details (column names, storage formats) from the API domain model
/// - make it explicit where parsing/validation occurs (e.g., string enums → domain enums)
/// - keep migration/schema evolution localized
#[derive(Debug, Clone, FromRow)]
struct DbStream {
    tenant_id: String,
    namespace: String,
    stream: String,
    kind: String,
    shards: i32,
    retention_max_age_seconds: Option<i64>,
    retention_max_size_bytes: Option<i64>,
    consistency: String,
    delivery: String,
    durable: bool,
}

/// Row shape for `tenants` table (minimal mapping needed by the API).
#[derive(Debug, Clone, FromRow)]
struct DbTenant {
    tenant_id: String,
    display_name: String,
}

/// Row shape for `namespaces` table.
#[derive(Debug, Clone, FromRow)]
struct DbNamespace {
    tenant_id: String,
    namespace: String,
    display_name: String,
}

/// Row shape for `caches` table.
#[derive(Debug, Clone, FromRow)]
struct DbCache {
    tenant_id: String,
    namespace: String,
    cache: String,
    display_name: String,
}

/// Row shape for the `tenant_changes` table.
///
/// `seq` is a monotonic, append-only sequence number used for incremental sync.
/// `payload` is optional (e.g., deletions often store `NULL` payloads).
#[derive(Debug, Clone, FromRow)]
struct TenantChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    payload: Option<Value>,
}

/// Row shape for the `namespace_changes` table.
#[derive(Debug, Clone, FromRow)]
struct NamespaceChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    payload: Option<Value>,
}

/// Row shape for the `stream_changes` table.
#[derive(Debug, Clone, FromRow)]
struct StreamChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Option<Value>,
}

/// Row shape for the `cache_changes` table.
#[derive(Debug, Clone, FromRow)]
struct CacheChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    cache: String,
    payload: Option<Value>,
}

impl PostgresStore {
    /// Connect to Postgres, run migrations, and optionally start retention maintenance.
    ///
    /// Notes:
    /// - Pool timeouts are important in production to avoid indefinite hangs when DB is unhealthy.
    /// - `sqlx::migrate!("./migrations")` embeds migrations at compile time, and at runtime applies
    ///   any pending migrations on startup (before serving).
    /// - If `change_retention_max_rows` is set, we spawn a best-effort task to keep change tables
    ///   bounded. This is not a substitute for operational retention policies/backups.
    pub async fn connect(pg: &PostgresConfig, config: StoreConfig) -> StoreResult<Self> {
        // Connection pool tuning matters for control-plane stability:
        // - `max_connections` caps concurrent DB work and protects the DB from overload.
        // - `acquire_timeout` bounds how long a request will wait for a pooled connection before failing fast.
        // - `connect_timeout` bounds how long we wait when establishing a new physical connection.
        //
        // In production, prefer failing fast + surfacing health failures over hanging indefinitely.
        let connect_options =
            PgConnectOptions::from_str(&pg.url).map_err(|e| StoreError::Unexpected(e.into()))?;
        let pool = PgPoolOptions::new()
            .max_connections(pg.max_connections)
            .acquire_timeout(Duration::from_millis(pg.acquire_timeout_ms))
            .connect_with(connect_options)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        // Migrations run *before* serving requests so handlers can assume the schema exists.
        // If migrations fail, we fail startup rather than serving partially functional endpoints.
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        // Optional change-log retention: bounds append-only tables so they don't grow forever.
        // Tradeoff: reduces how far behind a client can fall before requiring a full snapshot bootstrap.
        if let Some(retention) = config.change_retention_max_rows {
            spawn_retention_task(pool.clone(), retention);
        }

        Ok(Self { pool, config })
    }

    /// Page size limit for change queries.
    ///
    /// This bounds response size, memory usage, and tail latency for callers polling changes.
    fn limit(&self) -> i64 {
        self.config.changes_limit as i64
    }
}

/// Spawn a best-effort background task that bounds each change table to the most recent `max_rows`.
///
/// Why:
/// - Change tables are append-only, which is great for incremental sync, but they can grow without bound.
/// - Bounding them keeps storage predictable for long-lived clusters.
///
/// How:
/// - Every 60s, for each change table:
///   - compute a cutoff: `MAX(seq) - max_rows + 1`
///   - delete rows where `seq < cutoff`
/// - If the table is empty, `COALESCE(..., 0)` makes the delete a no-op.
///
/// Tradeoffs / failure modes:
/// - If a client falls behind beyond the retained window, it cannot catch up from changes alone and
///   must re-bootstrap from a snapshot.
/// - This is not time-based retention; it is “last N changes”.
/// - This is best-effort: transient DB errors are ignored; the task retries next tick.
///
/// Indexing:
/// - For predictable performance, `seq` should be indexed (typically via the primary key) so deletes
///   and `MAX(seq)` are efficient even as tables grow.
fn spawn_retention_task(pool: PgPool, max_rows: i64) {
    let tables = [
        "tenant_changes",
        "namespace_changes",
        "stream_changes",
        "cache_changes",
    ];
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;
            for table in tables {
                // Delete all rows older than the newest `max_rows` entries.
                //
                // The inner SELECT computes the cutoff seq:
                //   MAX(seq) - max_rows + 1
                // If the table is empty, COALESCE returns 0 and DELETE is a no-op.
                //
                // NOTE: `format!` is used to inject the table name. This is safe here because `table`
                // comes from a hard-coded allowlist (`tables` array above). Do NOT pass user input
                // into this format string.
                let query = format!(
                    "DELETE FROM {table} WHERE seq < (SELECT COALESCE(MAX(seq) - $1 + 1, 0) FROM {table})"
                );
                let _ = sqlx::query(&query).bind(max_rows).execute(&pool).await;
            }
        }
    });
}

#[async_trait]
impl ControlPlaneStore for PostgresStore {
    // -----------------------------
    // Tenants
    // -----------------------------

    /// Return all tenants (authoritative state).
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        let rows = sqlx::query_as::<_, DbTenant>(
            "SELECT tenant_id, display_name FROM tenants ORDER BY tenant_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(rows
            .into_iter()
            .map(|row| Tenant {
                tenant_id: row.tenant_id,
                display_name: row.display_name,
            })
            .collect())
    }

    /// Create a tenant and emit a corresponding change-log entry in a single transaction.
    ///
    /// Transactionality matters: we want the authoritative row and its change event to be consistent
    /// (no “created row without change” or “change without row” states).
    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let insert =
            sqlx::query(r#"INSERT INTO tenants (tenant_id, display_name) VALUES ($1, $2)"#)
                .bind(&tenant.tenant_id)
                .bind(&tenant.display_name)
                .execute(&mut *tx)
                .await;
        if let Err(err) = insert {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict("tenant exists".into()));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        // Append to change log for incremental watchers.
        sqlx::query(r#"INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)"#)
            .bind("Created")
            .bind(&tenant.tenant_id)
            .bind(serde_json::to_value(&tenant).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(tenant)
    }

    /// Delete a tenant and all dependent resources, emitting “Deleted” change events.
    ///
    /// Important: We fetch namespaces/streams/caches *before* deleting so we can emit “Deleted”
    /// change events with the prior payload. This is useful for caches/watchers that want tombstones
    /// with context (not just keys).
    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        // Validate tenant exists to return a proper 404 semantics.
        let tenant_exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tenants WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))?
                > 0;
        if !tenant_exists {
            return Err(StoreError::NotFound("tenant".into()));
        }

        // Prefetch dependents so we can produce Deleted payloads in change logs.
        let namespaces = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        let streams = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        // Delete tenant (schema should cascade or you rely on application-level deletes; either way
        // we emit explicit change events below based on the prefetched rows).
        sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        // Emit cache deletion events (payload included).
        for cache in caches {
            let key = CacheKey {
                tenant_id: cache.tenant_id.clone(),
                namespace: cache.namespace.clone(),
                cache: cache.cache.clone(),
            };
            let payload = Cache {
                tenant_id: cache.tenant_id,
                namespace: cache.namespace,
                cache: cache.cache,
                display_name: cache.display_name,
            };
            sqlx::query(
                r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
            )
            .bind("Deleted")
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .bind(&key.cache)
            .bind(serde_json::to_value(&payload).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        }

        // Emit stream deletion events (payload included).
        for stream in streams {
            let key = StreamKey {
                tenant_id: stream.tenant_id.clone(),
                namespace: stream.namespace.clone(),
                stream: stream.stream.clone(),
            };
            let payload = Stream {
                tenant_id: stream.tenant_id,
                namespace: stream.namespace,
                stream: stream.stream,
                kind: parse_stream_kind(&stream.kind)?,
                shards: stream.shards as u32,
                retention: RetentionPolicy {
                    max_age_seconds: stream.retention_max_age_seconds.map(|v| v as u64),
                    max_size_bytes: stream.retention_max_size_bytes.map(|v| v as u64),
                },
                consistency: parse_consistency(&stream.consistency)?,
                delivery: parse_delivery(&stream.delivery)?,
                durable: stream.durable,
            };
            sqlx::query(
                r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
            )
            .bind("Deleted")
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .bind(&key.stream)
            .bind(serde_json::to_value(&payload).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        }

        for namespace in namespaces {
            let key = NamespaceKey {
                tenant_id: namespace.tenant_id.clone(),
                namespace: namespace.namespace.clone(),
            };
            let payload = Namespace {
                tenant_id: namespace.tenant_id,
                namespace: namespace.namespace,
                display_name: namespace.display_name,
            };
            sqlx::query(
                r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
            )
            .bind("Deleted")
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .bind(serde_json::to_value(&payload).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        }

        sqlx::query(r#"INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)"#)
            .bind("Deleted")
            .bind(tenant_id)
            .bind(Option::<Value>::None)
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        self.refresh_counts().await?;
        Ok(())
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        let items = self.list_tenants().await?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM tenant_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(Snapshot { items, next_seq })
    }

    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        let rows = sqlx::query_as::<_, TenantChangeRow>(
            r#"SELECT seq, op, tenant_id, payload FROM tenant_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let op = match row.op.as_str() {
                "Created" => TenantChangeOp::Created,
                "Deleted" => TenantChangeOp::Deleted,
                _ => TenantChangeOp::Deleted,
            };
            let tenant = row
                .payload
                .and_then(|v| serde_json::from_value::<Tenant>(v).ok());
            items.push(TenantChange {
                seq: row.seq as u64,
                op,
                tenant_id: row.tenant_id,
                tenant,
            });
        }

        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM tenant_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;

        Ok(ChangeSet { items, next_seq })
    }

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        let rows = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 ORDER BY namespace"#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        Ok(rows
            .into_iter()
            .map(|row| Namespace {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                display_name: row.display_name,
            })
            .collect())
    }

    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
                .bind(&namespace.tenant_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))?;
        if !exists {
            return Err(StoreError::NotFound("tenant".into()));
        }

        let insert = sqlx::query(
            r#"INSERT INTO namespaces (tenant_id, namespace, display_name) VALUES ($1, $2, $3)"#,
        )
        .bind(&namespace.tenant_id)
        .bind(&namespace.namespace)
        .bind(&namespace.display_name)
        .execute(&mut *tx)
        .await;
        if let Err(err) = insert {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict("namespace exists".into()));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        sqlx::query(
            r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
        )
        .bind("Created")
        .bind(&namespace.tenant_id)
        .bind(&namespace.namespace)
        .bind(serde_json::to_value(&namespace).ok())
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(namespace)
    }

    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        let ns_row = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        if ns_row.is_none() {
            return Err(StoreError::NotFound("namespace".into()));
        }
        let namespace_payload = ns_row.map(|row| Namespace {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            display_name: row.display_name,
        });

        let streams = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        sqlx::query(r#"DELETE FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#)
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        for cache in caches {
            let key = CacheKey {
                tenant_id: cache.tenant_id.clone(),
                namespace: cache.namespace.clone(),
                cache: cache.cache.clone(),
            };
            let payload = Cache {
                tenant_id: cache.tenant_id,
                namespace: cache.namespace,
                cache: cache.cache,
                display_name: cache.display_name,
            };
            sqlx::query(
                r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
            )
            .bind("Deleted")
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .bind(&key.cache)
            .bind(serde_json::to_value(&payload).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        }

        for stream in streams {
            let key = StreamKey {
                tenant_id: stream.tenant_id.clone(),
                namespace: stream.namespace.clone(),
                stream: stream.stream.clone(),
            };
            let payload = Stream {
                tenant_id: stream.tenant_id,
                namespace: stream.namespace,
                stream: stream.stream,
                kind: parse_stream_kind(&stream.kind)?,
                shards: stream.shards as u32,
                retention: RetentionPolicy {
                    max_age_seconds: stream.retention_max_age_seconds.map(|v| v as u64),
                    max_size_bytes: stream.retention_max_size_bytes.map(|v| v as u64),
                },
                consistency: parse_consistency(&stream.consistency)?,
                delivery: parse_delivery(&stream.delivery)?,
                durable: stream.durable,
            };
            sqlx::query(
                r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
            )
            .bind("Deleted")
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .bind(&key.stream)
            .bind(serde_json::to_value(&payload).ok())
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        }

        sqlx::query(
            r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(namespace_payload.and_then(|ns| serde_json::to_value(&ns).ok()))
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        self.refresh_counts().await?;
        Ok(())
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        let items = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces ORDER BY tenant_id, namespace"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?
        .into_iter()
        .map(|row| Namespace {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            display_name: row.display_name,
        })
        .collect();
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM namespace_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(Snapshot { items, next_seq })
    }

    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        let rows = sqlx::query_as::<_, NamespaceChangeRow>(
            r#"SELECT seq, op, tenant_id, namespace, payload FROM namespace_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let op = match row.op.as_str() {
                "Created" => NamespaceChangeOp::Created,
                _ => NamespaceChangeOp::Deleted,
            };
            let namespace = row
                .payload
                .and_then(|v| serde_json::from_value::<Namespace>(v).ok());
            items.push(NamespaceChange {
                seq: row.seq as u64,
                op,
                key: NamespaceKey {
                    tenant_id: row.tenant_id,
                    namespace: row.namespace,
                },
                namespace,
            });
        }
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM namespace_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>> {
        let rows = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 ORDER BY stream"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        rows.into_iter()
            .map(stream_from_db)
            .collect::<Result<Vec<_>, StoreError>>()
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        let row = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        match row {
            Some(row) => stream_from_db(row),
            None => Err(StoreError::NotFound("stream".into())),
        }
    }

    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;

        let ns_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&stream.tenant_id)
        .bind(&stream.namespace)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        if !ns_exists {
            return Err(StoreError::NotFound("namespace".into()));
        }

        let insert = sqlx::query(
            r#"INSERT INTO streams (tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
        )
        .bind(&stream.tenant_id)
        .bind(&stream.namespace)
        .bind(&stream.stream)
        .bind(stream_kind_to_str(&stream.kind))
        .bind(stream.shards as i32)
        .bind(stream.retention.max_age_seconds.map(|v| v as i64))
        .bind(stream.retention.max_size_bytes.map(|v| v as i64))
        .bind(consistency_to_str(&stream.consistency))
        .bind(delivery_to_str(&stream.delivery))
        .bind(stream.durable)
        .execute(&mut *tx)
        .await;
        if let Err(err) = insert {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict("stream exists".into()));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        sqlx::query(
            r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Created")
        .bind(&stream.tenant_id)
        .bind(&stream.namespace)
        .bind(&stream.stream)
        .bind(serde_json::to_value(&stream).ok())
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
        self.refresh_counts().await?;
        Ok(stream)
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let current = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 FOR UPDATE"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let current = match current {
            Some(row) => row,
            None => return Err(StoreError::NotFound("stream".into())),
        };

        let mut updated = stream_from_db(current.clone())?;
        if let Some(retention) = patch.retention {
            updated.retention = retention;
        }
        if let Some(consistency) = patch.consistency {
            updated.consistency = consistency;
        }
        if let Some(delivery) = patch.delivery {
            updated.delivery = delivery;
        }
        if let Some(durable) = patch.durable {
            updated.durable = durable;
        }

        sqlx::query(
            r#"UPDATE streams SET kind = $1, shards = $2, retention_max_age_seconds = $3, retention_max_size_bytes = $4, consistency = $5, delivery = $6, durable = $7, updated_at = now()
                WHERE tenant_id = $8 AND namespace = $9 AND stream = $10"#,
        )
        .bind(stream_kind_to_str(&updated.kind))
        .bind(updated.shards as i32)
        .bind(updated.retention.max_age_seconds.map(|v| v as i64))
        .bind(updated.retention.max_size_bytes.map(|v| v as i64))
        .bind(consistency_to_str(&updated.consistency))
        .bind(delivery_to_str(&updated.delivery))
        .bind(updated.durable)
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        sqlx::query(
            r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Updated")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .bind(serde_json::to_value(&updated).ok())
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
        Ok(updated)
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let removed = sqlx::query(
            r#"DELETE FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        if removed.rows_affected() == 0 {
            return Err(StoreError::NotFound("stream".into()));
        }

        sqlx::query(
            r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .bind(Option::<Value>::None)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
        self.refresh_counts().await?;
        Ok(())
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        let rows = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable FROM streams ORDER BY tenant_id, namespace, stream"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let items = rows
            .into_iter()
            .map(stream_from_db)
            .collect::<Result<Vec<_>, StoreError>>()?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM stream_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(Snapshot { items, next_seq })
    }

    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        let rows = sqlx::query_as::<_, StreamChangeRow>(
            r#"SELECT seq, op, tenant_id, namespace, stream, payload FROM stream_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let op = match row.op.as_str() {
                "Created" => StreamChangeOp::Created,
                "Updated" => StreamChangeOp::Updated,
                _ => StreamChangeOp::Deleted,
            };
            let stream = row
                .payload
                .and_then(|v| serde_json::from_value::<Stream>(v).ok());
            items.push(StreamChange {
                seq: row.seq as u64,
                op,
                key: StreamKey {
                    tenant_id: row.tenant_id,
                    namespace: row.namespace,
                    stream: row.stream,
                },
                stream,
            });
        }
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM stream_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        let rows = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2 ORDER BY cache"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(rows
            .into_iter()
            .map(|row| Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
            })
            .collect())
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        let row = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        match row {
            Some(row) => Ok(Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
            }),
            None => Err(StoreError::NotFound("cache".into())),
        }
    }

    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let ns_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&cache.tenant_id)
        .bind(&cache.namespace)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        if !ns_exists {
            return Err(StoreError::NotFound("namespace".into()));
        }

        let insert = sqlx::query(
            r#"INSERT INTO caches (tenant_id, namespace, cache, display_name) VALUES ($1, $2, $3, $4)"#,
        )
        .bind(&cache.tenant_id)
        .bind(&cache.namespace)
        .bind(&cache.cache)
        .bind(&cache.display_name)
        .execute(&mut *tx)
        .await;
        if let Err(err) = insert {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict("cache exists".into()));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        sqlx::query(
            r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Created")
        .bind(&cache.tenant_id)
        .bind(&cache.namespace)
        .bind(&cache.cache)
        .bind(serde_json::to_value(&cache).ok())
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        self.refresh_counts().await?;
        Ok(cache)
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let current = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3 FOR UPDATE"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let mut cache = match current {
            Some(row) => Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
            },
            None => return Err(StoreError::NotFound("cache".into())),
        };

        if let Some(display_name) = patch.display_name {
            cache.display_name = display_name;
        }

        sqlx::query(
            r#"UPDATE caches SET display_name = $1, updated_at = now() WHERE tenant_id = $2 AND namespace = $3 AND cache = $4"#,
        )
        .bind(&cache.display_name)
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        sqlx::query(
            r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Updated")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .bind(serde_json::to_value(&cache).ok())
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
        Ok(cache)
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        let removed = sqlx::query(
            r#"DELETE FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        if removed.rows_affected() == 0 {
            return Err(StoreError::NotFound("cache".into()));
        }

        sqlx::query(
            r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .bind(Option::<Value>::None)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
        self.refresh_counts().await?;
        Ok(())
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        let rows = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches ORDER BY tenant_id, namespace, cache"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let items = rows
            .into_iter()
            .map(|row| Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
            })
            .collect();
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM cache_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(Snapshot { items, next_seq })
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        let rows = sqlx::query_as::<_, CacheChangeRow>(
            r#"SELECT seq, op, tenant_id, namespace, cache, payload FROM cache_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let op = match row.op.as_str() {
                "Created" => CacheChangeOp::Created,
                "Updated" => CacheChangeOp::Updated,
                _ => CacheChangeOp::Deleted,
            };
            let cache = row
                .payload
                .and_then(|v| serde_json::from_value::<Cache>(v).ok());
            items.push(CacheChange {
                seq: row.seq as u64,
                op,
                key: CacheKey {
                    tenant_id: row.tenant_id,
                    namespace: row.namespace,
                    cache: row.cache,
                },
                cache,
            });
        }
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM cache_changes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
                .bind(tenant_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(exists)
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(exists)
    }

    async fn health_check(&self) -> StoreResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        Ok(())
    }

    fn is_durable(&self) -> bool {
        true
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        return db_err.code().map(|code| code == "23505").unwrap_or(false);
    }
    false
}

fn stream_from_db(row: DbStream) -> StoreResult<Stream> {
    Ok(Stream {
        tenant_id: row.tenant_id,
        namespace: row.namespace,
        stream: row.stream,
        kind: parse_stream_kind(&row.kind)?,
        shards: row.shards as u32,
        retention: RetentionPolicy {
            max_age_seconds: row.retention_max_age_seconds.map(|v| v as u64),
            max_size_bytes: row.retention_max_size_bytes.map(|v| v as u64),
        },
        consistency: parse_consistency(&row.consistency)?,
        delivery: parse_delivery(&row.delivery)?,
        durable: row.durable,
    })
}

fn parse_stream_kind(value: &str) -> StoreResult<StreamKind> {
    match value {
        "Stream" => Ok(StreamKind::Stream),
        "Queue" => Ok(StreamKind::Queue),
        "Cache" => Ok(StreamKind::Cache),
        _ => Err(StoreError::Unexpected(anyhow!(
            "invalid stream kind {value}"
        ))),
    }
}

fn stream_kind_to_str(kind: &StreamKind) -> &'static str {
    match kind {
        StreamKind::Stream => "Stream",
        StreamKind::Queue => "Queue",
        StreamKind::Cache => "Cache",
    }
}

fn parse_consistency(value: &str) -> StoreResult<crate::ConsistencyLevel> {
    match value {
        "Leader" => Ok(crate::ConsistencyLevel::Leader),
        "Quorum" => Ok(crate::ConsistencyLevel::Quorum),
        _ => Err(StoreError::Unexpected(anyhow!(
            "invalid consistency {value}"
        ))),
    }
}

fn consistency_to_str(value: &crate::ConsistencyLevel) -> &'static str {
    match value {
        crate::ConsistencyLevel::Leader => "Leader",
        crate::ConsistencyLevel::Quorum => "Quorum",
    }
}

fn parse_delivery(value: &str) -> StoreResult<crate::DeliveryGuarantee> {
    match value {
        "AtMostOnce" => Ok(crate::DeliveryGuarantee::AtMostOnce),
        "AtLeastOnce" => Ok(crate::DeliveryGuarantee::AtLeastOnce),
        _ => Err(StoreError::Unexpected(anyhow!("invalid delivery {value}"))),
    }
}

fn delivery_to_str(value: &crate::DeliveryGuarantee) -> &'static str {
    match value {
        crate::DeliveryGuarantee::AtMostOnce => "AtMostOnce",
        crate::DeliveryGuarantee::AtLeastOnce => "AtLeastOnce",
    }
}

impl PostgresStore {
    async fn refresh_counts(&self) -> StoreResult<()> {
        let stream_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM streams")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::gauge!("felix_streams_total").set(stream_total as f64);

        let cache_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM caches")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::Unexpected(e.into()))?;
        metrics::gauge!("felix_caches_total").set(cache_total as f64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_violation_detects_only_db_codes() {
        let err = sqlx::Error::RowNotFound;
        assert!(!is_unique_violation(&err));
    }

    #[test]
    fn stream_kind_round_trip() {
        assert!(matches!(
            parse_stream_kind("Stream").unwrap(),
            StreamKind::Stream
        ));
        assert!(matches!(
            parse_stream_kind("Queue").unwrap(),
            StreamKind::Queue
        ));
        assert!(matches!(
            parse_stream_kind("Cache").unwrap(),
            StreamKind::Cache
        ));
        assert!(parse_stream_kind("Unknown").is_err());
        assert_eq!(stream_kind_to_str(&StreamKind::Stream), "Stream");
        assert_eq!(stream_kind_to_str(&StreamKind::Queue), "Queue");
        assert_eq!(stream_kind_to_str(&StreamKind::Cache), "Cache");
    }

    #[test]
    fn consistency_round_trip() {
        assert!(matches!(
            parse_consistency("Leader").unwrap(),
            crate::ConsistencyLevel::Leader
        ));
        assert!(matches!(
            parse_consistency("Quorum").unwrap(),
            crate::ConsistencyLevel::Quorum
        ));
        assert!(parse_consistency("Unknown").is_err());
        assert_eq!(
            consistency_to_str(&crate::ConsistencyLevel::Leader),
            "Leader"
        );
        assert_eq!(
            consistency_to_str(&crate::ConsistencyLevel::Quorum),
            "Quorum"
        );
    }

    #[test]
    fn delivery_round_trip() {
        assert!(matches!(
            parse_delivery("AtMostOnce").unwrap(),
            crate::DeliveryGuarantee::AtMostOnce
        ));
        assert!(matches!(
            parse_delivery("AtLeastOnce").unwrap(),
            crate::DeliveryGuarantee::AtLeastOnce
        ));
        assert!(parse_delivery("Unknown").is_err());
        assert_eq!(
            delivery_to_str(&crate::DeliveryGuarantee::AtMostOnce),
            "AtMostOnce"
        );
        assert_eq!(
            delivery_to_str(&crate::DeliveryGuarantee::AtLeastOnce),
            "AtLeastOnce"
        );
    }

    #[test]
    fn stream_from_db_maps_fields() {
        let row = DbStream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s1".to_string(),
            kind: "Stream".to_string(),
            shards: 2,
            retention_max_age_seconds: Some(3600),
            retention_max_size_bytes: Some(2048),
            consistency: "Leader".to_string(),
            delivery: "AtLeastOnce".to_string(),
            durable: true,
        };
        let stream = stream_from_db(row).expect("stream");
        assert_eq!(stream.tenant_id, "t1");
        assert_eq!(stream.namespace, "ns");
        assert_eq!(stream.stream, "s1");
        assert!(matches!(stream.kind, StreamKind::Stream));
        assert_eq!(stream.shards, 2);
        assert_eq!(stream.retention.max_age_seconds, Some(3600));
        assert_eq!(stream.retention.max_size_bytes, Some(2048));
        assert!(matches!(
            stream.consistency,
            crate::ConsistencyLevel::Leader
        ));
        assert!(matches!(
            stream.delivery,
            crate::DeliveryGuarantee::AtLeastOnce
        ));
        assert!(stream.durable);
    }
}
