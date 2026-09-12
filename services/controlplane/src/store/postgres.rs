//! Postgres-backed implementation of `ControlPlaneStore`.
//!
//! Stores control-plane *metadata* (tenants, namespaces, streams, caches). Not
//! the data plane: no events, cache payloads, or broker logs live here.
//!
//! State is kept twice, and the pairing is the point. Authoritative tables hold
//! current state and serve `get_*`/`list_*`. Append-only `*_changes` tables hold
//! an ordered log with a `seq` per table, so a client can bootstrap from
//! `*_snapshot()` once and then poll `*_changes(since = next_seq)` cheaply.
//! Every mutation writes both in one transaction, so "object exists but no
//! change was emitted" cannot happen.
//!
//! Three things about `seq` that are easy to get wrong:
//! - it is monotonic *per change table*, not global across entity types;
//! - `since` is inclusive (`seq >= since`);
//! - `next_seq` is `MAX(seq) + 1`, i.e. the first sequence not yet observed.
//!
//! The optional retention task bounds each change table to the most recent N
//! rows. That caps growth at the cost of the catch-up window: a client that
//! falls further behind than N must re-bootstrap from a snapshot. It is
//! best-effort and never fatal -- a failed pass just retries on the next tick.
//!
//! Dynamic SQL is confined to those retention deletes, against a fixed
//! allowlist of table names defined in code.

use super::{
    AuthStore, ChangeSet, ControlPlaneStore, Snapshot, StoreConfig, StoreError, StoreResult,
};
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::config::PostgresConfig;
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, Namespace, NamespaceChange,
    NamespaceChangeOp, NamespaceKey, Node, NodeCapacity, NodeChange, NodeChangeOp, NodeLifecycle,
    NodePatchRequest, NodeSpec, NodeStatus, NodeValidationError, RetentionPolicy, ShardAssignment,
    ShardAssignmentChange, ShardAssignmentChangeOp, ShardKey, ShardKind, ShardState,
    ShardValidationError, Stream, StreamChange, StreamChangeOp, StreamKey, StreamKind,
    StreamPatchRequest, Tenant, TenantChange, TenantChangeOp,
};
use anyhow::anyhow;
use async_trait::async_trait;
use jsonwebtoken::Algorithm;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{FromRow, PgPool};
use std::str::FromStr;
use std::time::Duration;

#[cfg(feature = "pg-tests")]
const RETENTION_TICK: Duration = Duration::from_secs(1);
#[cfg(not(feature = "pg-tests"))]
const RETENTION_TICK: Duration = Duration::from_secs(60);

/// Durable control-plane store backed by Postgres.
///
/// Implements [`ControlPlaneStore`] and [`AuthStore`] using Postgres as the
/// authoritative metadata store and change-log backend.
///
/// - Inputs: Postgres connection config and store config.
/// - Outputs: durable reads/writes for control-plane metadata.
///
/// # Errors
/// - Connection and query failures are surfaced as [`StoreError`].
///
/// - Database URLs may include credentials; avoid logging them.
/// - Use least-privilege DB roles and TLS in production.
///
/// # Example
/// ```rust,no_run
/// use controlplane::config::PostgresConfig;
/// use controlplane::store::{StoreConfig, postgres::PostgresStore};
///
/// async fn open(pg: PostgresConfig, cfg: StoreConfig) {
///     let _ = PostgresStore::connect(&pg, cfg).await;
/// }
/// ```
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
    replication_factor: i32,
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
    shards: i32,
    replication_factor: i32,
}

#[derive(Debug, Clone, FromRow)]
struct DbIdpIssuer {
    issuer: String,
    audiences: Value,
    discovery_url: Option<String>,
    jwks_url: Option<String>,
    subject_claim: String,
    groups_claim: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct DbSigningKey {
    kid: String,
    alg: String,
    private_pem: Vec<u8>,
    public_pem: Vec<u8>,
    status: String,
}

#[derive(Debug, Clone, FromRow)]
struct DbPolicy {
    subject: String,
    object: String,
    action: String,
}

#[derive(Debug, Clone, FromRow)]
struct DbGrouping {
    user_id: String,
    role: String,
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
    /// Creates a connection pool, applies embedded migrations, and starts a
    /// best-effort retention task when configured.
    ///
    /// - Inputs: `pg` connection config and `config` store settings.
    /// - Output: a ready-to-use [`PostgresStore`].
    ///
    /// # Errors
    /// - Connection, migration, or pool setup failures.
    ///
    /// - Avoid logging `pg.url` as it may contain credentials.
    /// - Use TLS and least-privilege DB roles in production.
    pub async fn connect(pg: &PostgresConfig, config: StoreConfig) -> StoreResult<Self> {
        #[cfg(any(test, feature = "pg-tests"))]
        let _ = Self::connect_without_migrations;
        Self::connect_internal(pg, config, true).await
    }

    /// Connect to Postgres without running migrations.
    ///
    /// Creates a connection pool without applying migrations. Intended for tests
    /// that manage migrations externally.
    ///
    /// - Inputs: `pg` connection config and `config` store settings.
    /// - Output: a [`PostgresStore`] using the existing schema.
    ///
    /// # Errors
    /// - Connection or pool setup failures.
    ///
    /// - Avoid logging `pg.url` as it may contain credentials.
    ///
    /// # Example
    /// ```rust,no_run
    /// use controlplane::config::PostgresConfig;
    /// use controlplane::store::{StoreConfig, postgres::PostgresStore};
    ///
    /// async fn open(pg: PostgresConfig, cfg: StoreConfig) {
    ///     let _ = PostgresStore::connect_without_migrations(&pg, cfg).await;
    /// }
    /// ```
    #[cfg(any(test, feature = "pg-tests"))]
    pub async fn connect_without_migrations(
        pg: &PostgresConfig,
        config: StoreConfig,
    ) -> StoreResult<Self> {
        Self::connect_internal(pg, config, false).await
    }

    async fn connect_internal(
        pg: &PostgresConfig,
        config: StoreConfig,
        run_migrations: bool,
    ) -> StoreResult<Self> {
        // Connection pool tuning matters for control-plane stability:
        // - `max_connections` caps concurrent DB work and protects the DB from overload.
        // - `acquire_timeout` bounds how long a request will wait for a pooled connection before failing fast.
        // - `connect_timeout` bounds how long we wait when establishing a new physical connection.
        //
        // In production, prefer failing fast + surfacing health failures over hanging indefinitely.
        // Avoid logging `pg.url` because it may contain credentials.
        let connect_options = PgConnectOptions::from_str(&pg.url)?;
        let pool = PgPoolOptions::new()
            .max_connections(pg.max_connections)
            .acquire_timeout(Duration::from_millis(pg.acquire_timeout_ms))
            .connect_with(connect_options)
            .await?;

        if run_migrations {
            // Migrations run *before* serving requests so handlers can assume the schema exists.
            // If migrations fail, we fail startup rather than serving partially functional endpoints.
            sqlx::migrate!("./migrations").run(&pool).await?;
        }

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
    // Delete all rows older than the newest `max_rows` entries.
    //
    // The inner SELECT computes the cutoff seq: MAX(seq) - max_rows + 1. If the table is empty,
    // COALESCE returns 0 and the DELETE is a no-op.
    //
    // The table name is baked into each statement at compile time rather than formatted in at
    // runtime, so no caller can reach this query text — that is also what lets sqlx accept these
    // as `&'static str` without an injection-audit escape hatch.
    macro_rules! retention_delete {
        ($table:literal) => {
            concat!(
                "DELETE FROM ",
                $table,
                " WHERE seq < (SELECT COALESCE(MAX(seq) - $1 + 1, 0) FROM ",
                $table,
                ")"
            )
        };
    }

    const RETENTION_DELETES: [&str; 4] = [
        retention_delete!("tenant_changes"),
        retention_delete!("namespace_changes"),
        retention_delete!("stream_changes"),
        retention_delete!("cache_changes"),
    ];

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RETENTION_TICK);
        loop {
            ticker.tick().await;
            for stmt in RETENTION_DELETES {
                let _ = sqlx::query(stmt).bind(max_rows).execute(&pool).await;
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
        .await?;
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
        let mut tx = self.pool.begin().await?;
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
            .await?;

        tx.commit().await?;
        Ok(tenant)
    }

    /// Delete a tenant and all dependent resources, emitting “Deleted” change events.
    ///
    /// Important: We fetch namespaces/streams/caches *before* deleting so we can emit “Deleted”
    /// change events with the prior payload. This is useful for caches/watchers that want tombstones
    /// with context (not just keys).
    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;

        // Validate tenant exists to return a proper 404 semantics.
        let tenant_exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tenants WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_one(&mut *tx)
                .await?
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
        .await?;

        let streams = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        ?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await?;

        // Delete tenant (schema should cascade or you rely on application-level deletes; either way
        // we emit explicit change events below based on the prefetched rows).
        sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await?;

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
                shards: cache.shards as u32,
                replication_factor: cache.replication_factor as u32,
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
            ?;
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
                replication_factor: stream.replication_factor as u32,
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
            ?;
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
            ?;
        }

        sqlx::query(r#"INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)"#)
            .bind("Deleted")
            .bind(tenant_id)
            .bind(Option::<Value>::None)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        self.refresh_counts().await?;
        Ok(())
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        let items = self.list_tenants().await?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM tenant_changes")
                .fetch_one(&self.pool)
                .await? as u64;
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
        ?;

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
                .await? as u64;

        Ok(ChangeSet { items, next_seq })
    }

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        let rows = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 ORDER BY namespace"#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        ?;

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
        let mut tx = self.pool.begin().await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
                .bind(&namespace.tenant_id)
                .fetch_one(&mut *tx)
                .await?;
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
        ?;

        tx.commit().await?;
        Ok(namespace)
    }

    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;

        let ns_row = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_optional(&mut *tx)
        .await
        ?;
        if ns_row.is_none() {
            return Err(StoreError::NotFound("namespace".into()));
        }
        let namespace_payload = ns_row.map(|row| Namespace {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            display_name: row.display_name,
        });

        let streams = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_all(&mut *tx)
        .await
        ?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_all(&mut *tx)
        .await
        ?;

        sqlx::query(r#"DELETE FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#)
            .bind(&key.tenant_id)
            .bind(&key.namespace)
            .execute(&mut *tx)
            .await?;

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
                shards: cache.shards as u32,
                replication_factor: cache.replication_factor as u32,
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
            ?;
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
                replication_factor: stream.replication_factor as u32,
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
            ?;
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
        ?;

        tx.commit().await?;
        self.refresh_counts().await?;
        Ok(())
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        let items = sqlx::query_as::<_, DbNamespace>(
            r#"SELECT tenant_id, namespace, display_name FROM namespaces ORDER BY tenant_id, namespace"#,
        )
        .fetch_all(&self.pool)
        .await
        ?
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
                .await? as u64;
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
        ?;
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
                .await? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>> {
        let rows = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 ORDER BY stream"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        ?;
        rows.into_iter()
            .map(stream_from_db)
            .collect::<Result<Vec<_>, StoreError>>()
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        let row = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .fetch_optional(&self.pool)
        .await
        ?;
        match row {
            Some(row) => stream_from_db(row),
            None => Err(StoreError::NotFound("stream".into())),
        }
    }

    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream> {
        let mut tx = self.pool.begin().await?;

        let ns_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&stream.tenant_id)
        .bind(&stream.namespace)
        .fetch_one(&mut *tx)
        .await?;
        if !ns_exists {
            return Err(StoreError::NotFound("namespace".into()));
        }

        let insert = sqlx::query(
            r#"INSERT INTO streams (tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#,
        )
        .bind(&stream.tenant_id)
        .bind(&stream.namespace)
        .bind(&stream.stream)
        .bind(stream_kind_to_str(&stream.kind))
        .bind(stream.shards as i32)
        .bind(stream.replication_factor as i32)
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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
        self.refresh_counts().await?;
        Ok(stream)
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        let mut tx = self.pool.begin().await?;
        let current = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 FOR UPDATE"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .fetch_optional(&mut *tx)
        .await
        ?;
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
            r#"UPDATE streams SET kind = $1, shards = $2, replication_factor = $3, retention_max_age_seconds = $4, retention_max_size_bytes = $5, consistency = $6, delivery = $7, durable = $8, updated_at = now()
                WHERE tenant_id = $9 AND namespace = $10 AND stream = $11"#,
        )
        .bind(stream_kind_to_str(&updated.kind))
        .bind(updated.shards as i32)
        .bind(updated.replication_factor as i32)
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
        ?;

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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
        Ok(updated)
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
        let removed = sqlx::query(
            r#"DELETE FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .execute(&mut *tx)
        .await?;
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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
        self.refresh_counts().await?;
        Ok(())
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        let rows = sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable FROM streams ORDER BY tenant_id, namespace, stream"#,
        )
        .fetch_all(&self.pool)
        .await
        ?;
        let items = rows
            .into_iter()
            .map(stream_from_db)
            .collect::<Result<Vec<_>, StoreError>>()?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM stream_changes")
                .fetch_one(&self.pool)
                .await? as u64;
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
        ?;
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
                .await? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        let rows = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches WHERE tenant_id = $1 AND namespace = $2 ORDER BY cache"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        ?;
        Ok(rows
            .into_iter()
            .map(|row| Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
                shards: row.shards as u32,
                replication_factor: row.replication_factor as u32,
            })
            .collect())
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        let row = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .fetch_optional(&self.pool)
        .await
        ?;
        match row {
            Some(row) => Ok(Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
                shards: row.shards as u32,
                replication_factor: row.replication_factor as u32,
            }),
            None => Err(StoreError::NotFound("cache".into())),
        }
    }

    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache> {
        let mut tx = self.pool.begin().await?;
        let ns_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&cache.tenant_id)
        .bind(&cache.namespace)
        .fetch_one(&mut *tx)
        .await?;
        if !ns_exists {
            return Err(StoreError::NotFound("namespace".into()));
        }

        let insert = sqlx::query(
            r#"INSERT INTO caches (tenant_id, namespace, cache, display_name, shards, replication_factor)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind(&cache.tenant_id)
        .bind(&cache.namespace)
        .bind(&cache.cache)
        .bind(&cache.display_name)
        .bind(cache.shards as i32)
        .bind(cache.replication_factor as i32)
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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        self.refresh_counts().await?;
        Ok(cache)
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        let mut tx = self.pool.begin().await?;
        let current = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3 FOR UPDATE"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .fetch_optional(&mut *tx)
        .await
        ?;
        let mut cache = match current {
            Some(row) => Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
                shards: row.shards as u32,
                replication_factor: row.replication_factor as u32,
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
        ?;

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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
        Ok(cache)
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
        let removed = sqlx::query(
            r#"DELETE FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .execute(&mut *tx)
        .await?;
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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
        self.refresh_counts().await?;
        Ok(())
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        let rows = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor FROM caches ORDER BY tenant_id, namespace, cache"#,
        )
        .fetch_all(&self.pool)
        .await
        ?;
        let items = rows
            .into_iter()
            .map(|row| Cache {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
                display_name: row.display_name,
                shards: row.shards as u32,
                replication_factor: row.replication_factor as u32,
            })
            .collect();
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM cache_changes")
                .fetch_one(&self.pool)
                .await? as u64;
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
        ?;
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
                .await? as u64;
        Ok(ChangeSet { items, next_seq })
    }

    async fn register_node(&self, node: Node) -> StoreResult<Node> {
        node.validate().map_err(invalid_node)?;
        let mut tx = self.pool.begin().await?;

        let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
            .bind(&node.node_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(node_from_db)
            .transpose()?;

        let stored = match existing {
            Some(existing) => {
                if !existing
                    .status
                    .lifecycle
                    .can_transition_to(node.status.lifecycle)
                {
                    return Err(invalid_node_transition(
                        existing.status.lifecycle,
                        node.status.lifecycle,
                    ));
                }
                Node {
                    status: NodeStatus {
                        // The identity outlives the process, so its first
                        // registration is what dates it.
                        registered_at_millis: existing.status.registered_at_millis,
                        incarnation: existing.status.incarnation + 1,
                        ..node.status
                    },
                    ..node
                }
            }
            None => Node {
                status: NodeStatus {
                    incarnation: 0,
                    ..node.status
                },
                ..node
            },
        };

        let upsert = sqlx::query(
            r#"INSERT INTO nodes (node_id, advertise_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, client_addr)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
               ON CONFLICT (node_id) DO UPDATE SET
                 advertise_addr = EXCLUDED.advertise_addr,
                 client_addr = EXCLUDED.client_addr,
                 region = EXCLUDED.region,
                 labels = EXCLUDED.labels,
                 capacity_max_shards = EXCLUDED.capacity_max_shards,
                 capacity_weight = EXCLUDED.capacity_weight,
                 lifecycle = EXCLUDED.lifecycle,
                 last_heartbeat_at_millis = EXCLUDED.last_heartbeat_at_millis,
                 registered_at_millis = EXCLUDED.registered_at_millis,
                 incarnation = EXCLUDED.incarnation,
                 updated_at = now()"#,
        );
        let upsert = bind_node(upsert, &stored).execute(&mut *tx).await;
        if let Err(err) = upsert {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict(format!(
                    "advertise_addr {} is already registered to another node",
                    stored.spec.advertise_addr
                )));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        record_node_change(
            &mut tx,
            NodeChangeOp::Registered,
            &stored.node_id,
            Some(&stored),
        )
        .await?;
        tx.commit().await?;
        crate::membership_metrics::record_registration(if stored.status.incarnation == 0 {
            "new"
        } else {
            "restart"
        });
        metrics::counter!("felix_node_changes_total", "op" => "registered").increment(1);
        Ok(stored)
    }

    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID)
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await?
            .map(node_from_db)
            .transpose()?
            .ok_or_else(|| StoreError::NotFound("node".into()))
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        let rows = sqlx::query_as::<_, DbNode>(NODE_SELECT_ALL)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(node_from_db).collect()
    }

    async fn patch_node(&self, node_id: &str, patch: NodePatchRequest) -> StoreResult<Node> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
            .bind(node_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(node_from_db)
            .transpose()?
            .ok_or_else(|| StoreError::NotFound("node".into()))?;

        let patched = patch.apply(&existing).map_err(invalid_node)?;

        let update = sqlx::query(
            r#"UPDATE nodes SET advertise_addr = $2, region = $3, labels = $4,
                 capacity_max_shards = $5, capacity_weight = $6, lifecycle = $7,
                 last_heartbeat_at_millis = $8, registered_at_millis = $9, incarnation = $10,
                 client_addr = $11,
                 updated_at = now()
               WHERE node_id = $1"#,
        );
        let update = bind_node(update, &patched).execute(&mut *tx).await;
        if let Err(err) = update {
            if is_unique_violation(&err) {
                return Err(StoreError::Conflict(format!(
                    "advertise_addr {} is already registered to another node",
                    patched.spec.advertise_addr
                )));
            }
            return Err(StoreError::Unexpected(err.into()));
        }

        record_node_change(&mut tx, NodeChangeOp::Updated, node_id, Some(&patched)).await?;
        tx.commit().await?;
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        Ok(patched)
    }

    async fn delete_node(&self, node_id: &str) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;

        // Refused rather than cascaded: deleting the assignment would erase the
        // only record of where that shard's data lives. Deliberately not a
        // foreign key, because a cascade is the behaviour being avoided.
        let led: i64 =
            sqlx::query_scalar("SELECT count(*) FROM shard_assignments WHERE leader = $1")
                .bind(node_id)
                .fetch_one(&mut *tx)
                .await?;
        if led > 0 {
            return Err(StoreError::Conflict(format!(
                "node {node_id} still leads {led} shard(s); reassign them first"
            )));
        }

        let deleted = sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(node_id)
            .execute(&mut *tx)
            .await?;
        if deleted.rows_affected() == 0 {
            return Err(StoreError::NotFound("node".into()));
        }
        record_node_change(&mut tx, NodeChangeOp::Deregistered, node_id, None).await?;
        tx.commit().await?;
        metrics::counter!("felix_node_changes_total", "op" => "deregistered").increment(1);
        Ok(())
    }

    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
            .bind(node_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(node_from_db)
            .transpose()?
            .ok_or_else(|| StoreError::NotFound("node".into()))?;

        if incarnation < existing.status.incarnation {
            return Err(StoreError::Conflict(format!(
                "heartbeat for incarnation {incarnation} of {node_id}, which is now at {}",
                existing.status.incarnation
            )));
        }

        // GREATEST, not assignment: heartbeats from two connections can arrive
        // out of order, and the newest observation is the one that matters.
        // No change is emitted -- see the trait for why.
        sqlx::query(
            r#"UPDATE nodes
               SET last_heartbeat_at_millis = GREATEST(last_heartbeat_at_millis, $2),
                   updated_at = now()
               WHERE node_id = $1"#,
        )
        .bind(node_id)
        .bind(at_millis as i64)
        .execute(&mut *tx)
        .await?;

        let mut updated = existing;
        updated.status.last_heartbeat_at_millis =
            updated.status.last_heartbeat_at_millis.max(at_millis);
        tx.commit().await?;
        Ok(updated)
    }

    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        let mut tx = self.pool.begin().await?;

        // The UPDATE both selects and claims: it takes a row lock and re-checks
        // the predicate, so a second control-plane instance running the same
        // sweep concurrently matches zero rows and publishes nothing.
        let rows = sqlx::query_as::<_, DbNode>(
            r#"UPDATE nodes SET lifecycle = 'down', updated_at = now()
               WHERE lifecycle IN ('live', 'draining') AND last_heartbeat_at_millis < $1
               RETURNING node_id, advertise_addr, client_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation"#,
        )
        .bind(expiry_before_millis as i64)
        .fetch_all(&mut *tx)
        .await?;

        let mut expired = Vec::with_capacity(rows.len());
        for row in rows {
            let node = node_from_db(row)?;
            // The row already reads `down`; the move it made is what the counter
            // is for, and the sweep only returns rows it actually claimed.
            crate::membership_metrics::record_transition(NodeLifecycle::Live, NodeLifecycle::Down);
            record_node_change(&mut tx, NodeChangeOp::Updated, &node.node_id, Some(&node)).await?;
            expired.push(node);
        }
        tx.commit().await?;

        for _ in &expired {
            metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        }
        expired.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(expired)
    }

    async fn set_node_lifecycle(
        &self,
        node_id: &str,
        lifecycle: NodeLifecycle,
    ) -> StoreResult<Option<Node>> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
            .bind(node_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(node_from_db)
            .transpose()?
            .ok_or_else(|| StoreError::NotFound("node".into()))?;

        if existing.status.lifecycle == lifecycle {
            return Ok(None);
        }
        if !existing.status.lifecycle.can_transition_to(lifecycle) {
            return Err(invalid_node_transition(
                existing.status.lifecycle,
                lifecycle,
            ));
        }

        let mut updated = existing;
        crate::membership_metrics::record_transition(updated.status.lifecycle, lifecycle);
        updated.status.lifecycle = lifecycle;
        sqlx::query("UPDATE nodes SET lifecycle = $2, updated_at = now() WHERE node_id = $1")
            .bind(node_id)
            .bind(node_lifecycle_to_str(lifecycle))
            .execute(&mut *tx)
            .await?;

        record_node_change(&mut tx, NodeChangeOp::Updated, node_id, Some(&updated)).await?;
        tx.commit().await?;
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        Ok(Some(updated))
    }

    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>> {
        // REPEATABLE READ, not just one transaction: under the default READ
        // COMMITTED the two statements below see different snapshots, so a node
        // committed between them is absent from `items` while its seq is
        // already counted in `next_seq` -- lost to every consumer that resumes
        // there. Together with seq being handed out in commit order (see
        // 0005_nodes.sql), this makes snapshot-then-poll exactly-once.
        let mut tx = self.pool.begin().await?;
        begin_consistent_read(&mut tx).await?;
        let rows = sqlx::query_as::<_, DbNode>(NODE_SELECT_ALL)
            .fetch_all(&mut *tx)
            .await?;
        let items = rows
            .into_iter()
            .map(node_from_db)
            .collect::<StoreResult<Vec<_>>>()?;
        let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM node_change_seq")
            .fetch_one(&mut *tx)
            .await? as u64;
        tx.commit().await?;
        Ok(Snapshot { items, next_seq })
    }

    async fn node_changes(&self, since: u64) -> StoreResult<ChangeSet<NodeChange>> {
        // Consistent for the same reason as `node_snapshot`: the rows and
        // `next_seq` have to describe one instant.
        let mut tx = self.pool.begin().await?;
        begin_consistent_read(&mut tx).await?;
        let rows = sqlx::query_as::<_, NodeChangeRow>(
            r#"SELECT seq, op, node_id, payload FROM node_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&mut *tx)
        .await?;
        let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM node_change_seq")
            .fetch_one(&mut *tx)
            .await? as u64;
        tx.commit().await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(NodeChange {
                seq: row.seq as u64,
                op: parse_node_change_op(&row.op)?,
                node_id: row.node_id,
                node: row.payload.map(serde_json::from_value).transpose()?,
            });
        }
        Ok(ChangeSet { items, next_seq })
    }

    async fn put_shard_assignment(
        &self,
        assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        assignment.validate().map_err(invalid_shard)?;
        let mut tx = self.pool.begin().await?;

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
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state
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
            r#"INSERT INTO shard_assignments (tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO UPDATE SET
                 leader = EXCLUDED.leader,
                 replicas = EXCLUDED.replicas,
                 generation = EXCLUDED.generation,
                 state = EXCLUDED.state,
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
        .execute(&mut *tx)
        .await?;

        record_shard_change(&mut tx, op, &stored.key, Some(&stored)).await?;
        tx.commit().await?;
        metrics::counter!("felix_shard_assignment_changes_total", "op" => shard_op_to_str(op))
            .increment(1);
        Ok(stored)
    }

    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment> {
        sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state
               FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .bind(key.shard as i32)
        .bind(key.kind.as_str())
        .fetch_optional(&self.pool)
        .await?
        .map(shard_from_db)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        let rows = sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(shard_from_db).collect()
    }

    async fn list_shard_assignments_for_node(
        &self,
        node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>> {
        let rows = sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state
               FROM shard_assignments WHERE leader = $1
               ORDER BY tenant_id, namespace, stream, shard, kind"#,
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(shard_from_db).collect()
    }

    async fn delete_shard_assignment(&self, key: &ShardKey) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
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
        metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned")
            .increment(1);
        Ok(())
    }

    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>> {
        // REPEATABLE READ for the same reason as `node_snapshot`: under READ
        // COMMITTED the two reads below see different snapshots, so an
        // assignment committed between them is missing from `items` while
        // already counted in `next_seq`.
        let mut tx = self.pool.begin().await?;
        begin_consistent_read(&mut tx).await?;
        let rows = sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
        )
        .fetch_all(&mut *tx)
        .await?;
        let items = rows
            .into_iter()
            .map(shard_from_db)
            .collect::<StoreResult<Vec<_>>>()?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
                .fetch_one(&mut *tx)
                .await? as u64;
        tx.commit().await?;
        Ok(Snapshot { items, next_seq })
    }

    async fn shard_assignment_changes(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
        let mut tx = self.pool.begin().await?;
        begin_consistent_read(&mut tx).await?;
        let rows = sqlx::query_as::<_, ShardAssignmentChangeRow>(
            r#"SELECT seq, op, tenant_id, namespace, stream, shard, kind, payload
               FROM shard_assignment_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
        )
        .bind(since as i64)
        .bind(self.limit())
        .fetch_all(&mut *tx)
        .await?;
        let next_seq =
            sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
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

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
                .bind(tenant_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists)
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    async fn health_check(&self) -> StoreResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    fn is_durable(&self) -> bool {
        true
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }
}

const NODE_SELECT_ALL: &str = r#"SELECT node_id, advertise_addr, client_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation FROM nodes ORDER BY node_id"#;
const NODE_SELECT_BY_ID: &str = r#"SELECT node_id, advertise_addr, client_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation FROM nodes WHERE node_id = $1"#;
/// `FOR UPDATE` so a concurrent register or patch of the same node waits rather
/// than reading the row this transaction is about to replace.
const NODE_SELECT_BY_ID_FOR_UPDATE: &str = r#"SELECT node_id, advertise_addr, client_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation FROM nodes WHERE node_id = $1 FOR UPDATE"#;

/// Bind a node in the column order the insert and the update both use.
fn bind_node<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    node: &'q Node,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(&node.node_id)
        .bind(&node.spec.advertise_addr)
        .bind(&node.spec.region)
        .bind(serde_json::to_value(&node.spec.labels).unwrap_or_default())
        .bind(node.spec.capacity.max_shards.map(|v| v as i32))
        .bind(node.spec.capacity.weight as i32)
        .bind(node_lifecycle_to_str(node.status.lifecycle))
        .bind(node.status.last_heartbeat_at_millis as i64)
        .bind(node.status.registered_at_millis as i64)
        .bind(node.status.incarnation as i64)
        // Last, so both statements above can name it as $11.
        .bind(node.spec.client_addr.as_deref())
}

/// Make the rest of the transaction read one consistent snapshot of the
/// database, rather than re-reading between statements.
async fn begin_consistent_read(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> StoreResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Append a change, taking its `seq` from the locked counter.
///
/// The `UPDATE ... RETURNING` holds a row lock until this transaction commits,
/// so a concurrent writer blocks and takes the next number only afterwards.
/// That is what makes `seq` order equal commit order, which is what lets a
/// consumer resume at `next_seq` without missing anything. See 0005_nodes.sql.
async fn record_node_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: NodeChangeOp,
    node_id: &str,
    node: Option<&Node>,
) -> StoreResult<()> {
    let seq = sqlx::query_scalar::<_, i64>(
        "UPDATE node_change_seq SET next_seq = next_seq + 1 RETURNING next_seq - 1",
    )
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query("INSERT INTO node_changes (seq, op, node_id, payload) VALUES ($1, $2, $3, $4)")
        .bind(seq)
        .bind(node_change_op_to_str(&op))
        .bind(node_id)
        .bind(node.map(serde_json::to_value).transpose()?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

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
    })
}

fn shard_state_to_str(state: ShardState) -> &'static str {
    match state {
        ShardState::Assigning => "assigning",
        ShardState::Active => "active",
        ShardState::Draining => "draining",
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

/// Row shape for the `nodes` table.
#[derive(Debug, Clone, FromRow)]
struct DbNode {
    node_id: String,
    advertise_addr: String,
    client_addr: Option<String>,
    region: String,
    labels: serde_json::Value,
    capacity_max_shards: Option<i32>,
    capacity_weight: i32,
    lifecycle: String,
    last_heartbeat_at_millis: i64,
    registered_at_millis: i64,
    incarnation: i64,
}

#[derive(Debug, Clone, FromRow)]
struct NodeChangeRow {
    seq: i64,
    op: String,
    node_id: String,
    payload: Option<serde_json::Value>,
}

fn node_from_db(row: DbNode) -> StoreResult<Node> {
    Ok(Node {
        node_id: row.node_id,
        spec: NodeSpec {
            advertise_addr: row.advertise_addr,
            client_addr: row.client_addr,
            region: row.region,
            labels: serde_json::from_value(row.labels)?,
            capacity: NodeCapacity {
                max_shards: row.capacity_max_shards.map(|v| v as u32),
                weight: row.capacity_weight as u32,
            },
        },
        status: NodeStatus {
            lifecycle: parse_node_lifecycle(&row.lifecycle)?,
            last_heartbeat_at_millis: row.last_heartbeat_at_millis as u64,
            registered_at_millis: row.registered_at_millis as u64,
            incarnation: row.incarnation as u64,
        },
    })
}

fn node_lifecycle_to_str(lifecycle: NodeLifecycle) -> &'static str {
    match lifecycle {
        NodeLifecycle::Live => "live",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Down => "down",
        NodeLifecycle::Left => "left",
    }
}

fn parse_node_lifecycle(value: &str) -> StoreResult<NodeLifecycle> {
    match value {
        "live" => Ok(NodeLifecycle::Live),
        "draining" => Ok(NodeLifecycle::Draining),
        "down" => Ok(NodeLifecycle::Down),
        "left" => Ok(NodeLifecycle::Left),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown node lifecycle: {other}"
        ))),
    }
}

fn node_change_op_to_str(op: &NodeChangeOp) -> &'static str {
    match op {
        NodeChangeOp::Registered => "Registered",
        NodeChangeOp::Updated => "Updated",
        NodeChangeOp::Deregistered => "Deregistered",
    }
}

fn parse_node_change_op(value: &str) -> StoreResult<NodeChangeOp> {
    match value {
        "Registered" => Ok(NodeChangeOp::Registered),
        "Updated" => Ok(NodeChangeOp::Updated),
        "Deregistered" => Ok(NodeChangeOp::Deregistered),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown node change op: {other}"
        ))),
    }
}

/// A model rejection is the caller's fault, so it surfaces as a conflict rather
/// than an internal error.
fn invalid_node(err: NodeValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

fn invalid_node_transition(from: NodeLifecycle, to: NodeLifecycle) -> StoreError {
    invalid_node(NodeValidationError::UnsupportedTransition { from, to })
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
        replication_factor: 1,
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

fn parse_consistency(value: &str) -> StoreResult<crate::model::ConsistencyLevel> {
    match value {
        "Leader" => Ok(crate::model::ConsistencyLevel::Leader),
        "Quorum" => Ok(crate::model::ConsistencyLevel::Quorum),
        _ => Err(StoreError::Unexpected(anyhow!(
            "invalid consistency {value}"
        ))),
    }
}

fn consistency_to_str(value: &crate::model::ConsistencyLevel) -> &'static str {
    match value {
        crate::model::ConsistencyLevel::Leader => "Leader",
        crate::model::ConsistencyLevel::Quorum => "Quorum",
    }
}

fn parse_delivery(value: &str) -> StoreResult<crate::model::DeliveryGuarantee> {
    match value {
        "AtMostOnce" => Ok(crate::model::DeliveryGuarantee::AtMostOnce),
        "AtLeastOnce" => Ok(crate::model::DeliveryGuarantee::AtLeastOnce),
        _ => Err(StoreError::Unexpected(anyhow!("invalid delivery {value}"))),
    }
}

fn delivery_to_str(value: &crate::model::DeliveryGuarantee) -> &'static str {
    match value {
        crate::model::DeliveryGuarantee::AtMostOnce => "AtMostOnce",
        crate::model::DeliveryGuarantee::AtLeastOnce => "AtLeastOnce",
    }
}

fn parse_algorithm(value: &str) -> StoreResult<Algorithm> {
    // Felix tokens must remain EdDSA; reject any other algorithm on load.
    match value {
        "EdDSA" => Ok(Algorithm::EdDSA),
        _ => Err(StoreError::Unexpected(anyhow!("invalid alg {value}"))),
    }
}

fn algorithm_to_str(value: Algorithm) -> &'static str {
    // Persist only EdDSA to prevent accidental RSA reintroduction.
    match value {
        Algorithm::EdDSA => "EdDSA",
        _ => "EdDSA",
    }
}

fn decode_key(value: &[u8], label: &str) -> StoreResult<[u8; 32]> {
    value
        .try_into()
        .map_err(|_| StoreError::Unexpected(anyhow!("invalid {label} length")))
}

impl PostgresStore {
    async fn refresh_counts(&self) -> StoreResult<()> {
        let stream_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM streams")
            .fetch_one(&self.pool)
            .await?;
        metrics::gauge!("felix_streams_total").set(stream_total as f64);

        let cache_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM caches")
            .fetch_one(&self.pool)
            .await?;
        metrics::gauge!("felix_caches_total").set(cache_total as f64);
        Ok(())
    }
}

#[async_trait]
impl AuthStore for PostgresStore {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        let rows: Vec<DbIdpIssuer> = sqlx::query_as(
            "SELECT issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim \
             FROM idp_issuers WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let mut issuers = Vec::with_capacity(rows.len());
        for row in rows {
            let audiences: Vec<String> = serde_json::from_value(row.audiences)
                .map_err(|err| StoreError::Unexpected(anyhow!("invalid audiences json: {err}")))?;
            issuers.push(IdpIssuerConfig {
                issuer: row.issuer,
                audiences,
                discovery_url: row.discovery_url,
                jwks_url: row.jwks_url,
                claim_mappings: ClaimMappings {
                    subject_claim: row.subject_claim,
                    groups_claim: row.groups_claim,
                },
            });
        }
        Ok(issuers)
    }

    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()> {
        let audiences = serde_json::to_value(&issuer.audiences)?;
        sqlx::query(
            "INSERT INTO idp_issuers (tenant_id, issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, issuer) DO UPDATE SET \
                audiences = EXCLUDED.audiences, \
                discovery_url = EXCLUDED.discovery_url, \
                jwks_url = EXCLUDED.jwks_url, \
                subject_claim = EXCLUDED.subject_claim, \
                groups_claim = EXCLUDED.groups_claim",
        )
        .bind(tenant_id)
        .bind(&issuer.issuer)
        .bind(audiences)
        .bind(&issuer.discovery_url)
        .bind(&issuer.jwks_url)
        .bind(&issuer.claim_mappings.subject_claim)
        .bind(&issuer.claim_mappings.groups_claim)
        .execute(&self.pool)
        .await
        ?;
        Ok(())
    }

    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()> {
        sqlx::query("DELETE FROM idp_issuers WHERE tenant_id = $1 AND issuer = $2")
            .bind(tenant_id)
            .bind(issuer)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        let rows: Vec<DbPolicy> = sqlx::query_as(
            "SELECT subject, object, action FROM rbac_policies WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| PolicyRule {
                subject: row.subject,
                object: row.object,
                action: row.action,
            })
            .collect())
    }

    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        let rows: Vec<DbGrouping> =
            sqlx::query_as("SELECT user_id, role FROM rbac_groupings WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|row| GroupingRule {
                user: row.user_id,
                role: row.role,
            })
            .collect())
    }

    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        sqlx::query(
            "INSERT INTO rbac_policies (tenant_id, subject, object, action) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id)
        .bind(&policy.subject)
        .bind(&policy.object)
        .bind(&policy.action)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()> {
        sqlx::query(
            "INSERT INTO rbac_groupings (tenant_id, user_id, role) \
             VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id)
        .bind(&grouping.user)
        .bind(&grouping.role)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        // We fetch all keys to support rotation; callers will try `current` first.
        let rows: Vec<DbSigningKey> = sqlx::query_as(
            "SELECT kid, alg, private_pem, public_pem, status \
             FROM tenant_signing_keys WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(StoreError::NotFound("signing keys".into()));
        }

        let mut current: Option<SigningKey> = None;
        let mut previous = Vec::new();
        for row in rows {
            // Parse and validate EdDSA-only key material from raw bytes.
            // Private key bytes are stored as raw Ed25519 seeds, not PKCS8.
            let key = SigningKey {
                kid: row.kid,
                alg: parse_algorithm(&row.alg)?,
                private_key: decode_key(&row.private_pem, "private key")?,
                public_key: decode_key(&row.public_pem, "public key")?,
            };
            match row.status.as_str() {
                "current" => current = Some(key),
                "previous" => previous.push(key),
                _ => {}
            }
        }

        let current = current.ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
        Ok(TenantSigningKeys { current, previous })
    }

    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        // Validate that keys are EdDSA and public keys match private seeds.
        // This prevents accidental RSA reintroduction and corrupted key storage.
        keys.validate()
            .map_err(|err| StoreError::Unexpected(anyhow!(err)))?;
        let mut tx = self.pool.begin().await?;
        // Replace all keys atomically to keep `current` and `previous` consistent.
        sqlx::query("DELETE FROM tenant_signing_keys WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await?;

        let current_alg = algorithm_to_str(keys.current.alg);
        // Store raw Ed25519 seeds; never serialize or log these values.
        sqlx::query(
            "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
             VALUES ($1, $2, $3, $4, $5, 'current')",
        )
        .bind(tenant_id)
        .bind(&keys.current.kid)
        .bind(current_alg)
        .bind(keys.current.private_key.as_slice())
        .bind(keys.current.public_key.as_slice())
        .execute(&mut *tx)
        .await
        ?;

        for key in &keys.previous {
            let alg = algorithm_to_str(key.alg);
            // Store previous keys for rotation; still valid for verification.
            sqlx::query(
                "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
                 VALUES ($1, $2, $3, $4, $5, 'previous')",
            )
            .bind(tenant_id)
            .bind(&key.kid)
            .bind(alg)
            .bind(key.private_key.as_slice())
            .bind(key.public_key.as_slice())
            .execute(&mut *tx)
            .await
            ?;
        }

        tx.commit().await?;
        // Invalidate derived key cache so new keys take effect immediately.
        crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
        Ok(())
    }

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool> {
        let row: Option<(bool,)> =
            sqlx::query_as("SELECT auth_bootstrapped FROM tenants WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(value,)| value).unwrap_or(false))
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()> {
        let result = sqlx::query("UPDATE tenants SET auth_bootstrapped = $2 WHERE tenant_id = $1")
            .bind(tenant_id)
            .bind(bootstrapped)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound("tenant".into()));
        }
        Ok(())
    }

    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        // If no keys exist, generate a new Ed25519 key set for the tenant.
        match self.get_tenant_signing_keys(tenant_id).await {
            Ok(keys) => Ok(keys),
            Err(StoreError::NotFound(_)) => {
                let keys = crate::auth::keys::generate_signing_keys()?;
                self.set_tenant_signing_keys(tenant_id, keys.clone())
                    .await?;
                Ok(keys)
            }
            Err(err) => Err(err),
        }
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        // Seed policy/grouping data atomically to avoid partial authorization state.
        let mut tx = self.pool.begin().await?;
        for policy in policies {
            sqlx::query(
                "INSERT INTO rbac_policies (tenant_id, subject, object, action) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(tenant_id)
            .bind(&policy.subject)
            .bind(&policy.object)
            .bind(&policy.action)
            .execute(&mut *tx)
            .await?;
        }
        for grouping in groupings {
            sqlx::query(
                "INSERT INTO rbac_groupings (tenant_id, user_id, role) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(tenant_id)
            .bind(&grouping.user)
            .bind(&grouping.role)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_violation_detects_only_db_codes() {
        // This test prevents false positives when inspecting non-DB errors.
        let err = sqlx::Error::RowNotFound;
        assert!(!is_unique_violation(&err));
    }

    #[test]
    fn stream_kind_round_trip() {
        // This test ensures DB string mapping stays stable for stream kinds.
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
        // This test ensures consistency levels map correctly between DB and API.
        assert!(matches!(
            parse_consistency("Leader").unwrap(),
            crate::model::ConsistencyLevel::Leader
        ));
        assert!(matches!(
            parse_consistency("Quorum").unwrap(),
            crate::model::ConsistencyLevel::Quorum
        ));
        assert!(parse_consistency("Unknown").is_err());
        assert_eq!(
            consistency_to_str(&crate::model::ConsistencyLevel::Leader),
            "Leader"
        );
        assert_eq!(
            consistency_to_str(&crate::model::ConsistencyLevel::Quorum),
            "Quorum"
        );
    }

    #[test]
    fn delivery_round_trip() {
        // This test ensures delivery guarantees map correctly between DB and API.
        assert!(matches!(
            parse_delivery("AtMostOnce").unwrap(),
            crate::model::DeliveryGuarantee::AtMostOnce
        ));
        assert!(matches!(
            parse_delivery("AtLeastOnce").unwrap(),
            crate::model::DeliveryGuarantee::AtLeastOnce
        ));
        assert!(parse_delivery("Unknown").is_err());
        assert_eq!(
            delivery_to_str(&crate::model::DeliveryGuarantee::AtMostOnce),
            "AtMostOnce"
        );
        assert_eq!(
            delivery_to_str(&crate::model::DeliveryGuarantee::AtLeastOnce),
            "AtLeastOnce"
        );
    }

    #[test]
    fn stream_from_db_maps_fields() {
        // This test guards against schema/model drift when parsing DB rows.
        let row = DbStream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s1".to_string(),
            kind: "Stream".to_string(),
            shards: 2,
            replication_factor: 1,
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
            crate::model::ConsistencyLevel::Leader
        ));
        assert!(matches!(
            stream.delivery,
            crate::model::DeliveryGuarantee::AtLeastOnce
        ));
        assert!(stream.durable);
    }

    #[test]
    fn algorithm_round_trip_and_rejects_unknown() {
        assert!(matches!(
            parse_algorithm("EdDSA").unwrap(),
            Algorithm::EdDSA
        ));
        assert!(parse_algorithm("RS256").is_err());
        assert_eq!(algorithm_to_str(Algorithm::EdDSA), "EdDSA");
    }

    #[test]
    fn algorithm_to_str_defaults_to_eddsa() {
        assert_eq!(algorithm_to_str(Algorithm::HS256), "EdDSA");
    }

    #[test]
    fn decode_key_rejects_invalid_length() {
        let err = decode_key(&[1, 2, 3], "key").unwrap_err();
        assert!(err.to_string().contains("invalid key length"));
        let ok = decode_key(&[0u8; 32], "key").unwrap();
        assert_eq!(ok, [0u8; 32]);
    }
}
