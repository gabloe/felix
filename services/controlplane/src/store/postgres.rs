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
//! # Key invariants
//! - Authoritative tables represent current state; change tables are append-only.
//! - Each change table has a monotonically increasing `seq`.
//! - Transactions update authoritative state and append change events atomically.
//!
//! # Security model / threat assumptions
//! - Database URLs may contain credentials; avoid logging them.
//! - We assume Postgres enforces durability and access control.
//! - Dynamic SQL is limited to a fixed, internal allowlist.
//!
//! # Concurrency model
//! - The store is shared across async handlers; `sqlx::PgPool` manages concurrency.
//! - Each method acquires a pooled connection; pool sizing controls throughput.
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
//! # How to use
//! Call [`PostgresStore::connect`] with a [`PostgresConfig`] and [`StoreConfig`], then use the
//! returned store via the [`ControlPlaneStore`] and [`AuthStore`] traits.
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
use super::{
    AuthStore, ChangeSet, ControlPlaneStore, Snapshot, StoreConfig, StoreError, StoreResult,
};
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::config::PostgresConfig;
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, Namespace, NamespaceChange,
    NamespaceChangeOp, NamespaceKey, RetentionPolicy, Stream, StreamChange, StreamChangeOp,
    StreamKey, StreamKind, StreamPatchRequest, Tenant, TenantChange, TenantChangeOp,
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
/// # What it does
/// Implements [`ControlPlaneStore`] and [`AuthStore`] using Postgres as the
/// authoritative metadata store and change-log backend.
///
/// # Inputs/outputs
/// - Inputs: Postgres connection config and store config.
/// - Outputs: durable reads/writes for control-plane metadata.
///
/// # Errors
/// - Connection and query failures are surfaced as [`StoreError`].
///
/// # Security notes
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
    /// # What it does
    /// Creates a connection pool, applies embedded migrations, and starts a
    /// best-effort retention task when configured.
    ///
    /// # Inputs/outputs
    /// - Inputs: `pg` connection config and `config` store settings.
    /// - Output: a ready-to-use [`PostgresStore`].
    ///
    /// # Errors
    /// - Connection, migration, or pool setup failures.
    ///
    /// # Security notes
    /// - Avoid logging `pg.url` as it may contain credentials.
    /// - Use TLS and least-privilege DB roles in production.
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
    pub async fn connect(pg: &PostgresConfig, config: StoreConfig) -> StoreResult<Self> {
        #[cfg(any(test, feature = "pg-tests"))]
        let _ = Self::connect_without_migrations;
        Self::connect_internal(pg, config, true).await
    }

    /// Connect to Postgres without running migrations.
    ///
    /// # What it does
    /// Creates a connection pool without applying migrations. Intended for tests
    /// that manage migrations externally.
    ///
    /// # Inputs/outputs
    /// - Inputs: `pg` connection config and `config` store settings.
    /// - Output: a [`PostgresStore`] using the existing schema.
    ///
    /// # Errors
    /// - Connection or pool setup failures.
    ///
    /// # Security notes
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
    let tables = [
        "tenant_changes",
        "namespace_changes",
        "stream_changes",
        "cache_changes",
    ];
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RETENTION_TICK);
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1"#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        ?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1"#,
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
               FROM streams WHERE tenant_id = $1 AND namespace = $2"#,
        )
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .fetch_all(&mut *tx)
        .await
        ?;

        let caches = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2"#,
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable
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
            r#"SELECT tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable FROM streams ORDER BY tenant_id, namespace, stream"#,
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
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2 ORDER BY cache"#,
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
        ?;
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
        ?;

        tx.commit().await?;
        metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        self.refresh_counts().await?;
        Ok(cache)
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        let mut tx = self.pool.begin().await?;
        let current = sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3 FOR UPDATE"#,
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
            r#"SELECT tenant_id, namespace, cache, display_name FROM caches ORDER BY tenant_id, namespace, cache"#,
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
