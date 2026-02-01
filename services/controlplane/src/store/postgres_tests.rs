//! Postgres store unit tests with real DB integration.
//!
//! # Purpose
//! Exercise the Postgres-backed store with real SQL to verify schema, migrations,
//! and CRUD/change-log behavior.
//!
//! # Key invariants
//! - Tests use a dedicated schema to avoid cross-test contamination.
//! - Migrations are run once per schema to minimize test time.
//! - Reset logic truncates tables to a clean baseline.
//!
//! # Security model / threat assumptions
//! - Database URLs may contain credentials; tests should not log them.
//! - Docker-based Postgres is used only in test environments.
//!
//! # Concurrency + ordering guarantees
//! - Tests are serialized to avoid shared schema races.
//! - OnceCell guards ensure container/migration setup occurs once.
//!
//! # How to use
//! Run with `cargo test -p controlplane --features pg-tests postgres_store_full_roundtrip`.
//!
//! These tests live in a separate module so coverage is attributed to the production
//! `postgres.rs` implementation without inflating that file's line counts.
#![cfg(feature = "pg-tests")]

use super::postgres::PostgresStore;
use super::{AuthStore, ControlPlaneStore, StoreConfig};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::keys::generate_signing_keys;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::config;
use crate::model::{
    Cache, CacheKey, CachePatchRequest, ConsistencyLevel, DeliveryGuarantee, Namespace,
    NamespaceKey, RetentionPolicy, Stream, StreamKey, StreamKind, StreamPatchRequest, Tenant,
};
use serial_test::serial;
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use testcontainers::clients::Cli;
use testcontainers::core::Container;
use testcontainers_modules::postgres::Postgres;

struct PgContainer {
    url: String,
    _container: Container<'static, Postgres>,
}

static PG_CONTAINER: tokio::sync::OnceCell<PgContainer> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

static MIGRATOR: Migrator = sqlx::migrate!();

fn docker_available() -> bool {
    // We probe docker availability to decide whether to spin up a container.
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .is_ok()
}

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<(), sqlx::Error> {
    // Poll until Postgres is ready or the timeout is reached to avoid flaky startup.
    let start = tokio::time::Instant::now();
    loop {
        let attempt = tokio::time::timeout(
            Duration::from_secs(5),
            PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(3))
                .connect(url),
        )
        .await;
        match attempt {
            Ok(Ok(pool)) => {
                let _ = pool.close().await;
                return Ok(());
            }
            Ok(Err(err)) => {
                if start.elapsed() >= timeout {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {
                if start.elapsed() >= timeout {
                    return Err(sqlx::Error::PoolTimedOut);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn pg_url() -> Option<String> {
    // Prefer explicitly configured URLs to avoid using docker in CI unless needed.
    if let Ok(url) = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        return Some(url);
    }
    if !docker_available() {
        // We skip tests gracefully when docker is unavailable.
        eprintln!("skipping pg-tests: docker not available");
        return None;
    }
    let container = PG_CONTAINER
        .get_or_try_init(|| async {
            // Use a single long-lived container to keep tests deterministic and fast.
            let docker = Box::leak(Box::new(Cli::default()));
            let container = docker.run(Postgres::default());
            let port = container.get_host_port_ipv4(5432);
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            wait_for_postgres(&url, Duration::from_secs(30)).await?;
            Ok::<_, sqlx::Error>(PgContainer {
                url,
                _container: container,
            })
        })
        .await
        .ok()?;
    Some(container.url.clone())
}

async fn test_schema_name() -> String {
    // Unique schema per test run avoids collisions across processes.
    PG_SCHEMA
        .get_or_init(|| async {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("felix_test_{}_{}", std::process::id(), nanos)
        })
        .await
        .clone()
}

fn url_with_schema(base_url: &str, schema: &str) -> String {
    // We pass search_path via connection options to isolate test schema.
    let encoded = format!("-csearch_path%3D{}", schema);
    if base_url.contains('?') {
        format!("{base_url}&options={encoded}")
    } else {
        format!("{base_url}?options={encoded}")
    }
}

async fn ensure_schema(base_url: &str) -> Result<String, sqlx::Error> {
    // Create the schema once so subsequent tests can reuse it.
    let schema = test_schema_name().await;
    let schema_clone = schema.clone();
    PG_SCHEMA_READY
        .get_or_try_init(|| async move {
            let mut conn = sqlx::PgConnection::connect(base_url).await?;
            let create_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema_clone);
            sqlx::query(&create_sql).execute(&mut conn).await?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    Ok(schema)
}

async fn run_migrations_once(url: &str) -> Result<(), sqlx::Error> {
    // Migrations are expensive; run them once per schema to reduce flakiness.
    PG_MIGRATED
        .get_or_try_init(|| async move {
            let mut conn = sqlx::PgConnection::connect(url).await?;
            MIGRATOR.run(&mut conn).await?;
            conn.close().await?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    Ok(())
}

async fn reset_db(url: &str, schema: &str) -> Result<(), sqlx::Error> {
    // This reset ensures each test starts from a clean slate.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await?;

    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{schema}.tenant_changes"))
        .fetch_one(&pool)
        .await?;
    if exists.is_none() {
        // If tables don't exist yet, there's nothing to truncate.
        pool.close().await;
        return Ok(());
    }

    let schema_ident = format!(r#""{}""#, schema);
    let truncate = format!(
        "TRUNCATE {schema_ident}.tenant_changes, {schema_ident}.namespace_changes, \
         {schema_ident}.stream_changes, {schema_ident}.cache_changes, {schema_ident}.streams, \
         {schema_ident}.caches, {schema_ident}.namespaces, {schema_ident}.idp_issuers, \
         {schema_ident}.rbac_policies, {schema_ident}.rbac_groupings, \
         {schema_ident}.tenant_signing_keys, {schema_ident}.tenants RESTART IDENTITY CASCADE",
    );
    sqlx::query(&truncate).execute(&pool).await.map(|_| ())
}

#[tokio::test]
#[serial]
async fn postgres_store_full_roundtrip() -> anyhow::Result<()> {
    // This test prevents regressions in schema, migrations, and change-log flows.
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let store = PostgresStore::connect_without_migrations(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;

    let stream_key = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
    };
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
        })
        .await?;
    store
        .patch_stream(
            &stream_key,
            StreamPatchRequest {
                retention: Some(RetentionPolicy {
                    max_age_seconds: Some(7200),
                    max_size_bytes: Some(2048),
                }),
                consistency: Some(ConsistencyLevel::Quorum),
                delivery: Some(DeliveryGuarantee::AtMostOnce),
                durable: Some(true),
            },
        )
        .await?;

    let cache_key = CacheKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
    };
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;
    store
        .patch_cache(
            &cache_key,
            CachePatchRequest {
                display_name: Some("Primary Updated".to_string()),
            },
        )
        .await?;

    let tenant_snapshot = store.tenant_snapshot().await?;
    assert!(!tenant_snapshot.items.is_empty());
    let ns_snapshot = store.namespace_snapshot().await?;
    assert!(!ns_snapshot.items.is_empty());
    let stream_snapshot = store.stream_snapshot().await?;
    assert!(!stream_snapshot.items.is_empty());
    let cache_snapshot = store.cache_snapshot().await?;
    assert!(!cache_snapshot.items.is_empty());

    let tenant_changes = store.tenant_changes(0).await?;
    assert!(!tenant_changes.items.is_empty());
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());
    let stream_changes = store.stream_changes(0).await?;
    assert!(!stream_changes.items.is_empty());
    let cache_changes = store.cache_changes(0).await?;
    assert!(!cache_changes.items.is_empty());

    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: Default::default(),
            },
        )
        .await?;
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:admin".to_string(),
                object: "tenant:*".to_string(),
                action: "tenant.admin".to_string(),
            },
        )
        .await?;
    store
        .add_rbac_grouping(
            "t1",
            GroupingRule {
                user: "p:admin".to_string(),
                role: "role:admin".to_string(),
            },
        )
        .await?;
    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys).await?;
    store.set_tenant_auth_bootstrapped("t1", true).await?;

    store.delete_cache(&cache_key).await?;
    store.delete_stream(&stream_key).await?;
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
        })
        .await?;
    store.delete_tenant("t1").await?;
    store
        .delete_idp_issuer("t1", "https://issuer.example.com")
        .await?;
    store.health_check().await?;
    assert!(store.is_durable());
    assert_eq!(store.backend_name(), "postgres");

    let _ = PostgresStore::connect(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    Ok(())
}
