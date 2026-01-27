#![cfg(feature = "pg-tests")]
//! Postgres-backed control-plane store end-to-end tests.
//!
//! # Purpose
//! These tests exercise the Postgres store against a real database to validate:
//! - schema migrations and connection setup
//! - CRUD + snapshot/changefeed behaviors
//! - auth/bootstrap tables (idp issuers, RBAC, signing keys)
//! - retention trimming and cascade delete semantics
//!
//! # Test infrastructure
//! We use `testcontainers` to spin up an ephemeral Postgres instance and reset state between tests.
//! Tests are serialized because they share a single database container per test run.

mod common;

use anyhow::Result;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::app;
use controlplane::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use controlplane::auth::keys::generate_signing_keys;
use controlplane::auth::oidc::OidcValidator;
use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use controlplane::model::{
    Cache, CachePatchRequest, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceKey,
    RetentionPolicy, Stream, StreamKey, StreamKind, StreamPatchRequest, Tenant,
};
use controlplane::store::{AuthStore, ControlPlaneStore, StoreConfig};
use controlplane::{config, store};
use serde_json::json;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::clients::Cli;
use testcontainers::core::Container;
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .is_ok()
}

struct PgFixture {
    store: Arc<store::postgres::PostgresStore>,
    url: String,
}

struct PgContainer {
    url: String,
    _container: Container<'static, Postgres>,
}

static PG_CONTAINER: tokio::sync::OnceCell<PgContainer> = tokio::sync::OnceCell::const_new();
static PG_STORE_DEFAULT: tokio::sync::OnceCell<Arc<store::postgres::PostgresStore>> =
    tokio::sync::OnceCell::const_new();
static PG_STORE_RETENTION_50: tokio::sync::OnceCell<Arc<store::postgres::PostgresStore>> =
    tokio::sync::OnceCell::const_new();
static PG_STORE_RETENTION_1: tokio::sync::OnceCell<Arc<store::postgres::PostgresStore>> =
    tokio::sync::OnceCell::const_new();

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<(), sqlx::Error> {
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

async fn reset_db(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        // Use a small, short-lived pool for reset to avoid competing with the store pool.
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await?;
    sqlx::query(
        "TRUNCATE tenant_changes, namespace_changes, stream_changes, cache_changes, \
         streams, caches, namespaces, idp_issuers, rbac_policies, rbac_groupings, \
         tenant_signing_keys, tenants RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .map(|_| ())
}

async fn pg_container() -> Result<Option<&'static PgContainer>> {
    if !docker_available() {
        eprintln!("skipping pg-tests: docker not available");
        return Ok(None);
    }
    let container = PG_CONTAINER
        .get_or_try_init(|| async {
            eprintln!("pg-tests: starting postgres container");
            let docker = Box::leak(Box::new(Cli::default()));
            let container = docker.run(Postgres::default());
            let port = container.get_host_port_ipv4(5432);
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            eprintln!("pg-tests: postgres ready on 127.0.0.1:{port}");
            eprintln!("pg-tests: waiting for postgres to accept connections");
            wait_for_postgres(&url, Duration::from_secs(30)).await?;
            eprintln!("pg-tests: postgres accepting connections");
            Ok::<_, sqlx::Error>(PgContainer {
                url,
                _container: container,
            })
        })
        .await;

    match container {
        Ok(container) => Ok(Some(container)),
        Err(err) => {
            eprintln!("skipping pg-tests: cannot connect to postgres: {err}");
            Ok(None)
        }
    }
}

async fn pg_store(retention: Option<i64>) -> Result<Option<PgFixture>> {
    let url_override = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();
    let url = if let Some(url) = url_override {
        eprintln!("pg-tests: using external postgres url (retention={retention:?})");
        url
    } else {
        let Some(container) = pg_container().await? else {
            return Ok(None);
        };
        eprintln!("pg-tests: using shared postgres container (retention={retention:?})");
        container.url.clone()
    };
    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        // Give the containerized Postgres more headroom to avoid pool timeouts in CI.
        max_connections: 10,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let retention = retention.unwrap_or(config::DEFAULT_CHANGE_RETENTION_MAX_ROWS);
    let store_cell = match retention {
        1 => &PG_STORE_RETENTION_1,
        50 => &PG_STORE_RETENTION_50,
        _ => &PG_STORE_DEFAULT,
    };
    let store = match store_cell
        .get_or_try_init(|| async {
            let store = store::postgres::PostgresStore::connect(
                &pg_cfg,
                StoreConfig {
                    changes_limit: config::DEFAULT_CHANGES_LIMIT,
                    change_retention_max_rows: Some(retention),
                },
            )
            .await?;
            Ok::<_, store::StoreError>(Arc::new(store))
        })
        .await
    {
        Ok(store) => Arc::clone(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return Ok(None);
        }
    };
    eprintln!("pg-tests: connected to postgres and ran migrations");
    let _ = reset_db(&url).await;
    eprintln!("pg-tests: database reset complete");

    // Keep docker client alive via return value.
    Ok(Some(PgFixture { store, url }))
}

async fn pg_store_with_config(config: StoreConfig) -> Result<Option<PgFixture>> {
    let url_override = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();
    let url = if let Some(url) = url_override {
        eprintln!("pg-tests: using external postgres url (custom config)");
        url
    } else {
        let Some(container) = pg_container().await? else {
            return Ok(None);
        };
        eprintln!("pg-tests: using shared postgres container (custom config)");
        container.url.clone()
    };
    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let store = store::postgres::PostgresStore::connect(&pg_cfg, config).await;
    let store = match store {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return Ok(None);
        }
    };
    eprintln!("pg-tests: connected to postgres and ran migrations (custom config)");
    let _ = reset_db(&url).await;
    eprintln!("pg-tests: database reset complete (custom config)");
    Ok(Some(PgFixture { store, url }))
}

#[tokio::test]
#[serial]
async fn pg_store_core_crud_and_auth() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_core_crud_and_auth begin");

    // Tenant CRUD + conflict + not found.
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let conflict = store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One Again".to_string(),
        })
        .await;
    assert!(matches!(conflict, Err(store::StoreError::Conflict(_))));
    assert!(store.tenant_exists("t1").await?);
    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 1);
    let missing_namespace = store
        .namespace_exists(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
        })
        .await?;
    assert!(!missing_namespace);

    // Namespace CRUD.
    let ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert_eq!(ns.namespace, "default");
    let conflict_ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default Again".to_string(),
        })
        .await;
    assert!(matches!(conflict_ns, Err(store::StoreError::Conflict(_))));
    let namespaces = store.list_namespaces("t1").await?;
    assert_eq!(namespaces.len(), 1);

    // Stream CRUD + patch + get + not found.
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
    let conflict_stream = store
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
        .await;
    assert!(matches!(
        conflict_stream,
        Err(store::StoreError::Conflict(_))
    ));
    let stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await?;
    assert_eq!(stream.stream, "orders");
    let patched = store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            },
            StreamPatchRequest {
                retention: Some(RetentionPolicy {
                    max_age_seconds: Some(7200),
                    max_size_bytes: Some(1024),
                }),
                consistency: Some(ConsistencyLevel::Quorum),
                delivery: Some(DeliveryGuarantee::AtMostOnce),
                durable: Some(true),
            },
        )
        .await?;
    assert_eq!(patched.retention.max_age_seconds, Some(7200));
    let missing_stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_stream,
        Err(store::StoreError::NotFound(_))
    ));

    // Cache CRUD + patch + get + not found.
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;
    let conflict_cache = store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary Again".to_string(),
        })
        .await;
    assert!(matches!(
        conflict_cache,
        Err(store::StoreError::Conflict(_))
    ));
    let cache = store
        .get_cache(&controlplane::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await?;
    assert_eq!(cache.cache, "primary");
    let patched = store
        .patch_cache(
            &controlplane::model::CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
            },
            CachePatchRequest {
                display_name: Some("Primary Updated".to_string()),
            },
        )
        .await?;
    assert_eq!(patched.display_name, "Primary Updated");
    let missing_cache = store
        .get_cache(&controlplane::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(missing_cache, Err(store::StoreError::NotFound(_))));

    // Change feeds + snapshots.
    let tenant_snapshot = store.tenant_snapshot().await?;
    assert_eq!(tenant_snapshot.items.len(), 1);
    let tenant_changes = store.tenant_changes(0).await?;
    assert!(!tenant_changes.items.is_empty());
    let ns_snapshot = store.namespace_snapshot().await?;
    assert_eq!(ns_snapshot.items.len(), 1);
    let stream_snapshot = store.stream_snapshot().await?;
    assert_eq!(stream_snapshot.items.len(), 1);
    let cache_snapshot = store.cache_snapshot().await?;
    assert_eq!(cache_snapshot.items.len(), 1);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());

    // Auth tables.
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: ClaimMappings {
                    subject_claim: "sub".to_string(),
                    groups_claim: Some("groups".to_string()),
                },
            },
        )
        .await?;
    let issuers = store.list_idp_issuers("t1").await?;
    assert_eq!(issuers.len(), 1);
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
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);
    store
        .delete_idp_issuer("t1", "https://issuer.example.com")
        .await?;
    assert!(store.list_idp_issuers("t1").await?.is_empty());

    // Signing keys + bootstrapped flag.
    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys.clone()).await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    assert!(!store.tenant_auth_is_bootstrapped("t1").await?);
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Cascade delete namespace -> stream/cache deletions in change feed.
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
        })
        .await?;
    let stream_changes = store.stream_changes(0).await?;
    assert!(
        stream_changes
            .items
            .iter()
            .any(|c| { matches!(c.op, controlplane::model::StreamChangeOp::Deleted) })
    );
    let cache_changes = store.cache_changes(0).await?;
    assert!(
        cache_changes
            .items
            .iter()
            .any(|c| { matches!(c.op, controlplane::model::CacheChangeOp::Deleted) })
    );

    store.delete_tenant("t1").await?;
    let missing_delete = store.delete_tenant("t1").await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_core_crud_and_auth done");
    Ok(())
}

#[tokio::test(start_paused = true)]
#[serial]
async fn pg_store_connect_and_retention_task_ticks() -> Result<()> {
    let Some(fixture) = pg_store_with_config(StoreConfig {
        changes_limit: 25,
        change_retention_max_rows: Some(1),
    })
    .await?
    else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_connect_and_retention_task_ticks begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    // Advance time to ensure the retention task executes at least one tick.
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;

    let changes = store.tenant_changes(0).await?;
    assert!(
        changes.items.len() <= 1,
        "retention should trim to max_rows"
    );

    eprintln!("pg-tests: pg_store_connect_and_retention_task_ticks done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_connect_without_retention() -> Result<()> {
    let Some(fixture) = pg_store_with_config(StoreConfig {
        changes_limit: 25,
        change_retention_max_rows: None,
    })
    .await?
    else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_connect_without_retention begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 1);

    eprintln!("pg-tests: pg_store_connect_without_retention done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_additional_paths() -> Result<()> {
    let Some(fixture) = pg_store(Some(50)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_additional_paths begin");

    store.health_check().await?;
    assert!(store.is_durable());
    assert_eq!(store.backend_name(), "postgres");

    assert!(!store.tenant_exists("t1").await?);
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    assert!(store.tenant_exists("t1").await?);

    let ns_key = NamespaceKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
    };
    assert!(!store.namespace_exists(&ns_key).await?);
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert!(store.namespace_exists(&ns_key).await?);

    assert!(store.list_streams("t1", "default").await?.is_empty());
    assert!(store.list_caches("t1", "default").await?.is_empty());

    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
        })
        .await?;
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;
    assert_eq!(store.list_streams("t1", "default").await?.len(), 1);
    assert_eq!(store.list_caches("t1", "default").await?.len(), 1);

    let missing_delete = store
        .delete_cache(&controlplane::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));

    store
        .delete_cache(&controlplane::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await?;
    assert!(store.list_caches("t1", "default").await?.is_empty());

    let missing_keys = store.get_tenant_signing_keys("t1").await;
    assert!(matches!(missing_keys, Err(store::StoreError::NotFound(_))));
    let keys = store.ensure_signing_key_current("t1").await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    let cached = store.ensure_signing_key_current("t1").await?;
    assert_eq!(cached.current.kid, keys.current.kid);

    let issuer = IdpIssuerConfig {
        issuer: "https://issuer.example.com".to_string(),
        audiences: vec!["felix-controlplane".to_string()],
        discovery_url: Some("https://issuer.example.com/.well-known".to_string()),
        jwks_url: Some("https://issuer.example.com/jwks".to_string()),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: Some("groups".to_string()),
        },
    };
    store.upsert_idp_issuer("t1", issuer).await?;
    let updated = IdpIssuerConfig {
        issuer: "https://issuer.example.com".to_string(),
        audiences: vec!["felix-api".to_string()],
        discovery_url: None,
        jwks_url: Some("https://issuer.example.com/jwks".to_string()),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: None,
        },
    };
    store.upsert_idp_issuer("t1", updated).await?;
    let issuers = store.list_idp_issuers("t1").await?;
    assert_eq!(issuers.len(), 1);
    assert_eq!(issuers[0].audiences, vec!["felix-api".to_string()]);
    assert!(issuers[0].claim_mappings.groups_claim.is_none());

    let policy = PolicyRule {
        subject: "role:admin".to_string(),
        object: "tenant:*".to_string(),
        action: "tenant.admin".to_string(),
    };
    let grouping = GroupingRule {
        user: "p:admin".to_string(),
        role: "role:admin".to_string(),
    };
    store
        .seed_rbac_policies_and_groupings("t1", vec![policy.clone()], vec![grouping.clone()])
        .await?;
    store
        .seed_rbac_policies_and_groupings("t1", vec![policy], vec![grouping])
        .await?;
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);

    let missing_bootstrap = store.set_tenant_auth_bootstrapped("missing", true).await;
    assert!(matches!(
        missing_bootstrap,
        Err(store::StoreError::NotFound(_))
    ));
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Insert a bogus change op to exercise the fallback branch in change parsing.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    sqlx::query("INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)")
        .bind("Bogus")
        .bind("t1")
        .bind(Option::<serde_json::Value>::None)
        .execute(&pool)
        .await?;
    let bogus = store.tenant_changes(0).await?;
    assert!(
        bogus
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::TenantChangeOp::Deleted) }),
        "bogus op should map to Deleted"
    );

    eprintln!("pg-tests: pg_store_additional_paths done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_list_and_change_roundtrip() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_list_and_change_roundtrip begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 2);

    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            display_name: "Namespace One".to_string(),
        })
        .await?;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns2".to_string(),
            display_name: "Namespace Two".to_string(),
        })
        .await?;
    let namespaces = store.list_namespaces("t1").await?;
    assert_eq!(namespaces.len(), 2);

    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 2,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: Some(1024),
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
        })
        .await?;
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "events".to_string(),
            kind: StreamKind::Queue,
            shards: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Quorum,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: false,
        })
        .await?;
    let streams = store.list_streams("t1", "ns1").await?;
    assert_eq!(streams.len(), 2);

    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            cache: "secondary".to_string(),
            display_name: "Secondary".to_string(),
        })
        .await?;
    let caches = store.list_caches("t1", "ns1").await?;
    assert_eq!(caches.len(), 2);

    let tenant_changes = store.tenant_changes(0).await?;
    assert!(tenant_changes.items.len() >= 2);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(ns_changes.items.len() >= 2);
    let stream_changes = store.stream_changes(0).await?;
    assert!(stream_changes.items.len() >= 2);
    let cache_changes = store.cache_changes(0).await?;
    assert!(cache_changes.items.len() >= 2);

    eprintln!("pg-tests: pg_store_list_and_change_roundtrip done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_not_found_and_noop_paths() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_not_found_and_noop_paths begin");

    // Empty snapshots/changes should return empty items with next_seq 0 for all entities.
    let empty_tenants = store.tenant_snapshot().await?;
    assert!(empty_tenants.items.is_empty());
    assert_eq!(empty_tenants.next_seq, 0);
    let empty_changes = store.tenant_changes(42).await?;
    assert!(empty_changes.items.is_empty());
    assert_eq!(empty_changes.next_seq, 0);
    let empty_namespaces = store.namespace_snapshot().await?;
    assert!(empty_namespaces.items.is_empty());
    assert_eq!(empty_namespaces.next_seq, 0);
    let empty_ns_changes = store.namespace_changes(0).await?;
    assert!(empty_ns_changes.items.is_empty());
    assert_eq!(empty_ns_changes.next_seq, 0);
    let empty_streams = store.stream_snapshot().await?;
    assert!(empty_streams.items.is_empty());
    assert_eq!(empty_streams.next_seq, 0);
    let empty_stream_changes = store.stream_changes(0).await?;
    assert!(empty_stream_changes.items.is_empty());
    assert_eq!(empty_stream_changes.next_seq, 0);
    let empty_caches = store.cache_snapshot().await?;
    assert!(empty_caches.items.is_empty());
    assert_eq!(empty_caches.next_seq, 0);
    let empty_cache_changes = store.cache_changes(0).await?;
    assert!(empty_cache_changes.items.is_empty());
    assert_eq!(empty_cache_changes.next_seq, 0);

    // Auth lookups against an empty database should return empty collections or defaults.
    assert!(store.list_idp_issuers("missing").await?.is_empty());
    assert!(store.list_rbac_policies("missing").await?.is_empty());
    assert!(store.list_rbac_groupings("missing").await?.is_empty());
    assert!(!store.tenant_auth_is_bootstrapped("missing").await?);

    // Namespace creation should fail if tenant is missing.
    let missing_ns = store
        .create_namespace(Namespace {
            tenant_id: "missing".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await;
    assert!(matches!(missing_ns, Err(store::StoreError::NotFound(_))));

    // Stream/cache creation should fail if namespace is missing.
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let missing_get_stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_get_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_get_cache = store
        .get_cache(&controlplane::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_get_cache,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_stream = store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
        })
        .await;
    assert!(matches!(
        missing_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_cache = store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await;
    assert!(matches!(missing_cache, Err(store::StoreError::NotFound(_))));

    // Delete namespace should fail for missing namespace.
    let missing_delete = store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_delete_stream = store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete_stream,
        Err(store::StoreError::NotFound(_))
    ));

    // Patch operations should fail for missing records.
    let missing_patch_stream = store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            StreamPatchRequest {
                retention: None,
                consistency: None,
                delivery: None,
                durable: None,
            },
        )
        .await;
    assert!(matches!(
        missing_patch_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_patch_cache = store
        .patch_cache(
            &controlplane::model::CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "missing".to_string(),
            },
            CachePatchRequest { display_name: None },
        )
        .await;
    assert!(matches!(
        missing_patch_cache,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_not_found_and_noop_paths done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_delete_tenant_with_dependents() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_delete_tenant_with_dependents begin");

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
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;

    store.delete_tenant("t1").await?;

    assert!(store.list_tenants().await?.is_empty());
    assert!(store.list_namespaces("t1").await?.is_empty());
    assert!(store.list_streams("t1", "default").await?.is_empty());
    assert!(store.list_caches("t1", "default").await?.is_empty());

    let ns_changes = store.namespace_changes(0).await?;
    assert!(
        ns_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::NamespaceChangeOp::Deleted) })
    );
    let stream_changes = store.stream_changes(0).await?;
    assert!(
        stream_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::StreamChangeOp::Deleted) })
    );
    let cache_changes = store.cache_changes(0).await?;
    assert!(
        cache_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::CacheChangeOp::Deleted) })
    );

    eprintln!("pg-tests: pg_store_delete_tenant_with_dependents done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_signing_keys_requires_current() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_signing_keys_requires_current begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    sqlx::query(
        "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("t1")
    .bind("k1")
    .bind("RS256")
    .bind("priv")
    .bind("pub")
    .bind("previous")
    .execute(&pool)
    .await?;

    let missing_current = store.get_tenant_signing_keys("t1").await;
    assert!(matches!(
        missing_current,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_signing_keys_requires_current done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_full_surface_area() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_full_surface_area begin");

    // Tenant CRUD + snapshots/changes.
    assert!(store.list_tenants().await?.is_empty());
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let tenant_snapshot = store.tenant_snapshot().await?;
    assert_eq!(tenant_snapshot.items.len(), 1);
    let tenant_changes = store.tenant_changes(tenant_snapshot.next_seq).await?;
    assert!(tenant_changes.items.is_empty());

    // Namespace CRUD.
    let ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert_eq!(ns.namespace, "default");
    let ns_snapshot = store.namespace_snapshot().await?;
    assert_eq!(ns_snapshot.items.len(), 1);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());

    // Stream CRUD + list + snapshot/changes.
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
    assert_eq!(store.list_streams("t1", "default").await?.len(), 1);
    let stream_snapshot = store.stream_snapshot().await?;
    assert_eq!(stream_snapshot.items.len(), 1);
    let stream_changes = store.stream_changes(0).await?;
    assert!(!stream_changes.items.is_empty());

    // Cache CRUD + list + snapshot/changes.
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await?;
    assert_eq!(store.list_caches("t1", "default").await?.len(), 1);
    let cache_snapshot = store.cache_snapshot().await?;
    assert_eq!(cache_snapshot.items.len(), 1);
    let cache_changes = store.cache_changes(0).await?;
    assert!(!cache_changes.items.is_empty());

    // Auth store methods.
    assert!(store.list_idp_issuers("t1").await?.is_empty());
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: ClaimMappings::default(),
            },
        )
        .await?;
    assert_eq!(store.list_idp_issuers("t1").await?.len(), 1);
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
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);

    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys.clone()).await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    assert!(!store.tenant_auth_is_bootstrapped("t1").await?);
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Exercise parsing error paths via direct inserts.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    sqlx::query(
        "INSERT INTO streams (tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind("t1")
    .bind("default")
    .bind("bad")
    .bind("BogusKind")
    .bind(1i32)
    .bind(Option::<i64>::None)
    .bind(Option::<i64>::None)
    .bind("BogusConsistency")
    .bind("BogusDelivery")
    .bind(false)
    .execute(&pool)
    .await?;
    let bad_key = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "bad".to_string(),
    };
    let parse_error = store.get_stream(&bad_key);
    assert!(parse_error.await.is_err());

    // Invalid signing key algorithm to cover parse_algorithm error path.
    sqlx::query(
        "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("t1")
    .bind("bad")
    .bind("BogusAlg")
    .bind("priv")
    .bind("pub")
    .bind("current")
    .execute(&pool)
    .await?;
    let bad_alg = store.get_tenant_signing_keys("t1").await;
    assert!(bad_alg.is_err());

    eprintln!("pg-tests: pg_store_full_surface_area done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_change_and_auth_parsing_errors() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_change_and_auth_parsing_errors begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;

    // Insert invalid change ops to exercise "default to Deleted" branches.
    sqlx::query(
        "INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let ns_changes = store.namespace_changes(0).await?;
    assert!(
        ns_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::NamespaceChangeOp::Deleted) })
    );

    sqlx::query(
        "INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind("s1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let stream_changes = store.stream_changes(0).await?;
    assert!(
        stream_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::StreamChangeOp::Deleted) })
    );

    sqlx::query(
        "INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind("c1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let cache_changes = store.cache_changes(0).await?;
    assert!(
        cache_changes
            .items
            .iter()
            .any(|item| { matches!(item.op, controlplane::model::CacheChangeOp::Deleted) })
    );

    // Invalid audiences JSON should error when listing IdP issuers.
    sqlx::query(
        "INSERT INTO idp_issuers (tenant_id, issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind("t1")
    .bind("https://issuer.example.com")
    .bind(serde_json::json!("not-an-array"))
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind("sub")
    .bind(Option::<String>::None)
    .execute(&pool)
    .await?;
    let issuers = store.list_idp_issuers("t1").await;
    assert!(issuers.is_err());

    eprintln!("pg-tests: pg_store_change_and_auth_parsing_errors done");
    Ok(())
}

#[tokio::test(start_paused = true)]
#[serial]
async fn pg_change_retention_trims_changes() -> Result<()> {
    let Some(fixture) = pg_store(Some(1)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_change_retention_trims_changes begin");
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;

    let changes = store.tenant_changes(0).await?;
    assert!(changes.items.len() <= 1);
    eprintln!(
        "pg-tests: pg_change_retention_trims_changes retained={}",
        changes.items.len()
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_changes_monotonic_and_delete_not_found() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_changes_monotonic_and_delete_not_found begin");
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
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await?;
    let changes = store.stream_changes(0).await?;
    let seqs: Vec<u64> = changes.items.iter().map(|c| c.seq).collect();
    assert!(seqs.windows(2).all(|w| w[1] > w[0]));
    if let Some(last) = seqs.last() {
        assert_eq!(changes.next_seq, last + 1);
    }
    let missing = store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(missing, Err(store::StoreError::NotFound(_))));
    eprintln!("pg-tests: pg_changes_monotonic_and_delete_not_found done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_bootstrap_initialize_and_jwks_includes_previous_keys() -> Result<()> {
    let Some(fixture) = pg_store(Some(100)).await? else {
        return Ok(());
    };
    let store = fixture.store.clone();
    eprintln!("pg-tests: pg_bootstrap_initialize_and_jwks_includes_previous_keys begin");
    let state = app::AppState {
        region: controlplane::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: controlplane::api::types::FeatureFlags {
            durable_storage: true,
            tiered_storage: false,
            bridges: false,
        },
        store: store.clone(),
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_token: Some("token".to_string()),
    };
    let bootstrap_app = app::build_bootstrap_router(state.clone());
    let body = json!({
        "display_name": "Tenant One",
        "idp_issuers": [{
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/jwks",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }],
        "initial_admin_principals": ["p:admin"],
        "policies": [],
        "groupings": []
    });
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "token")
        .body(Body::from(body.to_string()))?;
    let response = bootstrap_app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);
    assert_eq!(store.list_idp_issuers("t1").await?.len(), 1);
    assert!(!store.list_rbac_policies("t1").await?.is_empty());
    assert!(!store.list_rbac_groupings("t1").await?.is_empty());

    let mut keys = generate_signing_keys()?;
    let previous = generate_signing_keys()?;
    keys.previous.push(previous.current.clone());
    store.set_tenant_signing_keys("t1", keys.clone()).await?;

    let app = app::build_router(state);
    let jwks_request = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/.well-known/jwks.json")
        .body(Body::empty())?;
    let jwks_response = app.oneshot(jwks_request).await?;
    assert_eq!(jwks_response.status(), StatusCode::OK);
    let response_json = read_json(jwks_response).await;
    let keys_list = response_json["keys"].as_array().expect("jwks keys");
    assert_eq!(keys_list.len(), 2);
    eprintln!(
        "pg-tests: jwks contains {} keys (current + previous)",
        keys_list.len()
    );
    Ok(())
}
