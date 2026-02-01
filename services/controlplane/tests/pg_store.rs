#![cfg(feature = "pg-tests")]

use controlplane::config;
use controlplane::model::{
    Cache, CacheChangeOp, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceChangeOp,
    NamespaceKey, RetentionPolicy, Stream, StreamChangeOp, StreamKey, StreamKind,
    StreamPatchRequest, Tenant,
};
use controlplane::store::{ControlPlaneStore, StoreConfig};
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static PG_SCHEMA: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

static MIGRATOR: Migrator = sqlx::migrate!();

async fn test_schema_name() -> String {
    PG_SCHEMA
        .get_or_init(|| async {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("felix_test_{}_{}", std::process::id(), nanos)
        })
        .await
        .clone()
}

fn url_with_schema(base_url: &str, schema: &str) -> String {
    let encoded = format!("-csearch_path%3D{}", schema);
    if base_url.contains('?') {
        format!("{base_url}&options={encoded}")
    } else {
        format!("{base_url}?options={encoded}")
    }
}

async fn ensure_schema(base_url: &str) -> Result<String, sqlx::Error> {
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

async fn reset_postgres(url: &str, schema: &str) -> Result<(), sqlx::Error> {
    let pool = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect(url),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err(sqlx::Error::PoolTimedOut),
    };
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{schema}.tenant_changes"))
        .fetch_one(&pool)
        .await?;
    if exists.is_none() {
        pool.close().await;
        return Ok(());
    }
    let schema_ident = format!(r#""{}""#, schema);
    let truncate = format!(
        "TRUNCATE {schema_ident}.tenant_changes, {schema_ident}.namespace_changes, \
         {schema_ident}.stream_changes, {schema_ident}.cache_changes, {schema_ident}.streams, \
         {schema_ident}.caches, {schema_ident}.namespaces, {schema_ident}.tenants RESTART IDENTITY",
    );
    sqlx::query(&truncate).execute(&pool).await.map(|_| ())
}

async fn pg_store() -> Option<Arc<controlplane::store::postgres::PostgresStore>> {
    let base_url = match std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping pg-tests: set FELIX_CONTROLPLANE_POSTGRES_URL or DATABASE_URL");
            return None;
        }
    };
    let schema = match ensure_schema(&base_url).await {
        Ok(schema) => schema,
        Err(err) => {
            eprintln!("skipping pg-tests: cannot create schema: {err}");
            return None;
        }
    };
    let url = url_with_schema(&base_url, &schema);
    if let Err(err) = run_migrations_once(&url).await {
        eprintln!("skipping pg-tests: cannot run migrations: {err}");
        return None;
    }
    if let Err(err) = reset_postgres(&url, &schema).await {
        eprintln!("skipping pg-tests: cannot connect to postgres: {err}");
        return None;
    }
    let pg_cfg = config::PostgresConfig {
        url,
        max_connections: 5,
        connect_timeout_ms: 5_000,
        acquire_timeout_ms: 5_000,
    };
    let store = match controlplane::store::postgres::PostgresStore::connect_without_migrations(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await
    {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return None;
        }
    };
    Some(store)
}

#[tokio::test]
async fn pg_stream_sequences_monotonic() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    let mut stream = Stream {
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
    };
    store
        .create_stream(stream.clone())
        .await
        .expect("create stream");

    stream.retention.max_age_seconds = Some(7200);
    store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            },
            StreamPatchRequest {
                retention: Some(stream.retention.clone()),
                consistency: None,
                delivery: None,
                durable: None,
            },
        )
        .await
        .expect("patch stream");

    store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("delete stream");

    let changes = store.stream_changes(0).await.expect("stream changes");
    let seqs: Vec<u64> = changes.items.iter().map(|c| c.seq).collect();
    assert!(seqs.windows(2).all(|w| w[1] > w[0]));
    if let Some(last) = seqs.last() {
        assert_eq!(changes.next_seq, last + 1);
    }
    let snapshot = store.stream_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.next_seq, changes.next_seq);
}

#[tokio::test]
async fn pg_delete_namespace_emits_cascades() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            display_name: "NS1".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
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
        .await
        .expect("stream");
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
        })
        .await
        .expect("cache");

    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
        })
        .await
        .expect("delete namespace");

    let stream_changes = store.stream_changes(0).await.expect("stream changes");
    assert!(
        stream_changes
            .items
            .iter()
            .any(|c| matches!(c.op, StreamChangeOp::Deleted))
    );

    let cache_changes = store.cache_changes(0).await.expect("cache changes");
    assert!(
        cache_changes
            .items
            .iter()
            .any(|c| matches!(c.op, CacheChangeOp::Deleted))
    );

    let ns_changes = store.namespace_changes(0).await.expect("ns changes");
    assert!(
        ns_changes
            .items
            .iter()
            .any(|c| matches!(c.op, NamespaceChangeOp::Deleted))
    );
}
