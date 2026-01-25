//! Broker-side control-plane sync client.
//!
//! This module runs **inside the broker service** and keeps the broker's local registries
//! (tenants / namespaces / streams / caches) aligned with the control-plane.
//!
//! Design goals
//! - Keep the **hot data path** (publish / subscribe / cache ops) free of control-plane I/O.
//! - Provide **eventual consistency**: brokers converge to the control-plane state over time.
//! - Allow a broker to cold-start with a full snapshot, then poll incremental change feeds.
//!
//! What this code assumes about the control-plane HTTP API
//! - For each resource type there is a `snapshot` endpoint that returns the full current set
//!   plus a monotonically increasing `next_seq` cursor.
//! - For each resource type there is a `changes?since=<seq>` endpoint that returns all changes
//!   after `since`, plus the new `next_seq` cursor.
//! - `next_seq` is **monotonic** per resource type (not necessarily contiguous).
//! - Change feeds are allowed to be empty; the broker will simply advance `next_seq`.
//!
//! Failure mode philosophy
//! - Snapshot/changes fetch failures are **non-fatal**: we log a warning and keep going.
//! - Local application failures (e.g., broker rejects a registration) are treated as real errors
//!   and bubble up, because they indicate an invariant mismatch or corrupted input.
//!
//! NOTE: This file is a *client* of the control-plane. Persisting control-plane data
//! (e.g., in Postgres) is implemented on the **control-plane service**, not here.
use anyhow::{Context, Result};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Per-resource cursors into the control-plane change feeds.
///
/// Each resource type (tenants, namespaces, caches, streams) has an independent cursor.
/// A value of `0` is treated as "unseeded" and triggers an initial snapshot fetch.
///
/// Why separate cursors?
/// - Each feed can advance independently.
/// - It keeps payload sizes small and avoids cross-resource coupling.
///
/// NOTE: These cursors are **in-memory** in the broker process. On broker restart,
/// we re-seed from snapshots (safe but more expensive than resuming from a stored cursor).
#[derive(Debug, Clone, Copy)]
struct SyncState {
    /// Next sequence cursor for the tenants change feed.
    next_tenant_seq: u64,
    /// Next sequence cursor for the namespaces change feed.
    next_namespace_seq: u64,
    /// Next sequence cursor for the caches change feed.
    next_cache_seq: u64,
    /// Next sequence cursor for the streams change feed.
    next_stream_seq: u64,
}

impl SyncState {
    fn new() -> Self {
        Self {
            next_tenant_seq: 0,
            next_namespace_seq: 0,
            next_cache_seq: 0,
            next_stream_seq: 0,
        }
    }
}

/// Full snapshot response for streams; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
struct StreamSnapshotResponse {
    items: Vec<Stream>,
    next_seq: u64,
}

/// Full snapshot response for caches; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
struct CacheSnapshotResponse {
    items: Vec<Cache>,
    next_seq: u64,
}

/// Full snapshot response for tenants; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
struct TenantSnapshotResponse {
    items: Vec<Tenant>,
    next_seq: u64,
}

/// Full snapshot response for namespaces; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
struct NamespaceSnapshotResponse {
    items: Vec<Namespace>,
    next_seq: u64,
}

/// Incremental change feed response for streams; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
struct StreamChangesResponse {
    items: Vec<StreamChange>,
    next_seq: u64,
}

/// Incremental change feed response for caches; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
struct CacheChangesResponse {
    items: Vec<CacheChange>,
    next_seq: u64,
}

/// Incremental change feed response for tenants; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
struct TenantChangesResponse {
    items: Vec<TenantChange>,
    next_seq: u64,
}

/// Incremental change feed response for namespaces; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
struct NamespaceChangesResponse {
    items: Vec<NamespaceChange>,
    next_seq: u64,
}

/// Represents a single change-feed event for a tenant.
/// - `op` is the operation (Created/Deleted).
/// - `tenant_id` identifies the tenant.
/// - `tenant` is present for Created.
#[derive(Debug, Deserialize, Serialize)]
struct TenantChange {
    op: TenantChangeOp,
    tenant_id: String,
    tenant: Option<Tenant>,
}

/// Represents a single change-feed event for a namespace.
/// - `op` is the operation (Created/Deleted).
/// - `key` identifies the namespace (tenant_id + namespace).
/// - `namespace` is present for Created.
#[derive(Debug, Deserialize, Serialize)]
struct NamespaceChange {
    op: NamespaceChangeOp,
    key: NamespaceKey,
    namespace: Option<Namespace>,
}

/// Represents a single change-feed event for a stream.
/// - `op` is the operation (Created/Updated/Deleted).
/// - `key` identifies the stream (tenant_id + namespace + stream).
/// - `stream` is present for Created/Updated.
#[derive(Debug, Deserialize, Serialize)]
struct StreamChange {
    op: StreamChangeOp,
    key: StreamKey,
    stream: Option<Stream>,
}

/// Represents a single change-feed event for a cache.
/// - `op` is the operation (Created/Updated/Deleted).
/// - `key` identifies the cache (tenant_id + namespace + cache).
/// - `cache` is present for Created/Updated.
#[derive(Debug, Deserialize, Serialize)]
struct CacheChange {
    op: CacheChangeOp,
    key: CacheKey,
    cache: Option<Cache>,
}

/// Represents a tenant as registered in the broker registry.
/// Maps to the broker's tenant registry.
#[derive(Debug, Deserialize, Serialize)]
struct Tenant {
    tenant_id: String,
}

/// Represents a namespace as registered in the broker registry.
/// Maps to the broker's namespace registry.
#[derive(Debug, Deserialize, Serialize)]
struct Namespace {
    tenant_id: String,
    namespace: String,
}

/// Identifies a stream (tenant_id, namespace, stream).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

/// Identifies a cache (tenant_id, namespace, cache).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
}

/// Identifies a namespace (tenant_id, namespace).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

/// Change operation for tenants.
///
/// Serialized in camelCase for wire compatibility.
/// Only Created and Deleted are valid (no update).
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum TenantChangeOp {
    Created,
    Deleted,
}

/// Change operation for namespaces.
///
/// Serialized in camelCase for wire compatibility.
/// Only Created and Deleted are valid (no update).
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum NamespaceChangeOp {
    Created,
    Deleted,
}

/// Change operation for streams.
///
/// Serialized in camelCase for wire compatibility.
/// Streams may be Created, Updated, or Deleted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

/// Change operation for caches.
///
/// Serialized in camelCase for wire compatibility.
/// Caches may be Created, Updated, or Deleted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

/// Represents a stream as registered in the broker registry.
/// Maps to the broker's stream registry.
#[derive(Debug, Deserialize, Serialize)]
struct Stream {
    tenant_id: String,
    namespace: String,
    stream: String,
    shards: u32,
    durable: bool,
}

/// Represents a cache as registered in the broker registry.
/// Maps to the broker's cache registry.
#[derive(Debug, Deserialize, Serialize)]
struct Cache {
    tenant_id: String,
    namespace: String,
    cache: String,
}

/// Starts the control-plane sync loop as a background task.
///
/// This function runs forever, periodically fetching control-plane snapshots and change feeds,
/// and applying them to the broker's local registries.
///
/// - It is safe to run as a background task (spawns no additional tasks).
/// - It intentionally sleeps for `interval` between change feed polls.
/// - It only returns on unrecoverable local errors (e.g., broker rejects a registration),
///   which indicate a true invariant violation or corrupted input.
pub async fn start_sync(broker: Arc<Broker>, base_url: String, interval: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    // Sequence cursors for each change feed; 0 means "not yet seeded".
    let mut state = SyncState::new();
    loop {
        state = sync_once(&broker, &client, &base_url, state).await?;
        // Polling interval between change feed reads.
        tokio::time::sleep(interval).await;
    }
}

/// Performs a single sync iteration: fetches and applies any new control-plane state.
///
/// Algorithm:
/// - On cold start (`next_seq == 0`), fetch full snapshots in dependency order:
///   1. Tenants
///   2. Namespaces
///   3. Caches
///   4. Streams
///      This ensures that hierarchical dependencies (tenants -> namespaces -> caches/streams)
///      are satisfied before applying more granular resources.
///
/// - After seeding, fetch incremental change feeds in the same order.
///   - Each fetch is best-effort: errors are logged and the corresponding cursor is not advanced.
///   - On success, the cursor is advanced to the new `next_seq`.
///   - Deletion events use `remove_*` and ignore missing entries.
///
/// - The ordering ensures that removal of higher-level resources (e.g., tenant deletion)
///   occurs before dependent resources, preventing orphaned objects.
async fn sync_once(
    broker: &Arc<Broker>,
    client: &reqwest::Client,
    base_url: &str,
    mut state: SyncState,
) -> Result<SyncState> {
    let log_as_debug = cfg!(test) || std::env::var_os("RUST_TEST_THREADS").is_some();
    // === Cold start snapshot seeding ===
    // 1. Seed tenants first.
    if state.next_tenant_seq == 0 {
        match fetch_tenant_snapshot(client, base_url).await {
            Ok(snapshot) => {
                for tenant in snapshot.items {
                    broker.register_tenant(tenant.tenant_id).await?;
                }
                state.next_tenant_seq = snapshot.next_seq;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane tenant snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane tenant snapshot failed");
                }
            }
        }
    }

    // 2. Seed namespaces after tenants.
    if state.next_namespace_seq == 0 {
        match fetch_namespace_snapshot(client, base_url).await {
            Ok(snapshot) => {
                for namespace in snapshot.items {
                    broker
                        .register_namespace(namespace.tenant_id, namespace.namespace)
                        .await?;
                }
                state.next_namespace_seq = snapshot.next_seq;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane namespace snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane namespace snapshot failed");
                }
            }
        }
    }

    // 3. Seed caches after namespaces.
    if state.next_cache_seq == 0 {
        match fetch_cache_snapshot(client, base_url).await {
            Ok(snapshot) => {
                for cache in snapshot.items {
                    broker
                        .register_cache(
                            cache.tenant_id,
                            cache.namespace,
                            cache.cache,
                            CacheMetadata,
                        )
                        .await?;
                }
                state.next_cache_seq = snapshot.next_seq;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane cache snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane cache snapshot failed");
                }
            }
        }
    }

    // 4. Seed streams after caches.
    // Note: fetch_snapshot is the streams snapshot (legacy naming).
    if state.next_stream_seq == 0 {
        match fetch_snapshot(client, base_url).await {
            Ok(snapshot) => {
                for stream in snapshot.items {
                    broker
                        .register_stream(
                            stream.tenant_id,
                            stream.namespace,
                            stream.stream,
                            StreamMetadata {
                                durable: stream.durable,
                                shards: stream.shards,
                            },
                        )
                        .await?;
                }
                state.next_stream_seq = snapshot.next_seq;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane snapshot failed");
                }
            }
        }
    }

    // === Incremental change feed application ===
    // 1. Apply tenant changes first (ensures proper revocation/creation).
    match fetch_tenant_changes(client, base_url, state.next_tenant_seq).await {
        Ok(changes) => {
            for change in changes.items {
                match change.op {
                    TenantChangeOp::Created => {
                        if let Some(tenant) = change.tenant {
                            broker.register_tenant(tenant.tenant_id).await?;
                        }
                    }
                    TenantChangeOp::Deleted => {
                        let _ = broker.remove_tenant(&change.tenant_id).await?;
                    }
                }
            }
            state.next_tenant_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane tenant change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane tenant change poll failed");
            }
        }
    }

    // 2. Apply namespace changes after tenants.
    match fetch_namespace_changes(client, base_url, state.next_namespace_seq).await {
        Ok(changes) => {
            for change in changes.items {
                match change.op {
                    NamespaceChangeOp::Created => {
                        if let Some(namespace) = change.namespace {
                            broker
                                .register_namespace(namespace.tenant_id, namespace.namespace)
                                .await?;
                        }
                    }
                    NamespaceChangeOp::Deleted => {
                        let _ = broker
                            .remove_namespace(&change.key.tenant_id, &change.key.namespace)
                            .await?;
                    }
                }
            }
            state.next_namespace_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane namespace change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane namespace change poll failed");
            }
        }
    }

    // 3. Apply cache changes after namespaces.
    //    (Caches depend on tenant/namespace existence.)
    match fetch_cache_changes(client, base_url, state.next_cache_seq).await {
        Ok(changes) => {
            for change in changes.items {
                match change.op {
                    CacheChangeOp::Created | CacheChangeOp::Updated => {
                        if let Some(cache) = change.cache {
                            broker
                                .register_cache(
                                    cache.tenant_id,
                                    cache.namespace,
                                    cache.cache,
                                    CacheMetadata,
                                )
                                .await?;
                        }
                    }
                    CacheChangeOp::Deleted => {
                        let _ = broker
                            .remove_cache(
                                &change.key.tenant_id,
                                &change.key.namespace,
                                &change.key.cache,
                            )
                            .await?;
                    }
                }
            }
            state.next_cache_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane cache change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane cache change poll failed");
            }
        }
    }

    // 4. Apply stream changes last so existence checks have up-to-date scopes.
    match fetch_changes(client, base_url, state.next_stream_seq).await {
        Ok(changes) => {
            for change in changes.items {
                match change.op {
                    StreamChangeOp::Created | StreamChangeOp::Updated => {
                        if let Some(stream) = change.stream {
                            broker
                                .register_stream(
                                    stream.tenant_id,
                                    stream.namespace,
                                    stream.stream,
                                    StreamMetadata {
                                        durable: stream.durable,
                                        shards: stream.shards,
                                    },
                                )
                                .await?;
                        }
                    }
                    StreamChangeOp::Deleted => {
                        let _ = broker
                            .remove_stream(
                                &change.key.tenant_id,
                                &change.key.namespace,
                                &change.key.stream,
                            )
                            .await?;
                    }
                }
            }
            state.next_stream_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane change poll failed");
            }
        }
    }
    Ok(state)
}

/// Fetches the full stream snapshot from `/v1/streams/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_snapshot(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<StreamSnapshotResponse> {
    let url = format!("{}/v1/streams/snapshot", base_url.trim_end_matches('/'));
    let response = client
        .get(url)
        .send()
        .await
        .context("snapshot request")?
        .error_for_status()
        .context("snapshot status")?;
    response.json().await.context("snapshot body")
}

/// Fetches the full cache snapshot from `/v1/caches/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_cache_snapshot(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<CacheSnapshotResponse> {
    let url = format!("{}/v1/caches/snapshot", base_url.trim_end_matches('/'));
    let response = client
        .get(url)
        .send()
        .await
        .context("cache snapshot request")?
        .error_for_status()
        .context("cache snapshot status")?;
    response.json().await.context("cache snapshot body")
}

/// Fetches the full tenant snapshot from `/v1/tenants/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_tenant_snapshot(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<TenantSnapshotResponse> {
    let url = format!("{}/v1/tenants/snapshot", base_url.trim_end_matches('/'));
    let response = client
        .get(url)
        .send()
        .await
        .context("tenant snapshot request")?
        .error_for_status()
        .context("tenant snapshot status")?;
    response.json().await.context("tenant snapshot body")
}

/// Fetches the full namespace snapshot from `/v1/namespaces/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_namespace_snapshot(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<NamespaceSnapshotResponse> {
    let url = format!("{}/v1/namespaces/snapshot", base_url.trim_end_matches('/'));
    let response = client
        .get(url)
        .send()
        .await
        .context("namespace snapshot request")?
        .error_for_status()
        .context("namespace snapshot status")?;
    response.json().await.context("namespace snapshot body")
}

/// Fetches the stream change feed from `/v1/streams/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_changes(
    client: &reqwest::Client,
    base_url: &str,
    since: u64,
) -> Result<StreamChangesResponse> {
    let url = format!(
        "{}/v1/streams/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = client
        .get(url)
        .send()
        .await
        .context("changes request")?
        .error_for_status()
        .context("changes status")?;
    response.json().await.context("changes body")
}

/// Fetches the cache change feed from `/v1/caches/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_cache_changes(
    client: &reqwest::Client,
    base_url: &str,
    since: u64,
) -> Result<CacheChangesResponse> {
    let url = format!(
        "{}/v1/caches/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = client
        .get(url)
        .send()
        .await
        .context("cache changes request")?
        .error_for_status()
        .context("cache changes status")?;
    response.json().await.context("cache changes body")
}

/// Fetches the tenant change feed from `/v1/tenants/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_tenant_changes(
    client: &reqwest::Client,
    base_url: &str,
    since: u64,
) -> Result<TenantChangesResponse> {
    let url = format!(
        "{}/v1/tenants/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = client
        .get(url)
        .send()
        .await
        .context("tenant changes request")?
        .error_for_status()
        .context("tenant changes status")?;
    response.json().await.context("tenant changes body")
}

/// Fetches the namespace change feed from `/v1/namespaces/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
async fn fetch_namespace_changes(
    client: &reqwest::Client,
    base_url: &str,
    since: u64,
) -> Result<NamespaceChangesResponse> {
    let url = format!(
        "{}/v1/namespaces/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = client
        .get(url)
        .send()
        .await
        .context("namespace changes request")?
        .error_for_status()
        .context("namespace changes status")?;
    response.json().await.context("namespace changes body")
}

#[cfg(test)]
mod tests {
    // Tests cover error tolerance (skipping on fetch errors), deletion propagation, and cursor advancement.
    use super::*;
    use crate::test_support::http_test::{
        build_test_client, spawn_axum_with_shutdown, wait_for_listen,
    };
    use axum::{Json, Router, http::StatusCode, routing::get};
    use felix_storage::EphemeralCache;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::TcpListener;

    async fn serve_router(
        router: Router,
    ) -> Result<(
        SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    )> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, handle) = spawn_axum_with_shutdown(listener, router);
        wait_for_listen(addr).await?;
        Ok((addr, shutdown_tx, handle))
    }

    fn error_router() -> Router {
        Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
    }

    #[tokio::test]
    async fn sync_once_logs_and_skips_on_errors() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
            let (addr, shutdown_tx, handle) = serve_router(error_router()).await?;
            let base_url = format!("http://{}", addr);
            let client = build_test_client()?;

            let state = sync_once(&broker, &client, &base_url, SyncState::new()).await?;
            assert_eq!(state.next_tenant_seq, 0);
            assert_eq!(state.next_namespace_seq, 0);
            assert_eq!(state.next_cache_seq, 0);
            assert_eq!(state.next_stream_seq, 0);
            assert!(!broker.namespace_exists("t1", "ns").await);
            assert!(!broker.stream_exists("t1", "ns", "s1").await);
            assert!(!broker.cache_exists("t1", "ns", "c1").await);

            let _ = shutdown_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("server shutdown");
            Ok(())
        })
        .await
        .expect("test timeout")
    }

    #[tokio::test]
    async fn sync_once_handles_tenant_deletion() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
            let router = Router::new()
                .route(
                    "/v1/tenants/snapshot",
                    get(|| async {
                        Json(TenantSnapshotResponse {
                            items: vec![Tenant {
                                tenant_id: "t1".to_string(),
                            }],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/namespaces/snapshot",
                    get(|| async {
                        Json(NamespaceSnapshotResponse {
                            items: vec![],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/caches/snapshot",
                    get(|| async {
                        Json(CacheSnapshotResponse {
                            items: vec![],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/streams/snapshot",
                    get(|| async {
                        Json(StreamSnapshotResponse {
                            items: vec![],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/tenants/changes",
                    get(|| async {
                        Json(TenantChangesResponse {
                            items: vec![TenantChange {
                                op: TenantChangeOp::Deleted,
                                tenant_id: "t1".to_string(),
                                tenant: None,
                            }],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/namespaces/changes",
                    get(|| async {
                        Json(NamespaceChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/caches/changes",
                    get(|| async {
                        Json(CacheChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/streams/changes",
                    get(|| async {
                        Json(StreamChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                );
            let (addr, shutdown_tx, handle) = serve_router(router).await?;
            let base_url = format!("http://{}", addr);
            let client = build_test_client()?;

            let state = sync_once(&broker, &client, &base_url, SyncState::new()).await?;
            assert_eq!(state.next_tenant_seq, 2);
            let err = broker.register_namespace("t1", "ns").await;
            assert!(err.is_err());

            let _ = shutdown_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("server shutdown");
            Ok(())
        })
        .await
        .expect("test timeout")
    }

    #[tokio::test]
    async fn sync_once_handles_namespace_deletion() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
            let router = Router::new()
                .route(
                    "/v1/tenants/snapshot",
                    get(|| async {
                        Json(TenantSnapshotResponse {
                            items: vec![Tenant {
                                tenant_id: "t1".to_string(),
                            }],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/namespaces/snapshot",
                    get(|| async {
                        Json(NamespaceSnapshotResponse {
                            items: vec![Namespace {
                                tenant_id: "t1".to_string(),
                                namespace: "ns".to_string(),
                            }],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/caches/snapshot",
                    get(|| async {
                        Json(CacheSnapshotResponse {
                            items: vec![],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/streams/snapshot",
                    get(|| async {
                        Json(StreamSnapshotResponse {
                            items: vec![],
                            next_seq: 1,
                        })
                    }),
                )
                .route(
                    "/v1/tenants/changes",
                    get(|| async {
                        Json(TenantChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/namespaces/changes",
                    get(|| async {
                        Json(NamespaceChangesResponse {
                            items: vec![NamespaceChange {
                                op: NamespaceChangeOp::Deleted,
                                key: NamespaceKey {
                                    tenant_id: "t1".to_string(),
                                    namespace: "ns".to_string(),
                                },
                                namespace: None,
                            }],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/caches/changes",
                    get(|| async {
                        Json(CacheChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                )
                .route(
                    "/v1/streams/changes",
                    get(|| async {
                        Json(StreamChangesResponse {
                            items: vec![],
                            next_seq: 2,
                        })
                    }),
                );
            let (addr, shutdown_tx, handle) = serve_router(router).await?;
            let base_url = format!("http://{}", addr);
            let client = build_test_client()?;

            let state = sync_once(&broker, &client, &base_url, SyncState::new()).await?;
            assert_eq!(state.next_namespace_seq, 2);
            assert!(!broker.namespace_exists("t1", "ns").await);

            let _ = shutdown_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("server shutdown");
            Ok(())
        })
        .await
        .expect("test timeout")
    }
}
