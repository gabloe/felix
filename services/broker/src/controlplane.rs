// Control plane sync loop for the broker.
// Fetches snapshots on cold start, then polls change feeds to keep local
// tenant/namespace/stream/cache registries up to date without touching the hot path.
use anyhow::{Context, Result};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
struct SyncState {
    next_tenant_seq: u64,
    next_namespace_seq: u64,
    next_cache_seq: u64,
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

#[derive(Debug, Deserialize, Serialize)]
struct StreamSnapshotResponse {
    items: Vec<Stream>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheSnapshotResponse {
    items: Vec<Cache>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TenantSnapshotResponse {
    items: Vec<Tenant>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct NamespaceSnapshotResponse {
    items: Vec<Namespace>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct StreamChangesResponse {
    items: Vec<StreamChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheChangesResponse {
    items: Vec<CacheChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TenantChangesResponse {
    items: Vec<TenantChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct NamespaceChangesResponse {
    items: Vec<NamespaceChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TenantChange {
    op: TenantChangeOp,
    tenant_id: String,
    tenant: Option<Tenant>,
}

#[derive(Debug, Deserialize, Serialize)]
struct NamespaceChange {
    op: NamespaceChangeOp,
    key: NamespaceKey,
    namespace: Option<Namespace>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StreamChange {
    op: StreamChangeOp,
    key: StreamKey,
    stream: Option<Stream>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheChange {
    op: CacheChangeOp,
    key: CacheKey,
    cache: Option<Cache>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Tenant {
    tenant_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Namespace {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum TenantChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum NamespaceChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Deserialize, Serialize)]
struct Stream {
    tenant_id: String,
    namespace: String,
    stream: String,
    shards: u32,
    durable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Cache {
    tenant_id: String,
    namespace: String,
    cache: String,
}

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

async fn sync_once(
    broker: &Arc<Broker>,
    client: &reqwest::Client,
    base_url: &str,
    mut state: SyncState,
) -> Result<SyncState> {
    // On cold start, fetch full snapshots once to seed local registries.
    // Seed local caches on first run so the data plane can enforce existence checks.
    if state.next_tenant_seq == 0 {
        match fetch_tenant_snapshot(client, base_url).await {
            Ok(snapshot) => {
                for tenant in snapshot.items {
                    broker.register_tenant(tenant.tenant_id).await?;
                }
                state.next_tenant_seq = snapshot.next_seq;
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane tenant snapshot failed");
            }
        }
    }

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
                tracing::warn!(error = %err, "control plane namespace snapshot failed");
            }
        }
    }

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
                tracing::warn!(error = %err, "control plane cache snapshot failed");
            }
        }
    }

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
                tracing::warn!(error = %err, "control plane snapshot failed");
            }
        }
    }

    // Apply incremental tenant changes for revocation/creation.
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
            tracing::warn!(error = %err, "control plane tenant change poll failed");
        }
    }

    // Apply incremental namespace changes for revocation/creation.
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
            tracing::warn!(error = %err, "control plane namespace change poll failed");
        }
    }

    // Cache updates can be applied once tenants/namespaces are current.
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
            tracing::warn!(error = %err, "control plane cache change poll failed");
        }
    }

    // Apply incremental stream changes last so existence checks have up-to-date scopes.
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
            tracing::warn!(error = %err, "control plane change poll failed");
        }
    }
    Ok(state)
}

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
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::get};
    use felix_storage::EphemeralCache;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    async fn serve_router(router: Router) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router.into_make_service()).await;
        });
        Ok((addr, handle))
    }

    fn success_router() -> Router {
        Router::new()
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![Stream {
                            tenant_id: "t1".to_string(),
                            namespace: "ns".to_string(),
                            stream: "s1".to_string(),
                            shards: 2,
                            durable: true,
                        }],
                        next_seq: 10,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![Cache {
                            tenant_id: "t1".to_string(),
                            namespace: "ns".to_string(),
                            cache: "c1".to_string(),
                        }],
                        next_seq: 11,
                    })
                }),
            )
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t1".to_string(),
                        }],
                        next_seq: 12,
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
                        next_seq: 13,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![
                            StreamChange {
                                op: StreamChangeOp::Created,
                                key: StreamKey {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    stream: "s2".to_string(),
                                },
                                stream: Some(Stream {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    stream: "s2".to_string(),
                                    shards: 1,
                                    durable: false,
                                }),
                            },
                            StreamChange {
                                op: StreamChangeOp::Updated,
                                key: StreamKey {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    stream: "s3".to_string(),
                                },
                                stream: Some(Stream {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    stream: "s3".to_string(),
                                    shards: 3,
                                    durable: true,
                                }),
                            },
                            StreamChange {
                                op: StreamChangeOp::Deleted,
                                key: StreamKey {
                                    tenant_id: "t1".to_string(),
                                    namespace: "ns".to_string(),
                                    stream: "s1".to_string(),
                                },
                                stream: None,
                            },
                        ],
                        next_seq: 20,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![
                            CacheChange {
                                op: CacheChangeOp::Created,
                                key: CacheKey {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    cache: "c2".to_string(),
                                },
                                cache: Some(Cache {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    cache: "c2".to_string(),
                                }),
                            },
                            CacheChange {
                                op: CacheChangeOp::Updated,
                                key: CacheKey {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    cache: "c3".to_string(),
                                },
                                cache: Some(Cache {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                    cache: "c3".to_string(),
                                }),
                            },
                            CacheChange {
                                op: CacheChangeOp::Deleted,
                                key: CacheKey {
                                    tenant_id: "t1".to_string(),
                                    namespace: "ns".to_string(),
                                    cache: "c1".to_string(),
                                },
                                cache: None,
                            },
                        ],
                        next_seq: 21,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![
                            TenantChange {
                                op: TenantChangeOp::Created,
                                tenant_id: "t2".to_string(),
                                tenant: Some(Tenant {
                                    tenant_id: "t2".to_string(),
                                }),
                            },
                            TenantChange {
                                op: TenantChangeOp::Deleted,
                                tenant_id: "t1".to_string(),
                                tenant: None,
                            },
                        ],
                        next_seq: 22,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![
                            NamespaceChange {
                                op: NamespaceChangeOp::Created,
                                key: NamespaceKey {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                },
                                namespace: Some(Namespace {
                                    tenant_id: "t2".to_string(),
                                    namespace: "ns2".to_string(),
                                }),
                            },
                            NamespaceChange {
                                op: NamespaceChangeOp::Deleted,
                                key: NamespaceKey {
                                    tenant_id: "t1".to_string(),
                                    namespace: "ns".to_string(),
                                },
                                namespace: None,
                            },
                        ],
                        next_seq: 23,
                    })
                }),
            )
    }

    fn error_router() -> Router {
        Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
    }

    #[tokio::test]
    async fn sync_once_applies_snapshots_and_changes() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (addr, handle) = serve_router(success_router()).await?;
        let base_url = format!("http://{}", addr);
        let client = reqwest::Client::new();

        let state = sync_once(&broker, &client, &base_url, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 22);
        assert_eq!(state.next_namespace_seq, 23);
        assert_eq!(state.next_cache_seq, 21);
        assert_eq!(state.next_stream_seq, 20);

        assert!(!broker.namespace_exists("t1", "ns").await);
        assert!(broker.namespace_exists("t2", "ns2").await);
        assert!(broker.cache_exists("t2", "ns2", "c2").await);
        assert!(broker.cache_exists("t2", "ns2", "c3").await);
        assert!(!broker.cache_exists("t1", "ns", "c1").await);
        assert!(broker.stream_exists("t2", "ns2", "s2").await);
        assert!(broker.stream_exists("t2", "ns2", "s3").await);
        assert!(!broker.stream_exists("t1", "ns", "s1").await);

        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn sync_once_logs_and_skips_on_errors() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (addr, handle) = serve_router(error_router()).await?;
        let base_url = format!("http://{}", addr);
        let client = reqwest::Client::new();

        let state = sync_once(&broker, &client, &base_url, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 0);
        assert_eq!(state.next_namespace_seq, 0);
        assert_eq!(state.next_cache_seq, 0);
        assert_eq!(state.next_stream_seq, 0);
        assert!(!broker.namespace_exists("t1", "ns").await);
        assert!(!broker.stream_exists("t1", "ns", "s1").await);
        assert!(!broker.cache_exists("t1", "ns", "c1").await);

        handle.abort();
        Ok(())
    }
}
