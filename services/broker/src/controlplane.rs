// Control plane sync loop for the broker.
// Fetches snapshots on cold start, then polls change feeds to keep local
// tenant/namespace/stream registries up to date without touching the hot path.
use anyhow::{Context, Result};
use felix_broker::{Broker, StreamMetadata};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct StreamSnapshotResponse {
    items: Vec<Stream>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct TenantSnapshotResponse {
    items: Vec<Tenant>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct NamespaceSnapshotResponse {
    items: Vec<Namespace>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct StreamChangesResponse {
    items: Vec<StreamChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct TenantChangesResponse {
    items: Vec<TenantChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct NamespaceChangesResponse {
    items: Vec<NamespaceChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct TenantChange {
    op: TenantChangeOp,
    tenant_id: String,
    tenant: Option<Tenant>,
}

#[derive(Debug, Deserialize)]
struct NamespaceChange {
    op: NamespaceChangeOp,
    key: NamespaceKey,
    namespace: Option<Namespace>,
}

#[derive(Debug, Deserialize)]
struct StreamChange {
    op: StreamChangeOp,
    key: StreamKey,
    stream: Option<Stream>,
}

#[derive(Debug, Deserialize)]
struct Tenant {
    tenant_id: String,
}

#[derive(Debug, Deserialize)]
struct Namespace {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

#[derive(Debug, Deserialize)]
struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum TenantChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum NamespaceChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Deserialize)]
struct Stream {
    tenant_id: String,
    namespace: String,
    stream: String,
    shards: u32,
    durable: bool,
}

pub async fn start_sync(broker: Arc<Broker>, base_url: String, interval: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let mut next_tenant_seq = 0u64;
    let mut next_namespace_seq = 0u64;
    let mut next_stream_seq = 0u64;
    loop {
        // Seed local caches on first run so the data plane can enforce existence checks.
        if next_tenant_seq == 0 {
            match fetch_tenant_snapshot(&client, &base_url).await {
                Ok(snapshot) => {
                    for tenant in snapshot.items {
                        broker.register_tenant(tenant.tenant_id).await?;
                    }
                    next_tenant_seq = snapshot.next_seq;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "control plane tenant snapshot failed");
                }
            }
        }

        if next_namespace_seq == 0 {
            match fetch_namespace_snapshot(&client, &base_url).await {
                Ok(snapshot) => {
                    for namespace in snapshot.items {
                        broker
                            .register_namespace(namespace.tenant_id, namespace.namespace)
                            .await?;
                    }
                    next_namespace_seq = snapshot.next_seq;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "control plane namespace snapshot failed");
                }
            }
        }

        if next_stream_seq == 0 {
            match fetch_snapshot(&client, &base_url).await {
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
                    next_stream_seq = snapshot.next_seq;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "control plane snapshot failed");
                }
            }
        }

        // Apply incremental tenant changes for revocation/creation.
        match fetch_tenant_changes(&client, &base_url, next_tenant_seq).await {
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
                next_tenant_seq = changes.next_seq;
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane tenant change poll failed");
            }
        }

        // Apply incremental namespace changes for revocation/creation.
        match fetch_namespace_changes(&client, &base_url, next_namespace_seq).await {
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
                next_namespace_seq = changes.next_seq;
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane namespace change poll failed");
            }
        }

        // Apply incremental stream changes last so existence checks have up-to-date scopes.
        match fetch_changes(&client, &base_url, next_stream_seq).await {
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
                next_stream_seq = changes.next_seq;
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane change poll failed");
            }
        }
        tokio::time::sleep(interval).await;
    }
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
