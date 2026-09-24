//! HTTP reads of the control plane's snapshots and change feeds.

use anyhow::{Context, Result};

use super::wire::*;

/// Fetches the full stream snapshot from `/v1/streams/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<StreamSnapshotResponse> {
    let url = format!("{}/v1/streams/snapshot", base_url.trim_end_matches('/'));
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("snapshot request")?
        .error_for_status()
        .context("snapshot status")?;
    response.json().await.context("snapshot body")
}

/// Fetches the full cache snapshot from `/v1/caches/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_cache_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<CacheSnapshotResponse> {
    let url = format!("{}/v1/caches/snapshot", base_url.trim_end_matches('/'));
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("cache snapshot request")?
        .error_for_status()
        .context("cache snapshot status")?;
    response.json().await.context("cache snapshot body")
}

/// Fetches the full tenant snapshot from `/v1/tenants/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_tenant_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<TenantSnapshotResponse> {
    let url = format!("{}/v1/tenants/snapshot", base_url.trim_end_matches('/'));
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("tenant snapshot request")?
        .error_for_status()
        .context("tenant snapshot status")?;
    response.json().await.context("tenant snapshot body")
}

/// Fetches the full namespace snapshot from `/v1/namespaces/snapshot`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_namespace_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<NamespaceSnapshotResponse> {
    let url = format!("{}/v1/namespaces/snapshot", base_url.trim_end_matches('/'));
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("namespace snapshot request")?
        .error_for_status()
        .context("namespace snapshot status")?;
    response.json().await.context("namespace snapshot body")
}

/// Fetches the stream change feed from `/v1/streams/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_changes(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    since: u64,
) -> Result<StreamChangesResponse> {
    let url = format!(
        "{}/v1/streams/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("changes request")?
        .error_for_status()
        .context("changes status")?;
    response.json().await.context("changes body")
}

/// Fetches the cache change feed from `/v1/caches/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_cache_changes(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    since: u64,
) -> Result<CacheChangesResponse> {
    let url = format!(
        "{}/v1/caches/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("cache changes request")?
        .error_for_status()
        .context("cache changes status")?;
    response.json().await.context("cache changes body")
}

/// Fetches the tenant change feed from `/v1/tenants/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_tenant_changes(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    since: u64,
) -> Result<TenantChangesResponse> {
    let url = format!(
        "{}/v1/tenants/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("tenant changes request")?
        .error_for_status()
        .context("tenant changes status")?;
    response.json().await.context("tenant changes body")
}

/// Fetches the namespace change feed from `/v1/namespaces/changes?since=<seq>`.
/// `base_url` is trimmed of trailing `/`. Non-2xx is treated as error.
pub(super) async fn fetch_namespace_changes(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    since: u64,
) -> Result<NamespaceChangesResponse> {
    let url = format!(
        "{}/v1/namespaces/changes?since={}",
        base_url.trim_end_matches('/'),
        since
    );
    let response = with_bearer(client.get(url), bearer)
        .send()
        .await
        .context("namespace changes request")?
        .error_for_status()
        .context("namespace changes status")?;
    response.json().await.context("namespace changes body")
}

fn with_bearer(request: reqwest::RequestBuilder, bearer: Option<&str>) -> reqwest::RequestBuilder {
    match bearer {
        Some(bearer) => request.bearer_auth(bearer),
        None => request,
    }
}
