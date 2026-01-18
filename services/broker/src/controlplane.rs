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
struct StreamChangesResponse {
    items: Vec<StreamChange>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct StreamChange {
    op: StreamChangeOp,
    key: StreamKey,
    stream: Option<Stream>,
}

#[derive(Debug, Deserialize)]
struct StreamKey {
    stream: String,
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
    stream: String,
    shards: u32,
    durable: bool,
}

pub async fn start_sync(
    broker: Arc<Broker>,
    base_url: String,
    interval: Duration,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut next_seq = 0u64;
    loop {
        if next_seq == 0 {
            match fetch_snapshot(&client, &base_url).await {
                Ok(snapshot) => {
                    for stream in snapshot.items {
                        // For now, the broker keys streams by their name only.
                        broker
                            .register_stream(stream.stream, StreamMetadata {
                                durable: stream.durable,
                                shards: stream.shards,
                            })
                            .await?;
                    }
                    next_seq = snapshot.next_seq;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "control plane snapshot failed");
                }
            }
        }

        match fetch_changes(&client, &base_url, next_seq).await {
            Ok(changes) => {
                for change in changes.items {
                    match change.op {
                        StreamChangeOp::Created | StreamChangeOp::Updated => {
                            if let Some(stream) = change.stream {
                                // For now, the broker keys streams by their name only.
                                broker
                                    .register_stream(stream.stream, StreamMetadata {
                                        durable: stream.durable,
                                        shards: stream.shards,
                                    })
                                    .await?;
                            }
                        }
                        StreamChangeOp::Deleted => {
                            let _ = broker.remove_stream(&change.key.stream).await?;
                        }
                    }
                }
                next_seq = changes.next_seq;
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane change poll failed");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn fetch_snapshot(client: &reqwest::Client, base_url: &str) -> Result<StreamSnapshotResponse> {
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
