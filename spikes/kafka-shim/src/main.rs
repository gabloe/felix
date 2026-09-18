//! A Kafka-speaking front door, read-only, over a real Felix shard log.
//!
//! Spike for #488, option A: `ApiVersions`, `Metadata`, `ListOffsets`, `Fetch`.
//! The bar was a real client — librdkafka, via `kcat` — reading records out of
//! a Felix shard, and it does.
//!
//! Records come from `shard_log(..).read_from(offset, max_bytes)`, the same
//! call the replication driver ships with, so a Kafka `Fetch` and a follower's
//! catch-up read the same log the same way. Offsets need no translation: Felix
//! offsets are contiguous per shard, which is what Kafka assumes.
mod protocol;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, BytesMut};
use felix_broker::{Broker, DurableStorage, LogKind};
use protocol::{ApiKey, Record, encode_record_batch, parse_request_header, put_string};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
/// The stream this shim exposes as a Kafka topic. One shard, so one partition:
/// the mapping is the easy half, and a multi-shard topic needs only a wider
/// `Metadata` response.
const TOPIC: &str = "orders";
const PARTITIONS: i32 = 1;

/// Where the Kafka side reads from.
#[derive(Clone)]
struct Felix {
    broker: Arc<Broker>,
}

impl Felix {
    async fn log(&self) -> Option<felix_broker::StreamLog> {
        self.broker
            .shard_log(LogKind::Stream, TENANT, NAMESPACE, TOPIC, 0)
            .await
    }

    /// `(earliest, latest)` for `ListOffsets`.
    async fn bounds(&self) -> (i64, i64) {
        match self.log().await {
            Some(log) => (
                log.base_offset() as i64,
                log.tail_offset().await.unwrap_or(0) as i64,
            ),
            None => (0, 0),
        }
    }

    /// Records from `offset`, and the offset the batch actually starts at.
    async fn read(&self, offset: i64, max_bytes: usize) -> (i64, Vec<Record>) {
        let Some(log) = self.log().await else {
            return (offset, Vec::new());
        };
        let stored = log
            .read_from(offset.max(0) as u64, max_bytes.clamp(1024, 1 << 20))
            .await
            .unwrap_or_default();
        let base = stored.first().map_or(offset, |r| r.offset as i64);
        let records = stored
            .into_iter()
            .map(|r| Record {
                offset_delta: (r.offset as i64 - base) as i32,
                timestamp_delta: 0,
                key: None,
                value: r.payload.to_vec(),
            })
            .collect();
        (base, records)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let data_dir =
        std::env::var("SHIM_DATA_DIR").unwrap_or_else(|_| "/tmp/felix-kafka-spike".into());
    std::fs::create_dir_all(&data_dir).context("data dir")?;
    let storage = DurableStorage::open(
        std::path::Path::new(&data_dir),
        felix_storage::log::LogConfig::default(),
    )
    .context("open durable storage")?;
    let broker = Arc::new(
        Broker::new(felix_storage::EphemeralCache::new().into()).with_durable_storage(storage),
    );
    let felix = Felix { broker };

    // Seeded through the storage layer, not `Broker::publish`.
    //
    // A broker learns its streams from the control plane and refuses to publish
    // to one it has not been given, so publishing here would mean standing up a
    // control plane too. The log this writes is the same log, written by the
    // same code — what is missing is the catalog, not the storage. A shim that
    // shipped would live inside the broker and have both.
    let seed: usize = std::env::var("SHIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if seed > 0 {
        let log = felix
            .broker
            .durable_storage()
            .context("durable storage")?
            .open_stream(TENANT, NAMESPACE, TOPIC, 0)
            .context("open the shard log")?;
        for i in 0..seed {
            log.append(&[bytes::Bytes::from(format!("felix-record-{i}"))])
                .await
                .context("seed append")?;
        }
    }
    let (earliest, latest) = felix.bounds().await;
    tracing::info!(earliest, latest, "serving a felix shard as a kafka partition");

    let bind = std::env::var("SHIM_BIND").unwrap_or_else(|_| "0.0.0.0:9092".to_string());
    let listener = TcpListener::bind(&bind).await.context("bind")?;
    tracing::info!(%bind, "kafka shim listening");

    loop {
        let (socket, peer) = listener.accept().await.context("accept")?;
        tracing::info!(%peer, "client connected");
        let felix = felix.clone();
        tokio::spawn(async move {
            if let Err(err) = serve(socket, felix).await {
                tracing::info!(%peer, error = %err, "client finished");
            }
        });
    }
}

async fn serve(mut socket: TcpStream, felix: Felix) -> Result<()> {
    loop {
        // Every request is length-prefixed with an i32.
        let mut size = [0u8; 4];
        if socket.read_exact(&mut size).await.is_err() {
            return Ok(());
        }
        let size = i32::from_be_bytes(size) as usize;
        let mut frame = vec![0u8; size];
        socket.read_exact(&mut frame).await.context("read body")?;

        let mut buf = &frame[..];
        let header = parse_request_header(&mut buf);
        tracing::info!(
            api_key = header.api_key,
            api_version = header.api_version,
            client = header.client_id.as_deref().unwrap_or("-"),
            "request",
        );

        let body = match ApiKey::from_i16(header.api_key) {
            Some(ApiKey::ApiVersions) => api_versions(header.api_version),
            Some(ApiKey::Metadata) => metadata(header.api_version, &mut buf),
            Some(ApiKey::ListOffsets) => list_offsets(header.api_version, &mut buf, &felix).await,
            Some(ApiKey::Fetch) => fetch(header.api_version, &mut buf, &felix).await,
            None => {
                // Every group request lands here, and this is where a client
                // doing anything beyond a simple read stops.
                tracing::warn!(api_key = header.api_key, "unsupported api key");
                return Ok(());
            }
        };

        let mut response = BytesMut::new();
        response.put_i32((4 + body.len()) as i32);
        response.put_i32(header.correlation_id);
        response.put_slice(&body);
        socket.write_all(&response).await.context("write")?;
    }
}

/// The highest `ApiVersions` this shim answers.
///
/// Two, not three: v3 is "flexible" (KIP-482) — compact arrays and tagged
/// fields throughout — which is a second encoding to implement. librdkafka
/// opens with v3, so this is the first cost the spike turned up.
const MAX_API_VERSIONS: i16 = 2;

/// `ApiVersions`: the version ranges this shim supports, per key.
///
/// The spec carves out one exception here and a shim lives or dies by it: a
/// broker asked for an `ApiVersions` version it does not support answers
/// `UNSUPPORTED_VERSION` **in the v0 response format**, because the client
/// cannot know which format to parse until it knows the version was refused.
/// librdkafka then retries lower. Answer a v3 request in v0 shape *without*
/// that error and it reports "Bad message format" and gives up — which is
/// exactly what this spike did first.
fn api_versions(version: i16) -> BytesMut {
    let mut out = BytesMut::new();
    if version > MAX_API_VERSIONS {
        out.put_i16(35); // UNSUPPORTED_VERSION
        out.put_i32(0); // no api keys
        return out;
    }

    out.put_i16(0); // error_code
    let supported: &[(i16, i16, i16)] = &[
        (ApiKey::Fetch as i16, 0, 11),
        (ApiKey::ListOffsets as i16, 0, 5),
        (ApiKey::Metadata as i16, 0, 9),
        (ApiKey::ApiVersions as i16, 0, MAX_API_VERSIONS),
    ];
    out.put_i32(supported.len() as i32);
    for (key, min, max) in supported {
        out.put_i16(*key);
        out.put_i16(*min);
        out.put_i16(*max);
    }
    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
    out
}

/// `Metadata`: one broker, one topic, `PARTITIONS` partitions, all led here.
fn metadata(version: i16, _buf: &mut impl Buf) -> BytesMut {
    let host = std::env::var("SHIM_ADVERTISE_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: i32 = std::env::var("SHIM_ADVERTISE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9092);

    let mut out = BytesMut::new();
    if version >= 3 {
        out.put_i32(0); // throttle_time_ms
    }
    out.put_i32(1); // brokers
    out.put_i32(0); // node_id
    put_string(&mut out, &host);
    out.put_i32(port);
    if version >= 1 {
        out.put_i16(-1); // rack: null
    }
    if version >= 2 {
        put_string(&mut out, "felix-spike"); // cluster_id
    }
    if version >= 1 {
        out.put_i32(0); // controller_id
    }
    out.put_i32(1); // topics
    out.put_i16(0); // error_code
    put_string(&mut out, TOPIC);
    if version >= 1 {
        out.put_i8(0); // is_internal
    }
    out.put_i32(PARTITIONS);
    for partition in 0..PARTITIONS {
        out.put_i16(0); // error_code
        out.put_i32(partition);
        out.put_i32(0); // leader
        if version >= 7 {
            out.put_i32(-1); // leader_epoch
        }
        out.put_i32(1); // replicas
        out.put_i32(0);
        out.put_i32(1); // isr
        out.put_i32(0);
        if version >= 5 {
            out.put_i32(0); // offline_replicas
        }
    }
    if version >= 8 {
        out.put_i32(-2147483648); // cluster_authorized_operations
    }
    out
}

/// `ListOffsets`: earliest and latest come straight from the shard log.
async fn list_offsets(version: i16, buf: &mut impl Buf, felix: &Felix) -> BytesMut {
    let _replica_id = buf.get_i32();
    if version >= 2 {
        let _isolation = buf.get_i8();
    }
    let topics = buf.get_i32();

    let mut out = BytesMut::new();
    if version >= 2 {
        out.put_i32(0); // throttle_time_ms
    }
    out.put_i32(topics);
    for _ in 0..topics {
        let name = protocol::get_nullable_string(buf).unwrap_or_default();
        let partitions = buf.get_i32();
        put_string(&mut out, &name);
        out.put_i32(partitions);
        for _ in 0..partitions {
            let index = buf.get_i32();
            if version >= 4 {
                let _epoch = buf.get_i32();
            }
            let timestamp = buf.get_i64();
            if version == 0 {
                let _max = buf.get_i32();
            }
            out.put_i16(0); // error_code
            out.put_i32(index);
            // -2 is earliest, -1 is latest.
            let (earliest, latest) = felix.bounds().await;
            let offset = if timestamp == -2 { earliest } else { latest };
            if version == 0 {
                out.put_i32(1); // one offset
                out.put_i64(offset);
            } else {
                out.put_i64(-1); // timestamp
                out.put_i64(offset);
                if version >= 4 {
                    out.put_i32(-1); // leader_epoch
                }
            }
        }
    }
    out
}

/// `Fetch`: one record batch per requested partition, from the fetch offset.
async fn fetch(version: i16, buf: &mut impl Buf, felix: &Felix) -> BytesMut {
    let _replica_id = buf.get_i32();
    let _max_wait = buf.get_i32();
    let _min_bytes = buf.get_i32();
    if version >= 3 {
        let _max_bytes = buf.get_i32();
    }
    if version >= 4 {
        let _isolation = buf.get_i8();
    }
    if version >= 7 {
        let _session_id = buf.get_i32();
        let _session_epoch = buf.get_i32();
    }
    let topics = buf.get_i32();

    let mut out = BytesMut::new();
    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
    if version >= 7 {
        out.put_i16(0); // error_code
        out.put_i32(0); // session_id
    }
    out.put_i32(topics);
    for _ in 0..topics {
        let name = protocol::get_nullable_string(buf).unwrap_or_default();
        let partitions = buf.get_i32();
        put_string(&mut out, &name);
        out.put_i32(partitions);
        for _ in 0..partitions {
            let index = buf.get_i32();
            if version >= 9 {
                let _epoch = buf.get_i32();
            }
            let fetch_offset = buf.get_i64();
            if version >= 5 {
                let _log_start = buf.get_i64();
            }
            let max_bytes = buf.get_i32();
            let (_, high_watermark) = felix.bounds().await;
            let (base_offset, remaining) = felix.read(fetch_offset, max_bytes as usize).await;

            out.put_i32(index);
            out.put_i16(0); // error_code
            out.put_i64(high_watermark);
            if version >= 4 {
                out.put_i64(high_watermark); // last_stable_offset
                if version >= 5 {
                    out.put_i64(0); // log_start_offset
                }
                out.put_i32(0); // aborted_transactions
            }
            if version >= 11 {
                out.put_i32(-1); // preferred_read_replica
            }
            if remaining.is_empty() {
                out.put_i32(0); // no records
            } else {
                let batch = encode_record_batch(base_offset, 0, &remaining);
                out.put_i32(batch.len() as i32);
                out.put_slice(&batch);
            }
        }
    }
    out
}
