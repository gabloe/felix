// Publisher worker pool and single-writer stream logic.
use ahash::{AHasher, RandomState};
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{AckMode, FrameHeader, Message};
use hashbrown::HashMap;
use quinn::{RecvStream, SendStream};
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::{
    maybe_append_publish_ts, maybe_append_publish_ts_batch, wait_for_ack, write_frame_parts,
};

use super::sharding::PublishSharding;

#[derive(Clone)]
pub struct Publisher {
    pub(crate) inner: Arc<PublisherInner>,
}

pub(crate) struct PublisherInner {
    pub(crate) workers: Arc<Vec<PublishWorker>>,
    pub(crate) sharding: PublishSharding,
    pub(crate) rr: AtomicUsize,
    admission: Arc<PublishAdmission>,
    stream_cache: Mutex<StreamShardCache>,
    bench_embed_ts: bool,
    /// Intersection of every worker's advertised flags.
    ///
    /// The workers all talk to the same broker, so in practice these agree; the
    /// intersection is taken anyway so a single lagging stream can never cause
    /// an encoding to be used on a connection that cannot parse it.
    server_flags: u16,
}

impl PublisherInner {
    #[cfg(test)]
    pub(crate) fn new(workers: Arc<Vec<PublishWorker>>, sharding: PublishSharding) -> Self {
        Self::with_admission(
            workers,
            sharding,
            Arc::new(PublishAdmission::new(
                crate::config::DEFAULT_PUBLISH_INFLIGHT_BYTES,
            )),
        )
    }

    pub(crate) fn with_admission(
        workers: Arc<Vec<PublishWorker>>,
        sharding: PublishSharding,
        admission: Arc<PublishAdmission>,
    ) -> Self {
        let server_flags = workers
            .iter()
            .map(|worker| worker.server_flags)
            .fold(u16::MAX, |acc, flags| acc & flags);
        Self {
            workers,
            sharding,
            rr: AtomicUsize::new(0),
            admission,
            stream_cache: Mutex::new(StreamShardCache::new(STREAM_SHARD_CACHE_CAPACITY)),
            bench_embed_ts: false,
            server_flags,
        }
    }

    pub(crate) fn with_runtime_config(
        workers: Arc<Vec<PublishWorker>>,
        sharding: PublishSharding,
        admission: Arc<PublishAdmission>,
        bench_embed_ts: bool,
    ) -> Self {
        let mut inner = Self::with_admission(workers, sharding, admission);
        inner.bench_embed_ts = bench_embed_ts;
        inner
    }
}

pub(crate) struct PublishAdmission {
    semaphore: Arc<Semaphore>,
    limit: usize,
}

impl PublishAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.clamp(1, u32::MAX as usize);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    async fn acquire(&self, wire_bytes: usize) -> Result<OwnedSemaphorePermit> {
        let wire_bytes = wire_bytes.max(1);
        if wire_bytes > self.limit {
            return Err(anyhow::anyhow!(
                "publish frame estimate {wire_bytes} exceeds in-flight byte limit {}",
                self.limit
            ));
        }
        self.semaphore
            .clone()
            .acquire_many_owned(wire_bytes as u32)
            .await
            .map_err(|_| anyhow::anyhow!("publish admission closed"))
    }
}

pub(crate) struct PublishWorker {
    pub(crate) tx: mpsc::Sender<PublishRequest>,
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    pub(crate) request_counter: AtomicU64,
    /// Frame-flag bits the broker advertised for this stream during auth.
    pub(crate) server_flags: u16,
}

pub(crate) enum PublishRequest {
    Message {
        message: Message,
        ack: AckMode,
        request_id: Option<u64>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<()>>,
    },
    BinaryBytes {
        bytes: Bytes,
        item_count: usize,
        sample: bool,
        /// `AckMode::None` for fire-and-forget frames. Anything else means the
        /// frame was encoded with `FLAG_BINARY_PUBLISH_ACKED` and the writer must
        /// block for the broker's ack before reporting the result.
        ack: AckMode,
        request_id: Option<u64>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<()>>,
    },
    Finish {
        response: oneshot::Sender<Result<()>>,
    },
}

const STREAM_SHARD_CACHE_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

impl StreamKey {
    fn new(tenant_id: &str, namespace: &str, stream: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct StreamKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
}

impl<'a> StreamKeyRef<'a> {
    fn new(tenant_id: &'a str, namespace: &'a str, stream: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            stream,
        }
    }
}

impl<'a> hashbrown::Equivalent<StreamKey> for StreamKeyRef<'a> {
    fn equivalent(&self, key: &StreamKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
    }
}

struct StreamShardCache {
    capacity: usize,
    order: VecDeque<StreamKey>,
    entries: HashMap<StreamKey, usize, RandomState>,
}

impl StreamShardCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity.min(64)),
            entries: HashMap::with_capacity_and_hasher(capacity, RandomState::new()),
        }
    }

    fn get(&self, key: StreamKeyRef<'_>) -> Option<usize> {
        self.entries.get(&key).copied()
    }

    fn insert(&mut self, key: StreamKey, worker_index: usize) {
        if self.capacity == 0 || self.entries.contains_key(&key) {
            return;
        }
        if self.entries.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.entries.remove(&evicted);
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, worker_index);
    }
}

fn hash_stream_index(tenant_id: &str, namespace: &str, stream: &str, worker_count: usize) -> usize {
    let mut hasher = AHasher::default();
    tenant_id.hash(&mut hasher);
    namespace.hash(&mut hasher);
    stream.hash(&mut hasher);
    (hasher.finish() as usize) % worker_count
}

fn estimate_text_publish_bytes(message: &Message) -> usize {
    let payload_bytes = match message {
        Message::Publish { payload, .. } => payload.len(),
        Message::PublishBatch { payloads, .. } => payloads.iter().map(Vec::len).sum(),
        _ => 0,
    };
    // JSON byte arrays can require up to four characters per byte including separators.
    FrameHeader::LEN
        .saturating_add(payload_bytes.saturating_mul(4))
        .saturating_add(1024)
}

impl Publisher {
    fn select_worker(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<&PublishWorker> {
        let workers = &self.inner.workers;
        if workers.is_empty() {
            return Err(anyhow::anyhow!("publish pool is empty"));
        }
        let worker_count = workers.len();
        let index = match self.inner.sharding {
            PublishSharding::RoundRobin => {
                self.inner.rr.fetch_add(1, Ordering::Relaxed) % worker_count
            }
            PublishSharding::HashStream => {
                if let Ok(mut cache) = self.inner.stream_cache.try_lock() {
                    if let Some(index) = cache.get(StreamKeyRef::new(tenant_id, namespace, stream))
                    {
                        return Ok(&workers[index]);
                    }
                    let index = hash_stream_index(tenant_id, namespace, stream, worker_count);
                    cache.insert(StreamKey::new(tenant_id, namespace, stream), index);
                    return Ok(&workers[index]);
                }
                hash_stream_index(tenant_id, namespace, stream, worker_count)
            }
        };
        Ok(&workers[index])
    }

    /// Publish one payload using the binary data-plane encoding.
    ///
    /// Acked and unacked publishes both take the binary path: an unacked publish is
    /// a plain `FLAG_BINARY_PUBLISH_BATCH` frame, and an acked one adds
    /// `FLAG_BINARY_PUBLISH_ACKED` and waits for the broker's binary ack. Call
    /// `publish_json` to force the JSON compatibility encoding instead.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        if ack == AckMode::None {
            return self
                .publish_batch_binary(tenant_id, namespace, stream, &[payload])
                .await;
        }
        // A single acked publish is a one-item acked batch on the wire; there is no
        // separate binary encoding for single messages.
        self.publish_batch(tenant_id, namespace, stream, vec![payload], ack)
            .await
    }

    /// Frame-flag bits the broker advertised during the auth handshake,
    /// intersected across this publisher's streams.
    ///
    /// Resolves to `felix_wire::ORIGINAL_V1_FLAGS` against a broker that predates
    /// capability negotiation. Exposed so callers can log or assert what was
    /// actually negotiated rather than inferring it from behaviour.
    pub fn negotiated_server_flags(&self) -> u16 {
        self.inner.server_flags
    }

    /// Whether the broker advertised support for the acked binary publish frame.
    ///
    /// False against any broker that predates capability negotiation, because an
    /// unadvertised mask resolves to `ORIGINAL_V1_FLAGS`. That is the whole point:
    /// such a broker would match on `FLAG_BINARY_PUBLISH_BATCH`, know nothing of
    /// the `request_id` prefix, and read it as `tenant_len`.
    fn supports_binary_ack(&self) -> bool {
        felix_wire::supports(
            self.inner.server_flags,
            felix_wire::FLAG_BINARY_PUBLISH_ACKED,
        )
    }

    /// Publish one payload using the JSON compatibility encoding.
    pub async fn publish_json(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payload = maybe_append_publish_ts(payload, self.inner.bench_embed_ts);
        // Enqueue publish on the single-writer publisher task.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        let message = Message::Publish {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            payload,
            request_id,
            ack: Some(ack),
        };
        let permit = self
            .inner
            .admission
            .acquire(estimate_text_publish_bytes(&message))
            .await?;
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue publish")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx.await.context("publish response dropped")?
    }

    /// Publish a batch. Both acked and unacked batches use the binary encoding by
    /// default; call `publish_batch_json` for the JSON compatibility encoding.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        if ack == AckMode::None {
            return self
                .publish_batch_binary(tenant_id, namespace, stream, &payloads)
                .await;
        }
        // Fall back to the JSON encoding against a broker that has not advertised
        // the acked binary frame. Both paths are equivalent in semantics; only the
        // framing differs, so the fallback costs throughput, not correctness.
        if !self.supports_binary_ack() {
            return self
                .publish_batch_json(tenant_id, namespace, stream, payloads, ack)
                .await;
        }
        self.publish_batch_binary_acked(tenant_id, namespace, stream, payloads, ack)
            .await
    }

    /// Publish a batch that asks to be acknowledged, using the binary encoding.
    ///
    /// The frame is written with `FLAG_BINARY_PUBLISH_ACKED` and the broker replies
    /// with a binary ack frame.
    ///
    /// This is the unconditional form: it sends the acked binary frame whether or
    /// not the broker advertised support. Prefer `publish_batch`, which consults the
    /// mask negotiated during auth and falls back to JSON when the broker has not
    /// advertised `0x0008`.
    pub async fn publish_batch_binary_acked(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        if ack == AckMode::None {
            return self
                .publish_batch_binary(tenant_id, namespace, stream, &payloads)
                .await;
        }
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
        let request_id = worker.request_counter.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::t_now_if(sample);
        let bytes = felix_wire::binary::encode_acked_publish_batch_bytes(
            request_id, ack, tenant_id, namespace, stream, &payloads,
        )?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                ack,
                request_id: Some(request_id),
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue acked binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx
            .await
            .context("acked binary batch response dropped")?
    }

    /// Publish a batch using the JSON compatibility encoding.
    pub async fn publish_batch_json(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
        // Batch publish uses the same queue/writer as single messages.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        let message = Message::PublishBatch {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            payloads,
            request_id,
            ack: Some(ack),
        };
        let permit = self
            .inner
            .admission
            .acquire(estimate_text_publish_bytes(&message))
            .await?;
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue publish batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx
            .await
            .context("publish batch response dropped")?
    }

    pub async fn publish_batch_binary(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads_with_ts;
        let payloads = if self.inner.bench_embed_ts {
            payloads_with_ts = payloads
                .iter()
                .map(|payload| maybe_append_publish_ts(payload.clone(), true))
                .collect::<Vec<_>>();
            &payloads_with_ts
        } else {
            payloads
        };
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::t_now_if(sample);
        let (bytes, stats) = felix_wire::binary::encode_publish_batch_bytes_with_stats(
            tenant_id, namespace, stream, payloads,
        )?;
        #[cfg(not(feature = "telemetry"))]
        let _ = stats;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        if stats.reallocs > 0 {
            let counters = frame_counters();
            counters
                .binary_encode_reallocs
                .fetch_add(stats.reallocs, Ordering::Relaxed);
        }
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                ack: AckMode::None,
                request_id: None,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx.await.context("binary batch response dropped")?
    }

    pub async fn publish_batch_binary_bytes(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Bytes],
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::t_now_if(sample);
        let (bytes, stats) = felix_wire::binary::encode_publish_batch_bytes_with_stats_from_bytes(
            tenant_id, namespace, stream, payloads,
        )?;
        #[cfg(not(feature = "telemetry"))]
        let _ = stats;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        if stats.reallocs > 0 {
            let counters = frame_counters();
            counters
                .binary_encode_reallocs
                .fetch_add(stats.reallocs, Ordering::Relaxed);
        }
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                ack: AckMode::None,
                request_id: None,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx.await.context("binary batch response dropped")?
    }

    pub async fn finish(&self) -> Result<()> {
        let mut handles = Vec::new();
        for worker in self.inner.workers.iter() {
            let handle = {
                let mut guard = worker.handle.lock().await;
                guard.take()
            };
            if let Some(handle) = handle {
                let (response_tx, response_rx) = oneshot::channel();
                if worker
                    .tx
                    .send(PublishRequest::Finish {
                        response: response_tx,
                    })
                    .await
                    .is_ok()
                {
                    response_rx
                        .await
                        .context("publisher finish response dropped")??;
                }
                handles.push(handle);
            }
        }
        for handle in handles {
            handle.await.context("publisher writer task")??;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) async fn run_publisher_writer(
    send: SendStream,
    recv: RecvStream,
    rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
) -> Result<()> {
    run_publisher_writer_with_limit(
        send,
        recv,
        rx,
        _chunk_bytes,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
    .await
}

/// A publish that has been written to the stream but whose broker ack has not
/// arrived yet. The admission permit rides along so the in-flight byte budget
/// stays reserved until the broker answers, not merely until the frame is
/// written.
struct PendingAck {
    request_id: u64,
    response: oneshot::Sender<Result<()>>,
    _permit: OwnedSemaphorePermit,
    // Read only by the telemetry counters in the ack reader.
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    batch_count: u64,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    item_count: u64,
}

/// Upper bound on acked publishes in flight per stream. The byte budget
/// (`PublishAdmission`) is the real limit; this only bounds the queue's
/// memory when payloads are tiny.
const MAX_PENDING_ACKS: usize = 1024;

/// Resolve broker acks for pipelined acked publishes, in write order.
///
/// Runs beside the writer so an acked publish costs a queue hop instead of
/// stalling the stream for a full round trip: when the writer awaited each
/// ack inline, acked throughput per stream was capped at one request per RTT.
/// Acks arrive strictly in request order, so pendings resolve FIFO. Any ack
/// failure — timeout, decode error, broker `PublishError` — is fatal for the
/// worker exactly as it was inline; requests already queued behind the
/// failure are failed with the same message.
async fn run_publish_ack_reader(
    mut recv: RecvStream,
    mut pending_rx: mpsc::Receiver<PendingAck>,
    max_frame_bytes: usize,
) -> (RecvStream, Result<()>) {
    let mut ack_scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(pending) = pending_rx.recv().await {
        match wait_for_ack(
            &mut recv,
            pending.request_id,
            &mut ack_scratch,
            max_frame_bytes,
        )
        .await
        {
            Ok(()) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                    counters
                        .pub_batches_out_ok
                        .fetch_add(pending.batch_count, Ordering::Relaxed);
                    counters
                        .pub_items_out_ok
                        .fetch_add(pending.item_count, Ordering::Relaxed);
                }
                let _ = pending.response.send(Ok(()));
            }
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                    counters
                        .pub_batches_out_err
                        .fetch_add(pending.batch_count, Ordering::Relaxed);
                    counters
                        .pub_items_out_err
                        .fetch_add(pending.item_count, Ordering::Relaxed);
                }
                let message = err.to_string();
                let _ = pending.response.send(Err(err));
                pending_rx.close();
                while let Some(stale) = pending_rx.recv().await {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                        counters
                            .pub_batches_out_err
                            .fetch_add(stale.batch_count, Ordering::Relaxed);
                        counters
                            .pub_items_out_err
                            .fetch_add(stale.item_count, Ordering::Relaxed);
                    }
                    let _ = stale.response.send(Err(anyhow::anyhow!(message.clone())));
                }
                return (recv, Err(anyhow::anyhow!(message)));
            }
        }
    }
    (recv, Ok(()))
}

pub(crate) async fn run_publisher_writer_with_limit(
    mut send: SendStream,
    recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
    max_frame_bytes: usize,
) -> Result<()> {
    // Single writer: serialize publish requests over one bi-directional
    // stream. Acked publishes are pipelined — written back to back and
    // resolved by the ack reader as the broker answers — so the ordering
    // guarantee costs a queue hop per request instead of a round trip.
    let (pending_tx, pending_rx) = mpsc::channel::<PendingAck>(MAX_PENDING_ACKS);
    let mut reader = tokio::spawn(run_publish_ack_reader(recv, pending_rx, max_frame_bytes));
    let mut json_scratch = BytesMut::with_capacity(64 * 1024);
    // On any failure the worker tears down: fail the current caller, fail
    // everything the reader still holds, then drain the request queue with
    // the same message so later callers see why.
    macro_rules! fail_worker {
        ($message:expr) => {{
            let message: String = $message;
            drop(pending_tx);
            let _ = (&mut reader).await;
            drain_publish_queue(&mut rx, &message).await;
            return Err(anyhow::anyhow!(message));
        }};
    }
    // Hand a written acked request to the ack reader; fatal if the reader is
    // gone (it only exits early after an ack failure, which is fatal anyway).
    macro_rules! submit_pending {
        ($pending:expr) => {{
            if let Err(send_err) = pending_tx.send($pending).await {
                let message = match (&mut reader).await {
                    Ok((_recv, Err(reader_err))) => reader_err.to_string(),
                    _ => "publish ack reader stopped".to_string(),
                };
                let _ = send_err
                    .0
                    .response
                    .send(Err(anyhow::anyhow!(message.clone())));
                drain_publish_queue(&mut rx, &message).await;
                return Err(anyhow::anyhow!(message));
            }
        }};
    }
    let mut finish_response: Option<oneshot::Sender<Result<()>>> = None;
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit,
                response,
            } => match message {
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    request_id: msg_request_id,
                    ack: msg_ack,
                } => {
                    #[cfg(feature = "telemetry")]
                    let sample = crate::t_should_sample();
                    #[cfg(not(feature = "telemetry"))]
                    let sample = false;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = sample;
                    let json_len = felix_wire::text::publish_batch_json_len(
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        msg_request_id,
                        msg_ack,
                    )?;
                    json_scratch.clear();
                    json_scratch.reserve(FrameHeader::LEN + json_len);
                    json_scratch.resize(FrameHeader::LEN, 0);
                    #[cfg(feature = "telemetry")]
                    let encode_start = crate::t_now_if(sample);
                    let stats = felix_wire::text::write_publish_batch_json(
                        &mut json_scratch,
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        msg_request_id,
                        msg_ack,
                    )?;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = stats;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        timings::record_text_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if stats.reallocs > 0 {
                        let counters = frame_counters();
                        counters
                            .text_encode_reallocs
                            .fetch_add(stats.reallocs, Ordering::Relaxed);
                    }
                    #[cfg(feature = "telemetry")]
                    let build_start = crate::t_now_if(sample);
                    let payload_len = json_scratch.len() - FrameHeader::LEN;
                    let header = FrameHeader::new(0, payload_len as u32);
                    let mut header_bytes = [0u8; FrameHeader::LEN];
                    header.encode_into(&mut header_bytes);
                    json_scratch[..FrameHeader::LEN].copy_from_slice(&header_bytes);
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = build_start {
                        let build_ns = start.elapsed().as_nanos() as u64;
                        timings::record_text_batch_build_ns(build_ns);
                        t_histogram!("client_text_batch_build_ns").record(build_ns as f64);
                    }
                    let bytes = json_scratch.split().freeze();
                    #[cfg(feature = "telemetry")]
                    let write_start = crate::t_now_if(sample);
                    #[cfg(feature = "telemetry")]
                    let await_start = crate::t_now_if(sample);
                    let write_result = send
                        .write_all(&bytes)
                        .await
                        .context("write publish batch frame");
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = await_start {
                        let await_ns = start.elapsed().as_nanos() as u64;
                        timings::record_send_await_ns(await_ns);
                        t_histogram!("client_send_await_ns").record(await_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let item_count = payloads.len() as u64;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = item_count;
                    match write_result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .bytes_out
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            }
                            if ack == AckMode::None {
                                #[cfg(feature = "telemetry")]
                                {
                                    let counters = frame_counters();
                                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                    counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_ok
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                                let _ = response.send(Ok(()));
                            } else if let Some(request_id) = request_id {
                                submit_pending!(PendingAck {
                                    request_id,
                                    response,
                                    _permit,
                                    batch_count: 1,
                                    item_count,
                                });
                            } else {
                                let message = "missing request_id for acked publish".to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_err
                                    .fetch_add(item_count, Ordering::Relaxed);
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            fail_worker!(message);
                        }
                    }
                }
                other => {
                    #[cfg(feature = "telemetry")]
                    let sample = crate::t_should_sample();
                    #[cfg(feature = "telemetry")]
                    let encode_start = crate::t_now_if(sample);
                    let frame = match other.encode().context("encode message") {
                        Ok(frame) => frame,
                        Err(err) => {
                            let _ = response.send(Err(err));
                            continue;
                        }
                    };
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    let write_start = crate::t_now_if(sample);
                    let write_result = write_frame_parts(&mut send, &frame).await;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let (batch_count, item_count) = match &other {
                        Message::Publish { .. } => (1u64, 1u64),
                        Message::PublishBatch { payloads, .. } => (1u64, payloads.len() as u64),
                        _ => (0u64, 0u64),
                    };
                    #[cfg(not(feature = "telemetry"))]
                    let _ = (batch_count, item_count);
                    match write_result {
                        Ok(()) => {
                            if ack == AckMode::None {
                                #[cfg(feature = "telemetry")]
                                {
                                    let counters = frame_counters();
                                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                    if batch_count > 0 {
                                        counters
                                            .pub_batches_out_ok
                                            .fetch_add(batch_count, Ordering::Relaxed);
                                        counters
                                            .pub_items_out_ok
                                            .fetch_add(item_count, Ordering::Relaxed);
                                    }
                                }
                                let _ = response.send(Ok(()));
                            } else if let Some(request_id) = request_id {
                                submit_pending!(PendingAck {
                                    request_id,
                                    response,
                                    _permit,
                                    batch_count,
                                    item_count,
                                });
                            } else {
                                let message = "missing request_id for acked publish".to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                if batch_count > 0 {
                                    counters
                                        .pub_batches_out_err
                                        .fetch_add(batch_count, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_err
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            fail_worker!(message);
                        }
                    }
                }
            },
            PublishRequest::BinaryBytes {
                bytes,
                item_count,
                sample,
                ack,
                request_id,
                _permit,
                response,
            } => {
                #[cfg(not(feature = "telemetry"))]
                let _ = (item_count, sample);
                #[cfg(feature = "telemetry")]
                let write_start = crate::t_now_if(sample);
                #[cfg(feature = "telemetry")]
                let chunk_start = crate::t_now_if(sample);
                let write_result = send
                    .write_all(&bytes)
                    .await
                    .context("write binary batch frame");
                #[cfg(feature = "telemetry")]
                if let Some(start) = chunk_start {
                    let await_ns = start.elapsed().as_nanos() as u64;
                    timings::record_send_await_ns(await_ns);
                    t_histogram!("client_send_await_ns").record(await_ns as f64);
                }
                #[cfg(feature = "telemetry")]
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    t_histogram!("felix_client_write_ns").record(write_ns as f64);
                }
                match write_result {
                    Ok(()) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .bytes_out
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                        if ack == AckMode::None {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_ok
                                    .fetch_add(item_count as u64, Ordering::Relaxed);
                            }
                            let _ = response.send(Ok(()));
                        } else if let Some(request_id) = request_id {
                            submit_pending!(PendingAck {
                                request_id,
                                response,
                                _permit,
                                batch_count: 1,
                                item_count: item_count as u64,
                            });
                        } else {
                            let message = "missing request_id for acked publish".to_string();
                            let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                            fail_worker!(message);
                        }
                    }
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                            counters
                                .pub_items_out_err
                                .fetch_add(item_count as u64, Ordering::Relaxed);
                        }
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        fail_worker!(message);
                    }
                }
            }
            PublishRequest::Finish { response } => {
                finish_response = Some(response);
                break;
            }
        }
    }
    // Graceful shutdown: close our send side, then let the ack reader resolve
    // every pending ack and hand `recv` back so the stream can be drained to
    // EOF — the same close protocol `finish_publisher_stream` implements for
    // the non-pipelined paths.
    drop(pending_tx);
    let finish_result = send.finish().map_err(anyhow::Error::from);
    let result = match reader.await.context("join publish ack reader") {
        Ok((mut recv, reader_result)) => {
            let drain_result = async {
                reader_result?;
                let mut buf = [0u8; 8192];
                loop {
                    match recv.read(&mut buf).await {
                        Ok(Some(_)) => continue,
                        Ok(None) => break,
                        Err(err) => return Err(err.into()),
                    }
                }
                Ok(())
            }
            .await;
            finish_result.and(drain_result)
        }
        Err(err) => finish_result.and(Err(err)),
    };
    if let Some(response) = finish_response {
        let _ = response.send(match &result {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow::anyhow!(err.to_string())),
        });
    }
    result
}

#[cfg(test)]
pub(crate) async fn finish_publisher_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<()> {
    send.finish()?;
    let mut buf = [0u8; 8192];
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

pub(crate) async fn drain_publish_queue(rx: &mut mpsc::Receiver<PublishRequest>, message: &str) {
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message { response, .. }
            | PublishRequest::BinaryBytes { response, .. }
            | PublishRequest::Finish { response } => {
                let _ = response.send(Err(anyhow::anyhow!(message.to_string())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn test_publish_permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("test publish permit")
    }

    #[tokio::test]
    async fn publish_admission_bounds_shared_inflight_bytes() {
        let admission = PublishAdmission::new(4);
        let permit = admission.acquire(4).await.expect("initial permit");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
                .await
                .is_err()
        );
        drop(permit);
        let _permit = admission.acquire(1).await.expect("released permit");
    }

    #[tokio::test]
    async fn publish_admission_rejects_oversized_frame() {
        let err = PublishAdmission::new(4)
            .acquire(5)
            .await
            .expect_err("oversized publish");
        assert!(err.to_string().contains("exceeds in-flight byte limit"));
    }

    fn make_publisher(sharding: PublishSharding, workers: usize) -> Publisher {
        let mut publish_workers = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, mut rx) = mpsc::channel::<PublishRequest>(8);
            let handle = tokio::spawn(async move {
                while let Some(request) = rx.recv().await {
                    match request {
                        PublishRequest::Message { response, .. } => {
                            let _ = response.send(Ok(()));
                        }
                        PublishRequest::BinaryBytes { response, .. } => {
                            let _ = response.send(Ok(()));
                        }
                        PublishRequest::Finish { response } => {
                            let _ = response.send(Ok(()));
                            break;
                        }
                    }
                }
                Ok(())
            });
            publish_workers.push(PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(Some(handle)),
                request_counter: AtomicU64::new(1),
                server_flags: felix_wire::KNOWN_FLAGS,
            });
        }
        Publisher {
            inner: Arc::new(PublisherInner::new(Arc::new(publish_workers), sharding)),
        }
    }

    #[tokio::test]
    async fn publish_fails_when_pool_empty() {
        let publisher = make_publisher(PublishSharding::RoundRobin, 0);
        let err = publisher
            .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
            .await
            .expect_err("empty pool");
        assert!(err.to_string().contains("publish pool is empty"));
    }

    #[tokio::test]
    async fn publish_and_finish_success() {
        let publisher = make_publisher(PublishSharding::RoundRobin, 1);
        publisher
            .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
            .await
            .expect("publish");
        publisher
            .publish("t", "ns", "s", b"payload".to_vec(), AckMode::PerMessage)
            .await
            .expect("publish ack");
        publisher.finish().await.expect("finish");
    }

    #[tokio::test]
    async fn unacked_publish_defaults_to_binary_and_json_is_explicit() {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(2);
        let publisher = Publisher {
            inner: Arc::new(PublisherInner::new(
                Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(None),
                    request_counter: AtomicU64::new(1),
                    server_flags: felix_wire::KNOWN_FLAGS,
                }]),
                PublishSharding::RoundRobin,
            )),
        };

        let binary_publish = tokio::spawn({
            let publisher = publisher.clone();
            async move {
                publisher
                    .publish("t", "ns", "s", b"binary".to_vec(), AckMode::None)
                    .await
            }
        });
        let request = rx.recv().await.expect("binary request");
        match request {
            PublishRequest::BinaryBytes {
                bytes,
                item_count,
                response,
                ..
            } => {
                let frame = felix_wire::Frame::decode(bytes).expect("binary frame");
                let batch = felix_wire::binary::decode_publish_batch(&frame).expect("binary batch");
                assert_eq!(item_count, 1);
                assert_eq!(batch.payloads.len(), 1);
                assert_eq!(batch.payloads[0], b"binary");
                let _ = response.send(Ok(()));
            }
            _ => panic!("unacked publish should use binary encoding"),
        }
        binary_publish
            .await
            .expect("binary task")
            .expect("binary publish");

        let json_publish = tokio::spawn({
            let publisher = publisher.clone();
            async move {
                publisher
                    .publish_json("t", "ns", "s", b"json".to_vec(), AckMode::None)
                    .await
            }
        });
        let request = rx.recv().await.expect("json request");
        match request {
            PublishRequest::Message {
                message, response, ..
            } => {
                assert!(matches!(message, Message::Publish { .. }));
                let _ = response.send(Ok(()));
            }
            _ => panic!("explicit JSON publish should use message encoding"),
        }
        json_publish
            .await
            .expect("json task")
            .expect("json publish");
    }

    #[tokio::test]
    async fn publish_batch_and_binary_paths() {
        let publisher = make_publisher(PublishSharding::HashStream, 2);
        publisher
            .publish_batch(
                "t",
                "ns",
                "s",
                vec![b"a".to_vec(), b"b".to_vec()],
                AckMode::PerBatch,
            )
            .await
            .expect("publish batch");
        publisher
            .publish_batch_binary("t", "ns", "s", &[b"a".to_vec(), b"b".to_vec()])
            .await
            .expect("publish batch binary");
        publisher.finish().await.expect("finish");
    }

    #[tokio::test]
    #[cfg(feature = "telemetry")]
    async fn publish_records_enqueue_timings_when_sampling() {
        crate::timings::enable_collection(1);
        let publisher = make_publisher(PublishSharding::RoundRobin, 1);
        publisher
            .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
            .await
            .expect("publish");
        publisher
            .publish_batch(
                "t",
                "ns",
                "s",
                vec![b"a".to_vec(), b"b".to_vec()],
                AckMode::PerBatch,
            )
            .await
            .expect("publish batch");
        publisher
            .publish_batch_binary("t", "ns", "s", &[b"a".to_vec(), b"b".to_vec()])
            .await
            .expect("publish batch binary");
        publisher.finish().await.expect("finish");
    }

    #[tokio::test]
    #[cfg(feature = "telemetry")]
    async fn publish_batch_binary_appends_bench_ts_when_enabled() -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(1);
        let handle = tokio::spawn(async move {
            if let Some(PublishRequest::BinaryBytes {
                bytes, response, ..
            }) = rx.recv().await
            {
                let frame = felix_wire::Frame::decode(bytes).context("decode frame")?;
                let decoded = felix_wire::binary::decode_publish_batch(&frame)
                    .context("decode publish batch")?;
                assert_eq!(decoded.payloads.len(), 1);
                assert!(decoded.payloads[0].len() > 1);
                let _ = response.send(Ok(()));
            }
            Ok(())
        });

        let publisher = Publisher {
            inner: Arc::new(PublisherInner::with_runtime_config(
                Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                    server_flags: felix_wire::KNOWN_FLAGS,
                }]),
                PublishSharding::RoundRobin,
                Arc::new(PublishAdmission::new(
                    crate::config::DEFAULT_PUBLISH_INFLIGHT_BYTES,
                )),
                true,
            )),
        };
        publisher
            .publish_batch_binary("t", "ns", "s", &[b"x".to_vec()])
            .await
            .expect("publish batch binary");
        publisher.finish().await.expect("finish");
        Ok(())
    }

    #[tokio::test]
    async fn select_worker_round_robin_advances() {
        let publisher = make_publisher(PublishSharding::RoundRobin, 2);
        let rr_start = publisher.inner.rr.load(Ordering::Relaxed);
        let _ = publisher
            .publish("t", "ns", "s", b"p1".to_vec(), AckMode::None)
            .await;
        let _ = publisher
            .publish("t", "ns", "s", b"p2".to_vec(), AckMode::None)
            .await;
        let rr_end = publisher.inner.rr.load(Ordering::Relaxed);
        assert!(rr_end >= rr_start + 2);
        publisher.finish().await.expect("finish");
    }

    #[test]
    fn stream_cache_eviction_preserves_recent_entries() {
        let mut cache = StreamShardCache::new(2);
        cache.insert(StreamKey::new("t1", "ns", "a"), 0);
        cache.insert(StreamKey::new("t1", "ns", "b"), 1);
        assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "a")), Some(0));
        cache.insert(StreamKey::new("t1", "ns", "c"), 2);
        assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "a")), None);
        assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "b")), Some(1));
        assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "c")), Some(2));
    }

    fn make_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])
            .context("generate self-signed cert")?;
        let cert_der = cert.cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server_config = quinn::ServerConfig::with_single_cert(
            vec![cert_der.clone()],
            PrivateKeyDer::Pkcs8(key_der),
        )
        .context("build server config")?;
        Ok((server_config, cert_der))
    }

    fn make_client_config(cert: CertificateDer<'static>) -> Result<quinn::ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert).context("add root cert")?;
        Ok(quinn::ClientConfig::with_root_certificates(
            std::sync::Arc::new(roots),
        )?)
    }

    #[tokio::test]
    async fn acked_publishes_pipeline_without_waiting_per_ack() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        // The server refuses to ack anything until all three publish frames
        // have arrived. A writer that awaits each ack inline can never send
        // frame 2 before frame 1 is acked, so it deadlocks here (and this
        // test times out); the pipelined writer sends all three back to back.
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut scratch = BytesMut::with_capacity(4096);
            let mut request_ids = Vec::new();
            for _ in 0..3 {
                let frame = crate::wire::read_frame_into_with_limit(
                    &mut recv,
                    &mut scratch,
                    false,
                    1 << 20,
                )
                .await?
                .context("publish stream closed before all frames arrived")?;
                match Message::decode(frame).context("decode publish frame")? {
                    Message::PublishBatch { request_id, .. } => {
                        request_ids.push(request_id.context("acked batch missing request_id")?);
                    }
                    other => anyhow::bail!("unexpected message: {other:?}"),
                }
            }
            for request_id in request_ids {
                let frame = Message::PublishOk { request_id }.encode()?;
                send.write_all(&frame.encode()).await.context("write ack")?;
            }
            // Close like the broker does: finish the ack side so the client's
            // EOF drain completes, then wait out the client's own finish.
            send.finish()?;
            let _ = recv.read_to_end(1024).await;
            let _ = send.stopped().await;
            Result::<()>::Ok(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;
        let (tx, rx) = mpsc::channel(8);
        let worker = tokio::spawn(run_publisher_writer(send, recv, rx, 16 * 1024));

        let admission = Arc::new(Semaphore::new(1 << 20));
        let mut responses = Vec::new();
        for request_id in 1..=3u64 {
            let permit = admission
                .clone()
                .acquire_many_owned(64)
                .await
                .context("admission")?;
            let (response_tx, response_rx) = oneshot::channel();
            tx.send(PublishRequest::Message {
                message: Message::PublishBatch {
                    tenant_id: "t".into(),
                    namespace: "ns".into(),
                    stream: "s".into(),
                    payloads: vec![b"x".to_vec()],
                    request_id: Some(request_id),
                    ack: Some(AckMode::PerBatch),
                },
                ack: AckMode::PerBatch,
                request_id: Some(request_id),
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("queue publish")?;
            responses.push(response_rx);
        }

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for response in responses {
                response.await.context("worker dropped response")??;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("acked publishes did not pipeline (inline ack wait deadlocks this server)")??;

        let (finish_tx, finish_rx) = oneshot::channel();
        tx.send(PublishRequest::Finish {
            response: finish_tx,
        })
        .await
        .context("queue finish")?;
        finish_rx.await.context("finish dropped")??;
        worker.await.context("join worker")??;
        server_task.await.context("join server")??;
        Ok(())
    }

    #[tokio::test]
    async fn finish_publisher_stream_drains_recv() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let _ = recv.read_to_end(1024).await?;
            send.write_all(b"pong").await?;
            send.finish()?;
            let _ = send.stopped().await;
            Result::<()>::Ok(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(b"ping").await?;

        finish_publisher_stream(&mut send, &mut recv).await?;
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn finish_skips_finish_request_when_channel_closed() {
        let (tx, rx) = mpsc::channel::<PublishRequest>(1);
        drop(rx);
        let handle = tokio::spawn(async { Ok(()) });
        let publisher = Publisher {
            inner: Arc::new(PublisherInner::new(
                Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                    server_flags: felix_wire::KNOWN_FLAGS,
                }]),
                PublishSharding::RoundRobin,
            )),
        };
        publisher.finish().await.expect("finish");
    }

    #[tokio::test]
    async fn run_publisher_writer_publish_batch_no_ack_ok() -> Result<()> {
        crate::timings::enable_collection(1);
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            recv.read_to_end(1024 * 1024).await?;
            send.finish()?;
            let _ = shutdown_rx.await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (client_send, client_recv) = connection.open_bi().await?;
        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::PublishBatch {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payloads: vec![b"a".to_vec(), b"b".to_vec()],
                request_id: None,
                ack: Some(AckMode::None),
            },
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: response_tx,
        })
        .await
        .context("send request")?;

        let response = response_rx.await.context("response dropped");
        drop(tx);
        let writer_result = writer_task.await.context("writer join");
        let _ = shutdown_tx.send(());
        let server_result = server_task.await.context("server join");
        if let Err(err) = server_result {
            return Err(err);
        }
        if let Err(err) = writer_result {
            return Err(err);
        }
        if let Err(err) = response {
            return Err(err.context("publish batch response"));
        }
        writer_result.unwrap()?;
        server_result.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_binary_bytes_success() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            recv.read_to_end(1024 * 1024).await?;
            send.finish()?;
            let _ = shutdown_rx.await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (client_send, client_recv) = connection.open_bi().await?;
        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

        let payloads = vec![b"p1".to_vec(), b"p2".to_vec()];
        let (bytes, _) =
            felix_wire::binary::encode_publish_batch_bytes_with_stats("t1", "ns", "s", &payloads)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(PublishRequest::BinaryBytes {
            bytes,
            item_count: payloads.len(),
            sample: false,
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: response_tx,
        })
        .await
        .context("send binary request")?;

        let response = response_rx.await.context("response dropped");
        drop(tx);
        let writer_result = writer_task.await.context("writer join");
        let _ = shutdown_tx.send(());
        let server_result = server_task.await.context("server join");
        if let Err(err) = server_result {
            return Err(err);
        }
        if let Err(err) = writer_result {
            return Err(err);
        }
        if let Err(err) = response {
            return Err(err.context("binary batch response"));
        }
        writer_result.unwrap()?;
        server_result.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_error_drains_queue() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut scratch = BytesMut::new();
            let frame = crate::wire::read_frame_into(&mut recv, &mut scratch, false)
                .await?
                .context("missing publish frame")?;
            let message = Message::decode(frame).context("decode message")?;
            let request_id = match message {
                Message::Publish { request_id, .. } => request_id,
                other => return Err(anyhow::anyhow!("unexpected message: {other:?}")),
            }
            .context("missing request_id")?;
            let ack = Message::PublishError {
                request_id,
                message: "denied".to_string(),
            };
            let frame = ack.encode().context("encode ack")?;
            send.write_all(&frame.encode()).await.context("write ack")?;
            send.finish().context("finish ack")?;
            let _ = recv.read_to_end(1024 * 1024).await;
            // A pipelined client sends its whole flight (and FIN) up front, so
            // reaching EOF here no longer implies the ack was transmitted.
            // Dropping the connection discards untransmitted data; wait until
            // the client has read the ack stream to completion.
            let _ = send.stopped().await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;

        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"bad".to_vec(),
                request_id: Some(1),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(1),
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .context("send request1")?;
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"queued".to_vec(),
                request_id: Some(2),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(2),
            _permit: test_publish_permit(),
            response: resp_tx2,
        })
        .await
        .context("send request2")?;
        drop(tx);

        let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
        assert!(!err1.to_string().is_empty());
        let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
        assert!(!err2.to_string().is_empty());

        let writer_err = writer_task.await.context("writer join")?;
        assert!(writer_err.is_err());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_ack_mismatch_drains_queue() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        // The connection must outlive the assertions: a pipelined client sends
        // its whole flight (and FIN) up front, so server-side EOF no longer
        // implies the client has consumed the ack, and dropping the connection
        // races the client's read of buffered stream data.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut scratch = BytesMut::new();
            let frame = crate::wire::read_frame_into(&mut recv, &mut scratch, false)
                .await?
                .context("missing publish frame")?;
            let message = Message::decode(frame).context("decode message")?;
            let request_id = match message {
                Message::Publish { request_id, .. } => request_id,
                other => return Err(anyhow::anyhow!("unexpected message: {other:?}")),
            }
            .context("missing request_id")?;
            let ack = Message::PublishOk {
                request_id: request_id + 1,
            };
            let frame = ack.encode().context("encode ack")?;
            send.write_all(&frame.encode()).await.context("write ack")?;
            send.finish().context("finish ack")?;
            let _ = recv.read_to_end(1024 * 1024).await;
            let _ = shutdown_rx.await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;

        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"bad".to_vec(),
                request_id: Some(9),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(9),
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .context("send request1")?;
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"queued".to_vec(),
                request_id: Some(10),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(10),
            _permit: test_publish_permit(),
            response: resp_tx2,
        })
        .await
        .context("send request2")?;
        drop(tx);

        let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
        assert!(err1.to_string().contains("publish failed"));
        let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
        assert!(err2.to_string().contains("publish failed"));

        let writer_err = writer_task.await.context("writer join")?;
        assert!(writer_err.is_err());
        let _ = shutdown_tx.send(());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_binary_bytes_write_error_drains_queue() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (_send, mut recv) = connection.accept_bi().await?;
            let drain_task = tokio::spawn(async move {
                let _ = recv.read_to_end(1024 * 1024).await;
            });
            let _ = shutdown_rx.await;
            drain_task.abort();
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (mut client_send, client_recv) = connection.open_bi().await?;
        client_send.finish().context("finish client send")?;

        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

        let payloads = vec![b"p1".to_vec()];
        let (bytes, _) =
            felix_wire::binary::encode_publish_batch_bytes_with_stats("t1", "ns", "s", &payloads)?;
        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(PublishRequest::BinaryBytes {
            bytes,
            item_count: payloads.len(),
            sample: false,
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .context("send binary request")?;
        tx.send(PublishRequest::Finish { response: resp_tx2 })
            .await
            .context("send finish request")?;
        drop(tx);

        let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
        assert!(!err1.to_string().is_empty());
        let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
        assert!(!err2.to_string().is_empty());

        let writer_err = writer_task.await.context("writer join")?;
        assert!(writer_err.is_err());
        let _ = shutdown_tx.send(());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_send_closed_drains_queue() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let _ = recv.read_to_end(1024 * 1024).await;
            send.finish().context("finish server send")?;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (mut client_send, client_recv) = connection.open_bi().await?;
        client_send.finish().context("finish client send")?;

        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"bad".to_vec(),
                request_id: Some(7),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(7),
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .context("send publish request")?;
        tx.send(PublishRequest::Finish { response: resp_tx2 })
            .await
            .context("send finish request")?;
        drop(tx);

        let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
        assert!(!err1.to_string().is_empty());
        let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
        assert!(!err2.to_string().is_empty());

        let writer_err = writer_task.await.context("writer join")?;
        assert!(writer_err.is_err());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_publish_batch_error_drains_queue() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut scratch = BytesMut::new();
            let _ = crate::wire::read_frame_into(&mut recv, &mut scratch, false)
                .await?
                .context("missing publish batch frame")?;
            let ack = Message::PublishError {
                request_id: 99,
                message: "denied".to_string(),
            };
            let frame = ack.encode().context("encode ack")?;
            send.write_all(&frame.encode()).await.context("write ack")?;
            send.finish().context("finish ack")?;
            let _ = recv.read_to_end(1024 * 1024).await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;

        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::PublishBatch {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payloads: vec![b"a".to_vec(), b"b".to_vec()],
                request_id: Some(99),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(99),
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .context("send request1")?;
        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: b"queued".to_vec(),
                request_id: Some(100),
                ack: Some(AckMode::PerMessage),
            },
            ack: AckMode::PerMessage,
            request_id: Some(100),
            _permit: test_publish_permit(),
            response: resp_tx2,
        })
        .await
        .context("send request2")?;
        drop(tx);

        let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
        assert!(!err1.to_string().is_empty());
        let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
        assert!(!err2.to_string().is_empty());

        let writer_err = writer_task.await.context("writer join")?;
        assert!(writer_err.is_err());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_publisher_writer_non_publish_message_ok() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let _ = recv.read_to_end(1024 * 1024).await;
            send.finish().context("finish server send")?;
            let _ = shutdown_rx.await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;

        let (tx, rx) = mpsc::channel(2);
        let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(PublishRequest::Message {
            message: Message::Error {
                message: "oops".to_string(),
            },
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: resp_tx,
        })
        .await
        .context("send error message")?;
        drop(tx);

        resp_rx.await.context("response dropped")??;
        writer_task.await.context("writer join")??;
        let _ = shutdown_tx.send(());
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn drain_publish_queue_returns_errors() {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(4);
        let (resp_tx1, resp_rx1) = oneshot::channel();
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let (resp_tx3, resp_rx3) = oneshot::channel();

        tx.send(PublishRequest::Message {
            message: Message::Publish {
                tenant_id: "t".to_string(),
                namespace: "ns".to_string(),
                stream: "s".to_string(),
                payload: vec![],
                request_id: None,
                ack: Some(AckMode::None),
            },
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: resp_tx1,
        })
        .await
        .expect("send message");
        tx.send(PublishRequest::BinaryBytes {
            bytes: Bytes::from_static(b"bin"),
            item_count: 1,
            sample: false,
            ack: AckMode::None,
            request_id: None,
            _permit: test_publish_permit(),
            response: resp_tx2,
        })
        .await
        .expect("send binary");
        tx.send(PublishRequest::Finish { response: resp_tx3 })
            .await
            .expect("send finish");
        drop(tx);

        drain_publish_queue(&mut rx, "closed").await;

        let err1 = resp_rx1.await.expect("resp1").expect_err("expected error");
        let err2 = resp_rx2.await.expect("resp2").expect_err("expected error");
        let err3 = resp_rx3.await.expect("resp3").expect_err("expected error");
        assert!(err1.to_string().contains("closed"));
        assert!(err2.to_string().contains("closed"));
        assert!(err3.to_string().contains("closed"));
    }
}
