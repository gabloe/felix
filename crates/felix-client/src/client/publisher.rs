// Publisher worker pool and single-writer stream logic.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{AckMode, FrameHeader, Message};
use quinn::{RecvStream, SendStream};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::{
    bench_embed_ts_enabled, maybe_append_publish_ts, maybe_append_publish_ts_batch,
    maybe_wait_for_ack, write_frame_parts,
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
}

pub(crate) struct PublishWorker {
    pub(crate) tx: mpsc::Sender<PublishRequest>,
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    pub(crate) request_counter: AtomicU64,
}

pub(crate) enum PublishRequest {
    Message {
        message: Message,
        ack: AckMode,
        request_id: Option<u64>,
        response: oneshot::Sender<Result<()>>,
    },
    BinaryBytes {
        bytes: Bytes,
        item_count: usize,
        sample: bool,
        response: oneshot::Sender<Result<()>>,
    },
    Finish {
        response: oneshot::Sender<Result<()>>,
    },
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
        let index = match self.inner.sharding {
            PublishSharding::RoundRobin => {
                self.inner.rr.fetch_add(1, Ordering::Relaxed) % workers.len()
            }
            PublishSharding::HashStream => {
                let mut hasher = DefaultHasher::new();
                tenant_id.hash(&mut hasher);
                namespace.hash(&mut hasher);
                stream.hash(&mut hasher);
                (hasher.finish() as usize) % workers.len()
            }
        };
        Ok(&workers[index])
    }

    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payload = maybe_append_publish_ts(payload);
        // Enqueue publish on the single-writer publisher task.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message: Message::Publish {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payload,
                    request_id,
                    ack: Some(ack),
                },
                ack,
                request_id,
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

    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads = maybe_append_publish_ts_batch(payloads);
        // Batch publish uses the same queue/writer as single messages.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
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
                message: Message::PublishBatch {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payloads,
                    request_id,
                    ack: Some(ack),
                },
                ack,
                request_id,
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
        let payloads = if bench_embed_ts_enabled() {
            payloads_with_ts = payloads
                .iter()
                .map(|payload| maybe_append_publish_ts(payload.clone()))
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
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
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

pub(crate) async fn run_publisher_writer(
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
) -> Result<()> {
    // Single writer: serialize publish requests over one bi-directional stream.
    let mut ack_scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message {
                message,
                ack,
                request_id,
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
                    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + json_len);
                    buf.resize(FrameHeader::LEN, 0);
                    #[cfg(feature = "telemetry")]
                    let encode_start = crate::t_now_if(sample);
                    let stats = felix_wire::text::write_publish_batch_json(
                        &mut buf,
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
                    let payload_len = buf.len() - FrameHeader::LEN;
                    let header = FrameHeader::new(0, payload_len as u32);
                    let mut header_bytes = [0u8; FrameHeader::LEN];
                    header.encode_into(&mut header_bytes);
                    buf[..FrameHeader::LEN].copy_from_slice(&header_bytes);
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = build_start {
                        let build_ns = start.elapsed().as_nanos() as u64;
                        timings::record_text_batch_build_ns(build_ns);
                        t_histogram!("client_text_batch_build_ns").record(build_ns as f64);
                    }
                    let bytes = buf.freeze();
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
                    let result = match write_result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .bytes_out
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            }
                            maybe_wait_for_ack(&mut recv, ack, request_id, &mut ack_scratch).await
                        }
                        Err(err) => Err(err),
                    };
                    let item_count = payloads.len() as u64;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = item_count;
                    match result {
                        Ok(()) => {
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
                            drain_publish_queue(&mut rx, &message).await;
                            return Err(anyhow::anyhow!(message));
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
                    let result = match write_result {
                        Ok(()) => {
                            maybe_wait_for_ack(&mut recv, ack, request_id, &mut ack_scratch).await
                        }
                        Err(err) => Err(err),
                    };
                    let (batch_count, item_count) = match &other {
                        Message::Publish { .. } => (1u64, 1u64),
                        Message::PublishBatch { payloads, .. } => (1u64, payloads.len() as u64),
                        _ => (0u64, 0u64),
                    };
                    #[cfg(not(feature = "telemetry"))]
                    let _ = (batch_count, item_count);
                    match result {
                        Ok(()) => {
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
                            drain_publish_queue(&mut rx, &message).await;
                            return Err(anyhow::anyhow!(message));
                        }
                    }
                }
            },
            PublishRequest::BinaryBytes {
                bytes,
                item_count,
                sample,
                response,
            } => {
                #[cfg(not(feature = "telemetry"))]
                let _ = (item_count, sample);
                #[cfg(feature = "telemetry")]
                let write_start = crate::t_now_if(sample);
                #[cfg(feature = "telemetry")]
                let chunk_start = crate::t_now_if(sample);
                let result = send
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
                match result {
                    Ok(()) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .pub_items_out_ok
                                .fetch_add(item_count as u64, Ordering::Relaxed);
                            counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .bytes_out
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                        let _ = response.send(Ok(()));
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
                        drain_publish_queue(&mut rx, &message).await;
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
            PublishRequest::Finish { response } => {
                let result = finish_publisher_stream(&mut send, &mut recv).await;
                match result {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                        return Ok(());
                    }
                    Err(err) => {
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
        }
    }
    finish_publisher_stream(&mut send, &mut recv).await
}

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
            });
        }
        Publisher {
            inner: Arc::new(PublisherInner {
                workers: Arc::new(publish_workers),
                sharding,
                rr: AtomicUsize::new(0),
            }),
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
        use crate::config::{
            ClientRuntimeConfig, ClientSubQueuePolicy, DEFAULT_CLIENT_SUB_QUEUE_CAPACITY,
            DEFAULT_EVENT_ROUTER_MAX_PENDING, DEFAULT_MAX_FRAME_BYTES,
            install_runtime_config_for_tests, reset_runtime_config_for_tests,
        };

        reset_runtime_config_for_tests();
        install_runtime_config_for_tests(ClientRuntimeConfig {
            event_router_max_pending: DEFAULT_EVENT_ROUTER_MAX_PENDING,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            client_sub_queue_capacity: DEFAULT_CLIENT_SUB_QUEUE_CAPACITY,
            client_sub_queue_policy: ClientSubQueuePolicy::DropNew,
            bench_embed_ts: true,
        });

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
            inner: Arc::new(PublisherInner {
                workers: Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                }]),
                sharding: PublishSharding::RoundRobin,
                rr: AtomicUsize::new(0),
            }),
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
            inner: Arc::new(PublisherInner {
                workers: Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                }]),
                sharding: PublishSharding::RoundRobin,
                rr: AtomicUsize::new(0),
            }),
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
            response: response_tx,
        })
        .await
        .context("send request")?;

        let response = response_rx.await.context("response dropped");
        drop(tx);
        let _ = shutdown_tx.send(());
        let writer_result = writer_task.await.context("writer join");
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
            response: response_tx,
        })
        .await
        .context("send binary request")?;

        let response = response_rx.await.context("response dropped");
        drop(tx);
        let _ = shutdown_tx.send(());
        let writer_result = writer_task.await.context("writer join");
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
            response: resp_tx1,
        })
        .await
        .expect("send message");
        tx.send(PublishRequest::BinaryBytes {
            bytes: Bytes::from_static(b"bin"),
            item_count: 1,
            sample: false,
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
