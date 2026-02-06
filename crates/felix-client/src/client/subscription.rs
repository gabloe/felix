// Subscription reader pipeline for event streams.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, Message};
use quinn::RecvStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;

use crate::config::ClientSubQueuePolicy;
#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::{log_decode_error, read_frame_into, record_e2e_latency};

struct QueuedFrame {
    frame: Frame,
    enqueued_at: Instant,
}

enum QueuedEvent {
    Payload(Bytes),
    Error(anyhow::Error),
}

pub struct Subscription {
    event_rx: mpsc::Receiver<QueuedEvent>,
    #[cfg(feature = "telemetry")]
    pub(crate) last_poll: Option<Instant>,
    pub(crate) tenant_id: Arc<str>,
    pub(crate) namespace: Arc<str>,
    pub(crate) stream: Arc<str>,
    pub(crate) event_conn_index: usize,
    pub(crate) event_conn_counts: Arc<Vec<AtomicUsize>>,
}

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
}

pub(crate) struct SubscriptionPipelineConfig {
    pub(crate) recv: RecvStream,
    pub(crate) queue_capacity: usize,
    pub(crate) queue_policy: ClientSubQueuePolicy,
    pub(crate) subscription_id: u64,
    pub(crate) tenant_id: Arc<str>,
    pub(crate) namespace: Arc<str>,
    pub(crate) stream: Arc<str>,
    pub(crate) event_conn_index: usize,
    pub(crate) event_conn_counts: Arc<Vec<AtomicUsize>>,
}

impl Subscription {
    pub(crate) fn spawn_pipeline(config: SubscriptionPipelineConfig) -> Self {
        let capacity = config.queue_capacity.max(1);
        let (frame_tx, frame_rx) = mpsc::channel(capacity);
        let (event_tx, event_rx) = mpsc::channel(capacity);

        tokio::spawn(run_subscription_io_task(
            config.recv,
            frame_tx,
            config.queue_policy,
            capacity,
        ));
        tokio::spawn(run_subscription_dispatch_task(
            frame_rx,
            event_tx,
            config.queue_policy,
            capacity,
            config.subscription_id,
        ));

        Self {
            event_rx,
            #[cfg(feature = "telemetry")]
            last_poll: None,
            tenant_id: config.tenant_id,
            namespace: config.namespace,
            stream: config.stream,
            event_conn_index: config.event_conn_index,
            event_conn_counts: config.event_conn_counts,
        }
    }

    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        #[cfg(feature = "telemetry")]
        {
            let now = Instant::now();
            if let Some(last) = self.last_poll {
                let gap_ns = now.duration_since(last).as_nanos() as u64;
                t_histogram!("client_sub_consumer_gap_ns").record(gap_ns as f64);
                timings::record_sub_consumer_gap_ns(gap_ns);
            }
            self.last_poll = Some(now);
        }

        let Some(queued) = self.event_rx.recv().await else {
            return Ok(None);
        };
        match queued {
            QueuedEvent::Payload(payload) => {
                record_e2e_latency(&payload);
                Ok(Some(Event {
                    tenant_id: Arc::clone(&self.tenant_id),
                    namespace: Arc::clone(&self.namespace),
                    stream: Arc::clone(&self.stream),
                    payload,
                }))
            }
            QueuedEvent::Error(err) => Err(err),
        }
    }
}

async fn run_subscription_io_task(
    mut recv: RecvStream,
    frame_tx: mpsc::Sender<QueuedFrame>,
    queue_policy: ClientSubQueuePolicy,
    queue_capacity: usize,
) {
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
    #[cfg(feature = "telemetry")]
    let mut last_poll = Instant::now();
    loop {
        #[cfg(feature = "telemetry")]
        {
            let now = Instant::now();
            let poll_gap_ns = now.duration_since(last_poll).as_nanos() as u64;
            last_poll = now;
            timings::record_sub_poll_gap_ns(poll_gap_ns);
            t_histogram!("client_sub_poll_gap_ns").record(poll_gap_ns as f64);
        }

        let first = match read_frame_into(&mut recv, &mut frame_scratch, true).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(err) => {
                tracing::debug!(error = %err, "subscription io task stopped");
                break;
            }
        };
        if !enqueue_frame(
            &frame_tx,
            QueuedFrame {
                frame: first,
                enqueued_at: Instant::now(),
            },
            queue_policy,
            queue_capacity,
        )
        .await
        {
            break;
        }

        // Keep the IO task hot and predictable: read one frame, enqueue, then yield.
        // We intentionally avoid cancellation-based speculative reads here to preserve
        // stream framing safety across runtimes.
        tokio::task::yield_now().await;
    }
}

async fn run_subscription_dispatch_task(
    mut frame_rx: mpsc::Receiver<QueuedFrame>,
    event_tx: mpsc::Sender<QueuedEvent>,
    queue_policy: ClientSubQueuePolicy,
    queue_capacity: usize,
    subscription_id: u64,
) {
    while let Some(queued_frame) = frame_rx.recv().await {
        let queue_wait_ns = queued_frame.enqueued_at.elapsed().as_nanos() as u64;
        #[cfg(feature = "telemetry")]
        {
            timings::record_sub_queue_wait_ns(queue_wait_ns);
            t_histogram!("client_sub_queue_wait_ns").record(queue_wait_ns as f64);
            timings::record_sub_time_in_queue_ns(queue_wait_ns);
            t_histogram!("client_sub_time_in_queue_ns").record(queue_wait_ns as f64);
        }
        #[cfg(not(feature = "telemetry"))]
        let _ = queue_wait_ns;
        metrics::counter!("felix_client_sub_queue_dequeued_total").increment(1);
        metrics::gauge!("felix_client_sub_queue_len")
            .set((queue_capacity.saturating_sub(frame_rx.capacity())) as f64);

        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(feature = "telemetry")]
        let decode_start = crate::t_now_if(sample);

        let payloads = if queued_frame.frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH != 0
        {
            match felix_wire::binary::decode_event_batch(&queued_frame.frame)
                .context("decode binary event batch")
            {
                Ok(batch) => {
                    if batch.subscription_id != subscription_id {
                        tracing::debug!(
                            expected = subscription_id,
                            got = batch.subscription_id,
                            "subscription id mismatch in dispatch"
                        );
                        let _ = enqueue_event(
                            &event_tx,
                            QueuedEvent::Error(anyhow::anyhow!(
                                "subscription id mismatch: expected {} got {}",
                                subscription_id,
                                batch.subscription_id
                            )),
                            queue_policy,
                            queue_capacity,
                        )
                        .await;
                        return;
                    }
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                        counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                        counters
                            .sub_items_in_ok
                            .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
                    }
                    batch.payloads
                }
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    log_decode_error("binary_event_batch", &err, &queued_frame.frame);
                    let _ = enqueue_event(
                        &event_tx,
                        QueuedEvent::Error(err.context("decode binary event batch")),
                        queue_policy,
                        queue_capacity,
                    )
                    .await;
                    return;
                }
            }
        } else {
            let message =
                match Message::decode(queued_frame.frame.clone()).context("decode message") {
                    Ok(message) => message,
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        }
                        log_decode_error("event_message", &err, &queued_frame.frame);
                        let _ = enqueue_event(
                            &event_tx,
                            QueuedEvent::Error(err.context("decode message")),
                            queue_policy,
                            queue_capacity,
                        )
                        .await;
                        return;
                    }
                };
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
            }
            match message {
                Message::Event { payload, .. } => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                        counters.sub_items_in_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    vec![Bytes::from(payload)]
                }
                Message::EventBatch { payloads, .. } => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                        counters
                            .sub_items_in_ok
                            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
                    }
                    payloads.into_iter().map(Bytes::from).collect()
                }
                _ => {
                    let _ = enqueue_event(
                        &event_tx,
                        QueuedEvent::Error(anyhow::anyhow!(
                            "unexpected message on subscription stream"
                        )),
                        queue_policy,
                        queue_capacity,
                    )
                    .await;
                    return;
                }
            }
        };

        #[cfg(feature = "telemetry")]
        if let Some(start) = decode_start {
            let decode_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_decode_ns(decode_ns);
            t_histogram!("sub_decode_ns").record(decode_ns as f64);
        }

        #[cfg(feature = "telemetry")]
        let dispatch_start = crate::t_now_if(sample);
        for payload in payloads {
            if !enqueue_event(
                &event_tx,
                QueuedEvent::Payload(payload),
                queue_policy,
                queue_capacity,
            )
            .await
            {
                return;
            }
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = dispatch_start {
            let dispatch_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_dispatch_ns(dispatch_ns);
            t_histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
        }
    }
}

async fn enqueue_frame(
    tx: &mpsc::Sender<QueuedFrame>,
    item: QueuedFrame,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
) -> bool {
    enqueue_with_policy(
        tx,
        item,
        policy,
        queue_capacity,
        "felix_client_sub_queue_enqueued_total",
        "felix_client_sub_queue_dropped_total",
        "felix_client_sub_queue_drop_old_emulated_total",
    )
    .await
}

async fn enqueue_event(
    tx: &mpsc::Sender<QueuedEvent>,
    item: QueuedEvent,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
) -> bool {
    enqueue_with_policy(
        tx,
        item,
        policy,
        queue_capacity,
        "felix_client_sub_dispatch_enqueued_total",
        "felix_client_sub_dispatch_dropped_total",
        "felix_client_sub_dispatch_drop_old_emulated_total",
    )
    .await
}

async fn enqueue_with_policy<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
    enqueued_metric: &'static str,
    dropped_metric: &'static str,
    drop_old_emulated_metric: &'static str,
) -> bool {
    match policy {
        ClientSubQueuePolicy::Block => {
            if tx.send(item).await.is_err() {
                return false;
            }
            metrics::counter!(enqueued_metric).increment(1);
        }
        ClientSubQueuePolicy::DropNew => match tx.try_send(item) {
            Ok(()) => {
                metrics::counter!(enqueued_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(dropped_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        },
        ClientSubQueuePolicy::DropOld => match tx.try_send(item) {
            Ok(()) => {
                metrics::counter!(enqueued_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(dropped_metric).increment(1);
                metrics::counter!(drop_old_emulated_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        },
    }
    metrics::gauge!("felix_client_sub_queue_len")
        .set((queue_capacity.saturating_sub(tx.capacity())) as f64);
    true
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Update connection-level subscription counts for metrics.
        let counter = &self.event_conn_counts[self.event_conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!(
                        "felix_client_event_conn_subscriptions",
                        "conn" => self.event_conn_index.to_string()
                    )
                    .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }
    }
}
