// Subscription reader for event streams.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::Message;
use quinn::RecvStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::{log_decode_error, read_frame_into, record_e2e_latency};

pub struct Subscription {
    pub(crate) recv: RecvStream,
    pub(crate) frame_scratch: BytesMut,
    pub(crate) current_batch: Option<Vec<Bytes>>,
    pub(crate) current_index: usize,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    pub(crate) last_poll: Option<Instant>,
    pub(crate) tenant_id: Arc<str>,
    pub(crate) namespace: Arc<str>,
    pub(crate) stream: Arc<str>,
    pub(crate) subscription_id: Option<u64>,
    pub(crate) event_conn_index: usize,
    pub(crate) event_conn_counts: Arc<Vec<AtomicUsize>>,
}

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
}

impl Subscription {
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
        // Read from a cached batch first, then from the QUIC stream.
        if let Some(payload) = self.next_from_batch() {
            record_e2e_latency(&payload);
            return Ok(Some(self.event_from_payload(payload)));
        }
        #[cfg(feature = "telemetry")]
        let sample = crate::t_should_sample();
        #[cfg(feature = "telemetry")]
        let read_start = crate::t_now_if(sample);
        let frame = match read_frame_into(&mut self.recv, &mut self.frame_scratch, true).await? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        #[cfg(feature = "telemetry")]
        {
            t_histogram!("client_sub_frame_bytes").record(frame.header.length as f64);
            if let Some(start) = read_start {
                let read_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_read_wait_ns(read_ns);
                t_histogram!("sub_read_wait_ns").record(read_ns as f64);
            }
        }
        #[cfg(feature = "telemetry")]
        let decode_start = crate::t_now_if(sample);
        if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH != 0 {
            let batch = match felix_wire::binary::decode_event_batch(&frame)
                .context("decode binary event batch")
            {
                Ok(batch) => batch,
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    log_decode_error("binary_event_batch", &err, &frame);
                    return Err(err);
                }
            };
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                counters
                    .sub_items_in_ok
                    .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
            }
            if let Some(expected) = self.subscription_id
                && expected != batch.subscription_id
            {
                return Err(anyhow::anyhow!(
                    "subscription id mismatch: expected {expected} got {}",
                    batch.subscription_id
                ));
            }
            #[cfg(feature = "telemetry")]
            if let Some(start) = decode_start {
                let decode_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_decode_ns(decode_ns);
                t_histogram!("sub_decode_ns").record(decode_ns as f64);
            }
            return Ok(self.store_batch(batch.payloads));
        }
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                }
                log_decode_error("event_message", &err, &frame);
                return Err(err);
            }
        };
        #[cfg(feature = "telemetry")]
        {
            let counters = frame_counters();
            counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = decode_start {
            let decode_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_decode_ns(decode_ns);
            t_histogram!("sub_decode_ns").record(decode_ns as f64);
        }
        match message {
            Message::Event { payload, .. } => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters.sub_items_in_ok.fetch_add(1, Ordering::Relaxed);
                }
                Ok(self.store_batch(vec![Bytes::from(payload)]))
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
                let batch = payloads.into_iter().map(Bytes::from).collect();
                Ok(self.store_batch(batch))
            }
            _ => Err(anyhow::anyhow!("unexpected message on subscription stream")),
        }
    }

    fn store_batch(&mut self, batch: Vec<Bytes>) -> Option<Event> {
        // Store a new batch and return the first event immediately.
        if batch.is_empty() {
            return None;
        }
        self.current_index = 0;
        self.current_batch = Some(batch);
        self.next_from_batch().map(|payload| {
            record_e2e_latency(&payload);
            self.event_from_payload(payload)
        })
    }

    fn next_from_batch(&mut self) -> Option<Bytes> {
        let batch = self.current_batch.as_ref()?;
        if self.current_index >= batch.len() {
            self.current_batch = None;
            return None;
        }
        let payload = batch[self.current_index].clone();
        self.current_index += 1;
        if let Some(batch) = self.current_batch.as_ref()
            && self.current_index >= batch.len()
        {
            self.current_batch = None;
        }
        Some(payload)
    }

    fn event_from_payload(&self, payload: Bytes) -> Event {
        Event {
            tenant_id: Arc::clone(&self.tenant_id),
            namespace: Arc::clone(&self.namespace),
            stream: Arc::clone(&self.stream),
            payload,
        }
    }
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
