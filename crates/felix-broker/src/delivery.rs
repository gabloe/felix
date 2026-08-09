// Shared delivery batches and the queue-depth accounting that rides with them.
//
// `QueuedDelivery` owns one unit of queue depth: the count is incremented by the
// publisher between reserving a permit and sending, and released in `Drop`. Keeping
// the increment, the `Drop`, and `decrement_queue_depth` together is what makes
// depth accounting leak-free across receiver drops and cancelled `recv` calls.

use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct DeliveryEnvelope {
    inner: Arc<DeliveryBatch>,
}

#[derive(Debug)]
pub(crate) struct QueuedDelivery {
    envelope: Option<DeliveryEnvelope>,
    item_count: usize,
    queued_items: Arc<AtomicUsize>,
}

impl QueuedDelivery {
    pub(crate) fn new(envelope: DeliveryEnvelope, queued_items: Arc<AtomicUsize>) -> Self {
        let item_count = envelope.len();
        Self {
            envelope: Some(envelope),
            item_count,
            queued_items,
        }
    }

    pub(crate) fn into_envelope(mut self) -> DeliveryEnvelope {
        self.envelope.take().expect("queued delivery has envelope")
    }
}

impl Drop for QueuedDelivery {
    fn drop(&mut self) {
        decrement_queue_depth(&self.queued_items, self.item_count);
    }
}

#[derive(Debug)]
struct DeliveryBatch {
    payloads: Arc<[Bytes]>,
    enqueued_at: Instant,
    encoded_frame: Mutex<Option<Bytes>>,
}

impl DeliveryEnvelope {
    pub(crate) fn new(payloads: &[Bytes]) -> Self {
        Self {
            inner: Arc::new(DeliveryBatch {
                payloads: Arc::from(payloads),
                enqueued_at: Instant::now(),
                encoded_frame: Mutex::new(None),
            }),
        }
    }

    pub fn payloads(&self) -> &[Bytes] {
        &self.inner.payloads
    }

    pub fn len(&self) -> usize {
        self.inner.payloads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.payloads.is_empty()
    }

    pub fn shared_event_frame(&self) -> felix_wire::Result<Bytes> {
        let mut cached = self.inner.encoded_frame.lock();
        if let Some(frame) = cached.as_ref() {
            return Ok(frame.clone());
        }
        let frame = felix_wire::binary::encode_shared_event_batch_bytes(&self.inner.payloads)?;
        *cached = Some(frame.clone());
        Ok(frame)
    }

    pub fn enqueued_at(&self) -> Instant {
        self.inner.enqueued_at
    }
}

pub(crate) fn decrement_queue_depth(queued_items: &AtomicUsize, count: usize) {
    if queued_items
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_sub(count)
        })
        .is_ok()
    {
        metrics::gauge!("felix_sub_queue_len").decrement(count as f64);
        metrics::counter!("felix_sub_queue_dequeued_total").increment(count as u64);
    }
}
