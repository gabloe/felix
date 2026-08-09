// Per-stream state: the subscriber registry, its lock-free publish snapshot, and
// the bounded in-memory log that backs cursor replay.

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::Mutex;
use slab::Slab;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::mpsc;

use crate::config::SubQueuePolicy;
use crate::delivery::QueuedDelivery;
use crate::subscription::SubscriptionReceiver;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub(crate) next_seq: u64,
}

impl Cursor {
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[derive(Debug)]
pub(crate) struct LogEntry {
    pub(crate) seq: u64,
    pub(crate) payload: Bytes,
}

#[derive(Debug)]
pub(crate) struct StreamState {
    pub(crate) handle_id: u64,
    pub(crate) active: AtomicBool,
    // Snapshot used by publish hot path: lock-free read, no per-publish allocation.
    pub(crate) subscribers_snapshot: ArcSwap<Vec<SubscriberEntry>>,
    // Inner registry mutated only on subscribe/unsubscribe paths.
    pub(crate) subscribers: Mutex<SubscriberRegistry>,
    // In-memory log for cursor-based replay.
    pub(crate) log_state: Mutex<LogState>,
    // Per-subscriber bounded queue depth.
    pub(crate) subscriber_queue_capacity: usize,
    // Queue admission policy when subscriber queue is full.
    pub(crate) subscriber_queue_policy: SubQueuePolicy,
    // Approximate number of queued items across subscribers in this stream.
    pub(crate) queued_items: Arc<AtomicUsize>,
}

#[derive(Debug, Default)]
pub(crate) struct SubscriberRegistry {
    pub(crate) senders: Slab<mpsc::Sender<QueuedDelivery>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriberEntry {
    pub(crate) id: usize,
    pub(crate) sender: mpsc::Sender<QueuedDelivery>,
}

#[derive(Debug)]
pub(crate) struct LogState {
    // Bounded log; oldest entries are dropped as new ones arrive.
    pub(crate) log: VecDeque<LogEntry>,
    // Next sequence number to assign.
    pub(crate) next_seq: u64,
}

impl StreamState {
    pub(crate) fn new(
        handle_id: u64,
        subscriber_queue_capacity: usize,
        subscriber_queue_policy: SubQueuePolicy,
    ) -> Self {
        Self {
            handle_id,
            active: AtomicBool::new(true),
            subscribers_snapshot: ArcSwap::from_pointee(Vec::new()),
            subscribers: Mutex::new(SubscriberRegistry::default()),
            log_state: Mutex::new(LogState {
                log: VecDeque::new(),
                next_seq: 0,
            }),
            subscriber_queue_capacity,
            subscriber_queue_policy,
            queued_items: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    pub(crate) fn register_subscriber(&self) -> (u64, SubscriptionReceiver) {
        let mut state = self.subscribers.lock();
        let (tx, rx) = mpsc::channel(self.subscriber_queue_capacity);
        let id = state.senders.insert(tx);
        self.rebuild_subscriber_snapshot(&state);
        (id as u64, SubscriptionReceiver::new(rx))
    }

    pub(crate) fn remove_subscriber(&self, id: u64) {
        let mut state = self.subscribers.lock();
        let id = id as usize;
        if state.senders.contains(id) {
            state.senders.remove(id);
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    pub(crate) fn remove_subscribers(&self, subscriber_ids: &[u64]) {
        let mut state = self.subscribers.lock();
        let mut removed = false;
        for subscriber_id in subscriber_ids {
            let id = *subscriber_id as usize;
            if state.senders.contains(id) {
                state.senders.remove(id);
                removed = true;
            }
        }
        if removed {
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    #[inline]
    pub(crate) fn subscriber_snapshot(&self) -> Arc<Vec<SubscriberEntry>> {
        self.subscribers_snapshot.load_full()
    }

    pub(crate) fn rebuild_subscriber_snapshot(&self, state: &SubscriberRegistry) {
        let mut snapshot = Vec::with_capacity(state.senders.len());
        for (id, sender) in state.senders.iter() {
            snapshot.push(SubscriberEntry {
                id,
                sender: sender.clone(),
            });
        }
        self.subscribers_snapshot.store(Arc::new(snapshot));
    }

    #[cfg(test)]
    pub(crate) fn subscriber_count(&self) -> usize {
        let state = self.subscribers.lock();
        state.senders.len()
    }

    pub(crate) fn append_batch(&self, payloads: &[Bytes], log_capacity: usize) {
        if payloads.is_empty() {
            return;
        }

        // Hot path: one lock per publish batch (instead of per payload).
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let mut state = self.log_state.lock();
        #[cfg(feature = "perf_debug")]
        {
            let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
            metrics::histogram!("felix_perf_log_lock_wait_ns").record(wait_ns as f64);
        }
        #[cfg(debug_assertions)]
        {
            // Debug-only invariant check kept outside the append loop.
            if let Some(last) = state.log.back() {
                debug_assert!(last.seq < state.next_seq);
            }
        }

        for payload in payloads {
            let seq = state.next_seq;
            state.next_seq = state
                .next_seq
                .checked_add(1)
                .expect("log sequence overflow");
            state.log.push_back(LogEntry {
                seq,
                payload: payload.clone(),
            });
        }

        // Trim once after append to keep the newest `log_capacity` entries.
        let overflow = state.log.len().saturating_sub(log_capacity);
        if overflow > 0 {
            state.log.drain(..overflow);
        }
    }

    pub(crate) fn snapshot_range(&self, from_seq: u64, to_seq: u64) -> (u64, Vec<Bytes>) {
        let state = self.log_state.lock();

        // We return this to let the caller know if they need to indicate
        // they are requesting entries which are too far back in time.
        let oldest = state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq);

        let backlog = state
            .log
            .iter()
            .filter(|entry| entry.seq >= from_seq && entry.seq <= to_seq)
            .map(|entry| entry.payload.clone())
            .collect();
        (oldest, backlog)
    }

    pub(crate) fn snapshot_from(&self, from_seq: u64) -> (u64, Vec<Bytes>) {
        self.snapshot_range(from_seq, u64::MAX)
    }

    pub(crate) fn tail_seq(&self) -> u64 {
        let state = self.log_state.lock();
        // The cursor tail points to the next sequence to be published.
        state.next_seq
    }

    pub(crate) fn increment_queue_depth(&self, count: usize) {
        self.queued_items.fetch_add(count, Ordering::Relaxed);
        metrics::gauge!("felix_sub_queue_len").increment(count as f64);
        metrics::counter!("felix_sub_queue_enqueued_total").increment(count as u64);
    }
}
