//! Opt-in timing collection for broker hot paths — demos, benchmarks, and perf
//! investigations, not production metrics.
//!
//! Sampling is a global counter modulo `sample_every`: best-effort and lossy.
//! Samples land in `Mutex<Vec<u64>>`s, which is fine at sampled rates but is
//! exactly why the whole thing stays behind explicit enablement.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

struct TimingCollector {
    decode_ns: Mutex<Vec<u64>>,
    fanout_ns: Mutex<Vec<u64>>,
    ack_write_ns: Mutex<Vec<u64>>,
    quic_write_ns: Mutex<Vec<u64>>,
    sub_queue_wait_ns: Mutex<Vec<u64>>,
    sub_prefix_ns: Mutex<Vec<u64>>,
    sub_write_ns: Mutex<Vec<u64>>,
    sub_write_await_ns: Mutex<Vec<u64>>,
    sub_delivery_ns: Mutex<Vec<u64>>,
    cache_read_ns: Mutex<Vec<u64>>,
    cache_decode_ns: Mutex<Vec<u64>>,
    cache_lookup_ns: Mutex<Vec<u64>>,
    cache_insert_ns: Mutex<Vec<u64>>,
    cache_encode_ns: Mutex<Vec<u64>>,
    cache_write_ns: Mutex<Vec<u64>>,
    cache_finish_ns: Mutex<Vec<u64>>,
    sample_every: usize,
    enabled: AtomicBool,
    counter: AtomicUsize,
}

static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();

/// Broker samples, in order: decode, fanout, ack_write, quic_write,
/// sub_queue_wait, sub_prefix, sub_write, sub_write_await, sub_delivery.
pub type BrokerTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);

/// Cache samples, in order: read, decode, lookup, insert, encode, write, finish.
pub type BrokerCacheTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);

/// Turn collection on, recording one sample per `sample_every` events
/// (minimum 1). Only the first call initializes; later calls are no-ops.
pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    let _ = COLLECTOR.set(TimingCollector {
        decode_ns: Mutex::new(Vec::new()),
        fanout_ns: Mutex::new(Vec::new()),
        ack_write_ns: Mutex::new(Vec::new()),
        quic_write_ns: Mutex::new(Vec::new()),
        sub_queue_wait_ns: Mutex::new(Vec::new()),
        sub_prefix_ns: Mutex::new(Vec::new()),
        sub_write_ns: Mutex::new(Vec::new()),
        sub_write_await_ns: Mutex::new(Vec::new()),
        sub_delivery_ns: Mutex::new(Vec::new()),
        cache_read_ns: Mutex::new(Vec::new()),
        cache_decode_ns: Mutex::new(Vec::new()),
        cache_lookup_ns: Mutex::new(Vec::new()),
        cache_insert_ns: Mutex::new(Vec::new()),
        cache_encode_ns: Mutex::new(Vec::new()),
        cache_write_ns: Mutex::new(Vec::new()),
        cache_finish_ns: Mutex::new(Vec::new()),
        sample_every,
        enabled: AtomicBool::new(true),
        counter: AtomicUsize::new(0),
    });
}

/// Pause or resume recording. Existing samples are kept.
pub fn set_enabled(enabled: bool) {
    if let Some(collector) = COLLECTOR.get() {
        collector.enabled.store(enabled, Ordering::Relaxed);
    }
}

/// Whether this event falls on the sampling stride.
pub fn should_sample() -> bool {
    let Some(collector) = COLLECTOR.get() else {
        return false;
    };
    if !collector.enabled.load(Ordering::Relaxed) {
        return false;
    }
    let idx = collector.counter.fetch_add(1, Ordering::Relaxed);
    idx % collector.sample_every == 0
}

pub fn record_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.decode_ns.lock().expect("decode lock");
        guard.push(value);
    }
}

pub fn record_fanout_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.fanout_ns.lock().expect("fanout lock");
        guard.push(value);
    }
}

pub fn record_ack_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_write_ns.lock().expect("ack write lock");
        guard.push(value);
    }
}

pub fn record_quic_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.quic_write_ns.lock().expect("quic write lock");
        guard.push(value);
    }
}

pub fn record_sub_queue_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_queue_wait_ns
            .lock()
            .expect("sub queue wait lock");
        guard.push(value);
    }
}

pub fn record_sub_prefix_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_prefix_ns.lock().expect("sub prefix lock");
        guard.push(value);
    }
}

pub fn record_sub_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_write_ns.lock().expect("sub write lock");
        guard.push(value);
    }
}

pub fn record_sub_write_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_write_await_ns
            .lock()
            .expect("sub write await lock");
        guard.push(value);
    }
}

pub fn record_sub_delivery_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_delivery_ns.lock().expect("sub delivery lock");
        guard.push(value);
    }
}

pub fn record_cache_read_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_read_ns.lock().expect("cache read lock");
        guard.push(value);
    }
}

pub fn record_cache_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_decode_ns.lock().expect("cache decode lock");
        guard.push(value);
    }
}

pub fn record_cache_lookup_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_lookup_ns.lock().expect("cache lookup lock");
        guard.push(value);
    }
}

pub fn record_cache_insert_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_insert_ns.lock().expect("cache insert lock");
        guard.push(value);
    }
}

pub fn record_cache_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_encode_ns.lock().expect("cache encode lock");
        guard.push(value);
    }
}

pub fn record_cache_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_write_ns.lock().expect("cache write lock");
        guard.push(value);
    }
}

pub fn record_cache_finish_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_finish_ns.lock().expect("cache finish lock");
        guard.push(value);
    }
}

/// Drain and return all broker samples. Holds every broker lock at once, so
/// call it between runs, not under load.
pub fn take_samples() -> Option<BrokerTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut decode = collector.decode_ns.lock().expect("decode lock");
    let mut fanout = collector.fanout_ns.lock().expect("fanout lock");
    let mut ack_write = collector.ack_write_ns.lock().expect("ack write lock");
    let mut quic_write = collector.quic_write_ns.lock().expect("quic write lock");
    let mut sub_queue_wait = collector
        .sub_queue_wait_ns
        .lock()
        .expect("sub queue wait lock");
    let mut sub_prefix = collector.sub_prefix_ns.lock().expect("sub prefix lock");
    let mut sub_write = collector.sub_write_ns.lock().expect("sub write lock");
    let mut sub_write_await = collector
        .sub_write_await_ns
        .lock()
        .expect("sub write await lock");
    let mut sub_delivery = collector.sub_delivery_ns.lock().expect("sub delivery lock");
    Some((
        std::mem::take(&mut *decode),
        std::mem::take(&mut *fanout),
        std::mem::take(&mut *ack_write),
        std::mem::take(&mut *quic_write),
        std::mem::take(&mut *sub_queue_wait),
        std::mem::take(&mut *sub_prefix),
        std::mem::take(&mut *sub_write),
        std::mem::take(&mut *sub_write_await),
        std::mem::take(&mut *sub_delivery),
    ))
}

/// Drain and return all cache samples. Same locking caveat as
/// [`take_samples`].
pub fn take_cache_samples() -> Option<BrokerCacheTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut read = collector.cache_read_ns.lock().expect("cache read lock");
    let mut decode = collector.cache_decode_ns.lock().expect("cache decode lock");
    let mut lookup = collector.cache_lookup_ns.lock().expect("cache lookup lock");
    let mut insert = collector.cache_insert_ns.lock().expect("cache insert lock");
    let mut encode = collector.cache_encode_ns.lock().expect("cache encode lock");
    let mut write = collector.cache_write_ns.lock().expect("cache write lock");
    let mut finish = collector.cache_finish_ns.lock().expect("cache finish lock");
    Some((
        std::mem::take(&mut *read),
        std::mem::take(&mut *decode),
        std::mem::take(&mut *lookup),
        std::mem::take(&mut *insert),
        std::mem::take(&mut *encode),
        std::mem::take(&mut *write),
        std::mem::take(&mut *finish),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_records_and_samples() {
        // Drain any prior samples from other tests to keep this test deterministic.
        set_enabled(false);
        let _ = take_samples();
        let _ = take_cache_samples();

        assert!(!should_sample());
        if let Some(samples) = take_samples() {
            assert!(samples.0.is_empty());
            assert!(samples.1.is_empty());
            assert!(samples.2.is_empty());
            assert!(samples.3.is_empty());
            assert!(samples.4.is_empty());
            assert!(samples.5.is_empty());
            assert!(samples.6.is_empty());
            assert!(samples.7.is_empty());
            assert!(samples.8.is_empty());
        }

        enable_collection(2);
        set_enabled(true);

        let _ = COLLECTOR.get().expect("collector");
        let _ = should_sample();
        let _ = should_sample();
        let _ = should_sample();

        record_decode_ns(10);
        record_fanout_ns(11);
        record_ack_write_ns(12);
        record_quic_write_ns(13);
        record_sub_queue_wait_ns(14);
        record_sub_prefix_ns(15);
        record_sub_write_ns(16);
        record_sub_write_await_ns(17);
        record_sub_delivery_ns(18);

        record_cache_read_ns(20);
        record_cache_decode_ns(21);
        record_cache_lookup_ns(22);
        record_cache_insert_ns(23);
        record_cache_encode_ns(24);
        record_cache_write_ns(25);
        record_cache_finish_ns(26);

        let samples = take_samples().expect("samples");
        assert!(samples.0.contains(&10));
        assert!(samples.1.contains(&11));
        assert!(samples.2.contains(&12));
        assert!(samples.3.contains(&13));
        assert!(samples.4.contains(&14));
        assert!(samples.5.contains(&15));
        assert!(samples.6.contains(&16));
        assert!(samples.7.contains(&17));
        assert!(samples.8.contains(&18));

        let cache_samples = take_cache_samples().expect("cache samples");
        assert!(cache_samples.0.contains(&20));
        assert!(cache_samples.1.contains(&21));
        assert!(cache_samples.2.contains(&22));
        assert!(cache_samples.3.contains(&23));
        assert!(cache_samples.4.contains(&24));
        assert!(cache_samples.5.contains(&25));
        assert!(cache_samples.6.contains(&26));

        set_enabled(false);
        assert!(!should_sample());
    }
}
