// Optional, low-level timing collection for broker hot paths.
//
// This module provides facilities to collect timing samples for various broker operations,
// primarily intended for demos, benchmarks, and performance investigations.
// It is not designed as a production-grade metrics solution.
//
// # Sampling Model
//
// Sampling is controlled via a global `sample_every` parameter, which determines the frequency
// of samples collected. An atomic counter is incremented on each potential sample point,
// and samples are collected when the counter modulo `sample_every` equals zero.
// This model is best-effort and lossy: not all events are sampled, and no guarantees
// are made about sample completeness or ordering.
//
// # Data Structures and Trade-offs
//
// Timing data is stored in `Mutex<Vec<u64>>` containers. The use of `Mutex` ensures thread-safe
// mutable access to the vectors, which are dynamically sized to accommodate varying sample counts.
// This approach is simple and effective for low-frequency sampling but may incur lock contention
// under high concurrency and does not provide real-time guarantees.
//
// Because of these trade-offs and the potential performance impact, this timing collection
// functionality is guarded behind a feature flag or explicit enablement.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

/// Internal collector of timing samples for broker operations.
///
/// This struct holds timing samples for different broker processing stages,
/// divided into two main groups:
///
/// - Broker timing samples: timings related to message processing, fanout, acknowledgments,
///   QUIC writes, and subscriber queue handling.
/// - Cache timing samples: timings related to cache read, decode, lookup, insert, encode,
///   write, and finish operations.
///
/// Each timing vector is protected by a `Mutex` to allow concurrent access from multiple threads.
/// Sampling frequency and enabled state are controlled via atomic variables.
struct TimingCollector {
    /// Broker timing samples for decode stage.
    decode_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for fanout stage.
    fanout_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for acknowledgment write stage.
    ack_write_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for QUIC write stage.
    quic_write_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for subscriber queue wait stage.
    sub_queue_wait_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for subscriber frame-prefix build stage.
    sub_prefix_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for subscriber write stage.
    sub_write_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for subscriber write-await stage.
    sub_write_await_ns: Mutex<Vec<u64>>,
    /// Broker timing samples for subscriber delivery stage.
    sub_delivery_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache read stage.
    cache_read_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache decode stage.
    cache_decode_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache lookup stage.
    cache_lookup_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache insert stage.
    cache_insert_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache encode stage.
    cache_encode_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache write stage.
    cache_write_ns: Mutex<Vec<u64>>,
    /// Cache timing samples for cache finish stage.
    cache_finish_ns: Mutex<Vec<u64>>,
    /// Sampling frequency: collect one sample every `sample_every` events.
    sample_every: usize,
    /// Flag indicating whether collection is enabled.
    enabled: AtomicBool,
    /// Atomic counter used to determine sampling points.
    counter: AtomicUsize,
}

/// Global singleton instance of the timing collector.
///
/// `OnceLock` is used to ensure the collector is initialized exactly once.
/// This avoids the overhead of synchronization on every sample recording.
static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();

/// Tuple of broker timing sample vectors.
///
/// The order corresponds to:
/// (decode_ns, fanout_ns, ack_write_ns, quic_write_ns, sub_queue_wait_ns, sub_prefix_ns, sub_write_ns, sub_write_await_ns, sub_delivery_ns)
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

/// Tuple of cache timing sample vectors.
///
/// The order corresponds to:
/// (cache_read_ns, cache_decode_ns, cache_lookup_ns, cache_insert_ns, cache_encode_ns, cache_write_ns, cache_finish_ns)
pub type BrokerCacheTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);

/// Enables timing collection with the specified sampling frequency.
///
/// `sample_every` controls how often samples are collected: one sample is recorded every
/// `sample_every` events. The minimum value is 1 (collect every event).
///
/// This function initializes the global collector and should be called once before recording.
///
/// # Costs
///
/// Allocates empty vectors for sample storage. Sampling incurs lock acquisition costs
/// and atomic counter increments during recording.
///
/// # Safety
///
/// Calling this multiple times has no effect after the first initialization.
pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    // Initialize the collector exactly once. Subsequent calls will be ignored.
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

/// Enables or disables timing collection at runtime.
///
/// When disabled, no new samples will be recorded, but existing samples remain.
///
/// # Costs
///
/// Atomic store operation with `Relaxed` ordering (low cost).
///
/// # Safety
///
/// Safe to call concurrently.
pub fn set_enabled(enabled: bool) {
    if let Some(collector) = COLLECTOR.get() {
        // Use Relaxed ordering for performance; exact synchronization is not critical here.
        collector.enabled.store(enabled, Ordering::Relaxed);
    }
}

/// Determines whether the current event should be sampled.
///
/// This function increments an atomic counter and returns `true` if the event should be sampled
/// according to the configured sampling frequency.
///
/// # Costs
///
/// Atomic fetch_add operation with `Relaxed` ordering (very low cost).
///
/// # Safety
///
/// Safe to call concurrently.
pub fn should_sample() -> bool {
    let Some(collector) = COLLECTOR.get() else {
        return false;
    };
    if !collector.enabled.load(Ordering::Relaxed) {
        return false;
    }
    // Increment counter atomically; sampling is determined by modulo operation.
    let idx = collector.counter.fetch_add(1, Ordering::Relaxed);
    idx % collector.sample_every == 0
}

/// Records a decode stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.decode_ns.lock().expect("decode lock");
        guard.push(value);
    }
}

/// Records a fanout stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_fanout_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.fanout_ns.lock().expect("fanout lock");
        guard.push(value);
    }
}

/// Records an acknowledgment write stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_ack_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_write_ns.lock().expect("ack write lock");
        guard.push(value);
    }
}

/// Records a QUIC write stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_quic_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.quic_write_ns.lock().expect("quic write lock");
        guard.push(value);
    }
}

/// Records a subscriber queue wait stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_sub_queue_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_queue_wait_ns
            .lock()
            .expect("sub queue wait lock");
        guard.push(value);
    }
}

/// Records a subscriber frame-prefix build timing sample in nanoseconds.
pub fn record_sub_prefix_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_prefix_ns.lock().expect("sub prefix lock");
        guard.push(value);
    }
}

/// Records a subscriber write stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_sub_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_write_ns.lock().expect("sub write lock");
        guard.push(value);
    }
}

/// Records a subscriber write-await timing sample in nanoseconds.
pub fn record_sub_write_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_write_await_ns
            .lock()
            .expect("sub write await lock");
        guard.push(value);
    }
}

/// Records a subscriber delivery stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_sub_delivery_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_delivery_ns.lock().expect("sub delivery lock");
        guard.push(value);
    }
}

/// Records a cache read stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_read_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_read_ns.lock().expect("cache read lock");
        guard.push(value);
    }
}

/// Records a cache decode stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_decode_ns.lock().expect("cache decode lock");
        guard.push(value);
    }
}

/// Records a cache lookup stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_lookup_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_lookup_ns.lock().expect("cache lookup lock");
        guard.push(value);
    }
}

/// Records a cache insert stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_insert_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_insert_ns.lock().expect("cache insert lock");
        guard.push(value);
    }
}

/// Records a cache encode stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_encode_ns.lock().expect("cache encode lock");
        guard.push(value);
    }
}

/// Records a cache write stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_write_ns.lock().expect("cache write lock");
        guard.push(value);
    }
}

/// Records a cache finish stage timing sample in nanoseconds.
///
/// # Costs
///
/// Acquires a mutex lock to append to the internal vector (may block).
///
/// # Safety
///
/// Safe to call concurrently. Lock contention may occur under high concurrency.
pub fn record_cache_finish_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_finish_ns.lock().expect("cache finish lock");
        guard.push(value);
    }
}

/// Takes and clears all collected broker timing samples.
///
/// This function drains the internal vectors and returns the collected samples as tuples.
/// It uses `std::mem::take` to replace the vectors with empty ones, avoiding allocation
/// but requiring exclusive access via mutex locks.
///
/// # Costs
///
/// Acquires multiple mutex locks simultaneously, which may block other recording threads.
///
/// # Safety
///
/// Should be called when no recording is expected or during controlled shutdown to avoid
/// contention and ensure sample consistency.
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
        // Replace vectors with empty ones, returning the collected samples.
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

/// Takes and clears all collected cache timing samples.
///
/// This function drains the internal vectors and returns the collected samples as tuples.
/// It uses `std::mem::take` to replace the vectors with empty ones, avoiding allocation
/// but requiring exclusive access via mutex locks.
///
/// # Costs
///
/// Acquires multiple mutex locks simultaneously, which may block other recording threads.
///
/// # Safety
///
/// Should be called when no recording is expected or during controlled shutdown to avoid
/// contention and ensure sample consistency.
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
        // Replace vectors with empty ones, returning the collected samples.
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
