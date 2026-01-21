use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

struct TimingCollector {
    decode_ns: Mutex<Vec<u64>>,
    fanout_ns: Mutex<Vec<u64>>,
    ack_write_ns: Mutex<Vec<u64>>,
    quic_write_ns: Mutex<Vec<u64>>,
    sub_queue_wait_ns: Mutex<Vec<u64>>,
    sub_write_ns: Mutex<Vec<u64>>,
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

pub type BrokerTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);
pub type BrokerCacheTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);

pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    let _ = COLLECTOR.set(TimingCollector {
        decode_ns: Mutex::new(Vec::new()),
        fanout_ns: Mutex::new(Vec::new()),
        ack_write_ns: Mutex::new(Vec::new()),
        quic_write_ns: Mutex::new(Vec::new()),
        sub_queue_wait_ns: Mutex::new(Vec::new()),
        sub_write_ns: Mutex::new(Vec::new()),
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

pub fn set_enabled(enabled: bool) {
    if let Some(collector) = COLLECTOR.get() {
        collector.enabled.store(enabled, Ordering::Relaxed);
    }
}

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

pub fn record_sub_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_write_ns.lock().expect("sub write lock");
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
    let mut sub_write = collector.sub_write_ns.lock().expect("sub write lock");
    let mut sub_delivery = collector.sub_delivery_ns.lock().expect("sub delivery lock");
    Some((
        std::mem::take(&mut *decode),
        std::mem::take(&mut *fanout),
        std::mem::take(&mut *ack_write),
        std::mem::take(&mut *quic_write),
        std::mem::take(&mut *sub_queue_wait),
        std::mem::take(&mut *sub_write),
        std::mem::take(&mut *sub_delivery),
    ))
}

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
