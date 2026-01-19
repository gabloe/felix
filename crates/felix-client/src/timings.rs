use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

struct TimingCollector {
    encode_ns: Mutex<Vec<u64>>,
    write_ns: Mutex<Vec<u64>>,
    send_await_ns: Mutex<Vec<u64>>,
    sub_read_wait_ns: Mutex<Vec<u64>>,
    sub_decode_ns: Mutex<Vec<u64>>,
    sub_dispatch_ns: Mutex<Vec<u64>>,
    ack_read_wait_ns: Mutex<Vec<u64>>,
    ack_decode_ns: Mutex<Vec<u64>>,
    cache_encode_ns: Mutex<Vec<u64>>,
    cache_open_stream_ns: Mutex<Vec<u64>>,
    cache_write_ns: Mutex<Vec<u64>>,
    cache_finish_ns: Mutex<Vec<u64>>,
    cache_read_wait_ns: Mutex<Vec<u64>>,
    cache_read_drain_ns: Mutex<Vec<u64>>,
    cache_decode_ns: Mutex<Vec<u64>>,
    cache_validate_ns: Mutex<Vec<u64>>,
    sample_every: usize,
    enabled: AtomicBool,
    counter: AtomicUsize,
}

static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();

pub type ClientTimingSamples = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
);

pub type ClientCacheTimingSamples = (
    Vec<u64>,
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
        encode_ns: Mutex::new(Vec::new()),
        write_ns: Mutex::new(Vec::new()),
        send_await_ns: Mutex::new(Vec::new()),
        sub_read_wait_ns: Mutex::new(Vec::new()),
        sub_decode_ns: Mutex::new(Vec::new()),
        sub_dispatch_ns: Mutex::new(Vec::new()),
        ack_read_wait_ns: Mutex::new(Vec::new()),
        ack_decode_ns: Mutex::new(Vec::new()),
        cache_encode_ns: Mutex::new(Vec::new()),
        cache_open_stream_ns: Mutex::new(Vec::new()),
        cache_write_ns: Mutex::new(Vec::new()),
        cache_finish_ns: Mutex::new(Vec::new()),
        cache_read_wait_ns: Mutex::new(Vec::new()),
        cache_read_drain_ns: Mutex::new(Vec::new()),
        cache_decode_ns: Mutex::new(Vec::new()),
        cache_validate_ns: Mutex::new(Vec::new()),
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

pub fn record_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.encode_ns.lock().expect("encode lock");
        guard.push(value);
    }
}

pub fn record_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.write_ns.lock().expect("write lock");
        guard.push(value);
    }
}

pub fn record_send_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.send_await_ns.lock().expect("send await lock");
        guard.push(value);
    }
}

pub fn record_sub_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_read_wait_ns.lock().expect("sub read lock");
        guard.push(value);
    }
}

pub fn record_sub_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_decode_ns.lock().expect("sub decode lock");
        guard.push(value);
    }
}

pub fn record_sub_dispatch_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_dispatch_ns.lock().expect("sub dispatch lock");
        guard.push(value);
    }
}

pub fn record_ack_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_read_wait_ns.lock().expect("ack read lock");
        guard.push(value);
    }
}

pub fn record_ack_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_decode_ns.lock().expect("ack decode lock");
        guard.push(value);
    }
}

pub fn record_cache_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_encode_ns.lock().expect("cache encode lock");
        guard.push(value);
    }
}

pub fn record_cache_open_stream_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_open_stream_ns
            .lock()
            .expect("cache open stream lock");
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

pub fn record_cache_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_read_wait_ns
            .lock()
            .expect("cache read wait lock");
        guard.push(value);
    }
}

pub fn record_cache_read_drain_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_read_drain_ns
            .lock()
            .expect("cache read drain lock");
        guard.push(value);
    }
}

pub fn record_cache_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_decode_ns.lock().expect("cache decode lock");
        guard.push(value);
    }
}

pub fn record_cache_validate_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_validate_ns
            .lock()
            .expect("cache validate lock");
        guard.push(value);
    }
}

pub fn take_samples() -> Option<ClientTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut encode = collector.encode_ns.lock().expect("encode lock");
    let mut write = collector.write_ns.lock().expect("write lock");
    let mut send_await = collector.send_await_ns.lock().expect("send await lock");
    let mut sub_read = collector.sub_read_wait_ns.lock().expect("sub read lock");
    let mut sub_decode = collector.sub_decode_ns.lock().expect("sub decode lock");
    let mut sub_dispatch = collector.sub_dispatch_ns.lock().expect("sub dispatch lock");
    let mut ack_read = collector.ack_read_wait_ns.lock().expect("ack read lock");
    let mut ack_decode = collector.ack_decode_ns.lock().expect("ack decode lock");
    Some((
        std::mem::take(&mut *encode),
        std::mem::take(&mut *write),
        std::mem::take(&mut *send_await),
        std::mem::take(&mut *sub_read),
        std::mem::take(&mut *sub_decode),
        std::mem::take(&mut *sub_dispatch),
        std::mem::take(&mut *ack_read),
        std::mem::take(&mut *ack_decode),
    ))
}

pub fn take_cache_samples() -> Option<ClientCacheTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut encode = collector.cache_encode_ns.lock().expect("cache encode lock");
    let mut open_stream = collector
        .cache_open_stream_ns
        .lock()
        .expect("cache open stream lock");
    let mut write = collector.cache_write_ns.lock().expect("cache write lock");
    let mut finish = collector.cache_finish_ns.lock().expect("cache finish lock");
    let mut read_wait = collector
        .cache_read_wait_ns
        .lock()
        .expect("cache read wait lock");
    let mut read_drain = collector
        .cache_read_drain_ns
        .lock()
        .expect("cache read drain lock");
    let mut decode = collector.cache_decode_ns.lock().expect("cache decode lock");
    let mut validate = collector
        .cache_validate_ns
        .lock()
        .expect("cache validate lock");
    Some((
        std::mem::take(&mut *encode),
        std::mem::take(&mut *open_stream),
        std::mem::take(&mut *write),
        std::mem::take(&mut *finish),
        std::mem::take(&mut *read_wait),
        std::mem::take(&mut *read_drain),
        std::mem::take(&mut *decode),
        std::mem::take(&mut *validate),
    ))
}
