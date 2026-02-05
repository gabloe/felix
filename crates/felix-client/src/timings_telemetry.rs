use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

struct TimingCollector {
    publish_enqueue_wait_ns: Mutex<Vec<u64>>,
    encode_ns: Mutex<Vec<u64>>,
    binary_encode_ns: Mutex<Vec<u64>>,
    text_encode_ns: Mutex<Vec<u64>>,
    text_batch_build_ns: Mutex<Vec<u64>>,
    write_ns: Mutex<Vec<u64>>,
    send_await_ns: Mutex<Vec<u64>>,
    sub_read_wait_ns: Mutex<Vec<u64>>,
    sub_read_await_ns: Mutex<Vec<u64>>,
    sub_queue_wait_ns: Mutex<Vec<u64>>,
    sub_decode_ns: Mutex<Vec<u64>>,
    sub_dispatch_ns: Mutex<Vec<u64>>,
    sub_consumer_gap_ns: Mutex<Vec<u64>>,
    sub_poll_gap_ns: Mutex<Vec<u64>>,
    sub_time_in_queue_ns: Mutex<Vec<u64>>,
    sub_runtime_gap_ns: Mutex<Vec<u64>>,
    sub_delivery_chan_wait_ns: Mutex<Vec<u64>>,
    e2e_latency_ns: Mutex<Vec<u64>>,
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
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
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
        publish_enqueue_wait_ns: Mutex::new(Vec::new()),
        encode_ns: Mutex::new(Vec::new()),
        binary_encode_ns: Mutex::new(Vec::new()),
        text_encode_ns: Mutex::new(Vec::new()),
        text_batch_build_ns: Mutex::new(Vec::new()),
        write_ns: Mutex::new(Vec::new()),
        send_await_ns: Mutex::new(Vec::new()),
        sub_read_wait_ns: Mutex::new(Vec::new()),
        sub_read_await_ns: Mutex::new(Vec::new()),
        sub_queue_wait_ns: Mutex::new(Vec::new()),
        sub_decode_ns: Mutex::new(Vec::new()),
        sub_dispatch_ns: Mutex::new(Vec::new()),
        sub_consumer_gap_ns: Mutex::new(Vec::new()),
        sub_poll_gap_ns: Mutex::new(Vec::new()),
        sub_time_in_queue_ns: Mutex::new(Vec::new()),
        sub_runtime_gap_ns: Mutex::new(Vec::new()),
        sub_delivery_chan_wait_ns: Mutex::new(Vec::new()),
        e2e_latency_ns: Mutex::new(Vec::new()),
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

pub fn record_binary_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .binary_encode_ns
            .lock()
            .expect("binary encode lock");
        guard.push(value);
    }
}

pub fn record_text_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.text_encode_ns.lock().expect("text encode lock");
        guard.push(value);
    }
}

pub fn record_text_batch_build_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .text_batch_build_ns
            .lock()
            .expect("text batch build lock");
        guard.push(value);
    }
}

pub fn record_publish_enqueue_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .publish_enqueue_wait_ns
            .lock()
            .expect("publish enqueue wait lock");
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

pub fn record_sub_read_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_read_await_ns
            .lock()
            .expect("sub read await lock");
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

pub fn record_sub_consumer_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_consumer_gap_ns
            .lock()
            .expect("sub consumer gap lock");
        guard.push(value);
    }
}

pub fn record_sub_poll_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_poll_gap_ns
            .lock()
            .expect("sub poll gap lock");
        guard.push(value);
    }
}

pub fn record_sub_time_in_queue_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_time_in_queue_ns
            .lock()
            .expect("sub time in queue lock");
        guard.push(value);
    }
}

pub fn record_sub_runtime_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_runtime_gap_ns
            .lock()
            .expect("sub runtime gap lock");
        guard.push(value);
    }
}

pub fn record_sub_delivery_chan_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_delivery_chan_wait_ns
            .lock()
            .expect("sub delivery channel wait lock");
        guard.push(value);
    }
}

pub fn record_e2e_latency_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.e2e_latency_ns.lock().expect("e2e latency lock");
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
    let mut publish_enqueue_wait = collector
        .publish_enqueue_wait_ns
        .lock()
        .expect("publish enqueue wait lock");
    let mut encode = collector.encode_ns.lock().expect("encode lock");
    let mut binary_encode = collector
        .binary_encode_ns
        .lock()
        .expect("binary encode lock");
    let mut text_encode = collector.text_encode_ns.lock().expect("text encode lock");
    let mut text_batch_build = collector
        .text_batch_build_ns
        .lock()
        .expect("text batch build lock");
    let mut write = collector.write_ns.lock().expect("write lock");
    let mut send_await = collector.send_await_ns.lock().expect("send await lock");
    let mut sub_read = collector.sub_read_wait_ns.lock().expect("sub read lock");
    let mut sub_read_await = collector
        .sub_read_await_ns
        .lock()
        .expect("sub read await lock");
    let mut sub_queue_wait = collector
        .sub_queue_wait_ns
        .lock()
        .expect("sub queue wait lock");
    let mut sub_decode = collector.sub_decode_ns.lock().expect("sub decode lock");
    let mut sub_dispatch = collector.sub_dispatch_ns.lock().expect("sub dispatch lock");
    let mut sub_consumer_gap = collector
        .sub_consumer_gap_ns
        .lock()
        .expect("sub consumer gap lock");
    let mut sub_poll_gap = collector
        .sub_poll_gap_ns
        .lock()
        .expect("sub poll gap lock");
    let mut sub_time_in_queue = collector
        .sub_time_in_queue_ns
        .lock()
        .expect("sub time in queue lock");
    let mut sub_runtime_gap = collector
        .sub_runtime_gap_ns
        .lock()
        .expect("sub runtime gap lock");
    let mut sub_delivery_chan_wait = collector
        .sub_delivery_chan_wait_ns
        .lock()
        .expect("sub delivery channel wait lock");
    let mut e2e_latency = collector.e2e_latency_ns.lock().expect("e2e latency lock");
    let mut ack_read = collector.ack_read_wait_ns.lock().expect("ack read lock");
    let mut ack_decode = collector.ack_decode_ns.lock().expect("ack decode lock");
    Some((
        std::mem::take(&mut *publish_enqueue_wait),
        std::mem::take(&mut *encode),
        std::mem::take(&mut *binary_encode),
        std::mem::take(&mut *text_encode),
        std::mem::take(&mut *text_batch_build),
        std::mem::take(&mut *write),
        std::mem::take(&mut *send_await),
        std::mem::take(&mut *sub_read),
        std::mem::take(&mut *sub_read_await),
        std::mem::take(&mut *sub_queue_wait),
        std::mem::take(&mut *sub_decode),
        std::mem::take(&mut *sub_dispatch),
        std::mem::take(&mut *sub_consumer_gap),
        std::mem::take(&mut *sub_poll_gap),
        std::mem::take(&mut *sub_time_in_queue),
        std::mem::take(&mut *sub_runtime_gap),
        std::mem::take(&mut *sub_delivery_chan_wait),
        std::mem::take(&mut *e2e_latency),
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

#[cfg(test)]
pub(crate) fn reset_collector_for_tests() {
    // Safety: test-only helper to reset global state between tests.
    unsafe {
        let ptr = &COLLECTOR as *const OnceLock<TimingCollector> as *mut OnceLock<TimingCollector>;
        let _ = (*ptr).take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn collector_none_paths() {
        reset_collector_for_tests();
        assert!(!should_sample());
        set_enabled(true);
        set_enabled(false);
        record_encode_ns(1);
        record_binary_encode_ns(2);
        record_text_encode_ns(3);
        record_text_batch_build_ns(4);
        record_publish_enqueue_wait_ns(5);
        record_write_ns(6);
        record_send_await_ns(7);
        record_sub_read_wait_ns(8);
        record_sub_read_await_ns(9);
        record_sub_queue_wait_ns(10);
        record_sub_decode_ns(11);
        record_sub_dispatch_ns(12);
        record_sub_consumer_gap_ns(13);
        record_sub_poll_gap_ns(131);
        record_sub_time_in_queue_ns(132);
        record_sub_runtime_gap_ns(133);
        record_sub_delivery_chan_wait_ns(134);
        record_e2e_latency_ns(14);
        record_ack_read_wait_ns(15);
        record_ack_decode_ns(16);
        record_cache_encode_ns(16);
        record_cache_open_stream_ns(17);
        record_cache_write_ns(18);
        record_cache_finish_ns(19);
        record_cache_read_wait_ns(20);
        record_cache_read_drain_ns(21);
        record_cache_decode_ns(22);
        record_cache_validate_ns(23);
        assert!(take_samples().is_none());
        assert!(take_cache_samples().is_none());
    }

    #[test]
    #[serial]
    fn collector_records_and_samples() {
        reset_collector_for_tests();
        enable_collection(2);
        assert!(should_sample());
        assert!(!should_sample());
        set_enabled(false);
        assert!(!should_sample());
        set_enabled(true);
        assert!(should_sample());
        record_encode_ns(100);
        record_binary_encode_ns(200);
        record_text_encode_ns(300);
        record_text_batch_build_ns(400);
        record_publish_enqueue_wait_ns(500);
        record_write_ns(600);
        record_send_await_ns(700);
        record_sub_read_wait_ns(800);
        record_sub_read_await_ns(900);
        record_sub_queue_wait_ns(1000);
        record_sub_decode_ns(1100);
        record_sub_dispatch_ns(1200);
        record_sub_consumer_gap_ns(1300);
        record_sub_poll_gap_ns(1310);
        record_sub_time_in_queue_ns(1320);
        record_sub_runtime_gap_ns(1330);
        record_sub_delivery_chan_wait_ns(1340);
        record_e2e_latency_ns(1400);
        record_ack_read_wait_ns(1500);
        record_ack_decode_ns(1600);
        record_cache_encode_ns(1600);
        record_cache_open_stream_ns(1700);
        record_cache_write_ns(1800);
        record_cache_finish_ns(1900);
        record_cache_read_wait_ns(2000);
        record_cache_read_drain_ns(2100);
        record_cache_decode_ns(2200);
        record_cache_validate_ns(2300);
        let samples = take_samples().expect("samples");
        assert!(samples.0.contains(&500));
        assert!(samples.1.contains(&100));
        assert!(samples.2.contains(&200));
        assert!(samples.3.contains(&300));
        assert!(samples.4.contains(&400));
        assert!(samples.5.contains(&600));
        assert!(samples.6.contains(&700));
        assert!(samples.7.contains(&800));
        assert!(samples.8.contains(&900));
        assert!(samples.9.contains(&1000));
        assert!(samples.10.contains(&1100));
        assert!(samples.11.contains(&1200));
        assert!(samples.12.contains(&1300));
        assert!(samples.13.contains(&1310));
        assert!(samples.14.contains(&1320));
        assert!(samples.15.contains(&1330));
        assert!(samples.16.contains(&1340));
        assert!(samples.17.contains(&1400));
        assert!(samples.18.contains(&1500));
        assert!(samples.19.contains(&1600));
        let cache_samples = take_cache_samples().expect("cache samples");
        assert!(cache_samples.0.contains(&1600));
        assert!(cache_samples.1.contains(&1700));
        assert!(cache_samples.2.contains(&1800));
        assert!(cache_samples.3.contains(&1900));
        assert!(cache_samples.4.contains(&2000));
        assert!(cache_samples.5.contains(&2100));
        assert!(cache_samples.6.contains(&2200));
        assert!(cache_samples.7.contains(&2300));
    }
}
