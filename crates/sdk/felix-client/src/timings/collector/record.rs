//! One recording function per timed stage.

use super::COLLECTOR;

/// Record one `encode` sample, in nanoseconds.
pub fn record_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.encode_ns.lock().expect("encode lock");
        guard.push(value);
    }
}

/// Record one `binary_encode` sample, in nanoseconds.
pub fn record_binary_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .binary_encode_ns
            .lock()
            .expect("binary encode lock");
        guard.push(value);
    }
}

/// Record one `text_encode` sample, in nanoseconds.
pub fn record_text_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.text_encode_ns.lock().expect("text encode lock");
        guard.push(value);
    }
}

/// Record one `text_batch_build` sample, in nanoseconds.
pub fn record_text_batch_build_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .text_batch_build_ns
            .lock()
            .expect("text batch build lock");
        guard.push(value);
    }
}

/// Record one `publish_enqueue_wait` sample, in nanoseconds.
pub fn record_publish_enqueue_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .publish_enqueue_wait_ns
            .lock()
            .expect("publish enqueue wait lock");
        guard.push(value);
    }
}

/// Record one `write` sample, in nanoseconds.
pub fn record_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.write_ns.lock().expect("write lock");
        guard.push(value);
    }
}

/// Record one `send_await` sample, in nanoseconds.
pub fn record_send_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.send_await_ns.lock().expect("send await lock");
        guard.push(value);
    }
}

/// Record one `sub_read_wait` sample, in nanoseconds.
pub fn record_sub_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_read_wait_ns.lock().expect("sub read lock");
        guard.push(value);
    }
}

/// Record one `sub_read_await` sample, in nanoseconds.
pub fn record_sub_read_await_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_read_await_ns
            .lock()
            .expect("sub read await lock");
        guard.push(value);
    }
}

/// Record one `sub_queue_wait` sample, in nanoseconds.
pub fn record_sub_queue_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_queue_wait_ns
            .lock()
            .expect("sub queue wait lock");
        guard.push(value);
    }
}

/// Record one `sub_decode` sample, in nanoseconds.
pub fn record_sub_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_decode_ns.lock().expect("sub decode lock");
        guard.push(value);
    }
}

/// Record one `sub_dispatch` sample, in nanoseconds.
pub fn record_sub_dispatch_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_dispatch_ns.lock().expect("sub dispatch lock");
        guard.push(value);
    }
}

/// Record one `sub_consumer_gap` sample, in nanoseconds.
pub fn record_sub_consumer_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_consumer_gap_ns
            .lock()
            .expect("sub consumer gap lock");
        guard.push(value);
    }
}

/// Record one `sub_poll_gap` sample, in nanoseconds.
pub fn record_sub_poll_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.sub_poll_gap_ns.lock().expect("sub poll gap lock");
        guard.push(value);
    }
}

/// Record one `sub_time_in_queue` sample, in nanoseconds.
pub fn record_sub_time_in_queue_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_time_in_queue_ns
            .lock()
            .expect("sub time in queue lock");
        guard.push(value);
    }
}

/// Record one `sub_runtime_gap` sample, in nanoseconds.
pub fn record_sub_runtime_gap_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_runtime_gap_ns
            .lock()
            .expect("sub runtime gap lock");
        guard.push(value);
    }
}

/// Record one `sub_delivery_chan_wait` sample, in nanoseconds.
pub fn record_sub_delivery_chan_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .sub_delivery_chan_wait_ns
            .lock()
            .expect("sub delivery channel wait lock");
        guard.push(value);
    }
}

/// Record one `e2e_latency` sample, in nanoseconds.
pub fn record_e2e_latency_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.e2e_latency_ns.lock().expect("e2e latency lock");
        guard.push(value);
    }
}

/// Record one `ack_read_wait` sample, in nanoseconds.
pub fn record_ack_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_read_wait_ns.lock().expect("ack read lock");
        guard.push(value);
    }
}

/// Record one `ack_decode` sample, in nanoseconds.
pub fn record_ack_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.ack_decode_ns.lock().expect("ack decode lock");
        guard.push(value);
    }
}

/// Record one `cache_encode` sample, in nanoseconds.
pub fn record_cache_encode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_encode_ns.lock().expect("cache encode lock");
        guard.push(value);
    }
}

/// Record one `cache_open_stream` sample, in nanoseconds.
pub fn record_cache_open_stream_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_open_stream_ns
            .lock()
            .expect("cache open stream lock");
        guard.push(value);
    }
}

/// Record one `cache_write` sample, in nanoseconds.
pub fn record_cache_write_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_write_ns.lock().expect("cache write lock");
        guard.push(value);
    }
}

/// Record one `cache_finish` sample, in nanoseconds.
pub fn record_cache_finish_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_finish_ns.lock().expect("cache finish lock");
        guard.push(value);
    }
}

/// Record one `cache_read_wait` sample, in nanoseconds.
pub fn record_cache_read_wait_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_read_wait_ns
            .lock()
            .expect("cache read wait lock");
        guard.push(value);
    }
}

/// Record one `cache_read_drain` sample, in nanoseconds.
pub fn record_cache_read_drain_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_read_drain_ns
            .lock()
            .expect("cache read drain lock");
        guard.push(value);
    }
}

/// Record one `cache_decode` sample, in nanoseconds.
pub fn record_cache_decode_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.cache_decode_ns.lock().expect("cache decode lock");
        guard.push(value);
    }
}

/// Record one `cache_validate` sample, in nanoseconds.
pub fn record_cache_validate_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector
            .cache_validate_ns
            .lock()
            .expect("cache validate lock");
        guard.push(value);
    }
}
