use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

struct TimingCollector {
    lookup_ns: Mutex<Vec<u64>>,
    append_ns: Mutex<Vec<u64>>,
    send_ns: Mutex<Vec<u64>>,
    sample_every: usize,
    enabled: AtomicBool,
    counter: AtomicUsize,
}

static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();

pub type BrokerPublishSamples = (Vec<u64>, Vec<u64>, Vec<u64>);

pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    let _ = COLLECTOR.set(TimingCollector {
        lookup_ns: Mutex::new(Vec::new()),
        append_ns: Mutex::new(Vec::new()),
        send_ns: Mutex::new(Vec::new()),
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

pub fn record_lookup_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.lookup_ns.lock().expect("lookup lock");
        guard.push(value);
    }
}

pub fn record_append_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.append_ns.lock().expect("append lock");
        guard.push(value);
    }
}

pub fn record_send_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        let mut guard = collector.send_ns.lock().expect("send lock");
        guard.push(value);
    }
}

pub fn take_samples() -> Option<BrokerPublishSamples> {
    let collector = COLLECTOR.get()?;
    let mut lookup = collector.lookup_ns.lock().expect("lookup lock");
    let mut append = collector.append_ns.lock().expect("append lock");
    let mut send = collector.send_ns.lock().expect("send lock");
    Some((
        std::mem::take(&mut *lookup),
        std::mem::take(&mut *append),
        std::mem::take(&mut *send),
    ))
}
