//! The sampling collector behind `--features telemetry`: process-global
//! vectors of per-stage publish timings, drained by [`take_samples`].

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static TEST_RECORDING_ENABLED: Cell<bool> = const { Cell::new(false) };
}

struct TimingCollector {
    lookup_ns: Mutex<Vec<u64>>,
    append_ns: Mutex<Vec<u64>>,
    fanout_ns: Mutex<Vec<u64>>,
    enqueue_ns: Mutex<Vec<u64>>,
    send_ns: Mutex<Vec<u64>>,
    sample_every: AtomicUsize,
    initialized: AtomicBool,
    enabled: AtomicBool,
    counter: AtomicUsize,
}

static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();

pub type BrokerPublishSamples = (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>);

pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    let collector = COLLECTOR.get_or_init(|| TimingCollector {
        lookup_ns: Mutex::new(Vec::new()),
        append_ns: Mutex::new(Vec::new()),
        fanout_ns: Mutex::new(Vec::new()),
        enqueue_ns: Mutex::new(Vec::new()),
        send_ns: Mutex::new(Vec::new()),
        sample_every: AtomicUsize::new(sample_every),
        initialized: AtomicBool::new(false),
        enabled: AtomicBool::new(false),
        counter: AtomicUsize::new(0),
    });
    collector
        .sample_every
        .store(sample_every, Ordering::Relaxed);
    collector.counter.store(0, Ordering::Relaxed);
    collector.initialized.store(true, Ordering::Relaxed);
    collector.enabled.store(true, Ordering::Relaxed);
    #[cfg(test)]
    TEST_RECORDING_ENABLED.with(|enabled| enabled.set(true));
    {
        let mut guard = collector.lookup_ns.lock().expect("lookup lock");
        guard.clear();
    }
    {
        let mut guard = collector.append_ns.lock().expect("append lock");
        guard.clear();
    }
    {
        let mut guard = collector.fanout_ns.lock().expect("fanout lock");
        guard.clear();
    }
    {
        let mut guard = collector.enqueue_ns.lock().expect("enqueue lock");
        guard.clear();
    }
    {
        let mut guard = collector.send_ns.lock().expect("send lock");
        guard.clear();
    }
}

pub fn set_enabled(enabled: bool) {
    if let Some(collector) = COLLECTOR.get() {
        if !collector.initialized.load(Ordering::Relaxed) {
            return;
        }
        collector.enabled.store(enabled, Ordering::Relaxed);
    }
}

pub fn should_sample() -> bool {
    let Some(collector) = COLLECTOR.get() else {
        return false;
    };
    #[cfg(test)]
    if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
        return false;
    }
    if !collector.initialized.load(Ordering::Relaxed) {
        return false;
    }
    if !collector.enabled.load(Ordering::Relaxed) {
        return false;
    }
    let idx = collector.counter.fetch_add(1, Ordering::Relaxed);
    let sample_every = collector.sample_every.load(Ordering::Relaxed).max(1);
    idx % sample_every == 0
}

pub fn record_lookup_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        #[cfg(test)]
        if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
            return;
        }
        if !collector.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = collector.lookup_ns.lock().expect("lookup lock");
        guard.push(value);
    }
}

pub fn record_append_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        #[cfg(test)]
        if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
            return;
        }
        if !collector.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = collector.append_ns.lock().expect("append lock");
        guard.push(value);
    }
}

pub fn record_fanout_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        #[cfg(test)]
        if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
            return;
        }
        if !collector.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = collector.fanout_ns.lock().expect("fanout lock");
        guard.push(value);
    }
}

pub fn record_enqueue_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        #[cfg(test)]
        if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
            return;
        }
        if !collector.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = collector.enqueue_ns.lock().expect("enqueue lock");
        guard.push(value);
    }
}

pub fn record_send_ns(value: u64) {
    if let Some(collector) = COLLECTOR.get() {
        #[cfg(test)]
        if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
            return;
        }
        if !collector.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = collector.send_ns.lock().expect("send lock");
        guard.push(value);
    }
}

pub fn take_samples() -> Option<BrokerPublishSamples> {
    let collector = COLLECTOR.get()?;
    #[cfg(test)]
    if !TEST_RECORDING_ENABLED.with(|enabled| enabled.get()) {
        return None;
    }
    if !collector.initialized.load(Ordering::Relaxed) {
        return None;
    }
    if !collector.enabled.load(Ordering::Relaxed) {
        return None;
    }
    let mut lookup = collector.lookup_ns.lock().expect("lookup lock");
    let mut append = collector.append_ns.lock().expect("append lock");
    let mut fanout = collector.fanout_ns.lock().expect("fanout lock");
    let mut enqueue = collector.enqueue_ns.lock().expect("enqueue lock");
    let mut send = collector.send_ns.lock().expect("send lock");
    Some((
        std::mem::take(&mut *lookup),
        std::mem::take(&mut *append),
        std::mem::take(&mut *fanout),
        std::mem::take(&mut *enqueue),
        std::mem::take(&mut *send),
    ))
}

#[cfg(test)]
mod tests;
