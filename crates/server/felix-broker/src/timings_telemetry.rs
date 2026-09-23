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
    collector.sample_every.store(sample_every, Ordering::Relaxed);
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
mod tests {
    use super::*;
    use serial_test::serial;

    fn reset_collector() {
        let Some(collector) = COLLECTOR.get() else {
            return;
        };
        #[cfg(test)]
        TEST_RECORDING_ENABLED.with(|enabled| enabled.set(false));
        collector.initialized.store(false, Ordering::Relaxed);
        collector.enabled.store(false, Ordering::Relaxed);
        collector.sample_every.store(1, Ordering::Relaxed);
        collector.counter.store(0, Ordering::Relaxed);
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

    #[test]
    #[serial]
    fn should_sample_returns_false_when_not_enabled() {
        reset_collector();
        assert!(!should_sample());
    }

    #[test]
    #[serial]
    fn enable_collection_initializes_collector() {
        reset_collector();
        enable_collection(1);
        assert!(should_sample());
    }

    #[test]
    #[serial]
    fn should_sample_respects_sample_every() {
        reset_collector();
        enable_collection(3);
        // First call (idx 0) should sample
        assert!(should_sample());
        // Second and third calls should not
        assert!(!should_sample());
        assert!(!should_sample());
        // Fourth call (idx 3) should sample
        assert!(should_sample());
    }

    #[test]
    #[serial]
    fn enable_collection_enforces_minimum_sample_every() {
        reset_collector();
        enable_collection(0);
        // Should be clamped to 1
        assert!(should_sample());
        assert!(should_sample());
    }

    #[test]
    #[serial]
    fn set_enabled_disables_sampling() {
        reset_collector();
        enable_collection(1);
        set_enabled(false);
        assert!(!should_sample());
    }

    #[test]
    #[serial]
    fn set_enabled_enables_sampling() {
        reset_collector();
        enable_collection(1);
        set_enabled(false);
        set_enabled(true);
        assert!(should_sample());
    }

    #[test]
    #[serial]
    fn set_enabled_noop_when_not_initialized() {
        reset_collector();
        set_enabled(true);
        // Should not crash, just no-op
        assert!(!should_sample());
    }

    #[test]
    #[serial]
    fn record_lookup_ns_stores_value() {
        reset_collector();
        enable_collection(1);
        record_lookup_ns(100);
        record_lookup_ns(200);
        let (lookup, _, _, _, _) = take_samples().expect("samples");
        assert_eq!(lookup, vec![100, 200]);
    }

    #[test]
    #[serial]
    fn record_append_ns_stores_value() {
        reset_collector();
        enable_collection(1);
        record_append_ns(300);
        record_append_ns(400);
        let (_, append, _, _, _) = take_samples().expect("samples");
        assert_eq!(append, vec![300, 400]);
    }

    #[test]
    #[serial]
    fn record_fanout_enqueue_ns_stores_value() {
        reset_collector();
        let _ = take_samples();
        enable_collection(1);
        record_fanout_ns(410);
        record_enqueue_ns(420);
        let (_, _, fanout, enqueue, _) = take_samples().expect("samples");
        assert!(fanout.contains(&410));
        assert!(enqueue.contains(&420));
    }

    #[test]
    #[serial]
    fn record_send_ns_stores_value() {
        reset_collector();
        enable_collection(1);
        record_send_ns(500);
        record_send_ns(600);
        let (_, _, _, _, send) = take_samples().expect("samples");
        assert_eq!(send, vec![500, 600]);
    }

    #[test]
    #[serial]
    fn take_samples_clears_collected_data() {
        reset_collector();
        enable_collection(1);
        record_lookup_ns(100);
        record_append_ns(200);
        record_fanout_ns(250);
        record_enqueue_ns(275);
        record_send_ns(300);

        let (lookup1, append1, fanout1, enqueue1, send1) = take_samples().expect("samples");
        assert_eq!(lookup1.len(), 1);
        assert_eq!(append1.len(), 1);
        assert_eq!(fanout1.len(), 1);
        assert_eq!(enqueue1.len(), 1);
        assert_eq!(send1.len(), 1);

        // Second take should return empty vectors
        let (lookup2, append2, fanout2, enqueue2, send2) = take_samples().expect("samples");
        assert!(lookup2.is_empty());
        assert!(append2.is_empty());
        assert!(fanout2.is_empty());
        assert!(enqueue2.is_empty());
        assert!(send2.is_empty());
    }

    #[test]
    #[serial]
    fn take_samples_returns_none_when_not_enabled() {
        reset_collector();
        assert!(take_samples().is_none());
    }

    #[test]
    #[serial]
    fn record_functions_noop_when_not_enabled() {
        reset_collector();
        // Should not crash
        record_lookup_ns(100);
        record_append_ns(200);
        record_fanout_ns(250);
        record_enqueue_ns(275);
        record_send_ns(300);
        assert!(take_samples().is_none());
    }
}
