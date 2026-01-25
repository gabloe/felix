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

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Reset the global collector state for test isolation
    fn reset_collector() {
        unsafe {
            let ptr = &COLLECTOR as *const OnceLock<TimingCollector>
                as *mut OnceLock<TimingCollector>;
            let _ = (*ptr).take();
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
        let (lookup, _, _) = take_samples().expect("samples");
        assert_eq!(lookup, vec![100, 200]);
    }

    #[test]
    #[serial]
    fn record_append_ns_stores_value() {
        reset_collector();
        enable_collection(1);
        record_append_ns(300);
        record_append_ns(400);
        let (_, append, _) = take_samples().expect("samples");
        assert_eq!(append, vec![300, 400]);
    }

    #[test]
    #[serial]
    fn record_send_ns_stores_value() {
        reset_collector();
        enable_collection(1);
        record_send_ns(500);
        record_send_ns(600);
        let (_, _, send) = take_samples().expect("samples");
        assert_eq!(send, vec![500, 600]);
    }

    #[test]
    #[serial]
    fn take_samples_clears_collected_data() {
        reset_collector();
        enable_collection(1);
        record_lookup_ns(100);
        record_append_ns(200);
        record_send_ns(300);
        
        let (lookup1, append1, send1) = take_samples().expect("samples");
        assert_eq!(lookup1.len(), 1);
        assert_eq!(append1.len(), 1);
        assert_eq!(send1.len(), 1);
        
        // Second take should return empty vectors
        let (lookup2, append2, send2) = take_samples().expect("samples");
        assert!(lookup2.is_empty());
        assert!(append2.is_empty());
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
        record_send_ns(300);
        assert!(take_samples().is_none());
    }
}
