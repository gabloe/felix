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
