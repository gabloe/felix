use super::*;

/// The mapping is frozen. A client and a broker that disagree send records
/// to different shards for the same key, so these values are a contract
/// rather than a characterisation of the current implementation.
#[test]
fn the_mapping_is_stable() {
    assert_eq!(shard_for(8, Some(b"customer-0")), 0);
    assert_eq!(shard_for(8, Some(b"customer-4")), 2);
    assert_eq!(shard_for(8, Some(b"customer-1")), 3);
    assert_eq!(shard_for(8, Some(b"customer-6")), 4);
    assert_eq!(shard_for(8, Some(b"customer-2")), 6);
    assert_eq!(shard_for(12, Some(b"k0")), 6);
    assert_eq!(shard_for(12, Some(b"k1")), 10);
    assert_eq!(shard_for(4, Some(b"")), 3);
}

#[test]
fn one_shard_or_no_key_is_always_shard_zero() {
    assert_eq!(shard_for(0, Some(b"anything")), 0);
    assert_eq!(shard_for(1, Some(b"anything")), 0);
    assert_eq!(shard_for(16, None), 0);
}

#[test]
fn the_same_key_always_lands_on_the_same_shard() {
    for shards in 2..=64u32 {
        let first = shard_for(shards, Some(b"customer-42"));
        for _ in 0..8 {
            assert_eq!(shard_for(shards, Some(b"customer-42")), first);
        }
        assert!(first < shards);
    }
}

#[test]
fn keys_spread_across_shards() {
    let seen: std::collections::HashSet<_> = (0..64)
        .map(|i| shard_for(8, Some(format!("key-{i}").as_bytes())))
        .collect();
    assert!(seen.len() > 1, "every key landed on one shard: {seen:?}");
}
