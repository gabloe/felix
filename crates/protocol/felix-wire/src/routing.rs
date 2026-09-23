//! Which shard a routing key belongs to.
//!
//! Here, in the wire crate, because it is part of the protocol rather than an
//! implementation detail of either side. The broker decides which shard a
//! publish lands on; a client that routes its publishes to the shard's owner
//! has to reach the same answer, and two copies of a hash are two things that
//! can drift. One definition is the only way "the same key goes to the same
//! shard" survives a change to either side.
//!
//! A client's answer only has to be *self-consistent* to be useful -- it uses
//! the shard as a cache key for the owner it learned empirically, so a client
//! whose shard count is stale still routes to the right broker. Sharing the
//! function is about bounding that cache by shard count rather than by the
//! number of distinct keys.

/// Map a routing key to a shard number.
///
/// Deterministic and pure, so the same key lands on the same shard on every
/// broker, in every client, and across restarts.
///
/// No key means shard 0: an unkeyed publish has nothing to hash, so a
/// multi-shard stream published to without keys puts everything on one shard.
/// That is the behaviour, not a defect -- the key is what spreads records.
pub fn shard_for(shards: u32, routing_key: Option<&[u8]>) -> u32 {
    if shards <= 1 {
        return 0;
    }
    match routing_key {
        // FNV-1a with a finalizer, written out rather than taken from a hasher
        // so the mapping cannot shift with a toolchain change. The same
        // construction placement uses.
        Some(key) => (finalize(fnv1a(key)) % u64::from(shards)) as u32,
        None => 0,
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

#[cfg(test)]
mod tests {
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
}
