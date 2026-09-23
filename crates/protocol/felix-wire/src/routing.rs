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
mod tests;
