//! The stream cache's keys.

use super::*;

/// The hot-path decimal encoder, which exists to keep `core::fmt` off the
/// publish path. It has to agree with formatting for every value a shard can
/// take, or two shards could share a cache entry.
#[test]
fn push_decimal_matches_formatting() {
    for value in [0u32, 1, 7, 9, 10, 99, 100, 4095, 65_535, u32::MAX] {
        let mut buf = String::new();
        push_decimal(&mut buf, value);
        assert_eq!(buf, value.to_string(), "encoding {value}");
    }
}

/// **No two distinct tuples share a cache key.**
///
/// A cache hit hands a publish a resolved stream handle, so two tuples sharing
/// a key means one stream's publish written to another stream's log. Nothing
/// forbids a `\0` inside a tenant id, namespace or stream name, so the key
/// cannot rely on one being absent — every part is length-prefixed instead.
///
/// The pairs below are the ones that collided when the parts were merely joined
/// with `\0`, and each is a different way of moving the boundary between two
/// parts (#295).
#[test]
fn no_two_distinct_tuples_share_a_cache_key() {
    fn key(tenant: &str, namespace: &str, stream: &str, shard: u32) -> String {
        let mut buf = String::new();
        push_stream_cache_key(&mut buf, tenant, namespace, stream, shard);
        buf
    }

    let colliding_before = [
        // The boundary between tenant and namespace.
        (("a\0b", "c", "orders", 0), ("a", "b\0c", "orders", 0)),
        // The boundary between namespace and stream.
        (("t1", "ns\0orders", "x", 0), ("t1", "ns", "orders\0x", 0)),
        // The boundary between stream and shard, which #286 added.
        (("t1", "ns", "orders\u{0}1", 0), ("t1", "ns", "orders", 1)),
        // An empty part is still a part.
        (("", "a", "b", 0), ("a", "", "b", 0)),
    ];
    for (left, right) in colliding_before {
        let (lk, rk) = (
            key(left.0, left.1, left.2, left.3),
            key(right.0, right.1, right.2, right.3),
        );
        assert_ne!(lk, rk, "{left:?} and {right:?} produced the same key");
    }

    // The ordinary distinctions still hold.
    assert_ne!(key("t1", "ns", "orders", 0), key("t1", "ns", "orders", 1));
    assert_ne!(key("t1", "ns", "orders", 1), key("t1", "ns", "orders", 2));
    assert_ne!(key("t1", "ns", "orders", 0), key("t2", "ns", "orders", 0));

    // And the same tuple is still the same key, or nothing would ever hit.
    assert_eq!(key("t1", "ns", "orders", 0), key("t1", "ns", "orders", 0));
}

/// Exhaustive over a small alphabet including `\0`: every distinct tuple gets a
/// distinct key. A hand-picked list proves the cases someone thought of; this
/// proves there are no others in the space it covers.
#[test]
fn cache_keys_are_injective_over_an_alphabet_containing_nul() {
    use std::collections::HashMap;

    let parts = ["", "a", "\0", "a\0", "\0a", "ab", "a\0b"];
    let mut seen: HashMap<String, (&str, &str, &str, u32)> = HashMap::new();
    for tenant in parts {
        for namespace in parts {
            for stream in parts {
                for shard in [0u32, 1, 10] {
                    let mut buf = String::new();
                    push_stream_cache_key(&mut buf, tenant, namespace, stream, shard);
                    let tuple = (tenant, namespace, stream, shard);
                    if let Some(previous) = seen.insert(buf.clone(), tuple) {
                        panic!("{previous:?} and {tuple:?} share the key {buf:?}");
                    }
                }
            }
        }
    }
    assert_eq!(seen.len(), parts.len().pow(3) * 3);
}
