// Mapping a `ShardKey` onto a directory on disk.
//
// Tenant, namespace and stream names come from users, so they cannot be pasted
// into a path unescaped: `../` would escape the data root, and case-insensitive
// filesystems would collide `Orders` with `orders`. The directory name is
// therefore a *readable but lossy* rendering of the key plus a hash of the exact
// key, which restores uniqueness.

use std::path::{Path, PathBuf};

use crate::log::ShardKey;

/// Characters kept verbatim in a directory name. Everything else becomes `_`.
///
/// `.` is deliberately excluded along with `/`: with no dots in the readable
/// prefix, a name like `..` is not merely escaped but unrepresentable.
fn is_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-'
}

/// Longest run of a single key component kept in the readable prefix.
const MAX_COMPONENT_CHARS: usize = 32;

/// FNV-1a over the exact key bytes.
///
/// Deliberately not `DefaultHasher`: that is explicitly not stable across Rust
/// releases, and a directory name that changes when the toolchain changes would
/// orphan every existing segment on disk.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn readable(component: &str) -> String {
    let mut out: String = component
        .chars()
        .map(|c| if is_safe(c) { c } else { '_' })
        .take(MAX_COMPONENT_CHARS)
        .collect();
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Directory name for a shard: readable prefix plus a hash of the exact key.
pub fn shard_dir_name(shard: &ShardKey) -> String {
    // Length-prefixed so that ("a", "bc") and ("ab", "c") cannot hash alike.
    let mut material = Vec::new();
    for part in [
        shard.tenant.as_str(),
        shard.namespace.as_str(),
        shard.stream.as_str(),
    ] {
        material.extend_from_slice(&(part.len() as u64).to_be_bytes());
        material.extend_from_slice(part.as_bytes());
    }
    material.extend_from_slice(&shard.shard.to_be_bytes());

    format!(
        "{}_{}_{}_{}-{:016x}",
        readable(&shard.tenant),
        readable(&shard.namespace),
        readable(&shard.stream),
        shard.shard,
        fnv1a(&material)
    )
}

/// Full path to a shard's segment directory under `root`.
pub fn shard_dir(root: &Path, shard: &ShardKey) -> PathBuf {
    root.join(shard_dir_name(shard))
}

/// Human-readable shard identifier used in errors, logs and metrics.
///
/// Unlike the directory name this is not required to be unique or
/// filesystem-safe — it exists so a corruption report names the stream an
/// operator recognises.
pub fn shard_label(shard: &ShardKey) -> String {
    format!(
        "{}/{}/{}/{}",
        shard.tenant, shard.namespace, shard.stream, shard.shard
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(tenant: &str, namespace: &str, stream: &str, shard: u32) -> ShardKey {
        ShardKey {
            tenant: tenant.into(),
            namespace: namespace.into(),
            stream: stream.into(),
            shard,
        }
    }

    #[test]
    fn a_plain_key_reads_back_plainly() {
        let name = shard_dir_name(&shard("acme", "default", "orders", 3));
        assert!(name.starts_with("acme_default_orders_3-"), "{name}");
    }

    #[test]
    fn path_traversal_cannot_escape_the_root() {
        let name = shard_dir_name(&shard("../../etc", "..", "/passwd", 0));
        assert!(!name.contains('/'), "{name}");
        assert!(!name.contains(".."), "{name}");

        let root = Path::new("/data");
        let path = shard_dir(root, &shard("../../etc", "..", "/passwd", 0));
        assert!(path.starts_with(root), "{path:?}");
    }

    #[test]
    fn distinct_keys_get_distinct_directories() {
        let names = [
            shard_dir_name(&shard("a", "b", "c", 0)),
            shard_dir_name(&shard("a", "b", "c", 1)),
            shard_dir_name(&shard("a", "b", "d", 0)),
            shard_dir_name(&shard("a", "bc", "", 0)),
            shard_dir_name(&shard("ab", "c", "", 0)),
            // Same readable rendering, different exact keys.
            shard_dir_name(&shard("a/b", "n", "s", 0)),
            shard_dir_name(&shard("a?b", "n", "s", 0)),
        ];
        let mut unique = names.to_vec();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "{names:?}");
    }

    #[test]
    fn the_same_key_always_maps_to_the_same_directory() {
        let key = shard("acme", "default", "orders", 3);
        assert_eq!(shard_dir_name(&key), shard_dir_name(&key.clone()));
        // Pinned: changing this value orphans every segment already on disk.
        assert_eq!(
            shard_dir_name(&key),
            "acme_default_orders_3-0d3af04b998d2cb1"
        );
    }

    #[test]
    fn the_directory_name_in_the_format_spec_is_correct() {
        // `docs/storage-format.md` prints this exact name as its worked example.
        // A hash that has drifted from the documentation is worse than no
        // example, because a reader will trust it and go looking for a
        // directory that does not exist.
        assert_eq!(
            shard_dir_name(&shard("acme", "default", "orders", 0)),
            "acme_default_orders_0-0d3aed4b998d2798"
        );
    }

    #[test]
    fn long_components_are_truncated_but_stay_unique() {
        let long_a = "x".repeat(200);
        let long_b = format!("{}y", "x".repeat(199));
        let name_a = shard_dir_name(&shard(&long_a, "n", "s", 0));
        let name_b = shard_dir_name(&shard(&long_b, "n", "s", 0));
        assert!(name_a.len() < 200);
        assert_ne!(name_a, name_b);
    }

    #[test]
    fn empty_components_still_produce_a_name() {
        let name = shard_dir_name(&shard("", "", "", 0));
        assert!(name.starts_with("______0-"), "{name}");
    }

    #[test]
    fn the_label_is_the_human_readable_form() {
        assert_eq!(
            shard_label(&shard("acme", "default", "orders", 3)),
            "acme/default/orders/3"
        );
    }
}
