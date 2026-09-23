//! Mapping a `ShardKey` onto a directory on disk.
//!
//! Tenant, namespace and stream names come from users, so they cannot be pasted
//! into a path unescaped: `../` would escape the data root, and case-insensitive
//! filesystems would collide `Orders` with `orders`. The directory name is
//! therefore a *readable but lossy* rendering of the key plus a hash of the exact
//! key, which restores uniqueness.

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
mod tests;
