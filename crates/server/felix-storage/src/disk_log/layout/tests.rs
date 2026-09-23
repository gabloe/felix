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
