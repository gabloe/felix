use super::*;

#[test]
fn corruption_display_includes_the_site() {
    let err = Corruption::new(CorruptionKind::RecordChecksum {
        expected: 1,
        found: 2,
    })
    .in_segment("t/ns/s/0", 3)
    .at_position(128);
    let rendered = err.to_string();
    assert!(rendered.contains("shard=t/ns/s/0"), "{rendered}");
    assert!(rendered.contains("segment=3"), "{rendered}");
    assert!(rendered.contains("position=128"), "{rendered}");
}

#[test]
fn at_position_keeps_the_innermost_site() {
    let err = Corruption::new(CorruptionKind::Truncated {
        needed: 8,
        available: 2,
    })
    .at_position(10)
    .at_position(999);
    assert_eq!(err.site.position, Some(10));
}

#[test]
fn corruption_display_without_a_site_has_no_parentheses() {
    let err = Corruption::new(CorruptionKind::IndexVersion { found: 2 });
    assert_eq!(err.to_string(), "unsupported index version 2");
}
