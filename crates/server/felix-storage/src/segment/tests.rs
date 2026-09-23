use super::*;

#[test]
fn file_names_are_zero_padded() {
    assert_eq!(segment_file_name(7), "00000000000000000007.log");
    assert_eq!(index_file_name(7), "00000000000000000007.index");
}

#[test]
fn segment_file_names_round_trip() {
    for id in [0, 1, 42, u64::MAX] {
        assert_eq!(parse_segment_file_name(&segment_file_name(id)), Some(id));
    }
}

#[test]
fn unrelated_names_are_not_segments() {
    assert_eq!(parse_segment_file_name("00000000000000000007.index"), None);
    assert_eq!(parse_segment_file_name("notanumber.log"), None);
    assert_eq!(parse_segment_file_name("7.log.tmp"), None);
    assert_eq!(parse_segment_file_name(""), None);
    // Negative numbers are not offsets.
    assert_eq!(parse_segment_file_name("-1.log"), None);
}

#[test]
fn lexicographic_order_matches_numeric_order() {
    let mut names: Vec<String> = [10u64, 2, 33, 1]
        .iter()
        .map(|id| segment_file_name(*id))
        .collect();
    names.sort();
    let ids: Vec<SegmentId> = names
        .iter()
        .map(|n| parse_segment_file_name(n).expect("id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 10, 33]);
}
