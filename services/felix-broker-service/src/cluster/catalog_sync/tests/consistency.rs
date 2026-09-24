//! What the control plane sends for `consistency`, and what this broker
//! makes of it.

use super::*;

/// A control plane that predates replication sends no level at all, and
/// every stream written before this existed behaves as it did.
#[test]
fn an_absent_level_is_leader() {
    assert_eq!(
        read_consistency(None).expect("read"),
        ConsistencyLevel::Leader
    );
}

/// The exact strings the control plane serializes. Pinned on this side
/// too, because the two enums are compiled separately and nothing else
/// would catch them drifting apart.
#[test]
fn the_levels_are_read_by_their_wire_names() {
    assert_eq!(
        read_consistency(Some("Leader")).expect("read"),
        ConsistencyLevel::Leader,
    );
    assert_eq!(
        read_consistency(Some("Quorum")).expect("read"),
        ConsistencyLevel::Quorum,
    );
}

/// **An unrecognised level is refused, never defaulted.** Falling back
/// to `Leader` would serve a stream the operator asked to be
/// quorum-replicated at the weaker guarantee, and the acknowledgement
/// would keep its meaning on paper while losing it in fact.
#[test]
fn an_unknown_level_is_refused_rather_than_downgraded() {
    let err = read_consistency(Some("Everywhere")).expect_err("should refuse");
    assert!(err.to_string().contains("Everywhere"), "{err}");
}

/// Case matters: the wire form is what the control plane serializes, and
/// guessing at near-misses is how a downgrade slips through.
#[test]
fn a_near_miss_is_not_guessed_at() {
    assert!(read_consistency(Some("quorum")).is_err());
    assert!(read_consistency(Some("QUORUM")).is_err());
    assert!(read_consistency(Some("")).is_err());
}
