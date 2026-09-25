//! What a minted refresh token looks like before it reaches a store.
use super::*;

#[test]
fn a_new_chain_gets_its_own_family() {
    let (first, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (second, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));

    assert_ne!(
        first.family_id, second.family_id,
        "two exchanges shared a rotation chain, so a replay on one would revoke \
         the other's tokens",
    );
    assert_ne!(first.token_id, second.token_id);
}

#[test]
fn a_rotation_stays_in_its_family() {
    let (first, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (next, _) = issue(
        "acme",
        "p",
        vec![],
        Some(first.family_id.clone()),
        1_100,
        Duration::from_secs(60),
    );

    // The chain is what makes a replay actionable: presenting any spent token
    // in it must be able to revoke every other.
    assert_eq!(next.family_id, first.family_id);
    assert_ne!(next.token_id, first.token_id);
}

#[test]
fn the_record_never_carries_the_secret() {
    let (record, presented) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (token_id, secret) = refresh_token::split(&presented).expect("a well-formed token");

    assert_eq!(token_id, record.token_id);
    assert_eq!(record.secret_hash, refresh_token::hash_secret(secret));
    // A database read must not yield anything presentable.
    assert!(!presented.contains(&record.secret_hash));
    assert_ne!(record.secret_hash, secret);
}

#[test]
fn every_token_is_distinct() {
    // Both halves come from the OS. A repeat in either would mean one caller's
    // token opening another's chain.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..64 {
        let (_, presented) = issue("acme", "p", vec![], None, 0, Duration::from_secs(60));
        assert!(seen.insert(presented), "a refresh token repeated");
    }
}

#[test]
fn the_ttl_lands_on_the_record() {
    let (record, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(900));
    assert_eq!(record.issued_at_secs, 1_000);
    assert_eq!(record.expires_at_secs, 1_900);
    assert!(record.is_live(1_899));
    assert!(!record.is_live(1_900));
}

#[test]
fn the_claims_to_re_evaluate_are_kept() {
    let groups = vec!["eng".to_string(), "oncall".to_string()];
    let (record, _) = issue(
        "acme",
        "p",
        groups.clone(),
        None,
        0,
        Duration::from_secs(60),
    );
    // Claims, not permissions. Storing the granted permissions would freeze
    // them, which is the thing re-evaluation exists to avoid.
    assert_eq!(record.groups, groups);
}
