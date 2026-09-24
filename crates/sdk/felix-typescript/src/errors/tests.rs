use felix_wire::{ErrorCode, RetryClass};

use super::*;

#[test]
fn every_code_maps_to_a_kind_that_matches_its_retry_class() {
    let expected = [
        ("unauthenticated", CODE_AUTH),
        ("forbidden", CODE_AUTH),
        ("not_found", CODE_NOT_FOUND),
        ("invalid_request", CODE_GENERIC),
        ("shard_unavailable", CODE_SHARD_UNAVAILABLE),
        ("not_leader", CODE_SHARD_UNAVAILABLE),
        ("quorum_timeout", CODE_OUTCOME_UNKNOWN),
        ("leadership_lost", CODE_OUTCOME_UNKNOWN),
        ("unacknowledged", CODE_OUTCOME_UNKNOWN),
        ("overloaded", CODE_OVERLOADED),
        ("limit_exceeded", CODE_GENERIC),
        ("draining", CODE_CONNECTION),
        ("internal", CODE_OUTCOME_UNKNOWN),
        ("storage", CODE_OUTCOME_UNKNOWN),
    ];
    assert_eq!(expected.len(), ErrorCode::ALL.len(), "a code is unmapped");
    for code in ErrorCode::ALL {
        let (_, kind) = expected
            .iter()
            .find(|(name, _)| *name == code.as_str())
            .unwrap_or_else(|| panic!("{code} is not in the table"));
        assert_eq!(
            kind_for_code(code.as_str(), code.default_retry()),
            *kind,
            "{code}"
        );
    }
}

#[test]
fn an_outcome_unknown_retry_class_wins_over_the_code() {
    assert_eq!(
        kind_for_code("overloaded", RetryClass::OutcomeUnknown),
        CODE_OUTCOME_UNKNOWN
    );
    assert_eq!(kind_for_code("internal", RetryClass::Retry), CODE_GENERIC);
    assert_eq!(kind_for_code("brand_new", RetryClass::Retry), CODE_GENERIC);
}

#[test]
fn without_a_code_the_message_decides_and_keeps_the_old_prefix() {
    let cases = [
        ("publish failed: forbidden", CODE_AUTH),
        ("unknown stream t1/default/nope", CODE_NOT_FOUND),
        ("offset trimmed", CODE_CURSOR),
        ("connection lost", CODE_CONNECTION),
        ("something nobody foresaw", CODE_GENERIC),
    ];
    for (message, kind) in cases {
        assert_eq!(
            encode(&anyhow::anyhow!(message)),
            format!("{kind}: {message}")
        );
    }
}

#[test]
fn a_typed_refusal_carries_its_code_on_a_line_of_its_own() {
    let err = anyhow::Error::new(felix_client::NotLeaderError {
        node_id: "broker-2".to_string(),
        addr: None,
        generation: 3,
    })
    .context("publish failed: with a colon: and a\nnewline");
    let encoded = encode(&err);
    let (head, text) = encoded.split_once('\n').expect("a header line");
    assert_eq!(
        head,
        r#"FELIX_SHARD_UNAVAILABLE {"code":"not_leader","retry":"redirect"}"#
    );
    assert!(text.starts_with("publish failed: with a colon: and a\nnewline"));
}
