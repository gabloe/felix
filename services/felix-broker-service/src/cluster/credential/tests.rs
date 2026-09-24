//! Reading a token's own claims, and deciding when to act on them.
use base64::Engine;

use super::*;

fn jwt(payload: serde_json::Value) -> String {
    let encode = |value: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
    };
    format!(
        "{}.{}.{}",
        encode(&serde_json::json!({"alg": "EdDSA", "typ": "JWT"})),
        encode(&payload),
        "not-a-real-signature",
    )
}

#[test]
fn the_tenant_and_expiry_come_out_of_the_token() {
    // No new configuration needed: the token already says which tenant minted
    // it and when it stops working, which is exactly what a refresh needs.
    let token = jwt(serde_json::json!({"tid": "acme", "exp": 1_700_000_900, "sub": "node-1"}));
    let claims = read_claims(&token).expect("claims");
    assert_eq!(claims.tid, "acme");
    assert_eq!(claims.exp, 1_700_000_900);
}

#[test]
fn anything_that_is_not_a_jwt_is_declined() {
    // Declining means "cannot schedule a refresh", which the caller reports.
    // Guessing which piece is the payload would be worse: a wrong `exp` is a
    // refresh at the wrong time, and a wrong `tid` is a refresh sent nowhere.
    for not_a_token in [
        "",
        "opaque-token",
        "only.two",
        "a.b.c.d",
        "header.!!!not-base64!!!.sig",
    ] {
        assert!(
            read_claims(not_a_token).is_none(),
            "{not_a_token:?} was read as a token",
        );
    }
    // Valid base64 that is not the right shape, too.
    assert!(read_claims(&jwt(serde_json::json!({"sub": "node-1"}))).is_none());
}

#[test]
fn a_refresh_is_scheduled_with_room_to_retry() {
    // Two thirds through, so the last third is retry budget. At the 900s
    // default that is five minutes of failures before anything is at risk.
    assert_eq!(refresh_delay(0, 900), Duration::from_secs(600));
    assert_eq!(refresh_delay(300, 900), Duration::from_secs(400));
}

#[test]
fn a_token_at_or_past_its_deadline_is_retried_soon_not_instantly() {
    // A spin here would hammer the control plane at exactly the moment it is
    // most likely to be the thing that is unwell.
    for (now, exp) in [(900, 900), (1_000, 900), (899, 900)] {
        let delay = refresh_delay(now, exp);
        assert!(
            delay >= Duration::from_secs(5),
            "refresh_delay({now}, {exp}) was {delay:?}",
        );
    }
}

#[test]
fn a_swap_is_visible_through_a_clone_taken_before_it() {
    // The property the whole design rests on: the shard feed and the heartbeat
    // are handed a credential at startup and must see a refresh that happens
    // long afterwards.
    let credential = NodeCredential::new("first");
    let held_since_startup = credential.clone();
    assert_eq!(*held_since_startup.bearer(), "first");

    credential.replace("second".to_string());
    assert_eq!(
        *held_since_startup.bearer(),
        "second",
        "a caller handed the credential at startup kept using the expired token",
    );
}

#[test]
fn a_token_in_flight_is_not_torn_by_a_swap() {
    // A caller holds the value for one request. Taking it and then swapping
    // must leave the taken one intact and usable — a request cannot end up
    // presenting half of each.
    let credential = NodeCredential::new("first");
    let in_flight = credential.bearer();
    credential.replace("second".to_string());
    assert_eq!(*in_flight, "first");
    assert_eq!(*credential.bearer(), "second");
}

#[test]
fn the_token_never_reaches_debug_output() {
    let credential = NodeCredential::new("super-secret-token");
    let rendered = format!("{credential:?}");
    assert!(
        !rendered.contains("super-secret-token"),
        "the credential printed its token: {rendered}",
    );
}
