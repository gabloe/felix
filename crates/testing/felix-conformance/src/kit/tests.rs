use super::*;

fn outcome(id: &str, status: Status) -> Outcome {
    Outcome {
        id: id.to_string(),
        status,
        detail: None,
    }
}

fn all_required_passing() -> Results {
    let catalogue = catalogue().expect("catalogue");
    Results {
        client: "test".to_string(),
        version: "0".to_string(),
        outcomes: catalogue
            .scenarios
            .iter()
            .filter(|scenario| scenario.required)
            .map(|scenario| outcome(&scenario.id, Status::Pass))
            .collect(),
    }
}

#[test]
fn the_catalogue_parses_and_has_required_scenarios() {
    let catalogue = catalogue().expect("catalogue");
    assert_eq!(catalogue.version, 1);
    assert!(
        catalogue.scenarios.iter().any(|scenario| scenario.required),
        "a catalogue with nothing required cannot fail anyone",
    );
}

/// Ids are what results are keyed by, so a duplicate would make one
/// scenario's result silently stand in for another's.
#[test]
fn scenario_ids_are_unique() {
    let catalogue = catalogue().expect("catalogue");
    let mut seen = std::collections::HashSet::new();
    for scenario in &catalogue.scenarios {
        assert!(
            seen.insert(scenario.id.clone()),
            "duplicate scenario id {}",
            scenario.id,
        );
    }
}

#[test]
fn a_client_covering_every_required_scenario_is_conformant() {
    let catalogue = catalogue().expect("catalogue");
    let report = verify(&catalogue, &all_required_passing()).expect("verify");
    assert!(report.conformant(), "{report:?}");
}

/// The whole point: a client that quietly omits a required semantic is
/// reported rather than passing.
#[test]
fn omitting_a_required_scenario_is_not_conformant() {
    let catalogue = catalogue().expect("catalogue");
    let mut results = all_required_passing();
    let dropped = results.outcomes.pop().expect("at least one required");
    let report = verify(&catalogue, &results).expect("verify");
    assert!(!report.conformant());
    assert!(
        report
            .missing_required
            .iter()
            .any(|finding| finding.id == dropped.id),
        "the missing scenario was not named",
    );
}

/// Skipping is not a way around a required scenario.
#[test]
fn skipping_a_required_scenario_is_not_conformant() {
    let catalogue = catalogue().expect("catalogue");
    let mut results = all_required_passing();
    results.outcomes[0].status = Status::Skip;
    let report = verify(&catalogue, &results).expect("verify");
    assert!(!report.conformant());
    assert_eq!(report.skipped_required.len(), 1);
}

/// An optional scenario may go unclaimed, but claiming it and getting it
/// wrong still fails.
#[test]
fn an_optional_scenario_may_be_unclaimed_but_not_failed() {
    let catalogue = catalogue().expect("catalogue");
    let optional = catalogue
        .scenarios
        .iter()
        .find(|scenario| !scenario.required)
        .expect("an optional scenario");

    let unclaimed = verify(&catalogue, &all_required_passing()).expect("verify");
    assert!(
        unclaimed.conformant(),
        "an unclaimed optional must not fail"
    );

    let mut failing = all_required_passing();
    failing.outcomes.push(outcome(&optional.id, Status::Fail));
    let report = verify(&catalogue, &failing).expect("verify");
    assert!(
        !report.conformant(),
        "a failing optional scenario must not pass",
    );
}

/// A result naming a scenario that does not exist is a stale or misspelled
/// test, and letting it slide would let a client appear to cover something
/// it does not.
#[test]
fn an_unknown_scenario_id_is_reported() {
    let catalogue = catalogue().expect("catalogue");
    let mut results = all_required_passing();
    results
        .outcomes
        .push(outcome("pubsub.not_a_real_id", Status::Pass));
    let report = verify(&catalogue, &results).expect("verify");
    assert!(!report.conformant());
    assert_eq!(report.unknown, vec!["pubsub.not_a_real_id".to_string()]);
}
