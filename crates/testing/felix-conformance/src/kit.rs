//! The client conformance kit: a catalogue, a fixture, and a verdict.
//!
//! A Felix client can be written in any language, and each one has to
//! implement the same semantics — ordering, offset continuity, resume,
//! typed errors. What cannot be shared between languages is the *test code*:
//! a test has to exercise the client's own API, in its own idiom, to prove
//! anything about it. What can be shared is the specification.
//!
//! So this kit is three pieces:
//!
//! * `scenarios.toml` — the catalogue, with a stable `id` per semantic.
//! * `client-fixture` — starts a cluster, registers what the scenarios need,
//!   and writes a JSON file telling a client-under-test how to connect.
//! * `verify` — reads a results file the client produced and answers whether
//!   every required scenario is implemented and passing.
//!
//! The failure this exists to prevent is silent divergence: a second client
//! that looks right, passes its own tests, and differs from the first on
//! resume or error classification in a way nobody notices until it is in
//! production. Here a missing semantic is a missing `id`, which `verify`
//! reports by name.
use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// The catalogue, as parsed from `scenarios.toml`.
#[derive(Debug, Deserialize)]
pub struct Catalogue {
    pub version: u32,
    #[serde(rename = "scenario")]
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub id: String,
    pub required: bool,
    pub title: String,
    #[serde(default)]
    pub detail: String,
}

/// What a client-under-test reports back.
#[derive(Debug, Serialize, Deserialize)]
pub struct Results {
    /// Which client produced this, for the report.
    pub client: String,
    #[serde(default)]
    pub version: String,
    pub outcomes: Vec<Outcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub id: String,
    pub status: Status,
    /// Why, when the status is not `pass`. A skip without a reason is not
    /// reportable: "we did not run it" and "this client does not expose that
    /// surface" are different claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    Fail,
    Skip,
}

/// The catalogue that ships with this crate.
pub fn catalogue() -> Result<Catalogue> {
    let raw = include_str!("../scenarios.toml");
    let catalogue: Catalogue = toml::from_str(raw).context("parse scenarios.toml")?;
    Ok(catalogue)
}

/// Check a client's results against the catalogue.
///
/// Fails when a **required** scenario is missing, failed, or skipped. An
/// optional scenario may be absent or skipped — a binding is allowed not to
/// wrap a surface — but it may not *fail*: claiming a semantic and getting it
/// wrong is worse than not claiming it.
pub fn verify(catalogue: &Catalogue, results: &Results) -> Result<Report> {
    let reported: BTreeMap<&str, &Outcome> = results
        .outcomes
        .iter()
        .map(|outcome| (outcome.id.as_str(), outcome))
        .collect();

    let known: BTreeMap<&str, &Scenario> = catalogue
        .scenarios
        .iter()
        .map(|scenario| (scenario.id.as_str(), scenario))
        .collect();

    let mut report = Report::default();

    // A result naming a scenario the catalogue does not have is a typo or a
    // stale test, and silently ignoring it would let a client appear to cover
    // something it does not.
    for id in reported.keys() {
        if !known.contains_key(id) {
            report.unknown.push((*id).to_string());
        }
    }

    for scenario in &catalogue.scenarios {
        match reported.get(scenario.id.as_str()) {
            Some(outcome) => match outcome.status {
                Status::Pass => report.passed.push(scenario.id.clone()),
                Status::Fail => report.failed.push(Finding {
                    id: scenario.id.clone(),
                    title: scenario.title.clone(),
                    required: scenario.required,
                    detail: outcome.detail.clone(),
                }),
                Status::Skip => {
                    let finding = Finding {
                        id: scenario.id.clone(),
                        title: scenario.title.clone(),
                        required: scenario.required,
                        detail: outcome.detail.clone(),
                    };
                    if scenario.required {
                        report.skipped_required.push(finding);
                    } else {
                        report.skipped_optional.push(finding);
                    }
                }
            },
            None => {
                let finding = Finding {
                    id: scenario.id.clone(),
                    title: scenario.title.clone(),
                    required: scenario.required,
                    detail: None,
                };
                if scenario.required {
                    report.missing_required.push(finding);
                } else {
                    report.missing_optional.push(finding);
                }
            }
        }
    }

    Ok(report)
}

#[derive(Debug, Default)]
pub struct Report {
    pub passed: Vec<String>,
    pub failed: Vec<Finding>,
    pub skipped_required: Vec<Finding>,
    pub skipped_optional: Vec<Finding>,
    pub missing_required: Vec<Finding>,
    pub missing_optional: Vec<Finding>,
    pub unknown: Vec<String>,
}

#[derive(Debug)]
pub struct Finding {
    pub id: String,
    pub title: String,
    pub required: bool,
    pub detail: Option<String>,
}

impl Report {
    /// True when nothing required is missing, skipped, or failing, and nothing
    /// optional is failing.
    pub fn conformant(&self) -> bool {
        self.failed.is_empty()
            && self.skipped_required.is_empty()
            && self.missing_required.is_empty()
            && self.unknown.is_empty()
    }

    pub fn print(&self, client: &str) {
        println!("== Felix client conformance: {client} ==\n");
        println!("  passed              {}", self.passed.len());
        println!("  failed              {}", self.failed.len());
        println!("  required missing    {}", self.missing_required.len());
        println!("  required skipped    {}", self.skipped_required.len());
        println!(
            "  optional not run    {}",
            self.missing_optional.len() + self.skipped_optional.len()
        );

        for finding in &self.failed {
            let tag = if finding.required {
                "required"
            } else {
                "optional"
            };
            println!("\n  FAIL [{tag}] {}  {}", finding.id, finding.title);
            if let Some(detail) = &finding.detail {
                println!("       {detail}");
            }
        }
        for finding in self.missing_required.iter().chain(&self.skipped_required) {
            println!("\n  MISSING [required] {}  {}", finding.id, finding.title);
            if let Some(detail) = &finding.detail {
                println!("          {detail}");
            }
        }
        for id in &self.unknown {
            println!("\n  UNKNOWN scenario id {id} — not in the catalogue");
        }

        // Optional gaps are listed but do not fail: they are how a binding
        // says "not wrapped yet" without lying about it.
        if !self.missing_optional.is_empty() || !self.skipped_optional.is_empty() {
            println!("\n  not claimed (optional):");
            for finding in self.missing_optional.iter().chain(&self.skipped_optional) {
                match &finding.detail {
                    Some(detail) => println!("    {}  — {detail}", finding.id),
                    None => println!("    {}", finding.id),
                }
            }
        }

        println!(
            "\n{}",
            if self.conformant() {
                "CONFORMANT"
            } else {
                "NOT CONFORMANT"
            }
        );
    }
}

/// Read a results file a client-under-test produced.
pub fn read_results(path: &Path) -> Result<Results> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read the results file {}", path.display()))?;
    let results: Results = serde_json::from_str(&raw)
        .with_context(|| format!("parse {} as conformance results", path.display()))?;
    if results.outcomes.is_empty() {
        bail!(
            "{} reported no outcomes; a client that ran nothing is not conformant",
            path.display()
        );
    }
    Ok(results)
}

/// What a client-under-test needs in order to connect.
///
/// Written by `client-fixture` and read by the suite in whatever language is
/// being checked. Names of the fixtures are included rather than assumed, so
/// a suite does not hard-code them and drift.
#[derive(Debug, Serialize, Deserialize)]
pub struct Fixture {
    pub addrs: Vec<String>,
    pub tenant_id: String,
    pub namespace: String,
    /// Publishes, subscribes, cache reads and writes.
    pub token: String,
    /// A token deliberately holding *no* stream permissions, so a suite can
    /// prove its client reports authorization failure distinguishably.
    pub unauthorized_token: String,
    /// PEM holding the broker's certificate, for clients that verify properly
    /// rather than skipping verification.
    pub ca_file: String,
    /// A durable stream: offsets, resume, and ordering are observable on it.
    pub durable_stream: String,
    pub cache: String,
    /// A cache with exactly one shard.
    ///
    /// A *prefix* watch reads one shard, and keys sharing a prefix hash to
    /// different ones — so a prefix or retained watch over the multi-shard
    /// cache above sees only the fraction that landed on the shard it opened.
    /// Scenarios about prefix watches use this instead, which is the honest
    /// scope of the feature rather than a workaround.
    pub single_shard_cache: String,
    /// A stream name that is deliberately *not* registered, for the
    /// unknown-stream scenario.
    pub missing_stream: String,
}

#[cfg(test)]
mod tests {
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
}
