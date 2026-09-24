use super::*;
use crate::tests::{clean_outcome, sample};

#[test]
fn a_clean_run_produces_no_findings() {
    let findings = evaluate(&SoakConfig::default(), &clean_outcome());
    assert!(findings.is_empty(), "unexpected findings: {findings:?}");
}

#[test]
fn unsettled_resources_are_reported_with_the_shared_process_caveat() {
    let mut outcome = clean_outcome();
    outcome.settled = false;
    outcome.quiesced = sample(11_000, 19, 106);
    let findings = evaluate(&SoakConfig::default(), &outcome);
    let text = findings.join(" ");
    assert!(
        text.contains("did not return to the idle baseline"),
        "{findings:?}"
    );
    // The caveat is the point: without it a reader takes this as a broker leak,
    // which is exactly the misdiagnosis that produced a false CI failure.
    assert!(text.contains("load generators"), "{findings:?}");
}

/// Memory is judged across identical cycles, so growth *within* tolerance must
/// not fire and growth beyond it must.
#[test]
fn rss_growth_fires_only_beyond_the_tolerance() {
    let config = SoakConfig::default(); // 25% tolerance
    let mut outcome = clean_outcome();

    outcome.cycle_peaks = vec![100_000, 120_000]; // +20%
    assert!(
        evaluate(&config, &outcome).is_empty(),
        "20% growth is inside the 25% tolerance"
    );

    outcome.cycle_peaks = vec![100_000, 140_000]; // +40%
    let findings = evaluate(&config, &outcome);
    assert!(
        findings.iter().any(|f| f.contains("peak RSS grew")),
        "{findings:?}"
    );
}

#[test]
fn a_single_cycle_cannot_judge_memory_growth() {
    let mut outcome = clean_outcome();
    outcome.cycle_peaks = vec![100_000];
    assert!(
        evaluate(&SoakConfig::default(), &outcome).is_empty(),
        "one cycle gives nothing to compare against"
    );
}

/// The broker's own gauges are the authoritative leak signal, so each one must
/// be checked and a residue in any of them must fail.
#[test]
fn every_registration_gauge_is_checked_for_residue() {
    for gauge in [
        "felix_sub_active_connections",
        "felix_sub_connection_subscribers",
        "felix_broker_ingress_queue_depth",
        "felix_broker_out_ack_depth",
        "felix_sub_queue_len",
        "felix_sub_lane_queue_len",
    ] {
        let mut outcome = clean_outcome();
        outcome.gauges.insert(gauge.to_string(), 3.0);
        let findings = evaluate(&SoakConfig::default(), &outcome);
        assert!(
            findings.iter().any(|f| f.contains(gauge)),
            "{gauge} residue should be a finding, got {findings:?}"
        );
    }
}

#[test]
fn a_gauge_at_zero_is_not_a_finding() {
    let mut outcome = clean_outcome();
    outcome
        .gauges
        .insert("felix_sub_active_connections".to_string(), 0.0);
    assert!(evaluate(&SoakConfig::default(), &outcome).is_empty());
}

#[test]
fn an_unfinished_drain_is_reported() {
    let mut outcome = clean_outcome();
    outcome.unfinished = vec!["quic_connections"];
    let findings = evaluate(&SoakConfig::default(), &outcome);
    assert!(
        findings.iter().any(|f| f.contains("quic_connections")),
        "{findings:?}"
    );
}

/// A phase that moved no traffic proves nothing, so it must fail rather than
/// silently report a clean run.
#[test]
fn a_phase_that_published_nothing_is_a_finding() {
    let mut outcome = clean_outcome();
    outcome.phases[0].published = 0;
    let findings = evaluate(&SoakConfig::default(), &outcome);
    assert!(
        findings.iter().any(|f| f.contains("published nothing")),
        "{findings:?}"
    );
}

#[test]
fn restart_findings_are_carried_through() {
    let mut outcome = clean_outcome();
    outcome.restart_findings = vec!["cycle 0: child exited unsuccessfully".to_string()];
    let findings = evaluate(&SoakConfig::default(), &outcome);
    assert!(
        findings.iter().any(|f| f.contains("cycle 0")),
        "{findings:?}"
    );
}

#[test]
fn timeseries_is_written_as_one_json_object_per_sample() {
    let dir = std::env::temp_dir().join(format!("soak-ts-{}", std::process::id()));
    let path = dir.join("ts.jsonl");
    let outcome = clean_outcome();
    write_timeseries(path.to_str().expect("utf8 path"), &outcome.phases).expect("write");
    let body = std::fs::read_to_string(&path).expect("read back");
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 2, "one line per sample: {body}");
    assert!(
        lines[0].contains("\"phase\":\"sustained_load\""),
        "{}",
        lines[0]
    );
    assert!(lines[0].contains("\"rss_kb\":10000"), "{}", lines[0]);
    let _ = std::fs::remove_dir_all(&dir);
}
