//! Judging a run against its steady-state envelope, and writing it up.

use anyhow::Result;
use std::collections::HashMap;

use crate::config::SoakConfig;
use crate::phase::PhaseReport;
use crate::resources::ResourceSample;

/// Compare post-quiescence state against baseline and the broker's own gauges.
/// Everything one soak run produced, so evaluation and reporting take a single
/// argument instead of a long positional list that is easy to transpose.
pub(crate) struct SoakOutcome {
    pub(crate) baseline: ResourceSample,
    pub(crate) quiesced: ResourceSample,
    pub(crate) phases: Vec<PhaseReport>,
    pub(crate) gauges: HashMap<String, f64>,
    pub(crate) restart_findings: Vec<String>,
    pub(crate) unfinished: Vec<&'static str>,
    pub(crate) cycle_peaks: Vec<u64>,
    /// Whether process fds and tasks returned to the idle baseline before the cap.
    pub(crate) settled: bool,
}

pub(crate) fn evaluate(config: &SoakConfig, outcome: &SoakOutcome) -> Vec<String> {
    let SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        restart_findings,
        unfinished,
        cycle_peaks,
        settled,
    } = outcome;
    let mut findings: Vec<String> = restart_findings.clone();

    // Process-wide file descriptors and task counts are reported as a single
    // "did it come to rest" check rather than two exact comparisons.
    //
    // The distinction matters because this harness runs the load generators in
    // the *same process* as the broker. A raw `quiesced > baseline` comparison
    // therefore charges the broker for the harness's own client teardown —
    // `felix-client`'s `Subscription` spawns detached pipeline tasks that this
    // harness cannot join, so they wind down on their own schedule. Exact
    // equality at an arbitrary instant was measuring that race, not a leak.
    //
    // The broker's own gauges below are the authoritative assertion; these are
    // corroborating evidence that nothing outlived the run.
    if !settled {
        findings.push(format!(
            "process resources did not return to the idle baseline within the {}s cap: \
             fds {} -> {}, tasks {} -> {}. Note both the broker and the load generators \
             live in this process, so check the broker gauges below before reading this \
             as a broker leak.",
            config.quiesce_secs,
            baseline.open_fds,
            quiesced.open_fds,
            baseline.alive_tasks,
            quiesced.alive_tasks
        ));
    }

    // Memory is judged across identical repeated cycles, not against baseline.
    // Comparing a post-load RSS to a pre-load one only measures allocator
    // retention and would flag every healthy run. A leak is peak RSS still
    // climbing on the last identical cycle.
    if cycle_peaks.len() >= 2 {
        let first = cycle_peaks[0] as f64;
        let last = *cycle_peaks.last().expect("checked non-empty") as f64;
        if first > 0.0 {
            let growth = (last - first) / first;
            if growth > config.rss_growth_tolerance {
                findings.push(format!(
                    "peak RSS grew {:.1}% across {} identical load cycles ({} -> {} KiB), beyond \
                     the {:.0}% tolerance; retention would have plateaued",
                    growth * 100.0,
                    cycle_peaks.len(),
                    cycle_peaks[0],
                    last as u64,
                    config.rss_growth_tolerance * 100.0
                ));
            }
        }
    }

    // Registration gauges must return to exactly zero: every client has gone, so
    // any residue is an entry that will never be reclaimed.
    for gauge in [
        "felix_sub_active_connections",
        "felix_sub_connection_subscribers",
        "felix_broker_ingress_queue_depth",
        "felix_broker_out_ack_depth",
        "felix_sub_queue_len",
        "felix_sub_lane_queue_len",
    ] {
        if let Some(value) = gauges.get(gauge)
            && *value > 0.0
        {
            findings.push(format!(
                "gauge {gauge} did not return to zero after quiescence: {value}"
            ));
        }
    }

    if !unfinished.is_empty() {
        findings.push(format!(
            "final drain did not complete within its deadline: {unfinished:?}"
        ));
    }

    // A phase that moved no traffic proves nothing; treat it as a harness
    // failure rather than silently reporting a clean run.
    for phase in phases {
        if phase.published == 0 {
            findings.push(format!(
                "phase {} published nothing, so it did not exercise the broker",
                phase.name
            ));
        }
    }

    findings
}

/// Write the sampled series as JSONL so a run can be re-examined or charted
/// later, matching how `data/raw/latency_demo_runs.jsonl` records perf runs.
pub(crate) fn write_timeseries(path: &str, phases: &[PhaseReport]) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(path)?;
    for phase in phases {
        for sample in &phase.samples {
            writeln!(
                file,
                r#"{{"phase":"{}","unix_ms":{},"rss_kb":{},"open_fds":{},"alive_tasks":{}}}"#,
                phase.name, sample.unix_ms, sample.rss_kb, sample.open_fds, sample.alive_tasks
            )?;
        }
    }
    Ok(())
}

pub(crate) fn report(outcome: &SoakOutcome, findings: &[String]) {
    let SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        ..
    } = outcome;
    println!("\n== Soak report ==");
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "phase", "peak_rss", "peak_fds", "peak_tasks"
    );
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "baseline", baseline.rss_kb, baseline.open_fds, baseline.alive_tasks
    );
    for phase in phases {
        println!(
            "{:<20} {:>10} {:>8} {:>10}",
            phase.name,
            phase.peak_rss_kb(),
            phase.peak_fds(),
            phase.peak_tasks()
        );
        if let Some(last) = phase.last() {
            println!(
                "{:<20} {:>10} {:>8} {:>10}   (end of phase)",
                "", last.rss_kb, last.open_fds, last.alive_tasks
            );
        }
    }
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "quiesced", quiesced.rss_kb, quiesced.open_fds, quiesced.alive_tasks
    );

    println!("\nsteady-state gauges:");
    let mut names: Vec<&String> = gauges.keys().collect();
    names.sort();
    for name in names {
        println!("  {:<45} {}", name, gauges[name]);
    }

    if findings.is_empty() {
        println!("\nNo findings: resources returned to the steady-state envelope.");
    } else {
        println!("\n{} finding(s):", findings.len());
        for finding in findings {
            println!("  - {finding}");
        }
    }
}

#[cfg(test)]
mod tests;
