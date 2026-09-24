//! Fixtures shared by the soak harness's unit tests.

use std::collections::HashMap;

use crate::phase::PhaseReport;
use crate::report::SoakOutcome;
use crate::resources::ResourceSample;

pub(crate) fn sample(rss_kb: u64, open_fds: u64, alive_tasks: usize) -> ResourceSample {
    ResourceSample {
        unix_ms: 0,
        rss_kb,
        open_fds,
        alive_tasks,
    }
}

/// A run where nothing went wrong, used as the baseline every case mutates.
pub(crate) fn clean_outcome() -> SoakOutcome {
    SoakOutcome {
        baseline: sample(10_000, 11, 10),
        quiesced: sample(11_000, 11, 10),
        phases: vec![PhaseReport {
            name: "sustained_load",
            samples: vec![sample(10_000, 11, 10), sample(50_000, 35, 300)],
            published: 1_000,
            received: 3_000,
            errors: 0,
        }],
        gauges: HashMap::new(),
        restart_findings: Vec::new(),
        unfinished: Vec::new(),
        cycle_peaks: vec![100_000, 101_000],
        settled: true,
    }
}
