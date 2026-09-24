//! Command-line options.

use anyhow::{Context, Result, bail};

#[derive(Debug)]
pub(crate) struct SoakConfig {
    // Duration of each load-bearing phase.
    pub(crate) phase_secs: u64,
    // Upper bound on how long to wait for resources to settle after load stops.
    // This is a cap, not a sleep: a healthy run returns as soon as fds and tasks
    // are back at baseline, so raising it costs nothing and only buys tolerance
    // for a slow or busy machine.
    pub(crate) quiesce_secs: u64,
    pub(crate) publishers: usize,
    pub(crate) subscribers: usize,
    pub(crate) payload_bytes: usize,
    // Connect/disconnect iterations in the churn phase.
    pub(crate) churn_cycles: usize,
    // SIGTERM start/stop iterations in the restart phase.
    pub(crate) restart_cycles: usize,
    // Identical load repetitions used to separate a leak from allocator retention.
    pub(crate) load_cycles: usize,
    // Fraction of RSS growth over baseline tolerated after quiescence.
    pub(crate) rss_growth_tolerance: f64,
    // Where to write the sampled time series, so a run is reviewable after the
    // fact rather than only as console output.
    pub(crate) timeseries_path: Option<String>,
}

impl Default for SoakConfig {
    fn default() -> Self {
        Self {
            phase_secs: 30,
            quiesce_secs: 30,
            publishers: 4,
            subscribers: 4,
            payload_bytes: 1024,
            churn_cycles: 200,
            restart_cycles: 5,
            load_cycles: 4,
            rss_growth_tolerance: 0.25,
            timeseries_path: None,
        }
    }
}

pub(crate) fn parse_args() -> Result<(SoakConfig, bool)> {
    parse_args_from(std::env::args().skip(1))
}

/// Split from `parse_args` so the flag handling is reachable from tests without
/// going through process arguments.
pub(crate) fn parse_args_from<I: Iterator<Item = String>>(args: I) -> Result<(SoakConfig, bool)> {
    let mut config = SoakConfig::default();
    let mut serve_child = false;
    let args: Vec<String> = args.collect();
    let mut idx = 0;
    while idx < args.len() {
        let take = |idx: &mut usize| -> Result<String> {
            *idx += 1;
            args.get(*idx)
                .cloned()
                .with_context(|| format!("missing value for {}", args[*idx - 1]))
        };
        match args[idx].as_str() {
            "--serve-child" => serve_child = true,
            "--duration-secs" => config.phase_secs = take(&mut idx)?.parse()?,
            "--quiesce-secs" => config.quiesce_secs = take(&mut idx)?.parse()?,
            "--publishers" => config.publishers = take(&mut idx)?.parse()?,
            "--subscribers" => config.subscribers = take(&mut idx)?.parse()?,
            "--payload" => config.payload_bytes = take(&mut idx)?.parse()?,
            "--churn-cycles" => config.churn_cycles = take(&mut idx)?.parse()?,
            "--restart-cycles" => config.restart_cycles = take(&mut idx)?.parse()?,
            "--load-cycles" => config.load_cycles = take(&mut idx)?.parse()?,
            "--timeseries" => config.timeseries_path = Some(take(&mut idx)?),
            other => bail!("unknown argument: {other}"),
        }
        idx += 1;
    }
    Ok((config, serve_child))
}

#[cfg(test)]
mod tests;
