//! Process resource sampling for the soak harness.
//!
//! Deliberately dependency-free and best-effort: a sampler that fails to read a
//! counter must not abort a soak run, so every reader falls back to 0 and the
//! harness treats a flat-zero series as "unavailable on this platform" rather
//! than as evidence of anything.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// One point in the resource time series.
#[derive(Debug, Clone, Copy)]
pub struct ResourceSample {
    pub unix_ms: u128,
    pub rss_kb: u64,
    pub open_fds: u64,
    pub alive_tasks: usize,
}

impl ResourceSample {
    pub fn capture() -> Self {
        Self {
            unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
            rss_kb: read_rss_kb(),
            open_fds: count_open_fds(),
            alive_tasks: tokio::runtime::Handle::try_current()
                .map(|handle| handle.metrics().num_alive_tasks())
                .unwrap_or(0),
        }
    }
}

/// Resident set size in KiB.
///
/// Linux exposes this in `/proc/self/statm` as a page count. macOS has no
/// equivalent procfs entry, so we shell out to `ps` — slow, but the sampler
/// runs twice a second, not in a hot loop.
#[cfg(target_os = "linux")]
fn read_rss_kb() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    // Field 2 is resident pages.
    let Some(resident_pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    let pages: u64 = resident_pages.parse().unwrap_or(0);
    let page_kb = 4; // Linux page size on every platform Felix targets.
    pages * page_kb
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> u64 {
    let pid = std::process::id();
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
    else {
        return 0;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

/// Count of open file descriptors.
///
/// `/dev/fd` is present on both Linux (symlinked to `/proc/self/fd`) and macOS,
/// so one path covers both. The directory handle opened to read it is itself an
/// fd, but it is opened and closed identically on every sample, so it cancels
/// out of any comparison.
fn count_open_fds() -> u64 {
    let Ok(entries) = std::fs::read_dir("/dev/fd") else {
        return 0;
    };
    entries.count() as u64
}

/// Extract gauge values from rendered Prometheus text.
///
/// Only gauges are collected: counters grow monotonically by design and say
/// nothing about whether resources were released, whereas a gauge that fails to
/// return to zero after quiescence is exactly the leak signal we want. Labelled
/// series are summed across labels, since "any connection still registered" is
/// the question, not which one.
pub fn scrape_gauges(rendered: &str) -> HashMap<String, f64> {
    let mut gauge_names = Vec::new();
    let mut values: HashMap<String, f64> = HashMap::new();

    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            if let (Some(name), Some(kind)) = (parts.next(), parts.next())
                && kind == "gauge"
            {
                gauge_names.push(name.to_string());
            }
            continue;
        }
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((key, raw_value)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = key.split('{').next().unwrap_or(key).trim();
        if !gauge_names.iter().any(|g| g == name) {
            continue;
        }
        if let Ok(value) = raw_value.trim().parse::<f64>() {
            *values.entry(name.to_string()).or_insert(0.0) += value;
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_reports_plausible_values() {
        let sample = ResourceSample::capture();
        // A running process always has at least stdin/stdout/stderr open, and
        // some resident memory. Zero means the reader failed, not that the
        // process is weightless.
        assert!(sample.rss_kb > 0, "rss reader returned nothing");
        assert!(
            sample.open_fds >= 3,
            "fd reader returned {}",
            sample.open_fds
        );
        // This unit test does not run inside a Tokio runtime. The soak itself
        // does, where `alive_tasks` is populated from `Handle::metrics()`.
        assert_eq!(sample.alive_tasks, 0);
    }

    #[test]
    fn scrape_gauges_sums_labels_and_ignores_counters() {
        let rendered = "\
# TYPE felix_sub_active_connections gauge
felix_sub_active_connections 3
# TYPE felix_sub_connection_subscribers gauge
felix_sub_connection_subscribers{connection_id=\"1\"} 2
felix_sub_connection_subscribers{connection_id=\"2\"} 5
# TYPE felix_publish_requests_total counter
felix_publish_requests_total 900
";
        let gauges = scrape_gauges(rendered);
        assert_eq!(gauges.get("felix_sub_active_connections"), Some(&3.0));
        // Labelled series are summed, so "anything still registered" is visible
        // without needing to know the label set.
        assert_eq!(gauges.get("felix_sub_connection_subscribers"), Some(&7.0));
        assert!(
            !gauges.contains_key("felix_publish_requests_total"),
            "counters must not be treated as leak signals"
        );
    }

    #[test]
    fn scrape_gauges_tolerates_empty_input() {
        assert!(scrape_gauges("").is_empty());
    }
}
