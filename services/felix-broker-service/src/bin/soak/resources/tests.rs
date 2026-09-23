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
