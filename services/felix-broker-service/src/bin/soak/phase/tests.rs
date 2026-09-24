use super::*;
use crate::tests::clean_outcome;

#[test]
fn phase_report_summarises_its_samples() {
    let phase = &clean_outcome().phases[0];
    assert_eq!(phase.peak_rss_kb(), 50_000);
    assert_eq!(phase.peak_fds(), 35);
    assert_eq!(phase.peak_tasks(), 300);
    assert_eq!(phase.last().expect("a sample").open_fds, 35);
}

#[test]
fn phase_report_handles_having_no_samples() {
    let phase = PhaseReport {
        name: "empty",
        samples: Vec::new(),
        published: 0,
        received: 0,
        errors: 0,
    };
    assert_eq!(phase.peak_rss_kb(), 0);
    assert_eq!(phase.peak_fds(), 0);
    assert_eq!(phase.peak_tasks(), 0);
    assert!(phase.last().is_none());
}
