use super::*;

#[test]
fn percentiles_use_nearest_rank() {
    let sorted: Vec<u64> = (1..=100).collect();
    assert_eq!(percentile(&sorted, 0.50), 50.0);
    assert_eq!(percentile(&sorted, 0.99), 99.0);
    assert_eq!(percentile(&sorted, 1.0), 100.0);
    // A quantile below the first rank still returns a real sample.
    assert_eq!(percentile(&sorted, 0.0), 1.0);
}

#[test]
fn percentiles_of_an_empty_sample_are_zero() {
    assert_eq!(percentile(&[], 0.5), 0.0);
}

#[test]
fn fsync_modes_render_distinctly() {
    assert_eq!(describe_fsync(FsyncMode::None), "none");
    assert_eq!(describe_fsync(FsyncMode::OnCommit), "on_commit");
    assert_eq!(
        describe_fsync(FsyncMode::Periodic {
            interval: Duration::from_millis(40)
        }),
        "periodic:40ms"
    );
}
