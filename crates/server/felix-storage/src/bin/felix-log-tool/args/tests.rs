use super::*;

fn args(raw: &[&str]) -> Vec<String> {
    raw.iter().map(|s| s.to_string()).collect()
}

#[test]
fn write_defaults_are_applied() {
    let Command::Write(parsed) = parse(args(&["write", "--dir", "/tmp/log"])).expect("parse")
    else {
        panic!("expected write");
    };
    assert_eq!(parsed.dir, PathBuf::from("/tmp/log"));
    assert_eq!(parsed.records, 0);
    assert_eq!(parsed.payload_bytes, 128);
    assert_eq!(parsed.batch, 1);
    assert_eq!(parsed.config.fsync_mode, FsyncMode::OnCommit);
    assert!(!parsed.report_acks);
}

#[test]
fn both_flag_spellings_are_accepted() {
    let Command::Write(spaced) =
        parse(args(&["write", "--dir", "/tmp/a", "--records", "5"])).expect("parse")
    else {
        panic!("expected write");
    };
    let Command::Write(equals) =
        parse(args(&["write", "--dir=/tmp/a", "--records=5"])).expect("parse")
    else {
        panic!("expected write");
    };
    assert_eq!(spaced, equals);
}

#[test]
fn boolean_flags_need_no_value() {
    let Command::Write(parsed) = parse(args(&[
        "write",
        "--dir",
        "/tmp/a",
        "--report-acks",
        "--clean-shutdown",
        "--no-preallocate",
    ]))
    .expect("parse") else {
        panic!("expected write");
    };
    assert!(parsed.report_acks);
    assert!(parsed.clean_shutdown);
    assert!(!parsed.config.preallocate_segments);
}

#[test]
fn every_fsync_mode_parses() {
    for (raw, expected) in [
        ("none", FsyncMode::None),
        ("on_commit", FsyncMode::OnCommit),
        (
            "periodic",
            FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
        ),
    ] {
        let Command::Write(parsed) =
            parse(args(&["write", "--dir", "/tmp/a", "--fsync", raw])).expect("parse")
        else {
            panic!("expected write");
        };
        assert_eq!(parsed.config.fsync_mode, expected, "{raw}");
    }
}

#[test]
fn a_periodic_interval_is_read() {
    let Command::Write(parsed) = parse(args(&[
        "write",
        "--dir",
        "/tmp/a",
        "--fsync",
        "periodic",
        "--fsync-interval-ms",
        "10",
    ]))
    .expect("parse") else {
        panic!("expected write");
    };
    assert_eq!(
        parsed.config.fsync_mode,
        FsyncMode::Periodic {
            interval: Duration::from_millis(10)
        }
    );
}

#[test]
fn a_missing_directory_is_an_error() {
    let err = parse(args(&["write"])).expect_err("no dir");
    assert!(err.contains("--dir is required"), "{err}");
}

#[test]
fn an_unknown_option_is_rejected_rather_than_ignored() {
    let err = parse(args(&["write", "--dir", "/tmp/a", "--recrods", "5"])).expect_err("typo");
    assert!(err.contains("--recrods"), "{err}");
}

#[test]
fn an_unknown_subcommand_is_rejected() {
    let err = parse(args(&["frobnicate"])).expect_err("unknown");
    assert!(err.contains("frobnicate"), "{err}");
}

#[test]
fn a_non_numeric_count_is_rejected() {
    let err =
        parse(args(&["write", "--dir", "/tmp/a", "--records", "many"])).expect_err("bad number");
    assert!(err.contains("--records"), "{err}");
}

#[test]
fn an_invalid_fsync_mode_is_rejected() {
    let err = parse(args(&["write", "--dir", "/tmp/a", "--fsync", "sometimes"])).expect_err("bad");
    assert!(err.contains("--fsync"), "{err}");
}

#[test]
fn an_invalid_log_config_is_rejected_at_parse_time() {
    let err = parse(args(&["write", "--dir", "/tmp/a", "--segment-bytes", "4"]))
        .expect_err("tiny segment");
    assert!(err.contains("segment_size_bytes"), "{err}");
}

#[test]
fn verify_and_bench_parse_their_own_options() {
    let Command::Verify(verify) = parse(args(&[
        "verify",
        "--dir",
        "/tmp/a",
        "--expect-at-least",
        "100",
        "--payload-bytes",
        "64",
    ]))
    .expect("parse") else {
        panic!("expected verify");
    };
    assert_eq!(verify.expect_at_least, Some(100));
    assert_eq!(verify.payload_bytes, Some(64));

    let Command::Bench(bench) = parse(args(&[
        "bench",
        "--dir",
        "/tmp/a",
        "--records",
        "1000",
        "--concurrency",
        "8",
        "--label",
        "on_commit-c8",
    ]))
    .expect("parse") else {
        panic!("expected bench");
    };
    assert_eq!(bench.records, 1000);
    assert_eq!(bench.concurrency, 8);
    assert_eq!(bench.label, "on_commit-c8");
}

#[test]
fn zero_batch_and_concurrency_are_clamped_to_one() {
    let Command::Bench(bench) = parse(args(&[
        "bench",
        "--dir",
        "/tmp/a",
        "--batch",
        "0",
        "--concurrency",
        "0",
    ]))
    .expect("parse") else {
        panic!("expected bench");
    };
    assert_eq!(bench.batch, 1);
    assert_eq!(bench.concurrency, 1);
}

#[test]
fn help_prints_usage() {
    let err = parse(args(&["--help"])).expect_err("usage");
    assert!(err.contains("felix-log-tool"), "{err}");
}
