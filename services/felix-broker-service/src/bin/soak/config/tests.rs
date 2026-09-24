use super::*;

#[test]
fn parse_args_reads_flags_and_child_mode() {
    let (config, child) = parse_args_from(
        ["--duration-secs", "7", "--publishers", "9", "--serve-child"]
            .iter()
            .map(|s| s.to_string()),
    )
    .expect("parse");
    assert_eq!(config.phase_secs, 7);
    assert_eq!(config.publishers, 9);
    assert!(child);
}

#[test]
fn parse_args_rejects_unknown_and_valueless_flags() {
    assert!(
        parse_args_from(["--nonsense"].iter().map(|s| s.to_string()))
            .expect_err("unknown flag")
            .to_string()
            .contains("unknown argument")
    );
    assert!(
        parse_args_from(["--publishers"].iter().map(|s| s.to_string()))
            .expect_err("flag with no value")
            .to_string()
            .contains("missing value")
    );
}
