// Argument parsing for `felix-log-tool`.
//
// Hand-rolled rather than pulled from a crate: the tool ships inside the storage
// library, and adding an argument parser to that dependency tree for three
// subcommands is a poor trade. The parser is strict — an unknown flag is an
// error, not a silently ignored typo, because these commands are used to make
// durability claims and a misread flag would make a claim about the wrong thing.

use std::path::PathBuf;
use std::time::Duration;

use felix_storage::log::{FsyncMode, LogConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Append records until told to stop, for crash and benchmark runs.
    Write(WriteArgs),
    /// Recover a log and report what survived.
    Verify(VerifyArgs),
    /// Measure append latency and throughput under one durability policy.
    Bench(BenchArgs),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteArgs {
    pub dir: PathBuf,
    pub records: u64,
    pub payload_bytes: usize,
    pub batch: usize,
    pub config: LogConfig,
    /// Emit one line of JSON per acknowledged batch, so a crash harness knows
    /// exactly which records were promised durable before it pulled the plug.
    pub report_acks: bool,
    /// Flush and exit cleanly at the end instead of leaving the log open.
    pub clean_shutdown: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyArgs {
    pub dir: PathBuf,
    pub config: LogConfig,
    /// Fail unless at least this many records survived.
    pub expect_at_least: Option<u64>,
    /// Fail unless payloads match the generator's `--payload-bytes` shape.
    pub payload_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchArgs {
    pub dir: PathBuf,
    pub records: u64,
    pub payload_bytes: usize,
    pub batch: usize,
    /// Concurrent publishers. Group commit only pays off above one.
    pub concurrency: usize,
    pub warmup_records: u64,
    pub config: LogConfig,
    pub label: String,
}

pub const USAGE: &str = "\
felix-log-tool — exercise and measure the durable log

USAGE:
    felix-log-tool write  --dir <PATH> [OPTIONS]
    felix-log-tool verify --dir <PATH> [OPTIONS]
    felix-log-tool bench  --dir <PATH> [OPTIONS]

COMMON OPTIONS:
    --dir <PATH>                  Log directory (required)
    --fsync <none|periodic|on_commit>   Durability policy [default: on_commit]
    --fsync-interval-ms <N>       Interval for --fsync periodic [default: 250]
    --segment-bytes <N>           Segment rollover size [default: 67108864]
    --index-spacing-bytes <N>     Sparse index interval [default: 4096]
    --no-preallocate              Do not reserve segment blocks up front
    --rollover-threshold-percent <N>  Start the background roll at this % of
                                  --segment-bytes; 100 disables it [default: 80]
    --max-overshoot-percent <N>   How far past --segment-bytes a segment may
                                  grow while its replacement is prepared

write OPTIONS:
    --records <N>                 Records to append; 0 means run until killed
    --payload-bytes <N>           Payload size [default: 128]
    --batch <N>                   Records per append [default: 1]
    --report-acks                 Print one JSON line per acknowledged batch
    --clean-shutdown              Flush and exit cleanly when done

verify OPTIONS:
    --expect-at-least <N>         Fail unless at least N records survived
    --payload-bytes <N>           Also check payload contents

bench OPTIONS:
    --records <N>                 Records to measure [default: 20000]
    --payload-bytes <N>           Payload size [default: 128]
    --batch <N>                   Records per append [default: 1]
    --concurrency <N>             Concurrent publishers [default: 1]
    --warmup-records <N>          Records to discard before measuring [default: 2000]
    --label <TEXT>                Name for this run in the JSON output
";

/// Parse `args` (excluding the program name).
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Command, String> {
    let mut args = args.into_iter();
    let command = args
        .next()
        .ok_or_else(|| "missing subcommand".to_string())?;
    let flags = Flags::collect(args)?;

    match command.as_str() {
        "write" => {
            let config = flags.log_config()?;
            let parsed = WriteArgs {
                dir: flags.required_path("--dir")?,
                records: flags.number("--records")?.unwrap_or(0),
                payload_bytes: flags.number("--payload-bytes")?.unwrap_or(128) as usize,
                batch: flags.number("--batch")?.unwrap_or(1).max(1) as usize,
                config,
                report_acks: flags.flag("--report-acks"),
                clean_shutdown: flags.flag("--clean-shutdown"),
            };
            flags.finish()?;
            Ok(Command::Write(parsed))
        }
        "verify" => {
            let config = flags.log_config()?;
            let parsed = VerifyArgs {
                dir: flags.required_path("--dir")?,
                config,
                expect_at_least: flags.number("--expect-at-least")?,
                payload_bytes: flags.number("--payload-bytes")?.map(|n| n as usize),
            };
            flags.finish()?;
            Ok(Command::Verify(parsed))
        }
        "bench" => {
            let config = flags.log_config()?;
            let parsed = BenchArgs {
                dir: flags.required_path("--dir")?,
                records: flags.number("--records")?.unwrap_or(20_000),
                payload_bytes: flags.number("--payload-bytes")?.unwrap_or(128) as usize,
                batch: flags.number("--batch")?.unwrap_or(1).max(1) as usize,
                concurrency: flags.number("--concurrency")?.unwrap_or(1).max(1) as usize,
                warmup_records: flags.number("--warmup-records")?.unwrap_or(2_000),
                config,
                label: flags.text("--label").unwrap_or_else(|| "run".to_string()),
            };
            flags.finish()?;
            Ok(Command::Bench(parsed))
        }
        "help" | "--help" | "-h" => Err(USAGE.to_string()),
        other => Err(format!("unknown subcommand {other:?}\n\n{USAGE}")),
    }
}

/// Flags gathered before interpretation, so each subcommand can consume the
/// ones it knows about and reject whatever is left.
struct Flags {
    values: Vec<(String, Option<String>)>,
    consumed: std::cell::RefCell<Vec<String>>,
}

impl Flags {
    fn collect<I: Iterator<Item = String>>(args: I) -> Result<Self, String> {
        let mut values = Vec::new();
        let mut args = args.peekable();
        while let Some(token) = args.next() {
            if !token.starts_with("--") {
                return Err(format!("unexpected argument {token:?}\n\n{USAGE}"));
            }
            // `--flag=value` and `--flag value` are both accepted; a bare
            // `--flag` is a boolean.
            if let Some((name, value)) = token.split_once('=') {
                values.push((name.to_string(), Some(value.to_string())));
            } else if args.peek().is_some_and(|next| !next.starts_with("--")) {
                values.push((token, args.next()));
            } else {
                values.push((token, None));
            }
        }
        Ok(Self {
            values,
            consumed: std::cell::RefCell::new(Vec::new()),
        })
    }

    fn get(&self, name: &str) -> Option<&Option<String>> {
        self.consumed.borrow_mut().push(name.to_string());
        self.values
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }

    fn flag(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    fn text(&self, name: &str) -> Option<String> {
        self.get(name).and_then(|value| value.clone())
    }

    fn required_path(&self, name: &str) -> Result<PathBuf, String> {
        self.text(name)
            .map(PathBuf::from)
            .ok_or_else(|| format!("{name} is required\n\n{USAGE}"))
    }

    fn number(&self, name: &str) -> Result<Option<u64>, String> {
        match self.text(name) {
            None => Ok(None),
            Some(raw) => raw
                .parse()
                .map(Some)
                .map_err(|_| format!("{name} expects a number, got {raw:?}")),
        }
    }

    fn log_config(&self) -> Result<LogConfig, String> {
        let defaults = LogConfig::default();
        let interval = self
            .number("--fsync-interval-ms")?
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(250));
        let fsync_mode = match self.text("--fsync").as_deref() {
            None | Some("on_commit") => FsyncMode::OnCommit,
            Some("none") => FsyncMode::None,
            Some("periodic") => FsyncMode::Periodic { interval },
            Some(other) => {
                return Err(format!(
                    "--fsync expects none|periodic|on_commit, got {other:?}"
                ));
            }
        };
        let config = LogConfig {
            // A smaller default than the library's: this tool is used for
            // crash and benchmark runs where several rollovers per run is the
            // point, not an accident.
            segment_size_bytes: self.number("--segment-bytes")?.unwrap_or(64 * 1024 * 1024),
            index_spacing_bytes: self
                .number("--index-spacing-bytes")?
                .unwrap_or(defaults.index_spacing_bytes),
            fsync_mode,
            preallocate_segments: !self.flag("--no-preallocate"),
            // Exposed so a benchmark can A/B the background rollover in one
            // binary: at 100% no roll is ever started early, which is exactly
            // the behaviour that predates it.
            rollover_threshold_percent: self
                .number("--rollover-threshold-percent")?
                .unwrap_or(defaults.rollover_threshold_percent as u64)
                as u8,
            max_overshoot_percent: self
                .number("--max-overshoot-percent")?
                .unwrap_or(defaults.max_overshoot_percent as u64)
                as u8,
            ..defaults
        };
        config.validate().map_err(|err| err.to_string())?;
        Ok(config)
    }

    /// Reject anything the subcommand did not ask for.
    fn finish(&self) -> Result<(), String> {
        let consumed = self.consumed.borrow();
        for (name, _) in &self.values {
            if !consumed.contains(name) {
                return Err(format!("unknown option {name:?}\n\n{USAGE}"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
        let err = parse(args(&["write", "--dir", "/tmp/a", "--records", "many"]))
            .expect_err("bad number");
        assert!(err.contains("--records"), "{err}");
    }

    #[test]
    fn an_invalid_fsync_mode_is_rejected() {
        let err =
            parse(args(&["write", "--dir", "/tmp/a", "--fsync", "sometimes"])).expect_err("bad");
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
}
