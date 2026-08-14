// Environment configuration for the broker's durable storage.
//
// Kept out of `config.rs` deliberately: that module owns transport, batching and
// queue tuning, and durability is a separate concern with its own failure modes.
// The one thing they share is the `FELIX_*` naming convention.
//
// Durability is opt-in. With `FELIX_DURABLE_STORAGE_DIR` unset the broker runs
// exactly as it did before — in-memory only — and any stream the control plane
// marks `durable: true` is rejected at registration rather than silently
// downgraded to a guarantee the broker cannot keep.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_storage::log::{FsyncMode, LogConfig};

/// Durable storage settings resolved from the environment.
#[derive(Debug, Clone)]
pub struct DurableStorageConfig {
    /// Root directory holding one subdirectory per stream shard.
    pub root: PathBuf,
    /// Segment, index and fsync policy passed to every log.
    pub log: LogConfig,
}

impl DurableStorageConfig {
    /// Read the configuration, or `None` when durable storage is not enabled.
    pub fn from_env() -> Result<Option<Self>> {
        let Some(root) = std::env::var("FELIX_DURABLE_STORAGE_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let log = LogConfig {
            segment_size_bytes: parse_env("FELIX_DURABLE_SEGMENT_BYTES")?
                .unwrap_or(LogConfig::default().segment_size_bytes),
            index_spacing_bytes: parse_env("FELIX_DURABLE_INDEX_SPACING_BYTES")?
                .unwrap_or(LogConfig::default().index_spacing_bytes),
            fsync_mode: fsync_mode_from_env()?,
            max_records_per_read: parse_env("FELIX_DURABLE_MAX_RECORDS_PER_READ")?
                .unwrap_or(LogConfig::default().max_records_per_read),
            preallocate_segments: parse_bool_env("FELIX_DURABLE_PREALLOCATE")?
                .unwrap_or(LogConfig::default().preallocate_segments),
            verify_all_on_open: parse_bool_env("FELIX_DURABLE_VERIFY_ALL_ON_OPEN")?
                .unwrap_or(LogConfig::default().verify_all_on_open),
            repair_checksum_tail: parse_bool_env("FELIX_DURABLE_REPAIR_CHECKSUM_TAIL")?
                .unwrap_or(LogConfig::default().repair_checksum_tail),
        };
        // Fail at startup rather than at the first durable publish.
        log.validate()
            .map_err(|err| anyhow::anyhow!("invalid durable storage configuration: {err}"))?;

        Ok(Some(Self {
            root: PathBuf::from(root),
            log,
        }))
    }

    /// One-line summary for the startup log.
    pub fn summary(&self) -> String {
        let durability = match self.log.fsync_mode {
            FsyncMode::None => "no fsync (data at risk until the OS flushes)".to_string(),
            FsyncMode::Periodic { interval } => {
                format!("fsync every {}ms", interval.as_millis())
            }
            FsyncMode::OnCommit => "fsync before every acknowledgement".to_string(),
        };
        format!(
            "root={} segment={}B index_spacing={}B {durability}",
            self.root.display(),
            self.log.segment_size_bytes,
            self.log.index_spacing_bytes,
        )
    }
}

/// `none` | `periodic` | `on_commit`, defaulting to the `LogConfig` default.
///
/// `periodic` reads its interval from `FELIX_DURABLE_FSYNC_INTERVAL_MS`.
fn fsync_mode_from_env() -> Result<FsyncMode> {
    let interval = parse_env::<u64>("FELIX_DURABLE_FSYNC_INTERVAL_MS")?.map(Duration::from_millis);
    let Some(raw) = std::env::var("FELIX_DURABLE_FSYNC_MODE").ok() else {
        // No mode given: keep the default policy but honour an explicit
        // interval, since setting only the interval clearly means "periodic".
        return Ok(match (LogConfig::default().fsync_mode, interval) {
            (FsyncMode::Periodic { .. }, Some(interval)) => FsyncMode::Periodic { interval },
            (mode, _) => mode,
        });
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "none" | "off" => Ok(FsyncMode::None),
        "on_commit" | "on-commit" | "commit" => Ok(FsyncMode::OnCommit),
        "periodic" => Ok(FsyncMode::Periodic {
            interval: interval.unwrap_or(Duration::from_millis(250)),
        }),
        other => bail!(
            "FELIX_DURABLE_FSYNC_MODE must be one of none, periodic, on_commit (got {other:?})"
        ),
    }
}

fn parse_env<T: std::str::FromStr>(name: &str) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|err| anyhow::anyhow!("{err}"))
            .with_context(|| format!("parse {name}")),
    }
}

fn parse_bool_env(name: &str) -> Result<Option<bool>> {
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(None),
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            other => bail!("{name} must be a boolean (got {other:?})"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The environment is process-global, so these tests take a lock and clean
    /// up after themselves rather than running in parallel against each other.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    const VARS: &[&str] = &[
        "FELIX_DURABLE_STORAGE_DIR",
        "FELIX_DURABLE_SEGMENT_BYTES",
        "FELIX_DURABLE_INDEX_SPACING_BYTES",
        "FELIX_DURABLE_FSYNC_MODE",
        "FELIX_DURABLE_FSYNC_INTERVAL_MS",
        "FELIX_DURABLE_MAX_RECORDS_PER_READ",
        "FELIX_DURABLE_PREALLOCATE",
        "FELIX_DURABLE_VERIFY_ALL_ON_OPEN",
        "FELIX_DURABLE_REPAIR_CHECKSUM_TAIL",
    ];

    fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        for name in VARS {
            // SAFETY: the lock above makes this the only thread touching the
            // environment for the duration of the test.
            unsafe { std::env::remove_var(name) };
        }
        for (name, value) in pairs {
            // SAFETY: as above.
            unsafe { std::env::set_var(name, value) };
        }
        let result = body();
        for name in VARS {
            // SAFETY: as above.
            unsafe { std::env::remove_var(name) };
        }
        result
    }

    #[test]
    fn durability_is_off_unless_a_directory_is_set() {
        with_env(&[], || {
            assert!(DurableStorageConfig::from_env().expect("config").is_none());
        });
        with_env(&[("FELIX_DURABLE_STORAGE_DIR", "   ")], || {
            assert!(DurableStorageConfig::from_env().expect("config").is_none());
        });
    }

    #[test]
    fn a_directory_alone_enables_the_defaults() {
        with_env(&[("FELIX_DURABLE_STORAGE_DIR", "/var/lib/felix")], || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            assert_eq!(config.root, PathBuf::from("/var/lib/felix"));
            assert_eq!(config.log.fsync_mode, LogConfig::default().fsync_mode);
            assert_eq!(
                config.log.segment_size_bytes,
                LogConfig::default().segment_size_bytes
            );
        });
    }

    #[test]
    fn every_fsync_mode_is_selectable() {
        for (raw, expected) in [
            ("none", FsyncMode::None),
            ("off", FsyncMode::None),
            ("on_commit", FsyncMode::OnCommit),
            ("on-commit", FsyncMode::OnCommit),
            ("ON_COMMIT", FsyncMode::OnCommit),
            (
                "periodic",
                FsyncMode::Periodic {
                    interval: Duration::from_millis(250),
                },
            ),
        ] {
            with_env(
                &[
                    ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                    ("FELIX_DURABLE_FSYNC_MODE", raw),
                ],
                || {
                    let config = DurableStorageConfig::from_env()
                        .expect("config")
                        .expect("enabled");
                    assert_eq!(config.log.fsync_mode, expected, "mode {raw}");
                },
            );
        }
    }

    #[test]
    fn a_periodic_interval_is_honoured() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_MODE", "periodic"),
                ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "40"),
            ],
            || {
                let config = DurableStorageConfig::from_env()
                    .expect("config")
                    .expect("enabled");
                assert_eq!(
                    config.log.fsync_mode,
                    FsyncMode::Periodic {
                        interval: Duration::from_millis(40)
                    }
                );
            },
        );
    }

    #[test]
    fn an_interval_without_a_mode_still_applies() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "75"),
            ],
            || {
                let config = DurableStorageConfig::from_env()
                    .expect("config")
                    .expect("enabled");
                assert_eq!(
                    config.log.fsync_mode,
                    FsyncMode::Periodic {
                        interval: Duration::from_millis(75)
                    }
                );
            },
        );
    }

    #[test]
    fn an_unknown_fsync_mode_is_rejected() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_MODE", "sometimes"),
            ],
            || {
                let err = DurableStorageConfig::from_env().expect_err("bad mode");
                assert!(err.to_string().contains("FELIX_DURABLE_FSYNC_MODE"));
            },
        );
    }

    #[test]
    fn a_zero_periodic_interval_is_rejected_at_startup() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_MODE", "periodic"),
                ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "0"),
            ],
            || {
                // Zero is parsed as "unset" by `parse_env`'s caller only for
                // empty strings, so this reaches validation as a real zero.
                let config = DurableStorageConfig::from_env();
                match config {
                    Err(err) => assert!(err.to_string().contains("interval")),
                    Ok(Some(config)) => assert!(
                        !matches!(
                            config.log.fsync_mode,
                            FsyncMode::Periodic {
                                interval: Duration::ZERO
                            }
                        ),
                        "a zero interval must not survive validation"
                    ),
                    Ok(None) => panic!("durability should be enabled"),
                }
            },
        );
    }

    #[test]
    fn segment_and_index_sizes_are_configurable() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_SEGMENT_BYTES", "1048576"),
                ("FELIX_DURABLE_INDEX_SPACING_BYTES", "8192"),
                ("FELIX_DURABLE_MAX_RECORDS_PER_READ", "500"),
                ("FELIX_DURABLE_PREALLOCATE", "false"),
                ("FELIX_DURABLE_VERIFY_ALL_ON_OPEN", "yes"),
            ],
            || {
                let config = DurableStorageConfig::from_env()
                    .expect("config")
                    .expect("enabled");
                assert_eq!(config.log.segment_size_bytes, 1_048_576);
                assert_eq!(config.log.index_spacing_bytes, 8_192);
                assert_eq!(config.log.max_records_per_read, 500);
                assert!(!config.log.preallocate_segments);
                assert!(config.log.verify_all_on_open);
            },
        );
    }

    #[test]
    fn an_unparseable_size_is_reported_with_its_variable_name() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_SEGMENT_BYTES", "big"),
            ],
            || {
                let err = DurableStorageConfig::from_env().expect_err("bad size");
                assert!(
                    err.to_string().contains("FELIX_DURABLE_SEGMENT_BYTES"),
                    "{err}"
                );
            },
        );
    }

    #[test]
    fn a_segment_too_small_to_hold_a_record_is_rejected() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_SEGMENT_BYTES", "8"),
            ],
            || {
                let err = DurableStorageConfig::from_env().expect_err("tiny segment");
                assert!(err.to_string().contains("segment_size_bytes"), "{err}");
            },
        );
    }

    #[test]
    fn an_invalid_boolean_is_rejected() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_PREALLOCATE", "maybe"),
            ],
            || {
                let err = DurableStorageConfig::from_env().expect_err("bad bool");
                assert!(
                    err.to_string().contains("FELIX_DURABLE_PREALLOCATE"),
                    "{err}"
                );
            },
        );
    }

    #[test]
    fn the_summary_names_the_durability_policy() {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_MODE", "on_commit"),
            ],
            || {
                let config = DurableStorageConfig::from_env()
                    .expect("config")
                    .expect("enabled");
                let summary = config.summary();
                assert!(summary.contains("/tmp/felix"), "{summary}");
                assert!(summary.contains("acknowledgement"), "{summary}");
            },
        );
    }
}
