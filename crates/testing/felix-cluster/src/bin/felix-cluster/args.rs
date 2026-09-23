//! Flags and positional arguments, parsed by hand.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};

/// How long each narrated step pauses, so the demos can be read as they run.
pub(crate) fn pace_from(args: &[String]) -> Result<Duration> {
    Ok(Duration::from_millis(
        (flag(args, "--pace")?
            .map(|value| value.parse::<f64>())
            .transpose()
            .context("parse --pace")?
            .unwrap_or(2.5)
            * 1000.0) as u64,
    ))
}

pub(crate) fn flag(args: &[String], name: &str) -> Result<Option<String>> {
    match args.iter().position(|arg| arg == name) {
        Some(index) => Ok(Some(
            args.get(index + 1)
                .cloned()
                .with_context(|| format!("{name} needs a value"))?,
        )),
        None => Ok(None),
    }
}

/// A `--flag value` pair, or `None` when the flag is absent.
pub(crate) fn flag_value(args: &[String], name: &str) -> Result<Option<String>> {
    let Some(position) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    args.get(position + 1)
        .cloned()
        .map(Some)
        .ok_or_else(|| anyhow!("{name} needs a value"))
}

pub(crate) fn flag_usize(args: &[String], name: &str) -> Result<Option<usize>> {
    match flag(args, name)? {
        Some(value) => Ok(Some(
            value.parse().with_context(|| format!("parse {name}"))?,
        )),
        None => Ok(None),
    }
}

/// Everything that is neither the subcommand nor a flag or its value, in order.
pub(crate) fn positionals(args: &[String]) -> Vec<String> {
    let mut found = Vec::new();
    let mut skip_next = false;
    for arg in args.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg.starts_with("--") {
            skip_next = true;
            continue;
        }
        found.push(arg.clone());
    }
    found
}
