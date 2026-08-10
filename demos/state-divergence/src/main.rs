//! # Felix demo: local state divergence
//!
//! ## What this shows
//! Consumers maintaining a local copy of a config keyspace from a change stream.
//! One consumer stalls briefly, recovers, and then everything quiesces — and it is
//! still holding wrong values, permanently, with nothing to tell it so.
//!
//! ## Why this demo exists
//! This is the deliberate counterpart to the slow-consumer-isolation demo. That one
//! shows Felix's shipped strength; this one shows what that strength costs and why
//! the roadmap looks the way it does.
//!
//! `docs-site/docs/getting-started/what-felix-is-for.md` describes Felix's target as
//! coordination-store watch semantics — "read the current value, then receive every
//! subsequent change with no gap". It also marks distributed live-state
//! synchronisation as **not usable today**, and says why:
//!
//! > For an event feed, a dropped message means a consumer missed one update. For a
//! > consumer maintaining a local copy of state, a dropped message means its local
//! > copy is permanently wrong with no signal that would let it recover on its own.
//!
//! This demo makes that sentence executable. It is not a demonstration of a
//! feature; it is a demonstration of a gap, and it becomes the acceptance test for
//! gap-free subscribe when that lands.
//!
//! ## Honest limits
//! Single-node, loopback. The divergence shown here is a property of at-most-once
//! delivery, not of any particular scale.

mod render;
mod scenario;

use anyhow::{Result, bail};
use scenario::{Mode, Outcome, RunConfig};
use std::io::IsTerminal;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DemoArgs {
    pub rate: u64,
    pub keys: usize,
    pub consumers: usize,
    pub payload_bytes: usize,
    pub queue_capacity: usize,
    pub phase_secs: u64,
    pub modes: Vec<Mode>,
    pub tui: bool,
}

impl Default for DemoArgs {
    fn default() -> Self {
        Self {
            // A control-plane change feed, not a firehose. Real configuration is
            // thousands of keys changing a few hundred times a second; at firehose
            // rates over a tiny keyspace every key is rewritten constantly and
            // divergence repairs itself before it can be observed.
            rate: 400,
            keys: 2_000,
            consumers: 3,
            payload_bytes: 64,
            queue_capacity: 512,
            phase_secs: 4,
            modes: vec![Mode::Lossy, Mode::Lossless],
            tui: true,
        }
    }
}

fn parse_args<I: Iterator<Item = String>>(args: I) -> Result<DemoArgs> {
    let mut out = DemoArgs::default();
    let args: Vec<String> = args.collect();
    let mut idx = 0;
    while idx < args.len() {
        let take = |idx: &mut usize| -> Result<String> {
            *idx += 1;
            match args.get(*idx) {
                Some(value) => Ok(value.clone()),
                None => bail!("missing value for {}", args[*idx - 1]),
            }
        };
        match args[idx].as_str() {
            "--rate" => out.rate = take(&mut idx)?.parse()?,
            "--keys" => out.keys = take(&mut idx)?.parse()?,
            "--consumers" => out.consumers = take(&mut idx)?.parse()?,
            "--payload" => out.payload_bytes = take(&mut idx)?.parse()?,
            "--queue-capacity" => out.queue_capacity = take(&mut idx)?.parse()?,
            "--duration" => out.phase_secs = take(&mut idx)?.parse()?,
            "--mode" => {
                out.modes = match take(&mut idx)?.as_str() {
                    "lossy" => vec![Mode::Lossy],
                    "lossless" => vec![Mode::Lossless],
                    "both" => vec![Mode::Lossy, Mode::Lossless],
                    other => bail!("unknown mode {other}; expected lossy, lossless, or both"),
                }
            }
            "--no-tui" => out.tui = false,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
        idx += 1;
    }
    if out.consumers < 2 {
        bail!(
            "--consumers must be at least 2 so a healthy copy can be compared against a diverged one"
        );
    }
    Ok(out)
}

fn print_usage() {
    println!(
        "\
Felix demo: local state divergence

Consumers hold a local copy of a config keyspace built from a change stream. One
stalls, recovers, and is still permanently wrong once everything settles.

  --rate N            changes published per second (default 400)
  --keys N            size of the config keyspace (default 2000)
  --consumers N       consumer count, >= 2 (default 3)
  --payload N         payload bytes (default 64)
  --queue-capacity N  subscriber queue depth (default 512, the broker default)
  --duration N        seconds per phase; a run is 5 phases (default 4)
  --mode M            lossy | lossless | both (default both)
  --no-tui            plain text instead of the terminal UI
  -h, --help          this message
"
    );
}

pub async fn run_demo(args: DemoArgs) -> Result<Vec<Outcome>> {
    let interactive = args.tui && std::io::stdout().is_terminal();
    if args.tui && !interactive {
        eprintln!("stdout is not a terminal; using plain output");
    }
    let mut outcomes = Vec::new();
    for mode in args.modes.clone() {
        let config = RunConfig {
            mode,
            keys: args.keys,
            consumers: args.consumers,
            rate: args.rate,
            payload_bytes: args.payload_bytes,
            queue_capacity: args.queue_capacity,
            phase_secs: args.phase_secs,
        };
        let outcome = if interactive {
            render::run_with_tui(config).await?
        } else {
            render::run_with_plain(config).await?
        };
        outcomes.push(outcome);
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Ok(outcomes)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let outcomes = run_demo(args).await?;
    render::print_report(&outcomes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_requires_two_consumers() {
        let err = parse_args(["--consumers", "1"].iter().map(|s| s.to_string()))
            .expect_err("need a healthy consumer to compare against");
        assert!(err.to_string().contains("at least 2"));
    }

    /// The claim this demo exists to make: under production defaults a consumer
    /// that misses updates ends up with permanently wrong local state, while a
    /// healthy consumer on the same stream ends up correct.
    ///
    /// If gap-free subscribe ever lands, this test should start failing — at which
    /// point the demo becomes the proof that it works, and this assertion inverts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lossy_mode_leaves_the_stalled_consumer_permanently_wrong() {
        let args = DemoArgs {
            rate: 400,
            keys: 500,
            consumers: 3,
            payload_bytes: 64,
            queue_capacity: 64,
            phase_secs: 2,
            modes: vec![Mode::Lossy],
            tui: false,
        };
        let outcomes = tokio::time::timeout(Duration::from_secs(120), run_demo(args))
            .await
            .expect("demo should finish inside the timeout")
            .expect("demo run");
        let run = outcomes.first().expect("one outcome");

        assert!(
            run.stalled_wrong() > 0,
            "the stalled consumer should hold permanently wrong state under \
             at-most-once delivery; consumers: {:?}",
            run.consumers
        );
        assert_eq!(
            run.healthy_wrong(),
            0,
            "consumers that kept up should end with a correct copy; consumers: {:?}",
            run.consumers
        );
    }

    /// The other half: configuring every checkpoint to block removes the divergence
    /// entirely. This is what makes the demo a comparison rather than a complaint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lossless_mode_converges_every_consumer() {
        let args = DemoArgs {
            rate: 400,
            keys: 500,
            consumers: 3,
            payload_bytes: 64,
            queue_capacity: 64,
            phase_secs: 2,
            modes: vec![Mode::Lossless],
            tui: false,
        };
        let outcomes = tokio::time::timeout(Duration::from_secs(120), run_demo(args))
            .await
            .expect("demo should finish inside the timeout")
            .expect("demo run");
        let run = outcomes.first().expect("one outcome");

        assert_eq!(
            run.total_wrong(),
            0,
            "blocking at every checkpoint should leave every consumer correct; \
             consumers: {:?}",
            run.consumers
        );
    }
}
