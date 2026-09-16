//! Load generator for a *remote* Felix cluster.
//!
//! The measuring instrument of the real-network perf suite
//! (`docs/perf-real-network.md`). Every other driver in this repository is
//! loopback-bound — `latency-demo` runs an in-process broker, `soak` spawns
//! its own child — so this is the one that dials addresses it is given and
//! measures what a deployment's client would feel: acknowledgement round
//! trips, publish-to-delivery latency, cache/counter round trips, and watch
//! fanout delivery, all through the real routed paths (forwards and
//! redirects included).
//!
//! It measures the cluster; it is not part of it. Brokers and the control
//! plane under test run release artifacts. This binary may be built from a
//! pinned ref on the load-generator machine — the instrument's build does not
//! contaminate the measurement, so long as it is built *before* any run.
//!
//! ```text
//! felix-loadgen --brokers 10.0.0.4:5000,10.0.0.5:5000 \
//!     --tenant t1 --token-file /run/felix/token \
//!     --scenario pubsub --fanout 10 --payload-bytes 256 --total 20000
//! ```
//!
//! Output: human-readable lines in the shape `scripts/perf` already parses,
//! plus one `LOADGEN_JSON {...}` line per case — the machine contract the
//! Azure runner consumes.

mod scenarios;
mod stats;
mod tls;

use anyhow::{Context, Result, bail};
use scenarios::Common;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build runtime")?;
    runtime.block_on(run())
}

struct Args {
    common: Common,
    scenario: String,
    stream: String,
    cache: String,
    binary: bool,
}

fn usage() -> ! {
    eprintln!(
        "felix-loadgen — drive a remote Felix cluster and measure it

  --brokers <addr,addr,...>   client-facing broker addresses (required)
  --tenant <id>               tenant to authenticate as (required)
  --token <jwt>               Felix token, or --token-file <path>
  --namespace <ns>            default: default
  --scenario <name>           pubsub | cache | counter | watch | queue | retained (required)
  --stream <name>             stream for pubsub (default: perf)
  --cache <name>              cache scope for cache/counter/watch (default: perf)
  --warmup <n>                discarded operations (default: 2000)
  --total <n>                 measured operations (default: 20000)
  --payload-bytes <n>         payload size (default: 256)
  --fanout <n>                subscribers or watchers (default: 1)
  --batch <n>                 publish batch size; 1 = per-message ack (default: 1)
  --binary                    binary publish framing (no per-message ack)
  --concurrency <n>           workers for cache/counter (default: 8)
  --environment <label>       stamped into LOADGEN_JSON (default: unknown)

Certificates are accepted without verification — brokers self-sign and
publish nothing to verify against. Run this only against a cluster you own,
on a network you own."
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args> {
    let mut brokers = Vec::new();
    let mut tenant = None;
    let mut token = None;
    let mut namespace = "default".to_string();
    let mut scenario = None;
    let mut stream = "perf".to_string();
    let mut cache = "perf".to_string();
    let mut warmup = 2000usize;
    let mut total = 20000usize;
    let mut payload_bytes = 256usize;
    let mut fanout = 1usize;
    let mut batch = 1usize;
    let mut binary = false;
    let mut concurrency = 8usize;
    let mut environment = "unknown".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = |name: &str| -> Result<String> {
            args.next().with_context(|| format!("{name} needs a value"))
        };
        match flag.as_str() {
            "--brokers" => {
                for part in value("--brokers")?.split(',') {
                    brokers.push(
                        part.trim().parse().with_context(|| {
                            format!("--brokers entry {part:?} is not host:port")
                        })?,
                    );
                }
            }
            "--tenant" => tenant = Some(value("--tenant")?),
            "--token" => token = Some(value("--token")?),
            "--token-file" => {
                let path = value("--token-file")?;
                token = Some(
                    std::fs::read_to_string(&path)
                        .with_context(|| format!("read {path}"))?
                        .trim()
                        .to_string(),
                );
            }
            "--namespace" => namespace = value("--namespace")?,
            "--scenario" => scenario = Some(value("--scenario")?),
            "--stream" => stream = value("--stream")?,
            "--cache" => cache = value("--cache")?,
            "--warmup" => warmup = value("--warmup")?.parse().context("--warmup")?,
            "--total" => total = value("--total")?.parse().context("--total")?,
            "--payload-bytes" => {
                payload_bytes = value("--payload-bytes")?
                    .parse()
                    .context("--payload-bytes")?
            }
            "--fanout" => fanout = value("--fanout")?.parse().context("--fanout")?,
            "--batch" => batch = value("--batch")?.parse().context("--batch")?,
            "--binary" => binary = true,
            "--concurrency" => {
                concurrency = value("--concurrency")?.parse().context("--concurrency")?
            }
            "--environment" => environment = value("--environment")?,
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown flag {other}");
                usage();
            }
        }
    }

    if brokers.is_empty() {
        bail!("--brokers is required");
    }
    let tenant = tenant.context("--tenant is required")?;
    let token = token.context("--token or --token-file is required")?;
    let scenario = scenario.context("--scenario is required")?;

    Ok(Args {
        common: Common {
            brokers,
            tenant,
            namespace,
            token,
            warmup,
            total,
            payload_bytes,
            fanout,
            batch,
            concurrency,
            environment,
        },
        scenario,
        stream,
        cache,
        binary,
    })
}

async fn run() -> Result<()> {
    let args = parse_args()?;
    match args.scenario.as_str() {
        "pubsub" => scenarios::pubsub(&args.common, &args.stream, args.binary).await,
        "cache" => scenarios::cache(&args.common, &args.cache).await,
        "counter" => scenarios::counter(&args.common, &args.cache).await,
        "watch" => scenarios::watch(&args.common, &args.cache).await,
        "queue" => scenarios::queue(&args.common, &args.stream).await,
        "retained" => scenarios::retained(&args.common, &args.cache).await,
        "ingest" => scenarios::ingest(&args.common, &args.stream).await,
        other => bail!(
            "unknown scenario {other:?} (pubsub | cache | counter | watch | queue | retained | ingest)"
        ),
    }
}
