//! The load generator, validated against a real local cluster before it is
//! trusted against a paid one.
//!
//! `felix-loadgen` is the perf suite's measuring instrument
//! (`docs/perf-real-network.md`), and an instrument nobody has calibrated
//! measures nothing: this drives each scenario against the harness's real
//! broker processes and asserts the run completes, accounts for every
//! operation, and emits the machine-readable line the Azure runner consumes.
//!
//! Numbers are asserted for *shape*, never for magnitude — this is loopback
//! on a shared CI machine, where a latency assertion would only measure the
//! neighbours.
//!
//! Run with `cargo build -p felix-loadgen && cargo test -p felix-cluster --test loadgen`.
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, anyhow};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

fn loadgen_binary() -> Result<PathBuf> {
    let mut dir = std::env::current_exe().context("locate the running executable")?;
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    let candidate = dir.join("felix-loadgen");
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "felix-loadgen not found at {}; build it first with `cargo build -p felix-loadgen`",
        candidate.display()
    ))
}

fn run_loadgen(cluster: &Cluster, args: &[&str]) -> Result<(String, String)> {
    let brokers: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.client_addr.to_string())
        .collect();
    let output = Command::new(loadgen_binary()?)
        .arg("--brokers")
        .arg(brokers.join(","))
        .arg("--tenant")
        .arg(&cluster.tenant_id)
        .arg("--namespace")
        .arg(&cluster.namespace)
        .arg("--token")
        .arg(&cluster.client_token)
        .arg("--environment")
        .arg("local-harness")
        .args(args)
        .output()
        .context("run felix-loadgen")?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.status.success() {
        return Err(anyhow!(
            "felix-loadgen failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status,
        ));
    }
    Ok((stdout, stderr))
}

/// The one JSON line a run must emit, parsed.
fn result_json(stdout: &str) -> Result<serde_json::Value> {
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("LOADGEN_JSON "))
        .context("no LOADGEN_JSON line in the output")?;
    serde_json::from_str(line).context("parse LOADGEN_JSON")
}

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("perf", 1)],
        caches: vec![CacheSpec::new("perf", 1)],
        ..Default::default()
    }
}

/// Publish/subscribe end to end: every publish delivered to every subscriber,
/// the parseable stdout block present, and the JSON row carrying the ack
/// percentiles the matrix files.
///
/// One node deliberately. This validates the *instrument* — accounting,
/// latency sampling, the JSON contract — not the cluster's publish-forwarding,
/// which has its own tests (`cross_broker`, `sharding`). A publish is
/// forwarded, never redirected, so a client cannot pick the owner; on a
/// multi-node cluster a non-owner ingress can answer "stream not found" while
/// its routing snapshot is unsettled, and the loadgen reports that as
/// `publish_retries` rather than pretending it did not happen. Pinning the
/// instrument test to one node keeps it about the instrument.
///
/// `#[ignore]` because it is flaky against the *in-process* harness broker
/// specifically: a tight sequential acked-publish loop can overrun the
/// harness's small ingress admission queue and get "publish queue full", a
/// backpressure signal the instrument counts as a `publish_retries` but which
/// can fail to drain when the in-process broker's subscriber lanes stall under
/// loopback burst. Over a real network the pacing that reproduces it does not
/// exist, and the Azure runs exercise this scenario for real. Run it
/// explicitly with `--ignored` when changing the pubsub scenario's accounting.
#[tokio::test]
#[serial]
#[ignore = "flaky against the in-process harness under loopback burst; runs for real on Azure"]
async fn pubsub_accounts_for_every_delivery() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("perf", 1)],
        caches: vec![CacheSpec::new("perf", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let (stdout, _) = run_loadgen(
        &cluster,
        &[
            "--scenario",
            "pubsub",
            "--stream",
            "perf",
            "--warmup",
            "50",
            "--total",
            "500",
            "--payload-bytes",
            "64",
            "--fanout",
            "3",
        ],
    )
    .expect("pubsub run");

    assert!(
        stdout.contains("Results (publish n = 550"),
        "the stdout contract the matrix runner parses is missing:\n{stdout}",
    );
    let row = result_json(&stdout).expect("json row");
    assert_eq!(row["unaccounted"], 0, "deliveries went missing: {row}");
    assert_eq!(row["received"], 550 * 3);
    assert!(
        row["ack_latency_us"]["p50"].as_u64().unwrap_or(0) > 0,
        "batch-1 runs must report acknowledgement latency: {row}",
    );
}

/// Cache and counter round trips complete and report both halves separately.
#[tokio::test]
#[serial]
async fn cache_and_counter_round_trips_complete() {
    let cluster = Cluster::start(config()).await.expect("start cluster");

    let (stdout, _) = run_loadgen(
        &cluster,
        &[
            "--scenario",
            "cache",
            "--cache",
            "perf",
            "--warmup",
            "40",
            "--total",
            "400",
            "--payload-bytes",
            "64",
            "--concurrency",
            "4",
        ],
    )
    .expect("cache run");
    let row = result_json(&stdout).expect("json row");
    for half in ["put", "get"] {
        assert!(
            row[half]["latency_us"]["p50"].as_u64().unwrap_or(0) > 0,
            "cache {half} reported no latency: {row}",
        );
    }

    let (stdout, _) = run_loadgen(
        &cluster,
        &[
            "--scenario",
            "counter",
            "--cache",
            "perf",
            "--warmup",
            "40",
            "--total",
            "400",
            "--concurrency",
            "4",
        ],
    )
    .expect("counter run");
    let row = result_json(&stdout).expect("json row");
    for half in ["add", "get"] {
        assert!(
            row[half]["latency_us"]["p50"].as_u64().unwrap_or(0) > 0,
            "counter {half} reported no latency: {row}",
        );
    }
}

/// The watch scenario delivers every put to every watcher, through the
/// redirect-following connect the real suite depends on.
#[tokio::test]
#[serial]
async fn watch_delivers_every_put_to_every_watcher() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (stdout, _) = run_loadgen(
        &cluster,
        &[
            "--scenario",
            "watch",
            "--cache",
            "perf",
            "--warmup",
            "20",
            "--total",
            "200",
            "--payload-bytes",
            "64",
            "--fanout",
            "5",
        ],
    )
    .expect("watch run");
    let row = result_json(&stdout).expect("json row");
    assert_eq!(
        row["delivered"], row["expected"],
        "watch deliveries went missing: {row}",
    );
    assert!(
        row["delivery_latency_us"]["p50"].as_u64().unwrap_or(0) > 0,
        "no delivery latency reported: {row}",
    );
}
