//! Soak and resource-leak harness for the broker.
//!
//! # Purpose
//! Produces the empirical evidence for M0's concurrency and resource-leak exit
//! criterion (#154). It drives the broker through sustained load, connection
//! churn, slow-subscriber saturation, and repeated process restarts, sampling
//! resource counters throughout, then checks that everything returns to a
//! steady-state envelope once load stops.
//!
//! # Why a binary rather than a test
//! A soak is a measurement, not an assertion about a single code path. It needs
//! to run for minutes, emit a time series, and produce a report a human reviews.
//! `cargo test` is the wrong shape for that. Regression tests for anything this
//! *finds* belong in the normal test suite.
//!
//! # What it exercises
//! - The real accept loop (`quic::serve_with_shutdown`) and the real drain, not
//!   a synthetic shutdown future.
//! - Real QUIC connections over loopback, with the same auth path production
//!   uses (Ed25519-signed Felix tokens verified against a JWKS).
//! - A genuine `SIGTERM` to a genuine child process, which is the gap
//!   `services/broker/tests/graceful_shutdown.rs` could not cover in-process.
//!
//! # How to use
//! ```text
//! cargo run --release -p broker --bin soak -- --duration-secs 60
//! cargo run --release -p broker --bin soak -- --serve-child   # internal
//! ```
//! Exits non-zero if any steady-state check fails, so CI can gate on it.

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::auth::{BrokerAuth, ControlPlaneKeyStore};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore,
};
use felix_broker::{Broker, StreamMetadata};
use felix_client::{Client, ClientConfig};
use felix_common::lifecycle::{DrainBudget, Readiness};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// Fixed keypair so runs are deterministic and need no control plane.
const TEST_PRIVATE_KEY: [u8; 32] = [37u8; 32];
// Drain budget the child broker is configured with. Kept short so a restart
// cycle is quick, and passed to the child via the same env var production uses
// so the soak exercises the real configuration path. The per-connection grace
// inside the broker is derived from this, so the child's own deadline must be
// this value — not a separate constant that could be smaller than the grace it
// is meant to bound.
const CHILD_DRAIN_BUDGET_MS: u64 = 6_000;
const CHILD_DRAIN_DEADLINE: Duration = Duration::from_millis(CHILD_DRAIN_BUDGET_MS);
const TENANT: &str = "t1";
const NAMESPACE: &str = "default";
const STREAM: &str = "soak";

mod resources;
use resources::ResourceSample;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SoakConfig {
    // Duration of each load-bearing phase.
    phase_secs: u64,
    // Upper bound on how long to wait for resources to settle after load stops.
    // This is a cap, not a sleep: a healthy run returns as soon as fds and tasks
    // are back at baseline, so raising it costs nothing and only buys tolerance
    // for a slow or busy machine.
    quiesce_secs: u64,
    publishers: usize,
    subscribers: usize,
    payload_bytes: usize,
    // Connect/disconnect iterations in the churn phase.
    churn_cycles: usize,
    // SIGTERM start/stop iterations in the restart phase.
    restart_cycles: usize,
    // Identical load repetitions used to separate a leak from allocator retention.
    load_cycles: usize,
    // Fraction of RSS growth over baseline tolerated after quiescence.
    rss_growth_tolerance: f64,
    // Where to write the sampled time series, so a run is reviewable after the
    // fact rather than only as console output.
    timeseries_path: Option<String>,
}

impl Default for SoakConfig {
    fn default() -> Self {
        Self {
            phase_secs: 30,
            quiesce_secs: 30,
            publishers: 4,
            subscribers: 4,
            payload_bytes: 1024,
            churn_cycles: 200,
            restart_cycles: 5,
            load_cycles: 4,
            rss_growth_tolerance: 0.25,
            timeseries_path: None,
        }
    }
}

fn parse_args() -> Result<(SoakConfig, bool)> {
    parse_args_from(std::env::args().skip(1))
}

/// Split from `parse_args` so the flag handling is reachable from tests without
/// going through process arguments.
fn parse_args_from<I: Iterator<Item = String>>(args: I) -> Result<(SoakConfig, bool)> {
    let mut config = SoakConfig::default();
    let mut serve_child = false;
    let args: Vec<String> = args.collect();
    let mut idx = 0;
    while idx < args.len() {
        let take = |idx: &mut usize| -> Result<String> {
            *idx += 1;
            args.get(*idx)
                .cloned()
                .with_context(|| format!("missing value for {}", args[*idx - 1]))
        };
        match args[idx].as_str() {
            "--serve-child" => serve_child = true,
            "--duration-secs" => config.phase_secs = take(&mut idx)?.parse()?,
            "--quiesce-secs" => config.quiesce_secs = take(&mut idx)?.parse()?,
            "--publishers" => config.publishers = take(&mut idx)?.parse()?,
            "--subscribers" => config.subscribers = take(&mut idx)?.parse()?,
            "--payload" => config.payload_bytes = take(&mut idx)?.parse()?,
            "--churn-cycles" => config.churn_cycles = take(&mut idx)?.parse()?,
            "--restart-cycles" => config.restart_cycles = take(&mut idx)?.parse()?,
            "--load-cycles" => config.load_cycles = take(&mut idx)?.parse()?,
            "--timeseries" => config.timeseries_path = Some(take(&mut idx)?),
            other => bail!("unknown argument: {other}"),
        }
        idx += 1;
    }
    Ok((config, serve_child))
}

// ---------------------------------------------------------------------------
// Auth + transport fixtures
// ---------------------------------------------------------------------------

struct AuthFixture {
    token: String,
    broker_auth: Arc<BrokerAuth>,
}

// Mirrors the conformance runner's fixture: a deterministic Ed25519 keypair and
// an in-memory JWKS, so the soak needs no control plane while still going
// through the real token-verification path.
fn build_auth_fixture() -> Result<AuthFixture> {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let mut keys = HashMap::new();
    keys.insert(
        TENANT.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(3600),
        key_store,
    );
    let token = issuer.mint(
        &TenantId::new(TENANT),
        "soak",
        vec![
            format!("stream.publish:stream:{TENANT}/{NAMESPACE}/*"),
            format!("stream.subscribe:stream:{TENANT}/{NAMESPACE}/*"),
        ],
    )?;

    let cp_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    cp_store.insert_jwks(&TenantId::new(TENANT), jwks);
    Ok(AuthFixture {
        token,
        broker_auth: Arc::new(BrokerAuth::with_key_store(cp_store)),
    })
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

fn client_config(cert: &CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(TENANT.to_string());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

/// A running in-process broker plus the handles needed to drain it.
struct BrokerHarness {
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    accept_shutdown: CancellationToken,
    connections: TaskTracker,
    accept_task: tokio::task::JoinHandle<()>,
    _server: Arc<QuicServer>,
}

async fn start_broker(auth: &AuthFixture) -> Result<BrokerHarness> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant(TENANT).await?;
    broker.register_namespace(TENANT, NAMESPACE).await?;
    broker
        .register_stream(TENANT, NAMESPACE, STREAM, StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let accept_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    let accept_task = {
        let server = Arc::clone(&server);
        let accept_shutdown = accept_shutdown.clone();
        let connections = connections.clone();
        let auth = Arc::clone(&auth.broker_auth);
        tokio::spawn(async move {
            if let Err(err) = broker::quic::serve_with_shutdown(
                server,
                broker,
                config,
                auth,
                accept_shutdown,
                connections,
            )
            .await
            {
                eprintln!("accept loop exited: {err}");
            }
        })
    };

    Ok(BrokerHarness {
        addr,
        cert,
        accept_shutdown,
        connections,
        accept_task,
        _server: server,
    })
}

// ---------------------------------------------------------------------------
// Load generators
// ---------------------------------------------------------------------------

#[derive(Default)]
struct LoadStats {
    published: AtomicU64,
    publish_errors: AtomicU64,
    received: AtomicU64,
    connect_errors: AtomicU64,
}

/// Sustained publish load from `count` independent client connections.
async fn spawn_publishers(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    stop: Arc<AtomicBool>,
    count: usize,
    payload_bytes: usize,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let config = client_config(&harness.cert, auth)?;
        let addr = harness.addr;
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", config).await {
                Ok(client) => client,
                Err(_) => {
                    stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let publisher = match client.publisher().await {
                Ok(publisher) => publisher,
                Err(_) => return,
            };
            let payload = vec![0xABu8; payload_bytes];
            while !stop.load(Ordering::Relaxed) {
                match publisher
                    .publish(TENANT, NAMESPACE, STREAM, payload.clone(), AckMode::None)
                    .await
                {
                    Ok(()) => {
                        stats.published.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                        // Back off rather than spin on a broken connection.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                // Yield so a single publisher cannot monopolise its worker.
                tokio::task::yield_now().await;
            }
        }));
    }
    Ok(handles)
}

/// Subscribers that drain events promptly. `slow` inverts that: they subscribe
/// and then stop reading, which is what drives queue saturation and the drop
/// policy on the broker side.
async fn spawn_subscribers(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    stop: Arc<AtomicBool>,
    count: usize,
    slow: bool,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let config = client_config(&harness.cert, auth)?;
        let addr = harness.addr;
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", config).await {
                Ok(client) => client,
                Err(_) => {
                    stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let mut subscription = match client.subscribe(TENANT, NAMESPACE, STREAM).await {
                Ok(subscription) => subscription,
                Err(_) => return,
            };
            while !stop.load(Ordering::Relaxed) {
                if slow {
                    // Hold the subscription open without draining it.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                match tokio::time::timeout(Duration::from_millis(250), subscription.next_event())
                    .await
                {
                    Ok(Ok(Some(_))) => {
                        stats.received.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => break,
                    Err(_) => {}
                }
            }
        }));
    }
    Ok(handles)
}

/// Repeatedly connect, publish, and disconnect. This is the phase most likely to
/// surface connection, task, or file-descriptor leaks, because each cycle
/// allocates and must fully release a QUIC connection and its per-connection
/// broker state.
async fn run_connection_churn(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    cycles: usize,
    payload_bytes: usize,
) -> Result<()> {
    let payload = vec![0x5Au8; payload_bytes];
    for _ in 0..cycles {
        let config = client_config(&harness.cert, auth)?;
        let client = match Client::connect(harness.addr, "localhost", config).await {
            Ok(client) => client,
            Err(_) => {
                stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        if let Ok(publisher) = client.publisher().await
            && publisher
                .publish(
                    TENANT,
                    NAMESPACE,
                    STREAM,
                    payload.clone(),
                    AckMode::PerMessage,
                )
                .await
                .is_ok()
        {
            stats.published.fetch_add(1, Ordering::Relaxed);
        }
        // Dropping the client closes the connection; the broker side must
        // release its per-connection state without being told twice.
        drop(client);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

struct PhaseReport {
    name: &'static str,
    samples: Vec<ResourceSample>,
    published: u64,
    received: u64,
    errors: u64,
}

impl PhaseReport {
    fn peak_rss_kb(&self) -> u64 {
        self.samples.iter().map(|s| s.rss_kb).max().unwrap_or(0)
    }
    fn peak_fds(&self) -> u64 {
        self.samples.iter().map(|s| s.open_fds).max().unwrap_or(0)
    }
    fn peak_tasks(&self) -> usize {
        self.samples
            .iter()
            .map(|s| s.alive_tasks)
            .max()
            .unwrap_or(0)
    }
    fn last(&self) -> Option<&ResourceSample> {
        self.samples.last()
    }
}

/// How long a resource reading must hold steady before it counts as settled.
const SETTLE_POLL: Duration = Duration::from_millis(500);
const SETTLE_CONSECUTIVE: usize = 4;

/// Result of waiting for process resources to come to rest.
struct SettleOutcome {
    /// Whether the target was reached before the cap expired.
    settled: bool,
    waited: Duration,
    sample: ResourceSample,
}

/// Wait until the *idle broker* stops changing, then treat that as baseline.
///
/// # Why not just sample immediately
/// `start_broker` returns as soon as the listener is bound, but the runtime keeps
/// allocating for a moment afterwards — the accept task spawns, epoll registers,
/// timers arm. Sampling right then captures a baseline lower than the broker's
/// real idle state, which later makes an honest quiesced reading look like a leak.
/// This was visible across platforms: the same idle broker sampled 2 tasks on
/// Linux and 10 on macOS, purely from where the sample landed in startup.
async fn settle_to_stable(cap: Duration) -> SettleOutcome {
    let started = Instant::now();
    let mut previous = ResourceSample::capture();
    let mut stable = 0usize;
    while started.elapsed() < cap {
        tokio::time::sleep(SETTLE_POLL).await;
        let current = ResourceSample::capture();
        if current.open_fds == previous.open_fds && current.alive_tasks == previous.alive_tasks {
            stable += 1;
            if stable >= SETTLE_CONSECUTIVE {
                return SettleOutcome {
                    settled: true,
                    waited: started.elapsed(),
                    sample: current,
                };
            }
        } else {
            stable = 0;
        }
        previous = current;
    }
    SettleOutcome {
        settled: false,
        waited: started.elapsed(),
        sample: previous,
    }
}

/// Wait until process resources fall back to `baseline`, or the cap expires.
///
/// # Why polling rather than a fixed sleep
/// Teardown latency is not a constant. It varies with load, platform, and how
/// busy the machine is, so any fixed `--quiesce-secs` is either too short
/// somewhere (a false leak report) or wastes minutes everywhere. Measured
/// directly: an identical workload failed at a 15s sleep roughly half the time
/// and passed 3/3 at 60s. Polling makes the result depend on the system reaching
/// rest rather than on guessing how long that takes, and it returns as soon as it
/// does — so raising the cap costs nothing on a healthy run.
///
/// Reaching `<=` baseline is the success condition, not `==`: the broker may
/// legitimately hold fewer resources at rest than during startup.
async fn settle_to_baseline(baseline: &ResourceSample, cap: Duration) -> SettleOutcome {
    let started = Instant::now();
    let mut stable = 0usize;
    let mut last = ResourceSample::capture();
    while started.elapsed() < cap {
        if last.open_fds <= baseline.open_fds && last.alive_tasks <= baseline.alive_tasks {
            stable += 1;
            if stable >= SETTLE_CONSECUTIVE {
                return SettleOutcome {
                    settled: true,
                    waited: started.elapsed(),
                    sample: last,
                };
            }
        } else {
            stable = 0;
        }
        tokio::time::sleep(SETTLE_POLL).await;
        last = ResourceSample::capture();
    }
    SettleOutcome {
        settled: false,
        waited: started.elapsed(),
        sample: last,
    }
}

/// Sample resources on a fixed cadence until `stop` flips.
async fn sample_until(stop: Arc<AtomicBool>, interval: Duration) -> Vec<ResourceSample> {
    let mut samples = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        samples.push(ResourceSample::capture());
        tokio::time::sleep(interval).await;
    }
    // Always capture a final sample so a phase is never reported empty.
    samples.push(ResourceSample::capture());
    samples
}

async fn drain_broker(harness: BrokerHarness, deadline: Duration) -> Vec<&'static str> {
    // Exercise the same sequence main.rs uses, so the soak validates the real
    // drain rather than a bespoke teardown.
    let readiness = Readiness::ready();
    readiness.begin_draining();
    harness.accept_shutdown.cancel();

    let mut budget = DrainBudget::new(deadline);
    harness.connections.close();
    budget
        .drain("quic_connections", harness.connections.wait())
        .await;
    let mut accept_task = harness.accept_task;
    if !budget
        .drain("quic_accept_loop", async {
            let _ = (&mut accept_task).await;
        })
        .await
    {
        accept_task.abort();
    }
    budget.unfinished().to_vec()
}

// ---------------------------------------------------------------------------
// Child mode: a real broker process for SIGTERM testing
// ---------------------------------------------------------------------------

/// Run a broker until terminated, printing its address and certificate so the
/// parent can drive load against it.
///
/// This exists so the restart phase sends a genuine `SIGTERM` to a genuine
/// process. An in-process cancellation token cannot show that the signal
/// handler is wired up, that the drain completes, or that the process exits
/// zero — which is exactly the gap left open when #139 shipped.
async fn run_serve_child() -> Result<()> {
    let auth = build_auth_fixture()?;
    let harness = start_broker(&auth).await?;
    println!(
        "SOAK_CHILD_READY addr={} cert={}",
        harness.addr,
        URL_SAFE_NO_PAD.encode(&harness.cert)
    );
    use std::io::Write;
    std::io::stdout().flush()?;

    felix_common::lifecycle::termination_signal().await;
    let unfinished = drain_broker(harness, CHILD_DRAIN_DEADLINE).await;
    // Report the drain outcome rather than failing on it. A forced drain is a
    // WARN in `main.rs`, not an error exit, and the soak must observe the same
    // behaviour production does — the parent decides whether it is a finding.
    println!("SOAK_CHILD_DRAIN unfinished={}", unfinished.join(","));
    std::io::stdout().flush()?;
    Ok(())
}

#[cfg(unix)]
fn terminate_child(child: &std::process::Child) -> Result<()> {
    // SAFETY: `kill` with a pid we own and a valid signal number.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    if rc != 0 {
        bail!(
            "failed to send SIGTERM: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

/// Repeated real-process start/SIGTERM/exit cycles, each with live traffic.
///
/// Verifies the acceptance criterion that shutdown always terminates within the
/// configured deadline and exits successfully.
#[cfg(unix)]
async fn run_restart_cycles(config: &SoakConfig, auth: &AuthFixture) -> Result<Vec<String>> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let mut findings = Vec::new();
    let mut forced_drains: Vec<usize> = Vec::new();
    let exe = std::env::current_exe().context("locate soak binary")?;

    for cycle in 0..config.restart_cycles {
        let mut child = Command::new(&exe)
            .arg("--serve-child")
            .env(
                "FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS",
                CHILD_DRAIN_BUDGET_MS.to_string(),
            )
            .stdout(Stdio::piped())
            .spawn()
            .context("spawn child broker")?;

        let stdout = child.stdout.take().context("child stdout")?;
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).context("read child ready")?;
        let Some((addr, cert)) = parse_child_ready(&line) else {
            let _ = child.kill();
            findings.push(format!("cycle {cycle}: child never reported ready"));
            continue;
        };

        // Put real traffic in flight so the drain has something to drain.
        let stats = Arc::new(LoadStats::default());
        let stop = Arc::new(AtomicBool::new(false));
        let child_harness = BrokerHarness {
            addr,
            cert,
            accept_shutdown: CancellationToken::new(),
            connections: TaskTracker::new(),
            accept_task: tokio::spawn(async {}),
            _server: Arc::new(QuicServer::bind(
                "127.0.0.1:0".parse()?,
                build_server_config()?.0,
                TransportConfig::default(),
            )?),
        };
        let publishers = spawn_publishers(
            &child_harness,
            auth,
            Arc::clone(&stats),
            Arc::clone(&stop),
            2,
            config.payload_bytes,
        )
        .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let started = Instant::now();
        terminate_child(&child)?;
        let status = tokio::task::spawn_blocking(move || child.wait()).await??;
        let elapsed = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        for handle in publishers {
            let _ = handle.await;
        }

        // Whatever the child printed after the ready line, including its drain
        // outcome. Read after `wait` so the child has finished writing.
        let mut trailing = String::new();
        let _ = reader.read_line(&mut trailing);
        let forced = trailing
            .trim()
            .strip_prefix("SOAK_CHILD_DRAIN unfinished=")
            .map(|rest| rest.to_string())
            .filter(|rest| !rest.is_empty());

        if !status.success() {
            findings.push(format!(
                "cycle {cycle}: child exited unsuccessfully ({status})"
            ));
        }
        // The child bounds itself at CHILD_DRAIN_DEADLINE; exceeding that plus
        // scheduling slack would mean the bound is not actually enforced.
        let bound = CHILD_DRAIN_DEADLINE + Duration::from_secs(5);
        if elapsed > bound {
            findings.push(format!(
                "cycle {cycle}: shutdown took {elapsed:?}, exceeding the {bound:?} bound"
            ));
        }
        if stats.published.load(Ordering::Relaxed) == 0 {
            findings.push(format!(
                "cycle {cycle}: no traffic reached the child, so its drain was not exercised"
            ));
        }
        if let Some(unfinished) = &forced {
            forced_drains.push(cycle);
            println!("    drain forced at deadline; unfinished: {unfinished}");
        }
        println!(
            "  restart cycle {cycle}: exit={} shutdown={:?} published={} drained_cleanly={}",
            status.success(),
            elapsed,
            stats.published.load(Ordering::Relaxed),
            forced.is_none()
        );
    }

    // A drain forced at the deadline on *every* cycle is a design finding, not
    // a flake: it means shutdown never completes cooperatively while clients
    // hold connections open, which is the normal production state for
    // subscribers. Reported once with that framing rather than per cycle.
    if forced_drains.len() == config.restart_cycles && config.restart_cycles > 0 {
        findings.push(format!(
            "every restart cycle ({}/{}) hit the drain deadline and force-cancelled: connections \
             with a live peer never end on their own, so shutdown always burns the full deadline. \
             The drain waits for connection tasks to finish but has no way to tell them to stop.",
            forced_drains.len(),
            config.restart_cycles
        ));
    }
    Ok(findings)
}

#[cfg(not(unix))]
async fn run_restart_cycles(_config: &SoakConfig, _auth: &AuthFixture) -> Result<Vec<String>> {
    println!("  restart cycles skipped: SIGTERM is Unix-only");
    Ok(Vec::new())
}

fn parse_child_ready(line: &str) -> Option<(SocketAddr, CertificateDer<'static>)> {
    let rest = line.trim().strip_prefix("SOAK_CHILD_READY ")?;
    let mut addr = None;
    let mut cert = None;
    for field in rest.split_whitespace() {
        if let Some(value) = field.strip_prefix("addr=") {
            addr = value.parse().ok();
        } else if let Some(value) = field.strip_prefix("cert=") {
            cert = URL_SAFE_NO_PAD.decode(value).ok().map(CertificateDer::from);
        }
    }
    Some((addr?, cert?))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let (config, serve_child) = parse_args()?;
    if serve_child {
        return run_serve_child().await;
    }

    // A recorder must be installed for the broker's gauges to be readable.
    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("install metrics recorder")?;

    println!("== Felix soak harness ==");
    println!(
        "phases {}s, settle cap {}s, {} publishers, {} subscribers, {} churn cycles, {} restart cycles",
        config.phase_secs,
        config.quiesce_secs,
        config.publishers,
        config.subscribers,
        config.churn_cycles,
        config.restart_cycles
    );

    let auth = build_auth_fixture()?;
    // Baseline is captured after the broker is listening *and* has come to rest,
    // so the listener socket and the runtime's own startup allocations are part
    // of the baseline rather than surfacing later as a phantom leak.
    let harness = start_broker(&auth).await?;
    let baseline_settle = settle_to_stable(Duration::from_secs(30)).await;
    let baseline = baseline_settle.sample;
    println!(
        "baseline (broker listening, no clients, settled in {:?}{}): rss={} KiB fds={} tasks={}",
        baseline_settle.waited,
        if baseline_settle.settled {
            ""
        } else {
            "; NOT STABLE"
        },
        baseline.rss_kb,
        baseline.open_fds,
        baseline.alive_tasks
    );

    let mut phases = Vec::new();

    // Phase 1 — sustained publish/subscribe load.
    phases.push(
        run_phase("sustained_load", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
                let mut handles = spawn_subscribers(
                    harness,
                    auth,
                    Arc::clone(&stats),
                    Arc::clone(&stop),
                    config.subscribers,
                    false,
                )
                .await?;
                handles.extend(
                    spawn_publishers(
                        harness,
                        auth,
                        Arc::clone(&stats),
                        Arc::clone(&stop),
                        config.publishers,
                        config.payload_bytes,
                    )
                    .await?,
                );
                tokio::time::sleep(Duration::from_secs(config.phase_secs)).await;
                stop.store(true, Ordering::Relaxed);
                for handle in handles {
                    let _ = handle.await;
                }
                Ok(())
            }
        })
        .await?,
    );

    // Phase 2 — connection churn.
    phases.push(
        run_phase("connection_churn", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
                run_connection_churn(
                    harness,
                    auth,
                    Arc::clone(&stats),
                    config.churn_cycles,
                    config.payload_bytes,
                )
                .await?;
                stop.store(true, Ordering::Relaxed);
                Ok(())
            }
        })
        .await?,
    );

    // Phase 3 — slow subscribers driving queue saturation and the drop policy.
    phases.push(
        run_phase("slow_subscribers", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
                let mut handles = spawn_subscribers(
                    harness,
                    auth,
                    Arc::clone(&stats),
                    Arc::clone(&stop),
                    config.subscribers,
                    true,
                )
                .await?;
                handles.extend(
                    spawn_publishers(
                        harness,
                        auth,
                        Arc::clone(&stats),
                        Arc::clone(&stop),
                        config.publishers,
                        config.payload_bytes,
                    )
                    .await?,
                );
                tokio::time::sleep(Duration::from_secs(config.phase_secs)).await;
                stop.store(true, Ordering::Relaxed);
                for handle in handles {
                    let _ = handle.await;
                }
                Ok(())
            }
        })
        .await?,
    );

    // Phase 4 — identical repeated cycles, the actual memory-leak check.
    let cycle_peaks = run_repeated_load_cycles(&harness, &auth, &config).await?;

    // Phase 5 — wait for cleanup to actually finish, rather than sleeping a fixed
    // amount and hoping. `quiesce_secs` is the cap, not the wait.
    println!(
        "\n[quiesce] waiting for resources to settle (cap {}s)",
        config.quiesce_secs
    );
    let quiesce_settle =
        settle_to_baseline(&baseline, Duration::from_secs(config.quiesce_secs)).await;
    let quiesced = quiesce_settle.sample;
    let gauges = resources::scrape_gauges(&metrics.render());
    println!(
        "quiesced after {:?}{}: rss={} KiB fds={} tasks={}",
        quiesce_settle.waited,
        if quiesce_settle.settled {
            ""
        } else {
            " (CAP EXPIRED)"
        },
        quiesced.rss_kb,
        quiesced.open_fds,
        quiesced.alive_tasks
    );

    // Phase 6 — repeated real-process SIGTERM restarts under traffic.
    println!("\n[restart_cycles]");
    let restart_findings = run_restart_cycles(&config, &auth).await?;

    let unfinished = drain_broker(harness, Duration::from_secs(20)).await;

    let outcome = SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        restart_findings,
        unfinished,
        cycle_peaks,
        settled: quiesce_settle.settled,
    };
    let findings = evaluate(&config, &outcome);
    report(&outcome, &findings);

    if let Some(path) = &config.timeseries_path {
        write_timeseries(path, &outcome.phases)
            .with_context(|| format!("write timeseries to {path}"))?;
        println!("\ntime series written to {path}");
    }

    if findings.is_empty() {
        Ok(())
    } else {
        bail!("{} soak finding(s); see report above", findings.len())
    }
}

/// Run one phase with resource sampling alongside the workload.
async fn run_phase<F, Fut>(
    name: &'static str,
    _config: &SoakConfig,
    workload: F,
) -> Result<PhaseReport>
where
    F: FnOnce(Arc<LoadStats>, Arc<AtomicBool>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    println!("\n[{name}]");
    let stats = Arc::new(LoadStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn(sample_until(
        Arc::clone(&sampler_stop),
        Duration::from_millis(500),
    ));

    workload(Arc::clone(&stats), Arc::clone(&stop)).await?;

    sampler_stop.store(true, Ordering::Relaxed);
    let samples = sampler.await?;
    let report = PhaseReport {
        name,
        samples,
        published: stats.published.load(Ordering::Relaxed),
        received: stats.received.load(Ordering::Relaxed),
        errors: stats.publish_errors.load(Ordering::Relaxed)
            + stats.connect_errors.load(Ordering::Relaxed),
    };
    println!(
        "  published={} received={} errors={} peak_rss={} KiB peak_fds={} peak_tasks={}",
        report.published,
        report.received,
        report.errors,
        report.peak_rss_kb(),
        report.peak_fds(),
        report.peak_tasks()
    );
    Ok(report)
}

/// Run the *same* load several times and report peak RSS per cycle.
///
/// This is the check that actually distinguishes a leak from allocator
/// retention. A single load phase always leaves RSS elevated — allocators do not
/// return freed pages promptly, so that on its own proves nothing. A genuine
/// leak instead shows up as peak RSS climbing on every identical cycle, while
/// mere retention plateaus after the first.
async fn run_repeated_load_cycles(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    config: &SoakConfig,
) -> Result<Vec<u64>> {
    println!("\n[repeated_load_cycles]");
    let mut peaks = Vec::new();
    let cycle_secs = (config.phase_secs / 2).max(3);
    for cycle in 0..config.load_cycles {
        let stats = Arc::new(LoadStats::default());
        let stop = Arc::new(AtomicBool::new(false));
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let sampler = tokio::spawn(sample_until(
            Arc::clone(&sampler_stop),
            Duration::from_millis(250),
        ));

        let mut handles = spawn_subscribers(
            harness,
            auth,
            Arc::clone(&stats),
            Arc::clone(&stop),
            config.subscribers,
            false,
        )
        .await?;
        handles.extend(
            spawn_publishers(
                harness,
                auth,
                Arc::clone(&stats),
                Arc::clone(&stop),
                config.publishers,
                config.payload_bytes,
            )
            .await?,
        );
        tokio::time::sleep(Duration::from_secs(cycle_secs)).await;
        stop.store(true, Ordering::Relaxed);
        for handle in handles {
            let _ = handle.await;
        }
        // Let each cycle settle so the peak reflects the cycle, not the tail of
        // the previous one.
        tokio::time::sleep(Duration::from_secs(3)).await;
        sampler_stop.store(true, Ordering::Relaxed);

        let samples = sampler.await?;
        let peak = samples.iter().map(|s| s.rss_kb).max().unwrap_or(0);
        peaks.push(peak);
        println!(
            "  cycle {cycle}: published={} peak_rss={} KiB",
            stats.published.load(Ordering::Relaxed),
            peak
        );
    }
    Ok(peaks)
}

/// Compare post-quiescence state against baseline and the broker's own gauges.
/// Everything one soak run produced, so evaluation and reporting take a single
/// argument instead of a long positional list that is easy to transpose.
struct SoakOutcome {
    baseline: ResourceSample,
    quiesced: ResourceSample,
    phases: Vec<PhaseReport>,
    gauges: HashMap<String, f64>,
    restart_findings: Vec<String>,
    unfinished: Vec<&'static str>,
    cycle_peaks: Vec<u64>,
    /// Whether process fds and tasks returned to the idle baseline before the cap.
    settled: bool,
}

fn evaluate(config: &SoakConfig, outcome: &SoakOutcome) -> Vec<String> {
    let SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        restart_findings,
        unfinished,
        cycle_peaks,
        settled,
    } = outcome;
    let mut findings: Vec<String> = restart_findings.clone();

    // Process-wide file descriptors and task counts are reported as a single
    // "did it come to rest" check rather than two exact comparisons.
    //
    // The distinction matters because this harness runs the load generators in
    // the *same process* as the broker. A raw `quiesced > baseline` comparison
    // therefore charges the broker for the harness's own client teardown —
    // `felix-client`'s `Subscription` spawns detached pipeline tasks that this
    // harness cannot join, so they wind down on their own schedule. Exact
    // equality at an arbitrary instant was measuring that race, not a leak.
    //
    // The broker's own gauges below are the authoritative assertion; these are
    // corroborating evidence that nothing outlived the run.
    if !settled {
        findings.push(format!(
            "process resources did not return to the idle baseline within the {}s cap: \
             fds {} -> {}, tasks {} -> {}. Note both the broker and the load generators \
             live in this process, so check the broker gauges below before reading this \
             as a broker leak.",
            config.quiesce_secs,
            baseline.open_fds,
            quiesced.open_fds,
            baseline.alive_tasks,
            quiesced.alive_tasks
        ));
    }

    // Memory is judged across identical repeated cycles, not against baseline.
    // Comparing a post-load RSS to a pre-load one only measures allocator
    // retention and would flag every healthy run. A leak is peak RSS still
    // climbing on the last identical cycle.
    if cycle_peaks.len() >= 2 {
        let first = cycle_peaks[0] as f64;
        let last = *cycle_peaks.last().expect("checked non-empty") as f64;
        if first > 0.0 {
            let growth = (last - first) / first;
            if growth > config.rss_growth_tolerance {
                findings.push(format!(
                    "peak RSS grew {:.1}% across {} identical load cycles ({} -> {} KiB), beyond \
                     the {:.0}% tolerance; retention would have plateaued",
                    growth * 100.0,
                    cycle_peaks.len(),
                    cycle_peaks[0],
                    last as u64,
                    config.rss_growth_tolerance * 100.0
                ));
            }
        }
    }

    // Registration gauges must return to exactly zero: every client has gone, so
    // any residue is an entry that will never be reclaimed.
    for gauge in [
        "felix_sub_active_connections",
        "felix_sub_connection_subscribers",
        "felix_broker_ingress_queue_depth",
        "felix_broker_out_ack_depth",
        "felix_sub_queue_len",
        "felix_sub_lane_queue_len",
    ] {
        if let Some(value) = gauges.get(gauge)
            && *value > 0.0
        {
            findings.push(format!(
                "gauge {gauge} did not return to zero after quiescence: {value}"
            ));
        }
    }

    if !unfinished.is_empty() {
        findings.push(format!(
            "final drain did not complete within its deadline: {unfinished:?}"
        ));
    }

    // A phase that moved no traffic proves nothing; treat it as a harness
    // failure rather than silently reporting a clean run.
    for phase in phases {
        if phase.published == 0 {
            findings.push(format!(
                "phase {} published nothing, so it did not exercise the broker",
                phase.name
            ));
        }
    }

    findings
}

/// Write the sampled series as JSONL so a run can be re-examined or charted
/// later, matching how `data/raw/latency_demo_runs.jsonl` records perf runs.
fn write_timeseries(path: &str, phases: &[PhaseReport]) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(path)?;
    for phase in phases {
        for sample in &phase.samples {
            writeln!(
                file,
                r#"{{"phase":"{}","unix_ms":{},"rss_kb":{},"open_fds":{},"alive_tasks":{}}}"#,
                phase.name, sample.unix_ms, sample.rss_kb, sample.open_fds, sample.alive_tasks
            )?;
        }
    }
    Ok(())
}

fn report(outcome: &SoakOutcome, findings: &[String]) {
    let SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        ..
    } = outcome;
    println!("\n== Soak report ==");
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "phase", "peak_rss", "peak_fds", "peak_tasks"
    );
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "baseline", baseline.rss_kb, baseline.open_fds, baseline.alive_tasks
    );
    for phase in phases {
        println!(
            "{:<20} {:>10} {:>8} {:>10}",
            phase.name,
            phase.peak_rss_kb(),
            phase.peak_fds(),
            phase.peak_tasks()
        );
        if let Some(last) = phase.last() {
            println!(
                "{:<20} {:>10} {:>8} {:>10}   (end of phase)",
                "", last.rss_kb, last.open_fds, last.alive_tasks
            );
        }
    }
    println!(
        "{:<20} {:>10} {:>8} {:>10}",
        "quiesced", quiesced.rss_kb, quiesced.open_fds, quiesced.alive_tasks
    );

    println!("\nsteady-state gauges:");
    let mut names: Vec<&String> = gauges.keys().collect();
    names.sort();
    for name in names {
        println!("  {:<45} {}", name, gauges[name]);
    }

    if findings.is_empty() {
        println!("\nNo findings: resources returned to the steady-state envelope.");
    } else {
        println!("\n{} finding(s):", findings.len());
        for finding in findings {
            println!("  - {finding}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(rss_kb: u64, open_fds: u64, alive_tasks: usize) -> ResourceSample {
        ResourceSample {
            unix_ms: 0,
            rss_kb,
            open_fds,
            alive_tasks,
        }
    }

    /// A run where nothing went wrong, used as the baseline every case mutates.
    fn clean_outcome() -> SoakOutcome {
        SoakOutcome {
            baseline: sample(10_000, 11, 10),
            quiesced: sample(11_000, 11, 10),
            phases: vec![PhaseReport {
                name: "sustained_load",
                samples: vec![sample(10_000, 11, 10), sample(50_000, 35, 300)],
                published: 1_000,
                received: 3_000,
                errors: 0,
            }],
            gauges: HashMap::new(),
            restart_findings: Vec::new(),
            unfinished: Vec::new(),
            cycle_peaks: vec![100_000, 101_000],
            settled: true,
        }
    }

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

    #[test]
    fn a_clean_run_produces_no_findings() {
        let findings = evaluate(&SoakConfig::default(), &clean_outcome());
        assert!(findings.is_empty(), "unexpected findings: {findings:?}");
    }

    #[test]
    fn unsettled_resources_are_reported_with_the_shared_process_caveat() {
        let mut outcome = clean_outcome();
        outcome.settled = false;
        outcome.quiesced = sample(11_000, 19, 106);
        let findings = evaluate(&SoakConfig::default(), &outcome);
        let text = findings.join(" ");
        assert!(
            text.contains("did not return to the idle baseline"),
            "{findings:?}"
        );
        // The caveat is the point: without it a reader takes this as a broker leak,
        // which is exactly the misdiagnosis that produced a false CI failure.
        assert!(text.contains("load generators"), "{findings:?}");
    }

    /// Memory is judged across identical cycles, so growth *within* tolerance must
    /// not fire and growth beyond it must.
    #[test]
    fn rss_growth_fires_only_beyond_the_tolerance() {
        let config = SoakConfig::default(); // 25% tolerance
        let mut outcome = clean_outcome();

        outcome.cycle_peaks = vec![100_000, 120_000]; // +20%
        assert!(
            evaluate(&config, &outcome).is_empty(),
            "20% growth is inside the 25% tolerance"
        );

        outcome.cycle_peaks = vec![100_000, 140_000]; // +40%
        let findings = evaluate(&config, &outcome);
        assert!(
            findings.iter().any(|f| f.contains("peak RSS grew")),
            "{findings:?}"
        );
    }

    #[test]
    fn a_single_cycle_cannot_judge_memory_growth() {
        let mut outcome = clean_outcome();
        outcome.cycle_peaks = vec![100_000];
        assert!(
            evaluate(&SoakConfig::default(), &outcome).is_empty(),
            "one cycle gives nothing to compare against"
        );
    }

    /// The broker's own gauges are the authoritative leak signal, so each one must
    /// be checked and a residue in any of them must fail.
    #[test]
    fn every_registration_gauge_is_checked_for_residue() {
        for gauge in [
            "felix_sub_active_connections",
            "felix_sub_connection_subscribers",
            "felix_broker_ingress_queue_depth",
            "felix_broker_out_ack_depth",
            "felix_sub_queue_len",
            "felix_sub_lane_queue_len",
        ] {
            let mut outcome = clean_outcome();
            outcome.gauges.insert(gauge.to_string(), 3.0);
            let findings = evaluate(&SoakConfig::default(), &outcome);
            assert!(
                findings.iter().any(|f| f.contains(gauge)),
                "{gauge} residue should be a finding, got {findings:?}"
            );
        }
    }

    #[test]
    fn a_gauge_at_zero_is_not_a_finding() {
        let mut outcome = clean_outcome();
        outcome
            .gauges
            .insert("felix_sub_active_connections".to_string(), 0.0);
        assert!(evaluate(&SoakConfig::default(), &outcome).is_empty());
    }

    #[test]
    fn an_unfinished_drain_is_reported() {
        let mut outcome = clean_outcome();
        outcome.unfinished = vec!["quic_connections"];
        let findings = evaluate(&SoakConfig::default(), &outcome);
        assert!(
            findings.iter().any(|f| f.contains("quic_connections")),
            "{findings:?}"
        );
    }

    /// A phase that moved no traffic proves nothing, so it must fail rather than
    /// silently report a clean run.
    #[test]
    fn a_phase_that_published_nothing_is_a_finding() {
        let mut outcome = clean_outcome();
        outcome.phases[0].published = 0;
        let findings = evaluate(&SoakConfig::default(), &outcome);
        assert!(
            findings.iter().any(|f| f.contains("published nothing")),
            "{findings:?}"
        );
    }

    #[test]
    fn restart_findings_are_carried_through() {
        let mut outcome = clean_outcome();
        outcome.restart_findings = vec!["cycle 0: child exited unsuccessfully".to_string()];
        let findings = evaluate(&SoakConfig::default(), &outcome);
        assert!(
            findings.iter().any(|f| f.contains("cycle 0")),
            "{findings:?}"
        );
    }

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

    #[test]
    fn child_ready_line_round_trips() {
        let addr: SocketAddr = "127.0.0.1:4433".parse().expect("addr");
        let cert = CertificateDer::from(vec![1u8, 2, 3, 4]);
        let line = format!(
            "SOAK_CHILD_READY addr={} cert={}",
            addr,
            URL_SAFE_NO_PAD.encode(&cert)
        );
        let (parsed_addr, parsed_cert) = parse_child_ready(&line).expect("parse");
        assert_eq!(parsed_addr, addr);
        assert_eq!(parsed_cert, cert);
    }

    #[test]
    fn child_ready_rejects_lines_that_are_not_the_ready_marker() {
        assert!(parse_child_ready("some other output").is_none());
        assert!(parse_child_ready("SOAK_CHILD_READY addr=nonsense cert=zz").is_none());
        assert!(parse_child_ready("SOAK_CHILD_READY addr=127.0.0.1:1").is_none());
    }

    #[test]
    fn timeseries_is_written_as_one_json_object_per_sample() {
        let dir = std::env::temp_dir().join(format!("soak-ts-{}", std::process::id()));
        let path = dir.join("ts.jsonl");
        let outcome = clean_outcome();
        write_timeseries(path.to_str().expect("utf8 path"), &outcome.phases).expect("write");
        let body = std::fs::read_to_string(&path).expect("read back");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "one line per sample: {body}");
        assert!(
            lines[0].contains("\"phase\":\"sustained_load\""),
            "{}",
            lines[0]
        );
        assert!(lines[0].contains("\"rss_kb\":10000"), "{}", lines[0]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
