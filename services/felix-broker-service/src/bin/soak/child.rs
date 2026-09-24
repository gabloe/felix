//! Child mode: a real broker process, so SIGTERM and the drain are real too.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_transport::{QuicServer, TransportConfig};
use rustls::pki_types::CertificateDer;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::SoakConfig;
use crate::fixture::{
    AuthFixture, BrokerHarness, CHILD_DRAIN_BUDGET_MS, CHILD_DRAIN_DEADLINE, build_auth_fixture,
    build_server_config, start_broker,
};
use crate::load::{LoadStats, spawn_publishers};
use crate::phase::drain_broker;

/// Run a broker until terminated, printing its address and certificate so the
/// parent can drive load against it.
///
/// This exists so the restart phase sends a genuine `SIGTERM` to a genuine
/// process. An in-process cancellation token cannot show that the signal
/// handler is wired up, that the drain completes, or that the process exits
/// zero — which is exactly the gap left open when #139 shipped.
pub(crate) async fn run_serve_child() -> Result<()> {
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
pub(crate) fn terminate_child(child: &std::process::Child) -> Result<()> {
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
pub(crate) async fn run_restart_cycles(
    config: &SoakConfig,
    auth: &AuthFixture,
) -> Result<Vec<String>> {
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
pub(crate) async fn run_restart_cycles(
    _config: &SoakConfig,
    _auth: &AuthFixture,
) -> Result<Vec<String>> {
    println!("  restart cycles skipped: SIGTERM is Unix-only");
    Ok(Vec::new())
}

pub(crate) fn parse_child_ready(line: &str) -> Option<(SocketAddr, CertificateDer<'static>)> {
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

#[cfg(test)]
mod tests;
