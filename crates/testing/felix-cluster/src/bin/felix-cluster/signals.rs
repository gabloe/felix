//! Waiting for the signal that tears a held cluster down.

use anyhow::{Context, Result};

/// Wait for Ctrl-C, or for a `kill`.
///
/// Both, because the brokers are child processes: this process dying without
/// running its teardown orphans three of them, each holding a port and a data
/// directory. SIGINT alone leaves `kill` and most process supervisors doing
/// exactly that.
#[cfg(unix)]
pub(crate) async fn stop_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C")?,
        _ = term.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) async fn stop_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
}
