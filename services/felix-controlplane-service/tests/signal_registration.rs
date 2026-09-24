//! The termination handlers must exist before the process is reachable.
//!
//! This lives in `controlplane` rather than `felix-common` because
//! `felix-common`'s `lifecycle` module is behind a non-default feature, so
//! `cargo test -p felix-common` skips it and the coverage would be invisible to
//! the workspace test run.

#![cfg(unix)]

use std::process::Command;
use std::time::Duration;

/// Handlers must be installed by the *call*, not by the first poll of the
/// returned future.
///
/// The control plane binds its listeners and only then reaches the `select!`
/// that awaits this future. Anything registered lazily leaves a window where the
/// port answers but SIGTERM still has its default disposition, and a signal
/// there kills the process outright — no readiness flip, no drain. CI hit that
/// window as a flake.
///
/// The control is the test process itself: this raises SIGTERM at self without
/// ever having polled the future. If registration were lazy, the default
/// disposition would terminate the whole test binary rather than fail one
/// assertion. A dead runner is a loud failure, which is the right kind.
#[tokio::test]
async fn handlers_are_installed_before_the_future_is_polled() {
    let shutdown = felix_common::lifecycle::termination_signal();

    // Deliberately after the call and before the first poll.
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(std::process::id().to_string())
        .status()
        .expect("send SIGTERM to self");
    assert!(status.success());

    // Give the signal every chance to be delivered to a process that is not yet
    // awaiting it, so a pass cannot be this racing ahead of the default action.
    tokio::time::sleep(Duration::from_millis(250)).await;

    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("a SIGTERM raised before the first poll must still resolve the future");
}
