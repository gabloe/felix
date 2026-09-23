//! Felix conformance test runner.
//!
//! Exercises end-to-end broker and client behaviors (auth, QUIC, wire encoding)
//! to validate protocol and security invariants outside unit tests.
//!
//! This binary is intended for CI and developer verification, not production use.
//!
//! - With no arguments it runs the protocol suite against an in-process
//!   broker (see `suite`).
//! - `verify <results.json>` checks a client's results against the scenario
//!   catalogue.
//! - `scenarios` prints the catalogue, so a client author can see what to
//!   implement.

mod commands;
mod suite;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Subcommands are additive: with no arguments this is the protocol runner
    // it has always been, so existing invocations are unchanged.
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("verify") => commands::run_verify(&args[1..]),
        Some("scenarios") => commands::run_scenarios(),
        Some(other) => {
            anyhow::bail!("unknown subcommand {other:?} (expected `verify` or `scenarios`)")
        }
        // Argument parsing is kept out of `run_protocol_suite` so its test
        // can call it without the test harness's own argv reaching it.
        None => suite::run_protocol_suite().await,
    }
}
