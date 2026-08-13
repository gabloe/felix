//! `felix-log-tool` — write, verify and benchmark a durable log directly.
//!
//! It exists because the interesting durability questions cannot be answered
//! from inside a test process:
//!
//! * *Does a `SIGKILL` mid-append lose anything the log promised?* Only a real
//!   process, really killed, answers that. `write --report-acks` prints every
//!   acknowledgement so a harness can hold the log to exactly those records, and
//!   `verify` checks the survivors afterwards.
//! * *What does durability cost?* `bench` measures append latency and throughput
//!   under one fsync policy, so the policies can be compared on identical
//!   hardware rather than argued about.
//!
//! Output is line-delimited JSON on stdout; diagnostics go to stderr.
//!
//! ```text
//! felix-log-tool write  --dir /tmp/log --records 0 --fsync on_commit --report-acks
//! felix-log-tool verify --dir /tmp/log --payload-bytes 128
//! felix-log-tool bench  --dir /tmp/log --records 20000 --concurrency 8 --label on_commit-c8
//! ```

// The module tree lives in `log_tool/`; this file is only the entry point.
#[path = "log_tool/args.rs"]
mod args;
#[path = "log_tool/commands.rs"]
mod commands;
#[path = "log_tool/payload.rs"]
mod payload;

use args::Command;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let parsed = match args::parse(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return std::process::ExitCode::from(2);
        }
    };

    let outcome = match parsed {
        Command::Write(args) => commands::write(args).await,
        Command::Verify(args) => commands::verify(args).await,
        Command::Bench(args) => commands::bench(args).await,
    };

    match outcome {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("felix-log-tool: {message}");
            std::process::ExitCode::FAILURE
        }
    }
}
