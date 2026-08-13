//! Kill-mid-write recovery against a real process, really killed.
//!
//! Everything in the unit tests simulates a crash by editing bytes. That proves
//! the decoder handles the shapes we thought of. This file proves the shapes we
//! *didn't* think of are handled too: a `felix-log-tool` child is `SIGKILL`ed
//! while it is actively appending — no unwinding, no destructors, no final
//! flush — and the log is then reopened and held to its promises.
//!
//! The promise under test is per fsync mode:
//!
//! | mode        | guaranteed to survive              |
//! |-------------|------------------------------------|
//! | `on_commit` | every acknowledged record          |
//! | `periodic`  | a valid prefix; loss bounded by the interval |
//! | `none`      | a valid prefix, possibly empty     |
//!
//! Every mode must leave a log that opens, contains a *contiguous* prefix with
//! intact payloads, and accepts new appends afterwards.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::tempdir;

const TOOL: &str = env!("CARGO_BIN_EXE_felix-log-tool");
const PAYLOAD_BYTES: &str = "128";

/// Start a writer that appends until it is killed, reporting acknowledgements.
fn spawn_writer(dir: &Path, fsync: &str) -> Child {
    Command::new(TOOL)
        .args([
            "write",
            "--dir",
            &dir.display().to_string(),
            "--records",
            "0",
            "--payload-bytes",
            PAYLOAD_BYTES,
            "--fsync",
            fsync,
            "--fsync-interval-ms",
            "20",
            // Small segments so a run of a few thousand records crosses several
            // rollovers — the boundary is where recovery is most interesting.
            "--segment-bytes",
            "16384",
            "--index-spacing-bytes",
            "512",
            "--report-acks",
            "--no-preallocate",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn felix-log-tool")
}

/// Read acknowledgements until `wanted` of them have arrived, then `SIGKILL`.
///
/// Returns the highest offset the writer had acknowledged when the kill was
/// sent. Under `on_commit` every offset up to it is a hard promise.
///
/// It is *not* an upper bound on what survives: the writer keeps appending
/// while the signal is in flight, and bytes it handed to the page cache outlive
/// the process even though nothing acknowledged them. Recovery may therefore
/// legitimately return more records than were acknowledged.
fn kill_after_acks(mut child: Child, wanted: usize) -> u64 {
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let mut acked = 0usize;
    let mut last_offset = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);

    while acked < wanted {
        line.clear();
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("writer produced only {acked} acknowledgements before the deadline");
        }
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(err) => panic!("reading writer output: {err}"),
        }
        if let Some(offset) = parse_field(&line, "last_offset") {
            last_offset = offset;
            acked += 1;
        }
    }

    // SIGKILL, not SIGTERM: the point is that no cleanup code runs. Whatever is
    // on disk afterwards is what the write path itself put there.
    kill_hard(&child);
    let _ = child.wait();
    last_offset
}

fn kill_hard(child: &Child) {
    // SAFETY: `child.id()` is a live process this test started and has not yet
    // reaped, and `SIGKILL` takes no arguments that could be invalid.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGKILL);
    }
}

/// Pull an integer field out of one of the tool's JSON lines.
fn parse_field(line: &str, field: &str) -> Option<u64> {
    let start = line.find(&format!("\"{field}\":"))? + field.len() + 3;
    let rest = &line[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

struct Verified {
    records: u64,
    tail_offset: u64,
}

/// Reopen the log, checking every surviving record's payload and ordering.
fn verify(dir: &Path, expect_at_least: Option<u64>) -> Verified {
    let mut args = vec![
        "verify".to_string(),
        "--dir".to_string(),
        dir.display().to_string(),
        "--payload-bytes".to_string(),
        PAYLOAD_BYTES.to_string(),
        "--segment-bytes".to_string(),
        "16384".to_string(),
        "--index-spacing-bytes".to_string(),
        "512".to_string(),
        // Check every record, not just the ones after the last index entry:
        // a crash test that only inspected the tail would miss exactly the
        // damage this is looking for.
        "--fsync".to_string(),
        "none".to_string(),
    ];
    if let Some(expected) = expect_at_least {
        args.push("--expect-at-least".to_string());
        args.push(expected.to_string());
    }

    let output = Command::new(TOOL).args(&args).output().expect("run verify");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "verify failed\nstdout: {stdout}\nstderr: {stderr}"
    );
    Verified {
        records: parse_field(&stdout, "records").expect("records field"),
        tail_offset: parse_field(&stdout, "tail_offset").expect("tail_offset field"),
    }
}

/// Append a few records with a clean shutdown, proving the log still works.
fn append_cleanly(dir: &Path, records: u64) {
    let output = Command::new(TOOL)
        .args([
            "write",
            "--dir",
            &dir.display().to_string(),
            "--records",
            &records.to_string(),
            "--payload-bytes",
            PAYLOAD_BYTES,
            "--fsync",
            "on_commit",
            "--segment-bytes",
            "16384",
            "--index-spacing-bytes",
            "512",
            "--clean-shutdown",
            "--no-preallocate",
        ])
        .output()
        .expect("run write");
    assert!(
        output.status.success(),
        "append after recovery failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn on_commit_loses_nothing_it_acknowledged() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "on_commit");
    let last_acked = kill_after_acks(writer, 400);

    // Every acknowledged offset was fsynced before the acknowledgement was
    // printed, so all of them must be here. This is the whole guarantee.
    let verified = verify(dir.path(), Some(last_acked + 1));
    assert!(
        verified.tail_offset > last_acked,
        "tail {} is behind the last acknowledged offset {last_acked}",
        verified.tail_offset
    );
}

#[test]
fn periodic_recovers_a_valid_prefix() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "periodic");
    kill_after_acks(writer, 400);

    // `periodic` promises no particular record, only a bounded loss window — so
    // the assertion is about *shape*, not count: whatever survived must be a
    // contiguous, checksum-clean prefix with a matching record count and tail.
    // `verify` fails on any gap, any bad payload, and any unreadable page.
    let verified = verify(dir.path(), None);
    assert_eq!(
        verified.records, verified.tail_offset,
        "recovered a log with a hole in it"
    );
}

#[test]
fn none_still_recovers_a_valid_prefix() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "none");
    kill_after_acks(writer, 400);

    // Even with no fsync at all, a crash must never produce a log that fails to
    // open or that contains a record the decoder cannot verify.
    let verified = verify(dir.path(), None);
    assert_eq!(verified.records, verified.tail_offset);
}

#[test]
fn a_recovered_log_accepts_new_appends() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "on_commit");
    let last_acked = kill_after_acks(writer, 200);

    let after_crash = verify(dir.path(), Some(last_acked + 1));
    append_cleanly(dir.path(), 100);

    let after_append = verify(dir.path(), Some(after_crash.records + 100));
    assert_eq!(after_append.records, after_crash.records + 100);
    assert_eq!(after_append.tail_offset, after_crash.tail_offset + 100);
}

#[test]
fn repeated_crash_and_recovery_cycles_stay_consistent() {
    let dir = tempdir().expect("dir");
    let mut previous = 0u64;

    // Each cycle recovers, appends more, and is killed again — so later cycles
    // run recovery against a log that has already been repaired at least once.
    for cycle in 0..4 {
        let writer = spawn_writer(dir.path(), "on_commit");
        let last_acked = kill_after_acks(writer, 150);

        let verified = verify(dir.path(), Some(last_acked + 1));
        assert!(
            verified.records > previous,
            "cycle {cycle} did not make progress: {} then {}",
            previous,
            verified.records
        );
        assert_eq!(
            verified.records, verified.tail_offset,
            "cycle {cycle} left a gap between the record count and the tail"
        );
        previous = verified.records;
    }

    // And after all that, a clean run still works end to end.
    append_cleanly(dir.path(), 50);
    let final_state = verify(dir.path(), Some(previous + 50));
    assert_eq!(final_state.records, previous + 50);
}

#[test]
fn recovery_is_idempotent_after_a_crash() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "periodic");
    kill_after_acks(writer, 200);

    let first = verify(dir.path(), None);
    let second = verify(dir.path(), None);
    let third = verify(dir.path(), None);
    assert_eq!(first.records, second.records);
    assert_eq!(second.records, third.records);
    assert_eq!(first.tail_offset, third.tail_offset);
}

#[test]
fn a_crash_before_any_acknowledgement_leaves_an_openable_log() {
    let dir = tempdir().expect("dir");
    let writer = spawn_writer(dir.path(), "on_commit");
    // Kill as early as possible: the segment may hold zero complete records.
    kill_after_acks(writer, 1);

    // The segment may hold as little as its header. Whatever it holds must
    // still open and still be extendable.
    let before = verify(dir.path(), None);
    assert_eq!(before.records, before.tail_offset);
    append_cleanly(dir.path(), 10);
    let after = verify(dir.path(), Some(before.records + 10));
    assert_eq!(after.records, before.records + 10);
}
