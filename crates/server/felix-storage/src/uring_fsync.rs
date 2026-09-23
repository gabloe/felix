//! Device flushes submitted to `io_uring` instead of a blocking thread.
//!
//! Every other way of issuing an `fsync` from async Rust hands the call to a
//! thread and waits for it to come back: `spawn_blocking` uses the shared pool,
//! a dedicated flusher uses one warm thread. Both keep a reactor thread free,
//! which is the point, and both cost two thread wake-ups per flush.
//!
//! `IORING_OP_FSYNC` removes the hand-off rather than making it cheaper. The
//! request goes into a submission queue, the kernel performs it, and a
//! completion arrives. No thread is parked for the duration, and — unlike
//! running the sync inline — the `await` is still a yield point, so background
//! work like rollover and retention still gets scheduled.
//!
//! One ring serves the whole process. Flushes are already serialised per log by
//! the group-commit lock, so the concurrency that matters here is *across* logs:
//! a broker with fifty shards can have fifty flushes outstanding, and on the
//! blocking pool that is fifty threads. Here it is one ring and one thread.
//!
//! **Measured expectations, so nobody is surprised.** On a 4-way NVMe RAID0 the
//! hand-off is about 9% of a flush (583µs with it, 530µs without), and the
//! device itself sustains ~1,346 MB/s against Felix's ~940 MB/s. This is not a
//! throughput fix; it removes a hand-off that should not be there on a
//! Linux-only server, and the dividend is small. See #547 and #548.

use std::collections::HashMap;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use io_uring::{IoUring, opcode, types};
use tokio::sync::oneshot;

/// How many flushes may be outstanding in the ring at once.
///
/// One per shard log is the shape to size for; a broker with more shards than
/// this simply queues, which is what the blocking pool did anyway.
const RING_ENTRIES: u32 = 256;

struct Submission {
    fd: RawFd,
    reply: oneshot::Sender<io::Result<()>>,
}

struct Ring {
    tx: std::sync::mpsc::Sender<Submission>,
}

static RING: OnceLock<Option<Ring>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Whether flushes should go through `io_uring`.
///
/// `FELIX_STORAGE_IO_URING=1`. Off by default: the kernel floor is 5.1 for
/// `IORING_OP_FSYNC`, and a broker that cannot build a ring must keep working,
/// so this stays opt-in until it has run somewhere real.
pub(crate) fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("FELIX_STORAGE_IO_URING")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Start the ring and its service thread, once.
///
/// Returns `None` if the ring cannot be created — an old kernel, or a
/// container that forbids the syscall. The caller falls back to the blocking
/// path rather than failing the publish: a durability mechanism must not
/// depend on an optimisation being available.
fn ring() -> Option<&'static Ring> {
    RING.get_or_init(|| {
        let mut uring = match IoUring::new(RING_ENTRIES) {
            Ok(uring) => uring,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "io_uring unavailable; device flushes stay on the blocking pool"
                );
                return None;
            }
        };
        let (tx, rx) = std::sync::mpsc::channel::<Submission>();
        std::thread::Builder::new()
            .name("felix-uring-fsync".into())
            .spawn(move || {
                // Owned exclusively by this thread: the submission and
                // completion queues are not safe to touch from several.
                let mut waiting: HashMap<u64, oneshot::Sender<io::Result<()>>> = HashMap::new();
                loop {
                    // With nothing outstanding there is nothing to drain, so
                    // block for work. With requests in flight, never block on
                    // the channel: their completions must be collected even if
                    // no further flush is ever submitted. Getting this wrong
                    // strands a publish forever.
                    if waiting.is_empty() {
                        match rx.recv() {
                            Ok(first) => {
                                if !push(&mut uring, &mut waiting, first) {
                                    continue;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    // Whatever else is already queued joins this submit.
                    for submission in rx.try_iter() {
                        push(&mut uring, &mut waiting, submission);
                    }

                    if let Err(err) = uring.submit_and_wait(1) {
                        // Nothing outstanding can be called durable.
                        for (_, reply) in waiting.drain() {
                            let _ = reply.send(Err(io::Error::new(err.kind(), err.to_string())));
                        }
                        continue;
                    }
                    let completions: Vec<(u64, i32)> = uring
                        .completion()
                        .map(|cqe| (cqe.user_data(), cqe.result()))
                        .collect();
                    for (id, result) in completions {
                        if let Some(reply) = waiting.remove(&id) {
                            let outcome = if result < 0 {
                                Err(io::Error::from_raw_os_error(-result))
                            } else {
                                Ok(())
                            };
                            let _ = reply.send(outcome);
                        }
                    }
                }
            })
            .ok()?;
        Some(Ring { tx })
    })
    .as_ref()
}

/// Queue one fsync. Answers the caller directly if the ring has no room, so a
/// full queue is a reported error rather than a lost request.
fn push(
    uring: &mut IoUring,
    waiting: &mut HashMap<u64, oneshot::Sender<io::Result<()>>>,
    submission: Submission,
) -> bool {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let entry = opcode::Fsync::new(types::Fd(submission.fd))
        .build()
        .user_data(id);
    // Safety: the caller holds the `File` across its await, so the descriptor
    // stays open until the completion is delivered.
    let pushed = unsafe { uring.submission().push(&entry).is_ok() };
    if pushed {
        waiting.insert(id, submission.reply);
    } else {
        let _ = submission
            .reply
            .send(Err(io::Error::other("io_uring submission queue full")));
    }
    pushed
}

/// `fsync` the descriptor through the ring.
///
/// `None` means the ring is unavailable and the caller should use its own
/// fallback.
pub(crate) async fn fsync(fd: RawFd) -> Option<io::Result<()>> {
    let ring = ring()?;
    let (reply, wait) = oneshot::channel();
    // A send failure means the service thread is gone, which is the same
    // situation as no ring at all.
    if ring.tx.send(Submission { fd, reply }).is_err() {
        return None;
    }
    match wait.await {
        Ok(outcome) => Some(outcome),
        Err(_) => Some(Err(io::Error::other("io_uring service thread stopped"))),
    }
}
