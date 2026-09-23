//! Replica reports, batched the way the storage layer batches fsyncs.
//!
//! A report has to reach the control plane before the shard's quorum mark
//! moves, so it sits on the path of every `Quorum` publish. One POST per shard
//! per pass meant a broker leading many shards spent that path on round trips
//! that differ only in which shard they name — and the endpoint has always
//! taken a list, so they were single-element batches by habit rather than by
//! need.
//!
//! **Group commit, not a timer.** A window would add its own wait to a pass
//! that has only one shard to report, which is the common small deployment and
//! the one least able to spare it. Instead a flush takes everything queued at
//! that moment and sends it; reports arriving while that request is in flight
//! queue behind it and go together in the next one. Batches grow under load,
//! which is when they are worth having, and an idle broker waits for nothing.
//! `disk_log/sync.rs` makes the same trade for the same reason.
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::driver::{ReportTo, ShardReport, send_reports};

/// Most reports in one request.
///
/// The bound is the control plane's request size, not the broker's appetite: a
/// pass ships at most `SHARD_CONCURRENCY` shards at once, so this is only
/// reached when passes overlap or a broker leads a great many shards.
const MAX_BATCH: usize = 256;

/// How many reports may wait for a flush before submitting blocks.
///
/// Blocking is the right answer when it is reached — the caller is a shard
/// waiting to publish its mark, and dropping its report would release a
/// `Quorum` publish on a report nobody sent.
const QUEUE_DEPTH: usize = 1024;

struct Pending {
    report: ShardReport,
    landed: oneshot::Sender<bool>,
}

/// Submits replica reports and tells each caller whether its own landed.
#[derive(Clone)]
pub struct Reporter {
    tx: mpsc::Sender<Pending>,
}

impl Reporter {
    /// Start the flushing task. It runs until `shutdown`, or until every
    /// `Reporter` handle is dropped.
    pub fn spawn(to: ReportTo, shutdown: CancellationToken) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(QUEUE_DEPTH);
        let task = tokio::spawn(flush_loop(to, rx, shutdown));
        (Self { tx }, task)
    }

    /// Submit `report` and wait until it has reached the control plane.
    ///
    /// `false` means it did not, and the caller must leave its quorum mark
    /// where it was: releasing a publish on a report the control plane never
    /// saw is the window the report-before-mark ordering exists to close.
    pub async fn send(&self, report: ShardReport) -> bool {
        let (landed, answer) = oneshot::channel();
        if self.tx.send(Pending { report, landed }).await.is_err() {
            // The flushing task is gone, which means the broker is shutting
            // down. Nothing will carry this report, so it did not land.
            return false;
        }
        answer.await.unwrap_or(false)
    }
}

async fn flush_loop(to: ReportTo, mut rx: mpsc::Receiver<Pending>, shutdown: CancellationToken) {
    loop {
        let first = tokio::select! {
            _ = shutdown.cancelled() => break,
            received = rx.recv() => match received {
                Some(pending) => pending,
                // Every handle dropped.
                None => break,
            },
        };

        // Everything already waiting joins this request. Nothing is waited
        // *for*: what queues while the request is in flight goes in the next.
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(pending) => batch.push(pending),
                Err(_) => break,
            }
        }

        metrics::histogram!(super::metrics::REPORTS_PER_REQUEST).record(batch.len() as f64);
        let reports: Vec<ShardReport> =
            batch.iter().map(|pending| pending.report.clone()).collect();
        let landed = send_reports(&to, &reports).await;

        // One answer for the whole request, which is what the endpoint gives:
        // it walks the list, skips a shard it will not accept, and answers for
        // the request as a whole. A shard it skipped is not an error to the
        // caller either way — the next pass sends a fresher report.
        for pending in batch {
            let _ = pending.landed.send(landed);
        }
    }

    // Anything still queued will never be sent, and a caller blocked on an
    // answer must not wait out its publish timeout for one that is not coming.
    rx.close();
    while let Ok(pending) = rx.try_recv() {
        let _ = pending.landed.send(false);
    }
}

#[cfg(test)]
mod tests;
