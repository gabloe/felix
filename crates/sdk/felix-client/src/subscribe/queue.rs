//! What a client does when it cannot keep up with its own subscription.
//!
//! The queues between the read loop and the application are bounded, and
//! what happens when one fills is the application's choice
//! ([`crate::ClientSubQueuePolicy`]). Getting it wrong is quiet: the events
//! simply are not there.

use tokio::sync::mpsc;

use crate::config::ClientSubQueuePolicy;

/// Queue `item` under `policy`, counting the outcome on the named metrics.
/// `false` means the receiver is gone and the subscription is over.
pub(super) async fn enqueue_with_policy<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
    enqueued_metric: &'static str,
    dropped_metric: &'static str,
    drop_old_emulated_metric: &'static str,
) -> bool {
    match policy {
        ClientSubQueuePolicy::Block => {
            if tx.send(item).await.is_err() {
                return false;
            }
            metrics::counter!(enqueued_metric).increment(1);
        }
        ClientSubQueuePolicy::DropNew => match tx.try_send(item) {
            Ok(()) => {
                metrics::counter!(enqueued_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(dropped_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        },
        ClientSubQueuePolicy::DropOld => match tx.try_send(item) {
            Ok(()) => {
                metrics::counter!(enqueued_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(dropped_metric).increment(1);
                metrics::counter!(drop_old_emulated_metric).increment(1);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        },
    }
    metrics::gauge!("felix_client_sub_queue_len")
        .set((queue_capacity.saturating_sub(tx.capacity())) as f64);
    true
}
