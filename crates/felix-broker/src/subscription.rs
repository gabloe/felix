// Subscriber-facing handles: the receiver half plus the guard that unregisters
// the subscriber from its stream on drop.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Weak;
use tokio::sync::mpsc;

use crate::delivery::{DeliveryEnvelope, QueuedDelivery};
use crate::stream_state::StreamState;

/// RAII handle that unregisters a stream subscriber on drop.
#[derive(Debug)]
pub struct SubscriptionGuard {
    pub(crate) stream_state: Weak<StreamState>,
    pub(crate) subscriber_id: u64,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(stream_state) = self.stream_state.upgrade() {
            stream_state.remove_subscriber(self.subscriber_id);
        }
    }
}

/// Receiver wrapper that keeps the unsubscribe guard alive for the receiver lifetime.
#[derive(Debug)]
pub struct Subscription {
    pub(crate) receiver: SubscriptionReceiver,
    pub(crate) guard: SubscriptionGuard,
    pub(crate) pending: VecDeque<Bytes>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<Bytes> {
        if let Some(payload) = self.pending.pop_front() {
            return Some(payload);
        }
        let envelope = self.receiver.recv().await?;
        self.pending.extend(envelope.payloads().iter().cloned());
        self.pending.pop_front()
    }

    pub fn try_recv(&mut self) -> std::result::Result<Bytes, mpsc::error::TryRecvError> {
        if let Some(payload) = self.pending.pop_front() {
            return Ok(payload);
        }
        let envelope = self.receiver.try_recv()?;
        self.pending.extend(envelope.payloads().iter().cloned());
        self.pending
            .pop_front()
            .ok_or(mpsc::error::TryRecvError::Empty)
    }

    pub fn into_parts(self) -> (SubscriptionReceiver, SubscriptionGuard) {
        (self.receiver, self.guard)
    }
}

#[derive(Debug)]
pub struct SubscriptionReceiver {
    pub(crate) receiver: mpsc::Receiver<QueuedDelivery>,
}

impl SubscriptionReceiver {
    pub(crate) fn new(receiver: mpsc::Receiver<QueuedDelivery>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<DeliveryEnvelope> {
        Some(self.receiver.recv().await?.into_envelope())
    }

    pub fn try_recv(&mut self) -> std::result::Result<DeliveryEnvelope, mpsc::error::TryRecvError> {
        self.receiver.try_recv().map(QueuedDelivery::into_envelope)
    }
}

impl Subscription {
    /// Take whatever is already queued, without waiting.
    ///
    /// Used by resume to drain what accumulated while history was being read,
    /// so the handler can spot a queue drop -- a jump in offsets -- and fill it
    /// from disk before live delivery starts.
    pub fn drain_ready(&mut self) -> Vec<DeliveryEnvelope> {
        let mut drained = Vec::new();
        while let Ok(envelope) = self.receiver.try_recv() {
            drained.push(envelope);
        }
        drained
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) {
        self.receiver.close();
    }
}
