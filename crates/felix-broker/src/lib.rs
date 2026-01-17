// In-process pub/sub broker with a tiny cache hook.
use bytes::Bytes;
use felix_storage::EphemeralCache;
use std::collections::HashMap;
use tokio::sync::{RwLock, broadcast};

pub type Result<T> = std::result::Result<T, BrokerError>;

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("topic capacity too large")]
    CapacityTooLarge,
}

const DEFAULT_TOPIC_CAPACITY: usize = 1024;

/// In-process broker for pub/sub messaging.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_storage::EphemeralCache;
///
/// let broker = Broker::new(EphemeralCache::new());
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     let mut sub = broker.subscribe("topic").await.expect("subscribe");
///     broker
///         .publish("topic", Bytes::from_static(b"hello"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"hello"));
/// });
/// ```
#[derive(Debug)]
pub struct Broker {
    // Map of topic name -> broadcast sender.
    topics: RwLock<HashMap<String, broadcast::Sender<Bytes>>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: EphemeralCache,
    // Broadcast channel capacity for each topic.
    topic_capacity: usize,
}

impl Broker {
    // Start with an empty topic table and default capacity.
    pub fn new(cache: EphemeralCache) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            cache,
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
        }
    }

    pub fn with_topic_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        // Keep a single capacity value so new topics match existing ones.
        self.topic_capacity = capacity;
        Ok(self)
    }

    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<usize> {
        // Fan-out to current subscribers; ignore backpressure errors for now.
        let sender = self.get_or_create_topic(topic).await?;
        Ok(sender.send(payload).unwrap_or(0))
    }

    pub async fn subscribe(&self, topic: &str) -> Result<broadcast::Receiver<Bytes>> {
        // Create the topic lazily so callers don't need a setup step.
        let sender = self.get_or_create_topic(topic).await?;
        Ok(sender.subscribe())
    }

    pub fn cache(&self) -> &EphemeralCache {
        // Expose the cache for demos and integrations.
        &self.cache
    }

    async fn get_or_create_topic(&self, topic: &str) -> Result<broadcast::Sender<Bytes>> {
        // One write lock protects creation so we don't race two channels.
        let mut guard = self.topics.write().await;
        if let Some(sender) = guard.get(topic) {
            return Ok(sender.clone());
        }
        let (sender, _) = broadcast::channel(self.topic_capacity);
        guard.insert(topic.to_string(), sender.clone());
        Ok(sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        // Basic pub/sub flow with a single subscriber.
        let broker = Broker::new(EphemeralCache::new());
        let mut sub = broker.subscribe("orders").await.expect("subscribe");
        broker
            .publish("orders", Bytes::from_static(b"hello"))
            .await
            .expect("publish");
        let msg = sub.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn publish_without_subscribers_returns_zero() {
        let broker = Broker::new(EphemeralCache::new());
        let delivered = broker
            .publish("empty", Bytes::from_static(b"payload"))
            .await
            .expect("publish");
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_payload() {
        let broker = Broker::new(EphemeralCache::new());
        let mut sub_a = broker.subscribe("orders").await.expect("subscribe");
        let mut sub_b = broker.subscribe("orders").await.expect("subscribe");
        broker
            .publish("orders", Bytes::from_static(b"fanout"))
            .await
            .expect("publish");
        assert_eq!(sub_a.recv().await.expect("recv"), Bytes::from_static(b"fanout"));
        assert_eq!(sub_b.recv().await.expect("recv"), Bytes::from_static(b"fanout"));
    }

    #[test]
    fn zero_capacity_is_rejected() {
        let broker = Broker::new(EphemeralCache::new());
        let err = broker.with_topic_capacity(0).expect_err("capacity");
        assert!(matches!(err, BrokerError::CapacityTooLarge));
    }
}
