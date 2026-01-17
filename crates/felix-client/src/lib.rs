// Minimal client wrapper for talking to an in-process broker.
use bytes::Bytes;
use felix_broker::{Broker, Result};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Client wrapper that talks to an in-process broker.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_client::Client;
/// use felix_storage::EphemeralCache;
/// use std::sync::Arc;
///
/// let broker = Arc::new(Broker::new(EphemeralCache::new()));
/// let client = Client::in_process(broker);
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     let mut sub = client.subscribe("updates").await.expect("subscribe");
///     client
///         .publish("updates", Bytes::from_static(b"payload"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"payload"));
/// });
/// ```
#[derive(Clone)]
pub struct Client {
    // Keep an Arc so callers can clone the client cheaply.
    broker: Arc<Broker>,
}

impl Client {
    // Construct a client that shares the broker running in this process.
    pub fn in_process(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    // Forward publish calls directly to the broker.
    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<usize> {
        self.broker.publish(topic, payload).await
    }

    // Subscribe to a topic and return a broadcast receiver.
    pub async fn subscribe(&self, topic: &str) -> Result<broadcast::Receiver<Bytes>> {
        self.broker.subscribe(topic).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;

    #[tokio::test]
    async fn in_process_publish_and_subscribe() {
        // Smoke-test the in-process path without any network transport.
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
        let client = Client::in_process(broker);
        let mut receiver = client.subscribe("updates").await.expect("subscribe");
        client
            .publish("updates", Bytes::from_static(b"payload"))
            .await
            .expect("publish");
        let msg = receiver.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"payload"));
    }

    #[tokio::test]
    async fn clients_share_broker_state() {
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
        let publisher = Client::in_process(broker.clone());
        let subscriber = Client::in_process(broker);
        let mut receiver = subscriber.subscribe("shared").await.expect("subscribe");
        publisher
            .publish("shared", Bytes::from_static(b"from-publisher"))
            .await
            .expect("publish");
        let msg = receiver.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"from-publisher"));
    }
}
