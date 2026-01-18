// In-process pub/sub broker with a tiny cache hook.
use bytes::Bytes;
use felix_storage::EphemeralCache;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{RwLock, broadcast};

pub type Result<T> = std::result::Result<T, BrokerError>;

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("topic capacity too large")]
    CapacityTooLarge,
    #[error("cursor too old (oldest {oldest}, requested {requested})")]
    CursorTooOld { oldest: u64, requested: u64 },
}

const DEFAULT_TOPIC_CAPACITY: usize = 1024;
const DEFAULT_LOG_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    next_seq: u64,
}

impl Cursor {
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[derive(Debug)]
struct LogEntry {
    seq: u64,
    payload: Bytes,
}

#[derive(Debug)]
struct StreamState {
    sender: broadcast::Sender<Bytes>,
    log: VecDeque<LogEntry>,
    next_seq: u64,
}

impl StreamState {
    fn new(topic_capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(topic_capacity);
        Self {
            sender,
            log: VecDeque::new(),
            next_seq: 0,
        }
    }

    fn append(&mut self, payload: Bytes, log_capacity: usize) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.log.push_back(LogEntry { seq, payload });
        while self.log.len() > log_capacity {
            self.log.pop_front();
        }
        seq
    }

    fn oldest_seq(&self) -> u64 {
        self.log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(self.next_seq)
    }
}

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
    // Map of topic name -> stream state (broadcast + log).
    topics: RwLock<HashMap<String, StreamState>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: EphemeralCache,
    // Broadcast channel capacity for each topic.
    topic_capacity: usize,
    // Per-topic in-memory log capacity.
    log_capacity: usize,
}

impl Broker {
    // Start with an empty topic table and default capacity.
    pub fn new(cache: EphemeralCache) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            cache,
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            log_capacity: DEFAULT_LOG_CAPACITY,
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

    pub fn with_log_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        self.log_capacity = capacity;
        Ok(self)
    }

    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<usize> {
        // Fan-out to current subscribers; ignore backpressure errors for now.
        let sender = {
            let mut guard = self.topics.write().await;
            let stream = guard
                .entry(topic.to_string())
                .or_insert_with(|| StreamState::new(self.topic_capacity));
            stream.append(payload.clone(), self.log_capacity);
            stream.sender.clone()
        };
        Ok(sender.send(payload).unwrap_or(0))
    }

    pub async fn subscribe(&self, topic: &str) -> Result<broadcast::Receiver<Bytes>> {
        // Create the topic lazily so callers don't need a setup step.
        let mut guard = self.topics.write().await;
        let stream = guard
            .entry(topic.to_string())
            .or_insert_with(|| StreamState::new(self.topic_capacity));
        Ok(stream.sender.subscribe())
    }

    pub async fn cursor_tail(&self, topic: &str) -> Result<Cursor> {
        let mut guard = self.topics.write().await;
        let stream = guard
            .entry(topic.to_string())
            .or_insert_with(|| StreamState::new(self.topic_capacity));
        Ok(Cursor {
            next_seq: stream.next_seq,
        })
    }

    pub async fn subscribe_with_cursor(
        &self,
        topic: &str,
        cursor: Cursor,
    ) -> Result<(Vec<Bytes>, broadcast::Receiver<Bytes>)> {
        let mut guard = self.topics.write().await;
        let stream = guard
            .entry(topic.to_string())
            .or_insert_with(|| StreamState::new(self.topic_capacity));
        let oldest = stream.oldest_seq();
        if cursor.next_seq < oldest {
            return Err(BrokerError::CursorTooOld {
                oldest,
                requested: cursor.next_seq,
            });
        }
        let backlog = stream
            .log
            .iter()
            .filter(|entry| entry.seq >= cursor.next_seq)
            .map(|entry| entry.payload.clone())
            .collect();
        Ok((backlog, stream.sender.subscribe()))
    }

    pub fn cache(&self) -> &EphemeralCache {
        // Expose the cache for demos and integrations.
        &self.cache
    }

    // get_or_create_topic removed; stream creation handled inline for cursor/log support.
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
        assert_eq!(
            sub_a.recv().await.expect("recv"),
            Bytes::from_static(b"fanout")
        );
        assert_eq!(
            sub_b.recv().await.expect("recv"),
            Bytes::from_static(b"fanout")
        );
    }

    #[tokio::test]
    async fn cursor_replays_log_then_streams_new_events() {
        let broker = Broker::new(EphemeralCache::new());
        let cursor = broker.cursor_tail("orders").await.expect("cursor");
        broker
            .publish("orders", Bytes::from_static(b"one"))
            .await
            .expect("publish");
        broker
            .publish("orders", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        let (backlog, mut sub) = broker
            .subscribe_with_cursor("orders", cursor)
            .await
            .expect("subscribe");
        assert_eq!(
            backlog,
            vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")]
        );
        broker
            .publish("orders", Bytes::from_static(b"three"))
            .await
            .expect("publish");
        assert_eq!(
            sub.recv().await.expect("recv"),
            Bytes::from_static(b"three")
        );
    }

    #[test]
    fn zero_capacity_is_rejected() {
        let broker = Broker::new(EphemeralCache::new());
        let err = broker.with_topic_capacity(0).expect_err("capacity");
        assert!(matches!(err, BrokerError::CapacityTooLarge));
    }
}
