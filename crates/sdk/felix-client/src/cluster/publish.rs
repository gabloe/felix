//! Publishing through a [`ClusterClient`]: once, at least once, or
//! idempotently. See the parent module for why the first two differ.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_wire::AckMode;

use super::{ClusterClient, ShardKey, StreamKey, is_terminal};
use crate::client::Client;
use crate::publish::{AckOutcome, IdempotentProducer};

impl ClusterClient {
    /// Publish, reconnecting if the broker in use has gone.
    ///
    /// **The record is not sent again.** A failure is returned to the caller
    /// with the connection already replaced, so the next publish goes to a live
    /// broker. See [`Self::publish_at_least_once`] for the other choice.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        // The owner of this stream's shard 0, when a previous publish was
        // forwarded and the ack said who to use. Falls back to the client in
        // hand, which forwards -- correct, just slower.
        // An unkeyed publish has nothing to hash, so it always resolves to
        // shard 0 -- no width lookup needed.
        let key: ShardKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            0,
        );
        let (client, routed_to_owner) = match self.owners.read().await.get(&key) {
            Some(owner) => (Arc::clone(&owner.client), Some(owner.node_id.clone())),
            None => (self.client().await, None),
        };
        match publish_once(&client, tenant_id, namespace, stream, payload, ack).await {
            Ok(forwarded_to) => {
                if let Some(owner) = forwarded_to {
                    self.remember_owner(key, owner).await;
                }
                Ok(())
            }
            Err(err) if routed_to_owner.is_some() => Err(self
                .forget_owner(&key, routed_to_owner.unwrap_or_default(), err)
                .await),
            Err(err) => {
                // Reconnect before returning, so the caller's next publish does
                // not repeat this failure against the same dead broker.
                let reconnected = self.reconnect().await;
                match reconnected {
                    Ok(()) => Err(err.context("publish failed; reconnected to another broker")),
                    Err(reconnect_err) => Err(err.context(format!(
                        "publish failed and no other broker answered: {reconnect_err:#}"
                    ))),
                }
            }
        }
    }

    /// Publish with a routing key, which decides the shard.
    ///
    /// Without a key every record lands on shard 0, which makes a multi-shard
    /// stream behave like a single-shard one — the shards exist and only one
    /// is ever written to. The key is what spreads records, and records
    /// sharing a key share a shard and therefore stay ordered with respect to
    /// each other.
    pub async fn publish_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        key: bytes::Bytes,
        ack: AckMode,
    ) -> Result<()> {
        let stream_key: StreamKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        // The key decides the shard, and the shard decides the owner. Computed
        // with the same function the broker routes with, so the cache is
        // bounded by shard count rather than by distinct keys.
        let shard = self.shard_of(&stream_key, Some(key.as_ref())).await;
        let owner_key: ShardKey = (stream_key.0, stream_key.1, stream_key.2, shard);
        let (client, routed_to_owner) = match self.owners.read().await.get(&owner_key) {
            Some(owner) => (Arc::clone(&owner.client), Some(owner.node_id.clone())),
            None => (self.client().await, None),
        };
        match publish_once_keyed(
            &client,
            tenant_id,
            namespace,
            stream,
            payload,
            Some(key),
            ack,
        )
        .await
        {
            Ok(forwarded_to) => {
                if let Some(owner) = forwarded_to {
                    self.remember_owner(owner_key, owner).await;
                }
                Ok(())
            }
            Err(err) if routed_to_owner.is_some() => Err(self
                .forget_owner(&owner_key, routed_to_owner.unwrap_or_default(), err)
                .await),
            Err(err) => {
                let reconnected = self.reconnect().await;
                match reconnected {
                    Ok(()) => Err(err.context("publish failed; reconnected to another broker")),
                    Err(reconnect_err) => Err(err.context(format!(
                        "publish failed and no other broker answered: {reconnect_err:#}"
                    ))),
                }
            }
        }
    }

    /// Publish, reconnecting *and sending the record again* if the broker in
    /// use has gone.
    ///
    /// **This can produce duplicates.** A publish that failed after the broker
    /// had written the record will leave it in the stream twice, and nothing in
    /// the broker can tell the two apart — only the application holds an
    /// identity that would make deduplication possible. Use it for streams
    /// whose consumers tolerate that, which is what `AtLeastOnce` means.
    pub async fn publish_at_least_once(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        let mut last: Option<anyhow::Error> = None;

        for attempt in 0..self.policy.attempts.max(1) {
            if attempt > 0 {
                let delay = self.policy.delay_before(attempt - 1);
                // Checked before sleeping, not after: sleeping past a deadline
                // and then reporting it wastes exactly the time the deadline
                // exists to save.
                if let Some(budget) = self.policy.deadline
                    && started.elapsed() + delay >= budget
                {
                    break;
                }
                tokio::time::sleep(delay).await;
                if let Err(err) = self.reconnect().await {
                    last = Some(err.context("no broker answered"));
                    continue;
                }
            }
            let client = self.client().await;
            match publish_once(&client, tenant_id, namespace, stream, payload.clone(), ack).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    // No amount of reconnecting changes a forbidden credential
                    // or a stream that does not exist, and burning the whole
                    // backoff schedule only delays the answer the caller needs.
                    if is_terminal(&err) {
                        return Err(
                            err.context("not retried: this cannot succeed on another attempt")
                        );
                    }
                    last = Some(err);
                }
            }
        }

        Err(last
            .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
            .context(format!(
                "gave up after {:?} and at most {} attempts across {} endpoints",
                started.elapsed(),
                self.policy.attempts.max(1),
                self.endpoints.read().await.len()
            )))
    }

    /// A producer whose publishes land once, however many times they are
    /// sent, re-sent across reconnects like [`Self::publish_at_least_once`]
    /// and without the duplicate. See [`crate::IdempotentProducer`].
    pub async fn idempotent_producer(&self) -> Result<IdempotentProducer<'_>> {
        let producer_id = self.client().await.producer_init().await?;
        Ok(IdempotentProducer::for_cluster(self, producer_id))
    }
}

async fn publish_once(
    client: &Client,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payload: Vec<u8>,
    ack: AckMode,
) -> AckOutcome {
    publish_once_keyed(client, tenant_id, namespace, stream, payload, None, ack).await
}

async fn publish_once_keyed(
    client: &Client,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payload: Vec<u8>,
    key: Option<bytes::Bytes>,
    ack: AckMode,
) -> AckOutcome {
    let publisher = client.publisher().await.context("open publisher")?;
    match key {
        Some(key) => {
            publisher
                .publish_keyed_reporting_owner(tenant_id, namespace, stream, key, payload, ack)
                .await
        }
        None => {
            publisher
                .publish_reporting_owner(tenant_id, namespace, stream, payload, ack)
                .await
        }
    }
}
