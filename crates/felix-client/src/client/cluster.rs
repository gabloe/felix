//! A client that outlives the broker it is talking to.
//!
//! [`Client`] holds connection pools built when it was created, so a broker
//! going away takes that client with it. A cluster survives losing a broker;
//! until now an application could not, and had to notice the failure and
//! rebuild everything itself.
//!
//! This owns a [`Client`] and replaces it when the one it has stops working,
//! from the same seed list it was created with.
//!
//! # Reconnecting is not resending
//!
//! The two are separated on purpose, and the names say which is which.
//!
//! [`ClusterClient::publish`] reconnects and reports the failed record to the
//! caller. Nothing is sent twice, so nothing can arrive twice — but the record
//! that was in flight when the broker died is the caller's problem.
//!
//! [`ClusterClient::publish_at_least_once`] reconnects *and sends the record
//! again*. A publish that failed after the broker had already written it will
//! then exist twice, and the broker cannot tell the difference: only the
//! application holds the identity that would make deduplication possible. The
//! name is the warning, and it is the guarantee the method actually provides.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::AckMode;
use tokio::sync::RwLock;

use super::client::Client;
use crate::config::ClientConfig;

/// How long to wait between reconnection attempts, and how many to make.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Attempts to reach *some* broker before giving up. Each attempt tries
    /// every seed.
    pub attempts: usize,
    /// Wait before the first retry. Doubles up to `max_backoff`.
    pub backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            // Enough to outlast a failover on a healthy cluster — the control
            // plane has to notice the leader is gone, which is bounded by its
            // expiry timeout — without waiting out a cluster that is simply
            // down.
            attempts: 5,
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
        }
    }
}

/// A client that reconnects to another broker when the one it is using fails.
pub struct ClusterClient {
    seeds: Vec<SocketAddr>,
    server_name: String,
    config: ClientConfig,
    policy: ReconnectPolicy,
    /// Replaced wholesale on reconnect. `RwLock` rather than a swap because a
    /// reconnect must exclude the publishes that would otherwise keep using the
    /// client being replaced.
    client: RwLock<Arc<Client>>,
}

impl ClusterClient {
    /// Connect to whichever seed answers, and remember the rest.
    pub async fn connect(
        seeds: &[SocketAddr],
        server_name: &str,
        config: ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_policy(seeds, server_name, config, ReconnectPolicy::default()).await
    }

    pub async fn connect_with_policy(
        seeds: &[SocketAddr],
        server_name: &str,
        config: ClientConfig,
        policy: ReconnectPolicy,
    ) -> Result<Self> {
        let client = Client::connect_any(seeds, server_name, config.clone()).await?;
        Ok(Self {
            seeds: seeds.to_vec(),
            server_name: server_name.to_string(),
            config,
            policy,
            client: RwLock::new(Arc::new(client)),
        })
    }

    /// The client currently in use, for operations this wrapper does not cover.
    ///
    /// Subscriptions are the reason this is exposed: one is bound to the
    /// connection it was created on, so a reconnect cannot carry it across, and
    /// resuming it is the caller's to do with the offsets it has been given.
    pub async fn client(&self) -> Arc<Client> {
        Arc::clone(&*self.client.read().await)
    }

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
        let client = self.client().await;
        match publish_once(&client, tenant_id, namespace, stream, payload, ack).await {
            Ok(()) => Ok(()),
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
        let mut backoff = self.policy.backoff;
        let mut last: Option<anyhow::Error> = None;

        for attempt in 0..self.policy.attempts.max(1) {
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(self.policy.max_backoff);
                if let Err(err) = self.reconnect().await {
                    last = Some(err.context("no broker answered"));
                    continue;
                }
            }
            let client = self.client().await;
            match publish_once(&client, tenant_id, namespace, stream, payload.clone(), ack).await {
                Ok(()) => return Ok(()),
                Err(err) => last = Some(err),
            }
        }

        Err(last
            .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
            .context(format!(
                "gave up after {} attempts across {} seed endpoints",
                self.policy.attempts.max(1),
                self.seeds.len()
            )))
    }

    /// Replace the client with one connected to a seed that answers.
    ///
    /// Held exclusively while it runs, so publishes queue behind it rather than
    /// racing to build several replacements for the same failure.
    async fn reconnect(&self) -> Result<()> {
        let mut slot = self.client.write().await;
        let replacement =
            Client::connect_any(&self.seeds, &self.server_name, self.config.clone()).await?;
        *slot = Arc::new(replacement);
        Ok(())
    }
}

async fn publish_once(
    client: &Client,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payload: Vec<u8>,
    ack: AckMode,
) -> Result<()> {
    let publisher = client.publisher().await.context("open publisher")?;
    publisher
        .publish(tenant_id, namespace, stream, payload, ack)
        .await
}
