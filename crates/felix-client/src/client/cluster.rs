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
//!
//! # Discovery adds, it never replaces
//!
//! A broker that supports it is asked which brokers a client may connect to,
//! and the answer is added to the endpoints this client will try. The
//! configured seeds always stay in that list. A client can therefore never end
//! up with fewer ways in than it was given, however wrong or stale the
//! cluster's answer turns out to be -- which is what makes it safe to take the
//! answer at all.
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

/// How many times a subscribe will follow a redirect before giving up.
///
/// A correct cluster needs one hop. More than that means the answer is moving
/// while it is being followed, and a bound is what turns "the cluster has not
/// settled" into an error the caller sees rather than a loop it does not.
const MAX_REDIRECTS: usize = 3;

/// A client that reconnects to another broker when the one it is using fails.
pub struct ClusterClient {
    /// What the application configured. Never removed from `endpoints`: the
    /// cluster's account of itself can be wrong, and these are the addresses
    /// someone chose deliberately.
    seeds: Vec<SocketAddr>,
    /// Everywhere worth trying: the seeds, plus whatever discovery has added.
    endpoints: RwLock<Vec<SocketAddr>>,
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
        let cluster = Self {
            seeds: seeds.to_vec(),
            endpoints: RwLock::new(seeds.to_vec()),
            server_name: server_name.to_string(),
            config,
            policy,
            client: RwLock::new(Arc::new(client)),
        };
        cluster.discover().await;
        Ok(cluster)
    }

    /// Everywhere this client would try, seeds included.
    pub async fn endpoints(&self) -> Vec<SocketAddr> {
        self.endpoints.read().await.clone()
    }

    /// Ask the broker in use which brokers a client may connect to, and add
    /// them to the endpoints this client will try.
    ///
    /// Returns how many the cluster named. Zero is a legitimate answer: a
    /// broker with no cluster behind it, or one whose brokers do not advertise
    /// where clients reach them, has nothing to add. The configured seeds are
    /// kept regardless.
    ///
    /// Errors when the broker predates discovery, rather than reporting zero.
    /// A caller asking this question deserves to know the difference between
    /// "no brokers to name" and "cannot be asked".
    pub async fn refresh_topology(&self) -> Result<usize> {
        let client = self.client().await;
        let reported = client.topology().await?;
        let discovered: Vec<SocketAddr> = reported
            .iter()
            .filter_map(|broker| broker.addr.parse().ok())
            .collect();

        let mut endpoints = self.endpoints.write().await;
        // Rebuilt rather than appended to, so a broker that has left the
        // cluster stops being tried -- while the configured seeds stay whatever
        // the answer was.
        let mut next = discovered.clone();
        for seed in &self.seeds {
            if !next.contains(seed) {
                next.push(*seed);
            }
        }
        *endpoints = next;
        Ok(reported.len())
    }

    /// Refresh if the broker supports it, and carry on if it does not.
    ///
    /// Discovery is an improvement on the seed list, never a precondition for
    /// using one: a cluster that cannot be asked leaves the client exactly as
    /// well off as it was before it asked.
    async fn discover(&self) {
        let client = self.client().await;
        if !client.supports_topology() {
            return;
        }
        if let Err(err) = self.refresh_topology().await {
            tracing::debug!(error = %err, "topology refresh failed; keeping the endpoints in hand");
        }
    }

    /// The client currently in use, for operations this wrapper does not cover.
    ///
    /// Subscriptions are the reason this is exposed: one is bound to the
    /// connection it was created on, so a reconnect cannot carry it across, and
    /// resuming it is the caller's to do with the offsets it has been given.
    pub async fn client(&self) -> Arc<Client> {
        Arc::clone(&*self.client.read().await)
    }

    /// Subscribe, following the cluster to whichever broker owns the shard.
    ///
    /// A broker that does not own it answers `NotLeader` naming the one that
    /// does; this connects there and asks again. The returned [`Client`] must
    /// be kept alive for as long as the subscription: dropping it closes the
    /// connection the events arrive on.
    ///
    /// The client this wrapper holds is **not** replaced. A redirect is about
    /// one shard, not about which broker is generally worth talking to, and
    /// moving every future publish because one stream lives elsewhere would be
    /// a much larger claim than the answer supports.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<(Arc<Client>, crate::Subscription)> {
        let mut client = self.client().await;
        // Every broker this attempt has already asked. A cluster mid-rebalance
        // can name an owner that names another, and two brokers that disagree
        // would otherwise bounce a client between them until its deadline.
        let mut visited: Vec<String> = Vec::new();

        for _ in 0..=MAX_REDIRECTS {
            let error = match client.subscribe(tenant_id, namespace, stream).await {
                Ok(subscription) => return Ok((client, subscription)),
                Err(err) => err,
            };
            let Some(redirect) = error.downcast_ref::<crate::NotLeaderError>().cloned() else {
                return Err(error);
            };
            if visited.iter().any(|seen| seen == &redirect.node_id) {
                return Err(error.context(format!(
                    "redirected back to {}, which has already been asked",
                    redirect.node_id
                )));
            }
            let Some(addr) = redirect.addr.clone() else {
                return Err(error.context(
                    "the owner's client address is not published, so there is nowhere to follow to",
                ));
            };
            let addr: SocketAddr = addr
                .parse()
                .with_context(|| format!("the owner's address {addr:?} is not usable"))?;
            visited.push(redirect.node_id.clone());
            client = Arc::new(
                Client::connect(addr, &self.server_name, self.config.clone())
                    .await
                    .with_context(|| format!("connect to the shard owner at {addr}"))?,
            );
        }

        Err(anyhow::anyhow!(
            "still being redirected after {MAX_REDIRECTS} hops; the cluster has not settled on an owner"
        ))
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
                "gave up after {} attempts across {} endpoints",
                self.policy.attempts.max(1),
                self.endpoints.read().await.len()
            )))
    }

    /// Replace the client with one connected to a seed that answers.
    ///
    /// Held exclusively while it runs, so publishes queue behind it rather than
    /// racing to build several replacements for the same failure.
    async fn reconnect(&self) -> Result<()> {
        let endpoints = self.endpoints.read().await.clone();
        {
            let mut slot = self.client.write().await;
            let replacement =
                Client::connect_any(&endpoints, &self.server_name, self.config.clone()).await?;
            *slot = Arc::new(replacement);
        }
        // After the swap, and outside the write lock: the broker that answered
        // is the one that knows what the cluster looks like now, and a failover
        // is exactly when the answer has changed.
        self.discover().await;
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
