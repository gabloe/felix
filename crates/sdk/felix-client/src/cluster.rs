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

mod cache_watch;
mod publish;
mod retry;
mod routing;
mod sharded;
mod subscribe;

pub use retry::ReconnectPolicy;
pub use sharded::{
    ShardEvent, ShardOffsets, ShardedCacheWatch, ShardedCacheWatchItem, ShardedGroup,
    ShardedGroupRecord, ShardedSubscription,
};

pub(crate) use retry::is_terminal;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::client::Client;
use crate::config::ClientConfig;

/// How many times a subscribe will follow a redirect before giving up.
///
/// A correct cluster needs one hop. More than that means the answer is moving
/// while it is being followed, and a bound is what turns "the cluster has not
/// settled" into an error the caller sees rather than a loop it does not.
const MAX_REDIRECTS: usize = 3;

/// A client that reconnects to another broker when the one it is using fails.
///
/// See `docs/multi-node-client.md` for what to configure and what this does on
/// its own. The short version:
///
/// ```rust,no_run
/// use std::time::Duration;
/// use felix_client::{ClientConfig, ClusterClient, ReconnectPolicy};
/// use felix_wire::AckMode;
///
/// # async fn example(quinn: quinn::ClientConfig) -> anyhow::Result<()> {
/// let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
/// config.auth_tenant_id = Some("acme".to_string());
/// config.auth_token = Some(std::env::var("FELIX_TOKEN")?);
/// // For a long-running client, set `config.token_provider` so reconnects
/// // get a fresh token.
///
/// // One address is enough; the rest are discovered. Configure more anyway,
/// // because discovery needs *some* broker to answer first.
/// let seeds = ["10.0.0.4:5000".parse()?, "10.0.0.5:5000".parse()?];
///
/// let client = ClusterClient::connect_with_policy(
///     &seeds,
///     "broker.internal",
///     config,
///     ReconnectPolicy {
///         attempts: 5,
///         backoff: Duration::from_millis(200),
///         max_backoff: Duration::from_secs(2),
///         deadline: Some(Duration::from_secs(10)),
///     },
/// )
/// .await?;
///
/// client
///     .publish_at_least_once(
///         "acme",
///         "default",
///         "orders",
///         b"payload".to_vec(),
///         AckMode::PerMessage,
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
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
    /// Where a shard's publishes should go, learned from the acks of the ones
    /// that were forwarded.
    ///
    /// A forward is correct but costs a decrypt at the entry broker, a
    /// re-encrypt to the owner and a decrypt there -- roughly half the
    /// throughput per core (#536). The owner rides back on the ack, so the
    /// next batch for that shard can skip the hop.
    ///
    /// Keyed by shard, not by stream: a multi-shard stream has an owner per
    /// shard, and one entry for the whole stream would send every key to
    /// shard 0's owner. An unkeyed publish resolves to shard 0, a keyed one to
    /// `felix_wire::routing::shard_for` -- the function the broker routes
    /// with, shared so the two cannot drift.
    owners: RwLock<HashMap<ShardKey, Owner>>,
    /// How many shards each stream was placed with, as the cluster last said.
    ///
    /// Asked once per stream rather than per publish. A stale width is not a
    /// correctness problem: the shard number is only a cache key for an owner
    /// learned from an ack, so a client that computes a different number than
    /// the broker still routes to the right broker -- it just keys the entry
    /// differently. What the width buys is a cache bounded by shard count
    /// instead of by the number of distinct routing keys.
    shards: RwLock<HashMap<StreamKey, u32>>,
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

    /// [`ClusterClient::connect`] with an explicit reconnection policy.
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
            owners: RwLock::new(HashMap::new()),
            shards: RwLock::new(HashMap::new()),
        };
        cluster.discover().await;
        Ok(cluster)
    }

    /// Everywhere this client would try, seeds included.
    pub async fn endpoints(&self) -> Vec<SocketAddr> {
        self.endpoints.read().await.clone()
    }

    /// The client currently in use, for operations this wrapper does not cover.
    ///
    /// Subscriptions are the reason this is exposed: one is bound to the
    /// connection it was created on, so a reconnect cannot carry it across, and
    /// resuming it is the caller's to do with the offsets it has been given.
    pub async fn client(&self) -> Arc<Client> {
        Arc::clone(&*self.client.read().await)
    }

    pub(crate) fn policy(&self) -> &ReconnectPolicy {
        &self.policy
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

    /// A client to one broker, with this cluster client's name and config.
    pub(crate) async fn connect_to(&self, addr: SocketAddr) -> Result<Client> {
        Client::connect(addr, &self.server_name, self.config.clone()).await
    }

    /// Replace the client with one connected to a seed that answers.
    ///
    /// Held exclusively while it runs, so publishes queue behind it rather than
    /// racing to build several replacements for the same failure.
    pub(crate) async fn reconnect(&self) -> Result<()> {
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
}

/// A stream.
type StreamKey = (String, String, String);

/// One shard of one stream, as the owner cache keys it.
type ShardKey = (String, String, String, u32);

/// The broker that owns one shard of a stream, and a client connected to it.
struct Owner {
    node_id: String,
    /// The ownership epoch this was true for. A lower one is an older answer
    /// arriving late, and must not overwrite a newer one -- two brokers
    /// mid-rebalance would otherwise take turns replacing each other.
    generation: u64,
    client: Arc<Client>,
}

#[cfg(test)]
mod tests;
