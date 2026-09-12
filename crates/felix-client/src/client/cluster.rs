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

/// How long to wait between reconnection attempts, how many to make, and how
/// long the whole thing may take.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Attempts to reach *some* broker before giving up. Each attempt tries
    /// every endpoint.
    pub attempts: usize,
    /// The first retry waits somewhere in `[0, backoff]`. The ceiling doubles
    /// each attempt up to `max_backoff`.
    pub backoff: Duration,
    pub max_backoff: Duration,
    /// A ceiling on the whole operation, across every attempt and every sleep.
    ///
    /// Attempt counts alone do not bound time: five attempts against a broker
    /// that takes its full publish timeout to answer is minutes, which is not a
    /// number anybody chose.
    ///
    /// **`None` by default, and deliberately.** A deadline shorter than one
    /// attempt's own timeout prevents any retry at all — the first attempt
    /// spends the whole budget and the loop exits having tried once. The
    /// client's publish timeout is already tens of seconds, so any useful
    /// default here would have to be derived from that rather than picked, and
    /// picking one silently turns a client that recovers from a failover into
    /// one that does not.
    ///
    /// A caller that knows its own latency budget should set it.
    pub deadline: Option<Duration>,
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
            deadline: None,
        }
    }
}

impl ReconnectPolicy {
    /// How long to wait before attempt `attempt` (0-based), jittered.
    ///
    /// **Full jitter: uniform over `[0, ceiling]`, not the ceiling itself.**
    /// Every client of a cluster notices a failover at the same moment, and an
    /// unjittered backoff has all of them retry in step — arriving together at
    /// whichever broker was just promoted, which is the moment it can least
    /// afford a thundering herd. The broker's own peer pool jitters its redials
    /// for exactly this reason.
    fn delay_before(&self, attempt: usize) -> Duration {
        let ceiling = self
            .backoff
            .saturating_mul(1u32 << attempt.min(16) as u32)
            .min(self.max_backoff);
        ceiling.mul_f64(jitter_fraction())
    }
}

/// A uniform fraction in `[0, 1)`, without taking an RNG dependency.
///
/// The same approach the broker's reconnect backoff uses. Its only job is to
/// decorrelate clients that all woke up together, so it needs to be unbiased
/// rather than unpredictable.
fn jitter_fraction() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000_000) / 1_000_000.0
}

/// Whether an error is worth another attempt.
///
/// **Unknown errors are retried.** The client protocol carries an error as a
/// string with no code, so this is matching on prose, and prose changes. A
/// misclassified retryable error costs one wasted attempt; a misclassified
/// terminal error costs the operation. Defaulting to "retry" puts the cheaper
/// mistake on the likely side.
///
/// Terminal means *no amount of waiting or reconnecting changes the answer*:
/// the credential does not permit this, the stream does not exist, the offset
/// is gone. Retrying those is not merely wasteful, it delays the error the
/// caller needs to see behind the full backoff schedule.
pub(crate) fn is_terminal(error: &anyhow::Error) -> bool {
    // A cursor error is terminal by construction: the offset asked for is not
    // available, and it will not become available by asking again.
    if error
        .downcast_ref::<crate::SubscribeCursorError>()
        .is_some()
    {
        return true;
    }
    let text = format!("{error:#}").to_lowercase();
    const TERMINAL: [&str; 6] = [
        "forbidden",
        "auth rejected",
        "not authenticated",
        "unknown tenant",
        "unknown namespace",
        "stream not found",
    ];
    TERMINAL.iter().any(|marker| text.contains(marker))
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
                Ok(()) => return Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ReconnectPolicy {
        ReconnectPolicy {
            attempts: 5,
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
            deadline: None,
        }
    }

    /// **Every delay is inside its ceiling, and the ceiling doubles.** Full
    /// jitter means the delay is somewhere in `[0, ceiling]`, so the property
    /// worth pinning is the bound rather than the value.
    #[test]
    fn backoff_stays_inside_a_doubling_ceiling() {
        let policy = policy();
        for attempt in 0..6 {
            let ceiling = policy
                .backoff
                .saturating_mul(1u32 << attempt)
                .min(policy.max_backoff);
            for _ in 0..50 {
                let delay = policy.delay_before(attempt);
                assert!(
                    delay <= ceiling,
                    "attempt {attempt}: {delay:?} exceeds its ceiling {ceiling:?}",
                );
            }
        }
    }

    #[test]
    fn backoff_is_capped_by_max_backoff() {
        let policy = policy();
        for attempt in 0..20 {
            assert!(policy.delay_before(attempt) <= policy.max_backoff);
        }
    }

    /// **The delay actually varies.** A "jitter" that returned the same number
    /// every time would satisfy the bound above and still send every client of
    /// a cluster at a freshly promoted broker in step.
    #[test]
    fn backoff_is_jittered_rather_than_fixed() {
        let policy = ReconnectPolicy {
            backoff: Duration::from_secs(1),
            ..policy()
        };
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            seen.insert(policy.delay_before(0).as_micros());
            std::thread::sleep(Duration::from_micros(50));
        }
        assert!(
            seen.len() > 5,
            "the backoff produced {} distinct delays; it is not jittered",
            seen.len()
        );
    }

    #[test]
    fn a_forbidden_error_is_terminal() {
        assert!(is_terminal(&anyhow::anyhow!("forbidden")));
        assert!(is_terminal(
            &anyhow::anyhow!("publish failed").context("forbidden")
        ));
    }

    #[test]
    fn a_missing_stream_is_terminal() {
        assert!(is_terminal(&anyhow::anyhow!(
            "stream not found: tenant=t1 namespace=ns stream=orders"
        )));
    }

    /// A cursor error is terminal by construction, and typed, so it does not
    /// depend on matching prose.
    #[test]
    fn a_cursor_error_is_terminal() {
        let err: anyhow::Error = crate::SubscribeCursorError {
            reason: felix_wire::CursorErrorReason::TooOld,
            requested: 5,
            available: 100,
        }
        .into();
        assert!(is_terminal(&err));
    }

    /// **The failures a failover produces are retried.** These are the whole
    /// point of the policy, and classifying one of them as terminal would turn
    /// a recoverable blip into a lost publish.
    #[test]
    fn transient_failures_are_retried() {
        for message in [
            "connection lost",
            "publish commit timeout",
            "the batch is durable here but did not reach a majority within 5s",
            "shard leadership moved before the batch could reach a quorum",
            "no peer transport: this broker cannot forward to broker-2",
            "stream orders cannot be subscribed to right now: owner unavailable",
        ] {
            assert!(
                !is_terminal(&anyhow::anyhow!(message.to_string())),
                "{message:?} should be retried",
            );
        }
    }

    /// An error nobody has classified is retried, because a wasted attempt is
    /// cheaper than a lost operation.
    #[test]
    fn an_unrecognised_error_is_retried() {
        assert!(!is_terminal(&anyhow::anyhow!("something nobody foresaw")));
    }
}
