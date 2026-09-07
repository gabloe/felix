//! Broker-side cluster membership.
//!
//! The broker claims a stable identity on boot, reports health on an interval,
//! and says goodbye on the way out. The control plane turns that into the
//! catalog placement reads.
//!
//! The distinction this module exists to preserve: a broker that deregisters is
//! `left`, and one that simply stops is found `down` by heartbeat expiry. Both
//! remove it from placement, but only the first is intentional, so only the
//! first should be silent in the logs.
//!
//! Registration happens *after* the broker can serve. Registering earlier
//! advertises a node that placement may immediately use and that cannot yet
//! answer.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::config::MembershipConfig;
use crate::membership_metrics as mm;

/// Ceiling on heartbeat retry backoff.
///
/// Bounded so a broker that was unreachable for an hour resumes reporting
/// within one ceiling of the control plane coming back, rather than after a
/// doubling interval measured in hours.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Fraction of a delay that jitter may add.
///
/// Restarting a cluster puts every broker on the same schedule; without jitter
/// they heartbeat in lockstep and the control plane sees the whole fleet at
/// once, every interval.
const JITTER_FRACTION: f64 = 0.2;

#[derive(Debug, Serialize)]
struct RegistrationRequest<'a> {
    node_id: &'a str,
    advertise_addr: &'a str,
    region: &'a str,
}

#[derive(Debug, Deserialize)]
struct RegistrationResponse {
    node: NodeView,
    heartbeat_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct NodeView {
    status: NodeStatusView,
}

#[derive(Debug, Deserialize)]
struct NodeStatusView {
    incarnation: u64,
    lifecycle: String,
}

#[derive(Debug, Serialize)]
struct HeartbeatRequest {
    incarnation: u64,
}

#[derive(Debug, Deserialize)]
struct HeartbeatResponse {
    lifecycle: String,
    heartbeat_interval_ms: u64,
}

/// A registered identity, and what the control plane told us about it.
#[derive(Debug, Clone)]
pub struct Registration {
    pub node_id: String,
    /// This process's incarnation. Sent with every heartbeat so one delayed
    /// past a restart is rejected instead of counted for its successor.
    pub incarnation: u64,
    pub heartbeat_interval_ms: u64,
}

/// Why a membership call did not succeed.
///
/// The split decides whether to retry, and it is also what the metrics report:
/// a control plane that is still starting will accept the same request in a
/// moment, while one that refused the identity will refuse it forever.
/// Collapsing the two makes a misconfigured broker look like a flaky network.
#[derive(Debug)]
pub enum MembershipError {
    /// The control plane answered and said no. Terminal.
    Rejected(String),
    /// Nothing answered, or it failed internally.
    Unavailable(anyhow::Error),
}

impl MembershipError {
    /// Metric label for this failure. Bounded to two values.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Rejected(_) => mm::KIND_REJECTED,
            Self::Unavailable(_) => mm::KIND_UNAVAILABLE,
        }
    }

    /// Classify an HTTP response. A 4xx is the server refusing; anything else
    /// may simply be a control plane still coming up.
    fn from_status(status: reqwest::StatusCode, message: String) -> Self {
        if status.is_client_error() {
            Self::Rejected(message)
        } else {
            Self::Unavailable(anyhow!(message))
        }
    }
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "{message}"),
            Self::Unavailable(err) => write!(f, "{err}"),
        }
    }
}

/// Claim this broker's identity.
///
/// Fails loudly. A broker that cannot register is not a cluster member, and
/// carrying on as if it were means publishing to a node no one will route to.
pub async fn register(
    client: &reqwest::Client,
    base_url: &str,
    config: &MembershipConfig,
) -> std::result::Result<Registration, MembershipError> {
    let response = client
        .post(format!("{}/v1/nodes", base_url.trim_end_matches('/')))
        .json(&RegistrationRequest {
            node_id: &config.node_id,
            advertise_addr: &config.advertise_addr,
            region: &config.region,
        })
        .send()
        .await
        .with_context(|| format!("register node {} with {base_url}", config.node_id))
        .map_err(MembershipError::Unavailable)?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(MembershipError::from_status(
            status,
            format!(
                "control plane rejected registration of {} ({status}): {body}",
                config.node_id
            ),
        ));
    }

    let registered: RegistrationResponse = response
        .json()
        .await
        .context("decode node registration response")
        .map_err(MembershipError::Unavailable)?;

    mm::record_registration("registered");
    mm::record_membership_live(true);
    tracing::info!(
        node_id = %config.node_id,
        advertise_addr = %config.advertise_addr,
        incarnation = registered.node.status.incarnation,
        lifecycle = %registered.node.status.lifecycle,
        "registered with the control plane",
    );

    Ok(Registration {
        node_id: config.node_id.clone(),
        incarnation: registered.node.status.incarnation,
        heartbeat_interval_ms: registered.heartbeat_interval_ms,
    })
}

/// Report health until `shutdown` fires.
///
/// A failed heartbeat is retried with bounded exponential backoff. It is never
/// fatal: the control plane being briefly unreachable must not take down a
/// broker that is otherwise serving fine. If it stays unreachable past the
/// expiry timeout the control plane marks this node down on its own, which is
/// the correct outcome and needs no help from here.
pub async fn run_heartbeat(
    client: reqwest::Client,
    base_url: String,
    registration: Registration,
    shutdown: CancellationToken,
    consecutive_failures: Arc<AtomicU64>,
) {
    let base_url = base_url.trim_end_matches('/').to_string();
    let url = format!("{base_url}/v1/nodes/{}/heartbeat", registration.node_id);
    let mut interval = Duration::from_millis(registration.heartbeat_interval_ms.max(1));
    let mut last_success = std::time::Instant::now();

    loop {
        let failures = consecutive_failures.load(Ordering::Acquire);
        let delay = if failures == 0 {
            interval
        } else {
            backoff(interval, failures)
        };

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(jittered(delay)) => {}
        }

        match send_heartbeat(&client, &url, registration.incarnation).await {
            Ok(response) => {
                consecutive_failures.store(0, Ordering::Release);
                last_success = std::time::Instant::now();
                mm::record_heartbeat_success();
                // The control plane owns the cadence, so a change to it takes
                // effect without touching broker configuration.
                interval = Duration::from_millis(response.heartbeat_interval_ms.max(1));

                // Being told we are down means expiry already removed this node
                // from placement. Registering again is the broker's job, not
                // this loop's, so make the state visible and keep reporting.
                let placeable = response.lifecycle == "live" || response.lifecycle == "draining";
                mm::record_membership_live(placeable);
                if !placeable {
                    tracing::warn!(
                        node_id = %registration.node_id,
                        lifecycle = %response.lifecycle,
                        "the control plane no longer considers this broker live",
                    );
                }
            }
            Err(err) => {
                let failures = consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                mm::record_heartbeat_failure(err.kind());
                // Published on failure too: this is the number that keeps rising
                // while the control plane is unreachable, and the only warning a
                // broker gets that it is about to be declared down.
                mm::record_heartbeat_age(last_success.elapsed());
                tracing::warn!(
                    node_id = %registration.node_id,
                    consecutive_failures = failures,
                    error = %err,
                    "heartbeat failed; retrying with backoff",
                );
            }
        }
    }
}

async fn send_heartbeat(
    client: &reqwest::Client,
    url: &str,
    incarnation: u64,
) -> std::result::Result<HeartbeatResponse, MembershipError> {
    let response = client
        .post(url)
        .json(&HeartbeatRequest { incarnation })
        .send()
        .await
        .context("send heartbeat")
        .map_err(MembershipError::Unavailable)?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(MembershipError::from_status(
            status,
            format!("heartbeat rejected ({status}): {body}"),
        ));
    }
    response
        .json()
        .await
        .context("decode heartbeat response")
        .map_err(MembershipError::Unavailable)
}

/// Stop receiving new placement, without stopping service.
pub async fn drain(client: &reqwest::Client, base_url: &str, node_id: &str) -> Result<()> {
    post_lifecycle(client, base_url, node_id, "drain").await
}

/// Leave the cluster on purpose, so this is not mistaken for a crash.
pub async fn deregister(client: &reqwest::Client, base_url: &str, node_id: &str) -> Result<()> {
    post_lifecycle(client, base_url, node_id, "deregister").await
}

async fn post_lifecycle(
    client: &reqwest::Client,
    base_url: &str,
    node_id: &str,
    action: &str,
) -> Result<()> {
    let response = client
        .post(format!(
            "{}/v1/nodes/{node_id}/{action}",
            base_url.trim_end_matches('/')
        ))
        .send()
        .await
        .with_context(|| format!("{action} node {node_id}"))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("{action} rejected ({status}): {body}"));
    }
    Ok(())
}

/// Exponential backoff, capped.
///
/// `failures` is the count so far, so the first retry waits one interval rather
/// than doubling immediately.
fn backoff(interval: Duration, failures: u64) -> Duration {
    let shift = failures.saturating_sub(1).min(16) as u32;
    interval
        .saturating_mul(2u32.saturating_pow(shift))
        .min(MAX_RETRY_BACKOFF)
}

/// Spread a delay so a restarted fleet does not report in lockstep.
fn jittered(delay: Duration) -> Duration {
    let spread = delay.as_secs_f64() * JITTER_FRACTION;
    if spread <= 0.0 {
        return delay;
    }
    // Cheap and adequate: this only needs to decorrelate brokers, not resist
    // prediction, so it avoids pulling in an RNG on the shutdown path.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let fraction = f64::from(nanos) / f64::from(u32::MAX);
    delay + Duration::from_secs_f64(spread * fraction)
}

/// Everything the broker needs to keep its membership current.
pub struct MembershipTask {
    /// Ends when the broker stops.
    pub handle: tokio::task::JoinHandle<()>,
    /// Cancelled when registration was refused outright, so the process can
    /// exit with the reason rather than run on as a non-member.
    pub fatal: CancellationToken,
    /// Consecutive heartbeat failures, exposed so shutdown and metrics can see
    /// whether membership is currently healthy.
    pub consecutive_failures: Arc<AtomicU64>,
}

/// Register once the broker can serve, then report health until shutdown.
///
/// `serving` gates registration: advertising a node before it can answer means
/// placement may route to it and get nothing. A registration refused with a 4xx
/// cancels `fatal` instead of retrying, because a wrong identity stays wrong.
pub fn spawn(
    client: reqwest::Client,
    base_url: String,
    config: MembershipConfig,
    serving: CancellationToken,
    shutdown: CancellationToken,
) -> MembershipTask {
    let fatal = CancellationToken::new();
    let consecutive_failures = Arc::new(AtomicU64::new(0));

    let handle = tokio::spawn({
        let fatal = fatal.clone();
        let consecutive_failures = Arc::clone(&consecutive_failures);
        async move {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = serving.cancelled() => {}
            }

            let mut attempt = 0u64;
            let registration = loop {
                match register(&client, &base_url, &config).await {
                    Ok(registration) => break registration,
                    Err(MembershipError::Rejected(message)) => {
                        mm::record_registration(mm::KIND_REJECTED);
                        mm::record_membership_live(false);
                        tracing::error!(
                            node_id = %config.node_id,
                            error = %message,
                            "the control plane refused this identity; the broker is not a cluster member",
                        );
                        fatal.cancel();
                        return;
                    }
                    Err(MembershipError::Unavailable(err)) => {
                        attempt += 1;
                        mm::record_registration(mm::KIND_UNAVAILABLE);
                        tracing::warn!(
                            node_id = %config.node_id,
                            attempt,
                            error = %err,
                            "could not reach the control plane to register; retrying",
                        );
                        let delay = jittered(backoff(Duration::from_millis(500), attempt));
                        tokio::select! {
                            _ = shutdown.cancelled() => return,
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                }
            };

            run_heartbeat(
                client,
                base_url,
                registration,
                shutdown,
                consecutive_failures,
            )
            .await;
        }
    });

    MembershipTask {
        handle,
        fatal,
        consecutive_failures,
    }
}

/// Leave the cluster on the way out.
///
/// Drain first so nothing new is placed here while in-flight work finishes,
/// then deregister so this reads as intentional rather than as a crash.
/// Neither failure is worth aborting a shutdown over: if the control plane
/// cannot be told, heartbeat expiry reaches the same conclusion a timeout later.
pub async fn shutdown_membership(client: &reqwest::Client, base_url: &str, node_id: &str) {
    if let Err(err) = drain(client, base_url, node_id).await {
        tracing::warn!(node_id, error = %err, "could not mark this broker draining");
    }
    if let Err(err) = deregister(client, base_url, node_id).await {
        tracing::warn!(
            node_id,
            error = %err,
            "could not deregister; the control plane will expire this node instead",
        );
    } else {
        tracing::info!(node_id, "deregistered from the control plane");
    }
}

#[cfg(test)]
#[path = "membership_tests.rs"]
mod tests;
