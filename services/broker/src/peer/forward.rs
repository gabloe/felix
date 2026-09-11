//! Forwarding a publish to the broker that owns its shard.
//!
//! # What may be retried, and what may not
//!
//! The rule is one question: *could the owner already have applied this batch?*
//! If it could, a retry is a duplicate rather than a repair, and this broker has
//! no way to tell the two apart.
//!
//! - **Nothing was sent** — the peer was shed, in backoff, or unreachable.
//!   Retryable.
//! - **The owner refused** — `NotLeader`, `StaleRoute`, `Unavailable`,
//!   `Overload`. The refusal *is* the evidence nothing was applied. Retryable,
//!   and `NotLeader` carries where to go instead.
//! - **The answer was lost** — the connection dropped, or the request timed out.
//!   The batch may be on the owner's disk. **Never retried here**, for a stream
//!   of any delivery guarantee.
//!
//! That last rule is stricter than `AtLeastOnce` requires. It is deliberate: a
//! duplicate produced inside the broker is invisible to the client, which holds
//! the `request_id` and is the only layer that could deduplicate. A client that
//! wants a retry can reissue and know it did.
//!
//! Retries are bounded by [`MAX_ATTEMPTS`] so a shard being reassigned converges
//! or fails explicitly, rather than chasing `NotLeader` around a cluster.
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use felix_wire::internal::{AckMode, ForwardPublish, InternalMessage, ShardRef};

use super::metrics;
use super::pool::{PeerError, PeerPool};

/// Total attempts for one publish, across every owner it is redirected to.
///
/// Three covers the case this exists for — a reassignment observed mid-publish,
/// which costs one redirect — with one spare. Beyond that the routing view is
/// churning faster than a publish can complete, and failing explicitly beats
/// chasing it.
const MAX_ATTEMPTS: u32 = 3;

/// Where a publish is being sent, as this broker currently resolves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardTarget {
    pub node_id: String,
    pub advertise_addr: SocketAddr,
    pub generation: u64,
}

/// Which shard the batch belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
}

/// Why a forward did not succeed.
#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    /// The owner refused, or could not be reached, and retrying is allowed but
    /// exhausted. Nothing was applied.
    #[error("could not forward to the owner of {stream}: {detail}")]
    Refused { stream: String, detail: String },
    /// The batch was sent and its answer never arrived. It may or may not have
    /// been applied, and this broker cannot tell.
    #[error("forwarded publish to {node_id} was not acknowledged: {detail}")]
    Indeterminate { node_id: String, detail: String },
}

/// The one thing forwarding asks of the connection pool.
///
/// A trait rather than the pool itself so the retry rules above — which decide
/// whether a batch may be sent a second time — can be tested against an owner
/// that answers on command, including with the answers a healthy cluster
/// almost never produces.
pub trait PeerRequester {
    fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<Output = std::result::Result<InternalMessage, PeerError>> + Send;
}

impl<T: PeerRequester> PeerRequester for std::sync::Arc<T> {
    fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<Output = std::result::Result<InternalMessage, PeerError>> + Send
    {
        T::request(self, node_id, addr, message)
    }
}

impl PeerRequester for PeerPool {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        PeerPool::request(self, node_id, addr, message).await
    }
}

/// Forward one batch and wait for the owner's answer.
///
/// Returns the log offsets the owner assigned, when the stream has a log.
pub async fn forward_publish(
    pool: &impl PeerRequester,
    target: &ForwardTarget,
    key: &ForwardKey,
    ack: AckMode,
    payloads: Vec<Bytes>,
) -> Result<Option<(u64, u64)>, ForwardError> {
    let mut target = target.clone();
    let mut last = String::new();

    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            metrics::record_forward_retry();
        }
        let request = InternalMessage::ForwardPublish(ForwardPublish {
            // The pool assigns the real id; it owns the connection this lands on.
            correlation_id: 0,
            shard: ShardRef {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
                generation: target.generation,
            },
            ack,
            payloads: payloads.clone(),
        });

        match PeerRequester::request(pool, &target.node_id, target.advertise_addr, request).await {
            Ok(InternalMessage::ForwardPublishOk(ok)) => {
                metrics::record_forward(metrics::OUTCOME_OK);
                return Ok(Some((ok.first_offset, ok.last_offset)));
            }
            Ok(InternalMessage::NotLeader(moved)) => {
                // The owner refused, so nothing was applied, and it named where
                // to go. Following that beats waiting for this broker's watch to
                // catch up — but only within the attempt budget, so a shard
                // being reassigned repeatedly fails rather than loops.
                last = format!(
                    "shard moved to {} at generation {}",
                    moved.node_id, moved.generation
                );
                let Ok(advertise_addr) = moved.advertise_addr.parse() else {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        detail: format!(
                            "owner {} advertised an unusable address {}",
                            moved.node_id, moved.advertise_addr
                        ),
                    });
                };
                // A redirect that does not advance the generation would send the
                // batch straight back where it came from.
                if moved.generation <= target.generation {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        detail: format!(
                            "owner {} redirected to generation {}, not ahead of {}",
                            moved.node_id, moved.generation, target.generation
                        ),
                    });
                }
                target = ForwardTarget {
                    node_id: moved.node_id,
                    advertise_addr,
                    generation: moved.generation,
                };
            }
            Ok(InternalMessage::ForwardPublishError(err)) => {
                last = format!("{:?}: {}", err.code, err.detail);
                if !err.code.is_retryable() {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        detail: last,
                    });
                }
                // `StorageFailed` is not retryable and lands above; every code
                // that reaches here refused before writing.
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Ok(other) => {
                metrics::record_forward(metrics::OUTCOME_REFUSED);
                return Err(ForwardError::Refused {
                    stream: key.stream.clone(),
                    detail: format!("owner answered with an unexpected {:?}", other.kind()),
                });
            }
            Err(err) if err.is_retryable() => {
                last = err.to_string();
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(err @ (PeerError::Disconnected { .. } | PeerError::Timeout { .. })) => {
                // The batch is out there. Retrying would duplicate it, and this
                // broker cannot tell whether it landed.
                metrics::record_forward(metrics::OUTCOME_INDETERMINATE);
                return Err(ForwardError::Indeterminate {
                    node_id: target.node_id,
                    detail: err.to_string(),
                });
            }
            Err(err) => {
                metrics::record_forward(metrics::OUTCOME_REFUSED);
                return Err(ForwardError::Refused {
                    stream: key.stream.clone(),
                    detail: err.to_string(),
                });
            }
        }
    }

    metrics::record_forward(metrics::OUTCOME_EXHAUSTED);
    Err(ForwardError::Refused {
        stream: key.stream.clone(),
        detail: format!("gave up after {MAX_ATTEMPTS} attempts: {last}"),
    })
}

/// A short pause between attempts, so a shard mid-reassignment is not retried
/// before anything can have changed. Deliberately small: the client is waiting,
/// and the attempt budget is the real bound.
fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(5 << attempt.min(4))
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
