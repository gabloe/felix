//! The owner's side: applying a publish another broker forwarded.
//!
//! Ownership is re-checked here, against this broker's own view, and the
//! requester's generation has to match it exactly. That check is the whole
//! safety property: the requester resolved against a snapshot that may already
//! be wrong, and a broker that trusted it would write a shard it no longer
//! holds.
//!
//! A mismatch in either direction is answered, never guessed at:
//!
//! - **The requester is behind** — this broker no longer owns the shard, or owns
//!   it at a newer generation. `NotLeader`, naming the current owner.
//! - **The requester is ahead** — this broker's watch has not caught up.
//!   `StaleRoute`. It must not accept: it may already have lost the shard.
use std::sync::Arc;

use async_trait::async_trait;
use felix_broker::Broker;
use felix_router::{Resolution, ShardRouter};
use felix_wire::internal::{
    ErrorCode, ForwardPublish, ForwardPublishError, ForwardPublishOk, InternalMessage, NotLeader,
};

use super::metrics;
use super::server::PeerRequestHandler;
use crate::shard_routing::{Dispatch, IngressRouter};
use crate::shard_watch::ShardKey;

/// Applies forwarded publishes against the local broker.
pub struct ForwardingHandler {
    broker: Arc<Broker>,
    ingress: Arc<IngressRouter>,
    router: Arc<ShardRouter>,
    /// This broker's own internal address, so a `NotLeader` that names *this*
    /// node at a newer generation is one the requester can act on.
    advertise_addr: String,
    /// How far a majority of each shard's replica set has got.
    ///
    /// A forwarded publish is still a publish to this stream, so it owes the
    /// client the guarantee the stream asks for. Acknowledging on local
    /// durability alone made `Quorum` depend on which broker the client
    /// happened to reach — honoured when it talked to the leader, silently
    /// downgraded to `Leader` through any other broker, which is where a
    /// failover then lost the record.
    marks: Option<Arc<crate::replication::quorum::QuorumMarks>>,
    quorum_timeout: std::time::Duration,
}

impl ForwardingHandler {
    pub fn new(
        broker: Arc<Broker>,
        ingress: Arc<IngressRouter>,
        router: Arc<ShardRouter>,
        advertise_addr: String,
        marks: Option<Arc<crate::replication::quorum::QuorumMarks>>,
        quorum_timeout: std::time::Duration,
    ) -> Self {
        Self {
            broker,
            ingress,
            router,
            advertise_addr,
            marks,
            quorum_timeout,
        }
    }

    pub(super) async fn apply(&self, publish: ForwardPublish) -> InternalMessage {
        let correlation_id = publish.correlation_id;
        let key = ShardKey {
            tenant_id: publish.shard.tenant_id.clone(),
            namespace: publish.shard.namespace.clone(),
            stream: publish.shard.stream.clone(),
            shard: publish.shard.shard,
        };

        match self.ingress.dispatch(&key) {
            Dispatch::Local => {}
            Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
            } => {
                // The shard moved on. Answering `NotLeader` rather than
                // forwarding again is deliberate: a chain of brokers each
                // relaying to the next would make one publish's latency and
                // failure modes unbounded. The requester decides.
                metrics::record_served(metrics::OUTCOME_NOT_LEADER);
                return InternalMessage::NotLeader(NotLeader {
                    correlation_id,
                    node_id,
                    advertise_addr: advertise_addr.to_string(),
                    generation,
                });
            }
            Dispatch::Unavailable(reason) => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return error(correlation_id, ErrorCode::Unavailable, reason.to_string());
            }
        }

        // Local, but at which generation? `dispatch` already required local
        // readiness to match the router, so this reads the same number it
        // agreed on.
        let ours = match self.router.resolve(&felix_router::ShardKey {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
        }) {
            Resolution::Local { generation } => generation,
            // Between the dispatch above and here the view changed. Refusing is
            // the only safe answer; the requester retries and gets a definite
            // one.
            _ => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return error(
                    correlation_id,
                    ErrorCode::Unavailable,
                    "ownership changed while the request was being served".to_string(),
                );
            }
        };

        if publish.shard.generation != ours {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            return if publish.shard.generation > ours {
                // The requester has seen a newer assignment than this broker.
                // Accepting would be writing a shard we may already have lost.
                error(
                    correlation_id,
                    ErrorCode::StaleRoute,
                    format!(
                        "this broker is at generation {ours}, the requester at {}",
                        publish.shard.generation
                    ),
                )
            } else {
                InternalMessage::NotLeader(NotLeader {
                    correlation_id,
                    node_id: self.router.local_node_id().to_string(),
                    advertise_addr: self.advertise_addr.clone(),
                    generation: ours,
                })
            };
        }

        let handle = match self
            .broker
            .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
        {
            Ok(handle) => handle,
            Err(err) => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return error(correlation_id, ErrorCode::Unavailable, err.to_string());
            }
        };

        match self
            .broker
            .publish_batch_with_outcome(&handle, &publish.payloads)
            .await
        {
            Ok(outcome) => {
                // The same wait the direct publish path makes. A forwarded
                // publish is still a publish to this stream, and the client on
                // the other end of the forward asked for the stream's guarantee,
                // not for whichever one this path happened to provide.
                if let Err(err) = crate::replication::quorum::await_quorum(
                    &handle,
                    Some(&key),
                    &outcome,
                    self.marks.as_deref(),
                    Some(self.ingress.as_ref()),
                    self.quorum_timeout,
                )
                .await
                {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    return error(correlation_id, ErrorCode::StorageFailed, err.to_string());
                }
                metrics::record_served(metrics::OUTCOME_OK);
                // An ephemeral stream has no log, so there are no offsets to
                // report. Zero is not a lie here: the requester only relays an
                // acknowledgement, and the client protocol carries no offset on
                // a publish ack at all.
                let (first, last) = outcome.offsets.unwrap_or((0, 0));
                InternalMessage::ForwardPublishOk(ForwardPublishOk {
                    correlation_id,
                    first_offset: first,
                    last_offset: last,
                })
            }
            Err(err) => {
                // The owner accepted the request and the write failed. Distinct
                // from a refusal: the requester must not retry, because this
                // broker did try.
                metrics::record_served(metrics::OUTCOME_ERROR);
                error(correlation_id, ErrorCode::StorageFailed, err.to_string())
            }
        }
    }
}

#[async_trait]
impl PeerRequestHandler for ForwardingHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::ForwardPublish(publish) => self.apply(publish).await,
            // Responses have no business arriving as requests, and a broker that
            // answered one would be inventing a request that was never made.
            other => error(
                other.correlation_id(),
                ErrorCode::Malformed,
                format!("{:?} is not a request", other.kind()),
            ),
        }
    }
}

fn error(correlation_id: u64, code: ErrorCode, detail: String) -> InternalMessage {
    InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id,
        code,
        detail,
    })
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
