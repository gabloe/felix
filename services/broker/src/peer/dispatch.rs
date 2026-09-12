//! Routing a peer request to the half of this broker that answers it.
//!
//! Two halves, because a broker plays two roles at once and they are not the
//! same role for the same shard: it is the leader of some shards and a follower
//! of others. A forwarded publish is a request to *order* a record, and only a
//! leader may answer it; a replication batch is a request to *store* one
//! already ordered, and only a follower should. Keeping them apart is what
//! stops either check standing in for the other.
use async_trait::async_trait;
use felix_wire::internal::{ErrorCode, ForwardPublishError, InternalMessage};

use super::handler::ForwardingHandler;
use super::replica::ReplicaHandler;
use super::server::PeerRequestHandler;

/// Answers every request a peer may send this broker.
pub struct BrokerPeerHandler {
    forwarding: ForwardingHandler,
    replica: ReplicaHandler,
}

impl BrokerPeerHandler {
    pub fn new(forwarding: ForwardingHandler, replica: ReplicaHandler) -> Self {
        Self {
            forwarding,
            replica,
        }
    }
}

#[async_trait]
impl PeerRequestHandler for BrokerPeerHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::ForwardPublish(publish) => self.forwarding.apply(publish).await,
            InternalMessage::ForwardCacheOp(op) => self.forwarding.apply_cache_op(op).await,
            InternalMessage::ReplicateRecords(batch) => self.replica.apply(batch).await,
            InternalMessage::ReplicateBootstrap(request) => self.replica.bootstrap(request).await,
            // Responses have no business arriving as requests, and a broker that
            // answered one would be inventing a request that was never made.
            other => InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: other.correlation_id(),
                code: ErrorCode::Malformed,
                detail: format!("{:?} is not a request", other.kind()),
            }),
        }
    }
}
