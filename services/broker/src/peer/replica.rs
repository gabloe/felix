//! The follower's side: storing records the shard's leader shipped.
//!
//! Two checks stand between a batch and this broker's disk, and they answer
//! different questions:
//!
//! 1. **May this broker store these records at all?** Decided here, from the
//!    routing view: is this node in the shard's replica set, at the epoch the
//!    sender named? A sender at an older epoch has been superseded, and its
//!    records must not be stored — it may have written them after losing the
//!    shard. That is the fence, and it is the same one the durable-append check
//!    applies on the leader.
//! 2. **Do these records belong where the batch says?** Decided by
//!    [`felix_broker::replication::apply`], against the log's own tail.
//!
//! The order matters. Position is only meaningful once the sender is
//! established as the current leader: applying first and checking after would
//! let a fenced leader's bytes reach the disk, and records are never rewritten.
use std::sync::Arc;

use felix_broker::Broker;
use felix_broker::replication::{self, Divergence};
use felix_router::{ReplicaRole, ShardRouter};
use felix_wire::internal::{
    ErrorCode, InternalMessage, ReplicateError, ReplicateOk, ReplicateRecords,
};

use super::metrics;

/// Stores replicated records against this broker's local logs.
pub struct ReplicaHandler {
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
}

impl ReplicaHandler {
    pub fn new(broker: Arc<Broker>, router: Arc<ShardRouter>) -> Self {
        Self { broker, router }
    }

    pub async fn apply(&self, batch: ReplicateRecords) -> InternalMessage {
        let correlation_id = batch.correlation_id;
        let key = felix_router::ShardKey {
            tenant_id: batch.shard.tenant_id.clone(),
            namespace: batch.shard.namespace.clone(),
            stream: batch.shard.stream.clone(),
            shard: batch.shard.shard,
        };

        match self.router.replica_role(&key, batch.shard.generation) {
            ReplicaRole::Follower => {}
            ReplicaRole::Fenced { have, named } => {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                return refused(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                );
            }
            ReplicaRole::Behind { have, named } => {
                // Not a refusal of the leader, only of this moment: the watch
                // has not caught up. Retryable, and the leader will find us
                // ready once it has.
                metrics::record_replicated(metrics::OUTCOME_BEHIND);
                return refused(
                    correlation_id,
                    ErrorCode::StaleRoute,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                );
            }
            ReplicaRole::NotAReplica => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                return refused(
                    correlation_id,
                    ErrorCode::Unauthorized,
                    0,
                    "this broker is not a replica of that shard".to_string(),
                );
            }
        }

        let Some(storage) = self.broker.durable_storage() else {
            // A replica set naming a broker with no durable storage is a
            // configuration error, not a transient one. Saying so beats
            // accepting and silently keeping nothing.
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no durable storage".to_string(),
            );
        };

        let log = match storage.open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        {
            Ok(log) => log,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };

        match replication::apply(&log, batch.first_offset, batch.checksum, &batch.payloads).await {
            Ok(Ok(applied)) => {
                metrics::record_replicated(metrics::OUTCOME_OK);
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id,
                    durable_offset: applied.durable_offset,
                })
            }
            Ok(Err(divergence)) => {
                metrics::record_replicated(divergence_outcome(&divergence));
                refused(
                    correlation_id,
                    divergence_code(&divergence),
                    divergence.expected_offset(),
                    divergence.to_string(),
                )
            }
            Err(err) => {
                // This broker's own disk failed. Distinct from every refusal
                // above: nothing is wrong with what the leader sent, so the
                // leader must not treat it as divergence and stop.
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string())
            }
        }
    }
}

fn divergence_code(divergence: &Divergence) -> ErrorCode {
    match divergence {
        Divergence::Gap { .. } => ErrorCode::LogGap,
        Divergence::Conflict { .. } => ErrorCode::LogConflict,
        // A batch that did not survive the trip is a transport problem, and
        // resending the same records is the repair.
        Divergence::Corrupt { .. } => ErrorCode::Malformed,
    }
}

fn divergence_outcome(divergence: &Divergence) -> &'static str {
    match divergence {
        Divergence::Gap { .. } => metrics::OUTCOME_GAP,
        Divergence::Conflict { .. } => metrics::OUTCOME_CONFLICT,
        Divergence::Corrupt { .. } => metrics::OUTCOME_CORRUPT,
    }
}

fn refused(
    correlation_id: u64,
    code: ErrorCode,
    expected_offset: u64,
    detail: String,
) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id,
        code,
        expected_offset,
        detail,
    })
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
