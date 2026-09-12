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
    ErrorCode, InternalMessage, ReplicateBootstrap, ReplicateError, ReplicateOk, ReplicateRecords,
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

    /// May this broker store anything for `key` at `generation`?
    ///
    /// Shared by both entry points on purpose: storing records and placing the
    /// log that holds them are the same authority question, and a fence applied
    /// to one and not the other is a fence with a way round it.
    fn check_role(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        generation: u64,
    ) -> Option<InternalMessage> {
        match self.router.replica_role(key, generation) {
            ReplicaRole::Follower => None,
            ReplicaRole::Fenced { have, named } => {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                Some(refused(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                ))
            }
            ReplicaRole::Behind { have, named } => {
                // Not a refusal of the leader, only of this moment: the watch
                // has not caught up. Retryable, and the leader will find us
                // ready once it has.
                metrics::record_replicated(metrics::OUTCOME_BEHIND);
                Some(refused(
                    correlation_id,
                    ErrorCode::StaleRoute,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                ))
            }
            ReplicaRole::NotAReplica => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                Some(refused(
                    correlation_id,
                    ErrorCode::Unauthorized,
                    0,
                    "this broker is not a replica of that shard".to_string(),
                ))
            }
        }
    }

    /// Begin this shard's log where the leader's surviving log begins.
    ///
    /// The leader has nothing older left, so the records below `base_offset`
    /// are gone from every copy: a log that starts there is complete rather
    /// than truncated. The follower cannot work that out for itself, which is
    /// why the leader has to say it.
    ///
    /// **A follower holding records of its own refuses.** Discarding them is an
    /// operator's decision, not a leader's — and a log placed over them would
    /// have a hole between what it held and what it was given, which nothing
    /// downstream could detect.
    pub async fn bootstrap(&self, request: ReplicateBootstrap) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let key = felix_router::ShardKey {
            tenant_id: request.shard.tenant_id.clone(),
            namespace: request.shard.namespace.clone(),
            stream: request.shard.stream.clone(),
            shard: request.shard.shard,
            kind: felix_router::ShardKind::Stream,
        };

        if let Some(refusal) = self.check_role(correlation_id, &key, request.shard.generation) {
            return refusal;
        }
        let Some(storage) = self.broker.durable_storage() else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no durable storage".to_string(),
            );
        };

        // Creates the log at `base_offset` when this broker has never held the
        // shard, and opens what is there otherwise. The base it comes back with
        // is the authority either way.
        let log = match storage.open_stream_at(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            request.base_offset,
        ) {
            Ok(log) => log,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };

        let base = log.base_offset();
        if base != request.base_offset {
            // A log is already here and it starts somewhere else. Placing the
            // leader's base over it would leave a hole between the two.
            metrics::record_replicated(metrics::OUTCOME_CONFLICT);
            let tail = log.tail_offset().await.unwrap_or(base);
            tracing::error!(
                stream = %key.stream,
                shard = key.shard,
                held_from = base,
                held_to = tail,
                offered_from = request.base_offset,
                "refusing to bootstrap: this broker already holds records for that shard",
            );
            return refused(
                correlation_id,
                ErrorCode::LogConflict,
                tail,
                format!(
                    "this broker holds {base}..{tail} for that shard and cannot be \
                     re-based at {}",
                    request.base_offset
                ),
            );
        }

        let tail = match log.tail_offset().await {
            Ok(tail) => tail,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            base_offset = base,
            "shard log placed for bootstrap",
        );
        metrics::record_replicated(metrics::OUTCOME_BOOTSTRAPPED);
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id,
            durable_offset: tail,
        })
    }

    pub async fn apply(&self, batch: ReplicateRecords) -> InternalMessage {
        let correlation_id = batch.correlation_id;
        let key = felix_router::ShardKey {
            tenant_id: batch.shard.tenant_id.clone(),
            namespace: batch.shard.namespace.clone(),
            stream: batch.shard.stream.clone(),
            shard: batch.shard.shard,
            kind: felix_router::ShardKind::Stream,
        };

        if let Some(refusal) = self.check_role(correlation_id, &key, batch.shard.generation) {
            return refusal;
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
                // The records went straight to the log, so the stream's own view
                // of its tail has to be told. Without this the first publish
                // this broker accepts once promoted waits on commit turns that
                // were never taken.
                if let Err(err) = self
                    .broker
                    .adopt_replicated(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        applied.durable_offset,
                    )
                    .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "stored a replicated batch but could not advance the stream's tail",
                    );
                }
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
