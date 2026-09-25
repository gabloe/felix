//! `InitProducerId`: a producer id for an idempotent producer.
//!
//! The id is a Felix producer id, and the sequences it numbers live in each
//! shard's log (see `felix-broker`'s `publish/per_record.rs`), so no broker
//! has to remember having issued it: any leader of a shard, now or after a
//! failover, checks a batch against what its log holds. The epoch is always
//! 0. A producer that would bump its epoch (KIP-360) asks again and gets a new
//! id instead, which starts its sequences over just the same.
//!
//! A transactional id is refused; see `transactions`.

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::cluster::Principal;
use crate::service::Shared;

pub(super) fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: InitProducerIdRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let refused = |error: ResponseError| {
        let response = InitProducerIdResponse::default()
            .with_error_code(error.code())
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        super::encode(&response, version, error.code())
    };
    if request
        .transactional_id
        .as_ref()
        .is_some_and(|id| !id.is_empty())
    {
        tracing::info!("refused a kafka transactional producer: Felix has no transactions");
        return refused(super::transactions::REFUSED);
    }
    if principal.is_none() {
        return refused(ResponseError::ClusterAuthorizationFailed);
    }
    // Kafka producer ids are non-negative i64s; Felix's are u64s, never zero.
    let id = (shared.broker.new_producer_id() & i64::MAX as u64).max(1) as i64;
    let response = InitProducerIdResponse::default()
        .with_producer_id(ProducerId(id))
        .with_producer_epoch(0);
    super::encode(&response, version, 0)
}
