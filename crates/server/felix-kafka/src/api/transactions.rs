//! Kafka transactions, refused so a client can say why.
//!
//! Felix has no transaction coordinator. A transactional producer starts by
//! asking `FindCoordinator` for its transaction coordinator, which `groups`
//! answers with the refusal below; `InitProducerId` with a transactional id
//! and the transaction APIs get the same answer if a client sends one.
//!
//! `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` because librdkafka treats it as
//! fatal and hands it to the application at once, instead of retrying a
//! coordinator lookup that will never succeed.

use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey,
    EndTxnResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::Decodable;

pub(crate) const MESSAGE: &str = "Felix does not support Kafka transactions; unset transactional.id. See docs/kafka-compatibility.md";

pub(crate) const REFUSED: ResponseError = ResponseError::TransactionalIdAuthorizationFailed;

pub(super) fn refuse(api: ApiKey, frame: &mut Bytes, version: i16) -> Result<(Bytes, i16)> {
    let code = REFUSED.code();
    tracing::info!(
        ?api,
        "refused a kafka transaction: Felix has no transaction coordinator"
    );
    match api {
        ApiKey::AddPartitionsToTxn => {
            let request = AddPartitionsToTxnRequest::decode(frame, version)
                .context("decode AddPartitionsToTxn")?;
            let topics = request
                .v3_and_below_topics
                .into_iter()
                .map(|topic| {
                    AddPartitionsToTxnTopicResult::default()
                        .with_name(topic.name)
                        .with_results_by_partition(
                            topic
                                .partitions
                                .into_iter()
                                .map(|partition| {
                                    AddPartitionsToTxnPartitionResult::default()
                                        .with_partition_index(partition)
                                        .with_partition_error_code(code)
                                })
                                .collect(),
                        )
                })
                .collect();
            super::encode(
                &AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(topics),
                version,
                code,
            )
        }
        ApiKey::AddOffsetsToTxn => super::encode(
            &AddOffsetsToTxnResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::EndTxn => super::encode(
            &EndTxnResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::TxnOffsetCommit => {
            let request =
                TxnOffsetCommitRequest::decode(frame, version).context("decode TxnOffsetCommit")?;
            let topics = request
                .topics
                .into_iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .with_name(topic.name)
                        .with_partitions(
                            topic
                                .partitions
                                .into_iter()
                                .map(|partition| {
                                    TxnOffsetCommitResponsePartition::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(code)
                                })
                                .collect(),
                        )
                })
                .collect();
            super::encode(
                &TxnOffsetCommitResponse::default().with_topics(topics),
                version,
                code,
            )
        }
        other => anyhow::bail!("{other:?} is not a transaction api"),
    }
}
