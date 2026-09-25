//! `Produce`, refused: this listener is read-only.
//!
//! Offered anyway, because librdkafka will not use record-batch (v2) fetches
//! from a broker that does not list `Produce` v3: it takes the pair as the
//! sign of a broker new enough for them. Answering it also lets a producer
//! pointed here fail with a reason instead of "not supported by broker".

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

pub(crate) const MESSAGE: &str = "Felix's Kafka listener is read-only; publish with a Felix client";

pub(super) const REFUSED: ResponseError = ResponseError::PolicyViolation;

/// The refusal, or `None` for `acks=0`, which a Kafka broker never answers.
pub(super) fn refuse(request: ProduceRequest, version: i16) -> Result<Option<(Bytes, i16)>> {
    if request.acks == 0 {
        return Ok(None);
    }
    let code = REFUSED.code();
    let topics = request
        .topic_data
        .into_iter()
        .map(|topic| {
            TopicProduceResponse::default()
                .with_name(topic.name)
                .with_partition_responses(
                    topic
                        .partition_data
                        .into_iter()
                        .map(|partition| {
                            let answer = PartitionProduceResponse::default()
                                .with_index(partition.index)
                                .with_error_code(code);
                            // The message field exists from v8.
                            if version >= 8 {
                                answer.with_error_message(Some(StrBytes::from_static_str(MESSAGE)))
                            } else {
                                answer
                            }
                        })
                        .collect(),
                )
        })
        .collect();
    super::encode(
        &ProduceResponse::default().with_responses(topics),
        version,
        code,
    )
    .map(Some)
}
