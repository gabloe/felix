//! Consumer groups, refused so a client can say why.
//!
//! Felix has no Kafka group coordinator, and the protocol has no way to say
//! "this broker never will". Left unanswered, a group consumer waits for a
//! coordinator forever. So `FindCoordinator` is answered, always, with
//! `GROUP_AUTHORIZATION_FAILED` and a message naming the reason: librdkafka
//! treats that code as fatal for the group and hands it to the application,
//! where `kcat -G` prints it and exits. The group APIs behind it get the same
//! answer if a client sends one anyway.

use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatResponse,
    JoinGroupResponse, LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchResponse, SyncGroupResponse,
};
use kafka_protocol::protocol::{Decodable, StrBytes};

pub(crate) const MESSAGE: &str = "Felix has no Kafka consumer groups; assign partitions instead. See docs/kafka-compatibility.md";

const REFUSED: ResponseError = ResponseError::GroupAuthorizationFailed;

pub(super) fn refuse(api: ApiKey, frame: &mut Bytes, version: i16) -> Result<(Bytes, i16)> {
    let code = REFUSED.code();
    match api {
        ApiKey::FindCoordinator => {
            let request =
                FindCoordinatorRequest::decode(frame, version).context("decode FindCoordinator")?;
            tracing::info!(
                group = request.key.as_str(),
                "refused a kafka consumer group: Felix has no group coordinator"
            );
            let message = Some(StrBytes::from_static_str(MESSAGE));
            // v4 batches keys; earlier versions answer the one key at the top
            // level.
            let response = if version >= 4 {
                FindCoordinatorResponse::default().with_coordinators(
                    request
                        .coordinator_keys
                        .into_iter()
                        .map(|key| {
                            Coordinator::default()
                                .with_key(key)
                                .with_node_id(BrokerId(-1))
                                .with_host(StrBytes::default())
                                .with_port(-1)
                                .with_error_code(code)
                                .with_error_message(message.clone())
                        })
                        .collect(),
                )
            } else {
                let response = FindCoordinatorResponse::default()
                    .with_error_code(code)
                    .with_node_id(BrokerId(-1))
                    .with_host(StrBytes::default())
                    .with_port(-1);
                if version >= 1 {
                    response.with_error_message(message)
                } else {
                    response
                }
            };
            super::encode(&response, version, code)
        }
        ApiKey::JoinGroup => super::encode(
            &JoinGroupResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::SyncGroup => super::encode(
            &SyncGroupResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::Heartbeat => super::encode(
            &HeartbeatResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::LeaveGroup => super::encode(
            &LeaveGroupResponse::default().with_error_code(code),
            version,
            code,
        ),
        ApiKey::OffsetCommit => {
            let request =
                OffsetCommitRequest::decode(frame, version).context("decode OffsetCommit")?;
            let topics = request
                .topics
                .into_iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .with_name(topic.name)
                        .with_partitions(
                            topic
                                .partitions
                                .into_iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(code)
                                })
                                .collect(),
                        )
                })
                .collect();
            super::encode(
                &OffsetCommitResponse::default().with_topics(topics),
                version,
                code,
            )
        }
        ApiKey::OffsetFetch => super::encode(
            &OffsetFetchResponse::default().with_error_code(code),
            version,
            code,
        ),
        other => anyhow::bail!("{other:?} is not a group api"),
    }
}
