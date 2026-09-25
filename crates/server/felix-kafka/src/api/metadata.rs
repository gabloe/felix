//! `Metadata`: the brokers, and for each topic which broker leads each
//! partition.
//!
//! A partition is a shard and its leader is the shard's leader, so a client
//! fetches straight from the broker that holds the data. Every broker answers
//! for every partition from the same routing view, which is what lets a client
//! bootstrap from any of them.

use std::collections::HashSet;

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::cluster::{Placement, Principal, ShardRef, kafka_node_id};
use crate::service::Shared;
use crate::topic::{TopicStream, parse_topic, topic_name};

pub(super) async fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: MetadataRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let endpoints = shared.cluster.brokers();
    let reachable: HashSet<i32> = endpoints
        .iter()
        .map(|endpoint| kafka_node_id(&endpoint.node_id))
        .collect();
    let brokers = endpoints
        .into_iter()
        .map(|endpoint| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(kafka_node_id(&endpoint.node_id)))
                .with_host(StrBytes::from_string(endpoint.host))
                .with_port(i32::from(endpoint.port))
        })
        .collect();

    // v0 has no null: an empty list there means every topic.
    let requested = match request.topics {
        Some(topics) if !(version == 0 && topics.is_empty()) => Some(topics),
        _ => None,
    };
    let topics = match requested {
        None => every_topic(shared, principal, &reachable).await,
        Some(requested) => {
            let mut topics = Vec::with_capacity(requested.len());
            for topic in requested {
                let described = match topic.name {
                    Some(name) => named_topic(shared, principal, name, &reachable).await,
                    // Asked for by topic id. Felix streams have none.
                    None => MetadataResponseTopic::default()
                        .with_name(None)
                        .with_topic_id(topic.topic_id)
                        .with_error_code(ResponseError::UnknownTopicId.code()),
                };
                topics.push(described);
            }
            topics
        }
    };
    let error = super::first_error(topics.iter().map(|topic| topic.error_code));
    let response = MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from_string(
            shared.settings.cluster_id.clone(),
        )))
        .with_controller_id(BrokerId(kafka_node_id(&shared.cluster.local_node_id())))
        .with_topics(topics);
    super::encode(&response, version, error)
}

/// Every topic this principal may read. Streams Kafka cannot name, streams
/// with no disk log to read by offset, and streams the principal may not read
/// are left out rather than listed with an error.
async fn every_topic(
    shared: &Shared,
    principal: Option<&Principal>,
    reachable: &HashSet<i32>,
) -> Vec<MetadataResponseTopic> {
    let Some(principal) = principal else {
        return Vec::new();
    };
    let mut topics = Vec::new();
    for (namespace, stream, metadata) in shared.broker.tenant_streams(principal.tenant_id()).await {
        if !metadata.durable || !principal.may_read(&namespace, &stream) {
            continue;
        }
        let Some(name) = topic_name(&namespace, &stream) else {
            continue;
        };
        let shard = |shard| ShardRef {
            tenant_id: principal.tenant_id(),
            namespace: &namespace,
            stream: &stream,
            shard,
        };
        topics.push(describe(shared, name, metadata.shards, shard, reachable));
    }
    topics
}

async fn named_topic(
    shared: &Shared,
    principal: Option<&Principal>,
    name: TopicName,
    reachable: &HashSet<i32>,
) -> MetadataResponseTopic {
    let refused = |error: ResponseError| {
        MetadataResponseTopic::default()
            .with_name(Some(name.clone()))
            .with_error_code(error.code())
    };
    let Some(principal) = principal else {
        return refused(ResponseError::TopicAuthorizationFailed);
    };
    let Some(TopicStream { namespace, stream }) =
        parse_topic(name.as_str(), shared.settings.default_namespace.as_deref())
    else {
        return refused(ResponseError::UnknownTopicOrPartition);
    };
    if !principal.may_read(&namespace, &stream) {
        return refused(ResponseError::TopicAuthorizationFailed);
    }
    let Some(metadata) = shared
        .broker
        .stream_metadata(principal.tenant_id(), &namespace, &stream)
        .await
        .filter(|metadata| metadata.durable)
    else {
        return refused(ResponseError::UnknownTopicOrPartition);
    };
    let shard = |shard| ShardRef {
        tenant_id: principal.tenant_id(),
        namespace: &namespace,
        stream: &stream,
        shard,
    };
    // Answered under the name asked for, which may be the short form a
    // default namespace allows: the client matches the answer by name.
    describe(
        shared,
        name.as_str().to_string(),
        metadata.shards,
        shard,
        reachable,
    )
}

fn describe<'a>(
    shared: &Shared,
    name: String,
    shards: u32,
    shard: impl Fn(u32) -> ShardRef<'a>,
    reachable: &HashSet<i32>,
) -> MetadataResponseTopic {
    let local = shared.cluster.local_node_id();
    let partitions = (0..shards)
        .map(|index| {
            let partition = MetadataResponsePartition::default().with_partition_index(index as i32);
            let (leader, replicas) = match shared.cluster.placement(&shard(index)) {
                Placement::Local { replicas } => (local.clone(), replicas),
                Placement::Remote { leader, replicas } => (leader, replicas),
                Placement::Unavailable => return leader_not_available(partition),
            };
            let leader_id = kafka_node_id(&leader);
            // A leader whose Kafka listener this cluster has not been told
            // about cannot be fetched from. Saying there is no leader makes
            // the client ask again rather than connect somewhere unknown.
            if !reachable.contains(&leader_id) {
                return leader_not_available(partition);
            }
            let mut replica_ids = vec![BrokerId(leader_id)];
            for replica in replicas {
                let id = BrokerId(kafka_node_id(&replica));
                if !replica_ids.contains(&id) {
                    replica_ids.push(id);
                }
            }
            partition
                .with_leader_id(BrokerId(leader_id))
                .with_replica_nodes(replica_ids)
                // Only the leader is claimed in sync: Felix does not track
                // which followers are caught up in Kafka's sense.
                .with_isr_nodes(vec![BrokerId(leader_id)])
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(name))))
        .with_partitions(partitions)
}

fn leader_not_available(partition: MetadataResponsePartition) -> MetadataResponsePartition {
    partition
        .with_error_code(ResponseError::LeaderNotAvailable.code())
        .with_leader_id(BrokerId(-1))
}
