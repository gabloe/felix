//! Turning a topic and partition into a shard this broker may read.
//!
//! Every read answers the same questions in the same order, and the order
//! matters: the topic must parse, then the caller must be allowed to read it,
//! and only then is its existence looked up. Checking existence first would
//! tell an unauthorized caller which streams exist.

use std::sync::Arc;

use felix_broker::StreamLog;
use kafka_protocol::ResponseError;

use crate::cluster::{Placement, Principal, ShardRef};
use crate::service::Shared;
use crate::topic::{TopicStream, parse_topic};

/// A partition this broker leads and can read.
pub(super) struct Readable {
    pub(super) log: StreamLog,
    /// Woken when a publish to the shard commits.
    pub(super) appended: Arc<tokio::sync::Notify>,
}

pub(super) async fn resolve(
    shared: &Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
) -> Result<Readable, ResponseError> {
    let Some(principal) = principal else {
        return Err(ResponseError::TopicAuthorizationFailed);
    };
    let Some(TopicStream { namespace, stream }) =
        parse_topic(topic, shared.settings.default_namespace.as_deref())
    else {
        return Err(ResponseError::UnknownTopicOrPartition);
    };
    if !principal.may_read(&namespace, &stream) {
        return Err(ResponseError::TopicAuthorizationFailed);
    }
    let tenant_id = principal.tenant_id();
    let metadata = shared
        .broker
        .stream_metadata(tenant_id, &namespace, &stream)
        .await
        .filter(|metadata| metadata.durable)
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    let shard = u32::try_from(partition)
        .ok()
        .filter(|shard| *shard < metadata.shards)
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    match shared.cluster.placement(&ShardRef {
        tenant_id,
        namespace: &namespace,
        stream: &stream,
        shard,
    }) {
        Placement::Local { .. } => {}
        // Both send the client back to Metadata, which names the leader or
        // says there is none yet.
        Placement::Remote { .. } | Placement::Unavailable => {
            return Err(ResponseError::NotLeaderOrFollower);
        }
    }
    let handle = shared
        .broker
        .resolve_stream_handle(tenant_id, &namespace, &stream, shard)
        .await
        .map_err(|err| crate::errors::from_broker(&err))?;
    let log = handle
        .log()
        .cloned()
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    Ok(Readable {
        log,
        appended: handle.appended(),
    })
}
