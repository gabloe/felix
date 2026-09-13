//! Serving a consumer group, once ownership says this broker may.
//!
//! A group's in-flight state lives on whichever broker leads the shard. If two
//! brokers served the same group they would each hand out the same records —
//! the divergence cache routing exists to prevent, in a place where it would be
//! worse: a queue's whole promise is that one consumer holds a record at a time.
//!
//! So every operation here refuses unless this broker leads the shard. There is
//! no forwarding: unlike a cache operation, a poll returns records the consumer
//! then has to acknowledge, and relaying that through a second broker would put
//! the claim and the acknowledgement on different machines.
use std::time::Instant;

use felix_broker::Broker;
use felix_broker::group_reader::GroupKey;
use felix_wire::GroupRecord;

use crate::shard_routing::{Dispatch, dispatch};
use crate::shard_watch::{ShardKey, ShardKind};
use crate::transport::quic::handlers::publish::PublishContext;

/// Check that this broker leads the shard, and name the owner if it does not.
fn owned_here(
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<(), String> {
    let key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    };
    match dispatch(publish_ctx.ingress.as_deref(), &key) {
        Dispatch::Local => Ok(()),
        Dispatch::Forward { node_id, .. } => {
            Err(format!("shard {shard} of {stream} is served by {node_id}"))
        }
        Dispatch::Unavailable(reason) => Err(reason.to_string()),
    }
}

/// The pieces a group operation needs, or why it cannot run.
fn reader_and_log<'a>(
    broker: &'a Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<
    (
        &'a std::sync::Arc<felix_broker::group_reader::GroupReader>,
        felix_broker::durable::StreamLog,
    ),
    String,
> {
    owned_here(publish_ctx, tenant_id, namespace, stream, shard)?;
    let reader = broker
        .group_reader()
        .ok_or("this broker has no durable storage, so it serves no consumer groups")?;
    let log = broker
        .durable_storage()
        .ok_or("this broker has no durable storage")?
        .open_stream(tenant_id, namespace, stream, shard)
        .map_err(|err| err.to_string())?;
    Ok((reader, log))
}

fn group_key(tenant_id: &str, namespace: &str, stream: &str, shard: u32, group: &str) -> GroupKey {
    GroupKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        group: group.to_string(),
    }
}

/// Take up to `max_records` for a group.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    max_records: usize,
) -> Result<Vec<GroupRecord>, String> {
    let (reader, log) = reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let claimed = reader
        .poll(&key, &log, max_records, Instant::now())
        .await
        .map_err(|err| err.to_string())?;
    Ok(claimed
        .into_iter()
        .map(|claimed| GroupRecord {
            offset: claimed.offset,
            payload: claimed.payload,
        })
        .collect())
}

/// Finish a record, or hand it back.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn settle(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    offset: u64,
    finish: bool,
) -> Result<(), String> {
    let (reader, _log) = reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    if finish {
        reader.ack(&key, offset).await.map_err(|e| e.to_string())
    } else {
        reader.nack(&key, offset).await.map_err(|e| e.to_string())
    }
}
