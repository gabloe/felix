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
use std::time::{Duration, Instant};

use felix_broker::{Broker, GroupKey};
use felix_wire::GroupRecord;

use crate::serving::quic::handlers::publish::PublishContext;
use crate::shard_routing::{Dispatch, dispatch};
use crate::shard_watch::{ShardKey, ShardKind};

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
        &'a std::sync::Arc<felix_broker::GroupReader>,
        felix_broker::StreamLog,
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

/// How often a waiting poll re-checks for work.
///
/// The check is a read lock and a field read — no I/O, no allocation — so the
/// cost lands on the consumer that chose to wait and never on a publisher. An
/// append notification would wake it sooner, but only by adding work to the
/// publish path on behalf of a consumer that is by definition idle.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Take up to `max_records` for a group, waiting up to `wait` for work.
///
/// The wait happens on a stream of the client's own, so holding it open blocks
/// nothing else on that connection.
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
    wait: Duration,
) -> Result<Vec<GroupRecord>, String> {
    let (reader, log) = reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let deadline = Instant::now() + wait;

    loop {
        let claimed = reader
            .poll(&key, &log, max_records, Instant::now())
            .await
            .map_err(|err| err.to_string())?;
        if !claimed.is_empty() {
            return Ok(claimed
                .into_iter()
                .map(|claimed| GroupRecord {
                    offset: claimed.offset,
                    payload: claimed.payload,
                    attempts: claimed.attempts,
                })
                .collect());
        }
        let now = Instant::now();
        if now >= deadline {
            // An empty answer, which is an answer: nothing was available in the
            // time the consumer was willing to wait for it.
            return Ok(Vec::new());
        }
        // Ownership is re-checked every round, because the shard can move while
        // a poll is waiting. Serving one after that would hand out records the
        // new owner is handing out too.
        owned_here(publish_ctx, tenant_id, namespace, stream, shard)?;
        tokio::time::sleep(WAIT_POLL_INTERVAL.min(deadline - now)).await;
    }
}

/// Offsets this group gave up on.
pub(crate) async fn dead_letters(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
) -> Result<Vec<u64>, String> {
    let (reader, _log) = reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    reader
        .dead_lettered(&key)
        .await
        .map_err(|err| err.to_string())
}

/// Drop a dead letter, or put it back in the queue.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn manage_dead_letter(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    offset: u64,
    redrive: bool,
) -> Result<(), String> {
    let (reader, _log) = reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let taken = if redrive {
        reader.redrive(&key, offset).await
    } else {
        reader.discard(&key, offset).await
    }
    .map_err(|err| err.to_string())?;
    if taken {
        return Ok(());
    }
    // Refused rather than silently accepted. An operator told a redrive
    // succeeded when the offset was never dead-lettered would wait for a
    // delivery that is not coming.
    Err(format!("offset {offset} is not a dead letter of {group}"))
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
