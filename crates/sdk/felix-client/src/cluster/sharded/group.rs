//! One consumer group over every shard of a stream.
//!
//! A group is bound to one shard: its cursor and its claims live with that
//! shard's leader, which is the only broker that can serve it. Consuming a
//! whole multi-shard stream through a group therefore means one group per
//! shard, each served by whichever broker leads that shard. This does that,
//! following each shard's own `NotLeader`, and hands back records tagged with
//! the shard they came from so an acknowledgement goes to the same place.
//!
//! What it deliberately does not do is merge the shards into one order: see
//! [`ShardedGroup::poll`].

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result};
use tokio::sync::Mutex;

use crate::client::Client;
use crate::cluster::ClusterClient;

/// How many owners a single operation will follow before giving up. A settled
/// cluster needs one hop; more means ownership is moving under the request.
const MAX_REDIRECTS: usize = 3;

/// A consumer group read across every shard of a stream.
///
/// Built by [`ClusterClient::group_sharded`]. Each shard's group is served by
/// that shard's leader, found by following redirects and remembered until an
/// operation against it fails.
pub struct ShardedGroup {
    cluster: Arc<ClusterClient>,
    tenant_id: String,
    namespace: String,
    stream: String,
    group: String,
    shards: u32,
    /// The broker each shard was last served by. `None` until first used, and
    /// again after an operation against it fails, so the next one starts from
    /// the cluster client and follows the redirect afresh.
    owners: Mutex<Vec<Option<Arc<Client>>>>,
    /// Where the next poll starts, so no shard is favoured.
    next: AtomicU32,
}

impl ShardedGroup {
    pub(crate) fn new(
        cluster: Arc<ClusterClient>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        shards: u32,
    ) -> Self {
        Self {
            cluster,
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            group: group.to_string(),
            shards,
            owners: Mutex::new(vec![None; shards as usize]),
            next: AtomicU32::new(0),
        }
    }

    /// How many shards this group spans.
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Take up to `max_records` from one shard, visiting shards in turn.
    ///
    /// Each call starts at the shard after the one the last call started at and
    /// returns the first non-empty batch it finds, so every shard is visited
    /// and none can starve the others. An empty answer means no shard had
    /// anything.
    ///
    /// **Ordering is per shard.** Records from one shard arrive in offset order;
    /// records from different shards arrive in whatever order the polls find
    /// them, because there is no order between shards to preserve.
    ///
    /// Fails if a shard cannot be polled at all: skipping it would look exactly
    /// like a shard with nothing to deliver.
    pub async fn poll(&self, max_records: u32) -> Result<Vec<ShardedGroupRecord>> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.shards;
        for step in 0..self.shards {
            let shard = (start + step) % self.shards;
            let records = self
                .on_shard(shard, |client| async move {
                    client
                        .group_poll(
                            &self.tenant_id,
                            &self.namespace,
                            &self.stream,
                            shard,
                            &self.group,
                            max_records,
                        )
                        .await
                })
                .await
                .with_context(|| format!("poll shard {shard} of {}", self.stream))?;
            if !records.is_empty() {
                return Ok(records
                    .into_iter()
                    .map(|record| ShardedGroupRecord { shard, record })
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    /// Finish a record, on the shard it was claimed from.
    pub async fn ack(&self, record: &ShardedGroupRecord) -> Result<()> {
        self.settle(record, true).await
    }

    /// Hand a record back to be redelivered at once, on the shard it was
    /// claimed from.
    pub async fn nack(&self, record: &ShardedGroupRecord) -> Result<()> {
        self.settle(record, false).await
    }

    async fn settle(&self, record: &ShardedGroupRecord, finished: bool) -> Result<()> {
        let (shard, offset) = (record.shard, record.record.offset);
        anyhow::ensure!(
            shard < self.shards,
            "shard {shard} is not one of this group's {} shards",
            self.shards
        );
        self.on_shard(shard, |client| async move {
            if finished {
                client
                    .group_ack(
                        &self.tenant_id,
                        &self.namespace,
                        &self.stream,
                        shard,
                        &self.group,
                        offset,
                    )
                    .await
            } else {
                client
                    .group_nack(
                        &self.tenant_id,
                        &self.namespace,
                        &self.stream,
                        shard,
                        &self.group,
                        offset,
                    )
                    .await
            }
        })
        .await
    }

    /// Run `op` against the broker that leads `shard`, following redirects.
    async fn on_shard<T, F, Fut>(&self, shard: u32, op: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let cached = self.owners.lock().await[shard as usize].clone();
        let cached_route = cached.is_some();
        let mut client = match cached {
            Some(client) => client,
            None => self.cluster.client().await,
        };
        let mut visited: Vec<String> = Vec::new();
        let mut went_back = false;

        for _ in 0..=MAX_REDIRECTS {
            let error = match op(Arc::clone(&client)).await {
                Ok(value) => {
                    self.owners.lock().await[shard as usize] = Some(client);
                    return Ok(value);
                }
                Err(err) => err,
            };
            // Forgotten on any failure, so the next attempt asks afresh rather
            // than repeating a question to a broker that has stopped answering.
            self.owners.lock().await[shard as usize] = None;
            // A cached or redirected-to leader that answers `shard_unavailable` or
            // `draining` has lost the shard since it was named. The entry broker
            // routes by the current assignment, so ask it again, once.
            if (cached_route || !visited.is_empty())
                && !went_back
                && crate::cluster::route_went_stale(&error)
            {
                went_back = true;
                visited.clear();
                client = self.cluster.client().await;
                continue;
            }
            let Some(redirect) = error.downcast_ref::<crate::NotLeaderError>().cloned() else {
                return Err(error);
            };
            if visited.iter().any(|seen| seen == &redirect.node_id) {
                return Err(error.context(format!(
                    "redirected back to {}, which has already been asked",
                    redirect.node_id
                )));
            }
            let Some(addr) = redirect.addr.clone() else {
                return Err(error.context(
                    "the owner's client address is not published, so there is nowhere to follow to",
                ));
            };
            let addr: SocketAddr = addr
                .parse()
                .with_context(|| format!("the owner's address {addr:?} is not usable"))?;
            visited.push(redirect.node_id.clone());
            client = Arc::new(
                self.cluster
                    .connect_to(addr)
                    .await
                    .with_context(|| format!("connect to the shard owner at {addr}"))?,
            );
        }

        Err(anyhow::anyhow!(
            "still being redirected after {MAX_REDIRECTS} hops; the cluster has not settled on an owner"
        ))
    }
}

impl std::fmt::Debug for ShardedGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedGroup")
            .field("stream", &self.stream)
            .field("group", &self.group)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

/// A record claimed through a [`ShardedGroup`], and the shard it came from.
///
/// The shard is part of the record's identity: offsets are per shard, so
/// acknowledging offset 7 means nothing without saying which shard's 7.
#[derive(Debug, Clone)]
pub struct ShardedGroupRecord {
    /// The shard the record was claimed from.
    pub shard: u32,
    /// The record, with its offset in that shard.
    pub record: felix_wire::GroupRecord,
}
