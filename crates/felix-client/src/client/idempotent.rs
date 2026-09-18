//! A producer whose publishes land once, however many times they are sent.
//!
//! The broker hands out a producer id; the producer numbers its batches on
//! each stream from zero and sends the number with the batch. The shard's
//! leader appends the number it expects and answers a re-send of one it
//! already holds from memory, so a batch the producer never got an answer
//! for can be sent again without a second copy landing. That is the whole
//! contract, and it is what `retry.ambiguous_outcomes_are_not_silently_retried`
//! could not offer: with a sequence the ambiguous outcome is not ambiguous
//! any more.
//!
//! The sequence advances only on an acknowledgement. A publish that fails
//! for any reason but a typed refusal leaves it where it was, so the next
//! call re-sends the same batch under the same number; and a typed refusal
//! ends the producer on that stream, because a gap or a forgotten producer
//! is not something a re-send can mend.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Mutex;

use super::client::Client;
use super::cluster::{ClusterClient, is_terminal};
use crate::{PublishRefusalReason, PublishRefused};

/// Where a producer's batches go: one broker, or whichever a cluster client
/// is using, with the shard's leader remembered once a refusal names it.
enum Source<'a> {
    Single(&'a Client),
    Cluster(&'a ClusterClient),
}

/// The next sequence on one stream, or the refusal that ended it.
#[derive(Debug, Clone)]
enum Cursor {
    Next(u64),
    Ended(PublishRefused),
}

/// A producer whose batches are appended once, however many times they are
/// sent. See the module documentation.
///
/// One sequence per stream, so a producer may publish to several streams;
/// publishes to one stream are serialised, since the sequence has to be. A
/// batch of any size takes one sequence.
pub struct IdempotentProducer<'a> {
    source: Source<'a>,
    producer_id: u64,
    cursors: Mutex<HashMap<(String, String, String), Cursor>>,
    /// The broker a refusal named as the leader of a stream, kept so the next
    /// batch goes straight there rather than being refused again.
    leaders: Mutex<HashMap<(String, String, String), Arc<Client>>>,
}

impl<'a> IdempotentProducer<'a> {
    pub(crate) fn for_client(client: &'a Client, producer_id: u64) -> Self {
        Self::new(Source::Single(client), producer_id)
    }

    pub(crate) fn for_cluster(cluster: &'a ClusterClient, producer_id: u64) -> Self {
        Self::new(Source::Cluster(cluster), producer_id)
    }

    fn new(source: Source<'a>, producer_id: u64) -> Self {
        Self {
            source,
            producer_id,
            cursors: Mutex::new(HashMap::new()),
            leaders: Mutex::new(HashMap::new()),
        }
    }

    /// The id the broker assigned this producer.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    /// Publish one payload, once.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.publish_batch(tenant_id, namespace, stream, vec![payload])
            .await
    }

    /// Publish a batch, once. The batch takes one sequence whatever its size.
    ///
    /// Returns once the leader has acknowledged it, which under `Quorum`
    /// means a majority holds it. On any error but a typed refusal the
    /// sequence is not advanced, so calling again re-sends the same batch and
    /// cannot duplicate it. A [`PublishRefused`] ends this producer on the
    /// stream: every later call fails with the same reason, because the
    /// broker no longer knows where this producer is.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
    ) -> Result<()> {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        // Held for the whole publish: the sequence is only meaningful if the
        // batches carrying consecutive numbers are sent in that order.
        let mut cursors = self.cursors.lock().await;
        let sequence = match cursors.get(&key) {
            None => 0,
            Some(Cursor::Next(sequence)) => *sequence,
            Some(Cursor::Ended(refused)) => {
                return Err(refused.clone()).context("this producer was ended on the stream");
            }
        };
        let result = self
            .send(tenant_id, namespace, stream, payloads, sequence, &key)
            .await;
        match result {
            Ok(()) => {
                cursors.insert(key, Cursor::Next(sequence + 1));
                Ok(())
            }
            Err(err) => {
                if let Some(refused) = err.downcast_ref::<PublishRefused>()
                    && !matches!(refused.reason, PublishRefusalReason::NotLeader { .. })
                {
                    cursors.insert(key, Cursor::Ended(refused.clone()));
                }
                Err(err)
            }
        }
    }

    /// One batch under one sequence, re-sent until answered or the policy
    /// runs out. Only ever the same number: a re-send is safe *because* the
    /// number did not move.
    async fn send(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        sequence: u64,
        key: &(String, String, String),
    ) -> Result<()> {
        match self.source {
            Source::Single(client) => {
                self.send_via(
                    client, tenant_id, namespace, stream, payloads, sequence, key,
                )
                .await
            }
            Source::Cluster(cluster) => {
                let started = std::time::Instant::now();
                let policy = cluster.policy();
                let mut last: Option<anyhow::Error> = None;
                for attempt in 0..policy.attempts.max(1) {
                    if attempt > 0 {
                        let delay = policy.delay_before(attempt - 1);
                        if let Some(budget) = policy.deadline
                            && started.elapsed() + delay >= budget
                        {
                            break;
                        }
                        tokio::time::sleep(delay).await;
                        // The leader this producer remembered may be the
                        // broker that just failed; forget it and let the
                        // next refusal name the new one.
                        self.leaders.lock().await.remove(key);
                        if let Err(err) = cluster.reconnect().await {
                            last = Some(err.context("no broker answered"));
                            continue;
                        }
                    }
                    let client = cluster.client().await;
                    match self
                        .send_via(
                            &client,
                            tenant_id,
                            namespace,
                            stream,
                            payloads.clone(),
                            sequence,
                            key,
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            // A refusal is the broker's answer, not a failure
                            // to get one, and no other broker answers it
                            // differently.
                            if err.downcast_ref::<PublishRefused>().is_some() || is_terminal(&err) {
                                return Err(err);
                            }
                            last = Some(err);
                        }
                    }
                }
                Err(last
                    .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
                    .context(format!(
                        "gave up after {:?} and at most {} attempts; sequence {sequence} \
                         was not advanced and may be sent again",
                        started.elapsed(),
                        policy.attempts.max(1),
                    )))
            }
        }
    }

    /// Send to the stream's leader if one is remembered, else to `client`,
    /// following one not-leader refusal to the broker it names.
    #[allow(clippy::too_many_arguments)]
    async fn send_via(
        &self,
        client: &Client,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        sequence: u64,
        key: &(String, String, String),
    ) -> Result<()> {
        let remembered = self.leaders.lock().await.get(key).cloned();
        let first = match &remembered {
            Some(leader) => {
                self.publish_on(
                    leader,
                    tenant_id,
                    namespace,
                    stream,
                    payloads.clone(),
                    sequence,
                )
                .await
            }
            None => {
                self.publish_on(
                    client,
                    tenant_id,
                    namespace,
                    stream,
                    payloads.clone(),
                    sequence,
                )
                .await
            }
        };
        let err = match first {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        let (node_id, addr) = match err.downcast_ref::<PublishRefused>() {
            Some(PublishRefused {
                reason: PublishRefusalReason::NotLeader { node_id, addr },
                ..
            }) => (node_id.clone(), addr.clone()),
            _ => return Err(err),
        };
        // Only the leader holds the sequences, so the batch goes to it
        // rather than through a forward. One hop: a correct cluster needs
        // one, and a second refusal means the answer is moving.
        let Some(addr) = addr else {
            return Err(err.context(format!(
                "{node_id} leads the shard but its client address is not published, \
                 so there is nowhere to send the batch"
            )));
        };
        let addr: SocketAddr = addr
            .parse()
            .with_context(|| format!("the leader's address {addr:?} is not usable"))?;
        let leader = Arc::new(match self.source {
            Source::Single(_) => {
                return Err(err.context(format!(
                    "{node_id} at {addr} leads the shard; connect a client there, or use a \
                     ClusterClient, which follows the refusal itself"
                )));
            }
            Source::Cluster(cluster) => cluster
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the shard's leader {node_id} at {addr}"))?,
        });
        let result = self
            .publish_on(&leader, tenant_id, namespace, stream, payloads, sequence)
            .await;
        if result.is_ok() {
            self.leaders.lock().await.insert(key.clone(), leader);
        }
        result
    }

    async fn publish_on(
        &self,
        client: &Client,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        sequence: u64,
    ) -> Result<()> {
        client
            .publisher()
            .await?
            .publish_idempotent_batch(
                tenant_id,
                namespace,
                stream,
                payloads,
                self.producer_id,
                sequence,
            )
            .await
    }
}
