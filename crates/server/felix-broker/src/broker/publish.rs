//! The publish path: claim offsets and a place in the commit order, make the
//! batch durable, append it to the replay ring, then fan it out.
//!
//! The order is the design. Offsets are consumed before the durability wait so
//! a batch holds its place from the moment it has one, and fanout comes only
//! after the batch is durable.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::Broker;
use super::shards::StreamHandle;
use crate::error::{BrokerError, Result};
use crate::stream::{DeliveryEnvelope, QueuedDelivery, Sequenced, SubQueuePolicy};
use crate::telemetry::{t_histogram, t_now_if, t_should_sample};
use crate::timings;

impl Broker {
    /// Publish one payload to a single-shard stream.
    ///
    /// Shard 0 by construction: a caller with a routing key resolves the shard
    /// first and uses [`Broker::publish_batch`].
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        let payloads = [payload];
        self.publish_batch(tenant_id, namespace, stream, 0, &payloads)
            .await
    }

    /// Publish a batch to one shard, resolving the stream by name.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: &[Bytes],
    ) -> Result<usize> {
        let sample = t_should_sample();
        let lookup_start = t_now_if(sample);
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        if let Some(start) = lookup_start {
            let lookup_ns = start.elapsed().as_nanos() as u64;
            timings::record_lookup_ns(lookup_ns);
            t_histogram!("broker_publish_lookup_ns").record(lookup_ns as f64);
        }
        self.publish_batch_to_handle(&handle, payloads).await
    }

    /// Publish a batch through a handle resolved earlier.
    pub async fn publish_batch_to_handle(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<usize> {
        Ok(self
            .publish_batch_with_outcome(handle, payloads)
            .await?
            .subscribers)
    }

    /// Persist, append and fan out one batch, and report the log offsets it
    /// was assigned.
    ///
    /// Same path as [`Self::publish_batch_to_handle`]; the offsets are what a
    /// forwarding broker relays to the requester, which cannot see this log.
    ///
    /// The two phases back to back. A caller that needs the claim ordered
    /// against other publishes while the flushes overlap should call
    /// [`Broker::claim_publish`] and [`Broker::complete_publish`] itself.
    pub async fn publish_batch_with_outcome(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<PublishOutcome> {
        let claimed = self.claim_publish(handle, payloads).await?;
        self.complete_publish(claimed).await
    }

    /// Claim this batch's offsets and its place in the commit order.
    ///
    /// Split out of [`Broker::publish_batch_with_outcome`] so a caller that
    /// must preserve arrival order can do *this* part serially and let the
    /// durability wait overlap. That wait is a device flush under
    /// `FsyncMode::OnCommit` -- hundreds of microseconds against the handful
    /// this costs -- and group commit only has something to coalesce when
    /// several of them are in flight at once (#535).
    ///
    /// Offsets are consumed here, so the order calls return in *is* the order
    /// records land on disk. Complete every claim: dropping one releases its
    /// commit range, but the offsets it consumed stay consumed.
    pub async fn claim_publish(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<ClaimedPublish> {
        if !handle.state.active.load(Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }

        let sample = t_should_sample();
        let mut claimed = ClaimedPublish {
            handle: handle.clone(),
            payloads: payloads.to_vec(),
            durable: None,
            sample,
        };
        if payloads.is_empty() {
            return Ok(claimed);
        }

        if let Some(durable) = &handle.state.durable {
            let durable_start = t_now_if(sample);
            // Offsets are consumed here. The commit order has to be claimed
            // against them immediately, before the durability wait, because
            // from this point the records exist on disk and everything
            // behind them queues on this range. Claiming it only after a
            // *successful* wait stranded the stream: a failed or cancelled
            // publish abandoned its range, and every later publish waited
            // on a turn that could never arrive.
            let pending = durable.begin_append(payloads).await?;
            let turn = handle
                .state
                .commit_sequencer
                .reserve_owned(pending.first_offset(), pending.last_offset() + 1);
            claimed.durable = Some(ClaimedDurable {
                pending,
                turn,
                durable_start,
            });
        }
        Ok(claimed)
    }

    /// Make a [`ClaimedPublish`] durable, then append and fan it out.
    ///
    /// Safe to run concurrently with other completions on the same stream:
    /// the commit turn claimed in [`Broker::claim_publish`] is what keeps disk
    /// order, cursor order and delivery order in agreement, so overlapping the
    /// flushes does not disturb what anybody observes.
    pub async fn complete_publish(&self, claimed: ClaimedPublish) -> Result<PublishOutcome> {
        let ClaimedPublish {
            handle,
            payloads,
            durable,
            sample,
        } = claimed;
        let payloads = payloads.as_slice();
        let stream_state = &handle.state;

        if payloads.is_empty() {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        }

        let mut durable_first_offset = None;
        let _commit_turn = match (durable, &stream_state.durable) {
            (Some(claimed), Some(log)) => {
                let ClaimedDurable {
                    pending,
                    turn,
                    durable_start,
                } = claimed;
                durable_first_offset = Some(pending.first_offset());

                // From here every exit path -- `?`, a panic, or this future
                // being dropped mid-await -- releases the range through `turn`.
                log.commit(&pending).await?;
                turn.wait().await;
                // Replication waits on this. Under `Quorum` the publish is
                // about to block on a majority, so the shipping that produces
                // it should already be under way rather than waiting out a
                // tick.
                //
                // `notify_one`, not `notify_waiters`: the latter wakes only
                // waiters already registered, so an append landing while
                // replication is mid-pass would be lost and that record would
                // wait for the tick after all. `notify_one` leaves a permit, so
                // the next wait returns at once -- and it stores only one, so a
                // burst becomes a single extra pass rather than a storm.
                self.appended.notify_one();

                if let Some(start) = durable_start {
                    let durable_ns = start.elapsed().as_nanos() as u64;
                    t_histogram!("broker_publish_durable_append_ns").record(durable_ns as f64);
                }
                Some(turn)
            }
            _ => None,
        };

        let append_start = t_now_if(sample);
        // Append to the in-memory log so cursors can replay without touching
        // disk. A durable stream pins the sequence numbers to the offsets the
        // log assigned, so a cursor and a disk offset are the same value no
        // matter what happened to any publish in between.
        let senders =
            stream_state.append_batch_at(payloads, durable_first_offset, self.log_capacity);

        if let Some(start) = append_start {
            let append_ns = start.elapsed().as_nanos() as u64;
            timings::record_append_ns(append_ns);
            t_histogram!("broker_publish_append_ns").record(append_ns as f64);
        }

        let send_start = t_now_if(sample);
        let fanout = senders.len();
        #[cfg(feature = "telemetry")]
        let payload_bytes: usize = payloads.iter().map(Bytes::len).sum();
        #[cfg(feature = "telemetry")]
        let fanout_label = fanout.to_string();
        #[cfg(feature = "telemetry")]
        let payload_bytes_label = payload_bytes.to_string();
        #[cfg(not(feature = "telemetry"))]
        let _ = fanout;

        let fanout_start = t_now_if(sample);
        let mut closed_subscribers = Vec::new();
        let mut sent = 0usize;
        // The offsets the log just assigned travel with the batch, so live
        // delivery can report them exactly as replay does. Without this a
        // resumed subscriber gets offsets for its history and then nothing once
        // it reaches the live edge, which is precisely where it needs to start
        // checkpointing.
        let envelope = DeliveryEnvelope::with_base_offset(payloads, durable_first_offset);
        let item_count = envelope.len();
        let enqueue_start = t_now_if(sample);
        for subscriber in senders.iter() {
            match stream_state.subscriber_queue_policy {
                SubQueuePolicy::Block => {
                    if let Ok(permit) = subscriber.sender.reserve().await {
                        metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                        stream_state.increment_queue_depth(item_count);
                        permit.send(QueuedDelivery::new(
                            envelope.clone(),
                            Arc::clone(&stream_state.queued_items),
                        ));
                        sent += item_count;
                    } else {
                        closed_subscribers.push(subscriber.id);
                    }
                }
                SubQueuePolicy::DropNew | SubQueuePolicy::DropOld => {
                    match subscriber.sender.try_reserve() {
                        Ok(permit) => {
                            metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                            stream_state.increment_queue_depth(item_count);
                            permit.send(QueuedDelivery::new(
                                envelope.clone(),
                                Arc::clone(&stream_state.queued_items),
                            ));
                            sent += item_count;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            metrics::counter!("felix_subscribe_dropped_total")
                                .increment(item_count as u64);
                            metrics::counter!("felix_sub_queue_dropped_total")
                                .increment(item_count as u64);
                            if matches!(
                                stream_state.subscriber_queue_policy,
                                SubQueuePolicy::DropOld
                            ) {
                                metrics::counter!("felix_sub_queue_drop_old_emulated_total")
                                    .increment(item_count as u64);
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            closed_subscribers.push(subscriber.id);
                        }
                    }
                }
            }
        }
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_enqueue_ns(enqueue_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_enqueue_ns",
                    "fanout" => fanout_label.clone(),
                    "payload_bytes" => payload_bytes_label.clone()
                )
                .record(enqueue_ns as f64);
            }
        }

        if !closed_subscribers.is_empty() {
            closed_subscribers.sort_unstable();
            closed_subscribers.dedup();
            stream_state.remove_subscribers(&closed_subscribers);
        }
        if let Some(start) = fanout_start {
            let fanout_ns = start.elapsed().as_nanos() as u64;
            timings::record_fanout_ns(fanout_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_fanout_total_ns",
                    "fanout" => fanout_label.clone(),
                    "payload_bytes" => payload_bytes_label.clone()
                )
                .record(fanout_ns as f64);
            }
        }
        if let Some(start) = send_start {
            let send_ns = start.elapsed().as_nanos() as u64;
            timings::record_send_ns(send_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_send_ns",
                    "fanout" => fanout_label,
                    "payload_bytes" => payload_bytes_label
                )
                .record(send_ns as f64);
            }
        }
        Ok(PublishOutcome {
            subscribers: sent,
            // Inclusive, and contiguous by construction: a batch consumes one
            // run of offsets.
            offsets: durable_first_offset.map(|first| (first, first + item_count as u64 - 1)),
        })
    }

    /// A producer id no other producer of this broker holds.
    ///
    /// Random rather than counted, so ids from two brokers, or from one
    /// broker across a restart, do not collide with each other's sequences.
    pub fn new_producer_id(&self) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = self.producer_ids.build_hasher();
        hasher.write_u64(
            self.producer_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0),
        );
        // Zero is reserved for "no producer" in places that carry the id
        // beside an optional; never hand it out.
        hasher.finish().max(1)
    }

    /// Publish a batch that is appended once however many times it arrives.
    ///
    /// `sequence` is this producer's count of batches on this shard, from
    /// zero. The next expected is appended and remembered; one already
    /// appended is answered with its original outcome and nothing is written;
    /// a gap, a producer this broker does not know, or a sequence older than
    /// it remembers is refused with the matching [`BrokerError`], and nothing
    /// is written then either. See `stream/producers.rs`.
    pub async fn publish_batch_idempotent(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        sequence: u64,
        payloads: &[Bytes],
    ) -> Result<IdempotentOutcome> {
        let producers = &handle.state.producers;
        // The turn serialises this producer's batches, so two re-sends of one
        // sequence cannot both find it unappended. Taken before classifying,
        // and held across the append and the remembering, for that reason.
        let turn = producers.turn(producer_id, sequence)?;
        let _turn = turn.lock().await;
        match producers.classify(producer_id, sequence)? {
            Sequenced::Duplicate(outcome) => Ok(IdempotentOutcome {
                outcome,
                duplicate: true,
            }),
            Sequenced::Append => {
                let outcome = self.publish_batch_with_outcome(handle, payloads).await?;
                producers.remember(producer_id, sequence, outcome);
                Ok(IdempotentOutcome {
                    outcome,
                    duplicate: false,
                })
            }
        }
    }
}

/// What a publish did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Subscribers the batch was enqueued to.
    pub subscribers: usize,
    /// First and last log offset, inclusive. `None` for an ephemeral stream,
    /// which has no log and therefore no offsets to report.
    pub offsets: Option<(u64, u64)>,
}

/// A publish that has consumed its offsets and taken its place in the commit
/// order, but is not yet durable.
///
/// The point of the split is that the first half must be ordered and the second
/// half must not be: claiming is a few microseconds of offset arithmetic, while
/// completing waits on a device flush. Holding a claim does not hold the log --
/// other publishes claim and complete freely around it -- so several
/// completions overlap and group commit has something to coalesce (#535).
///
/// Complete it. Dropping a claim releases its commit range so later publishes
/// are not stranded, but the offsets it consumed are gone either way.
pub struct ClaimedPublish {
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    durable: Option<ClaimedDurable>,
    sample: bool,
}

impl ClaimedPublish {
    /// The first offset this batch consumed, on a durable stream.
    pub fn first_offset(&self) -> Option<u64> {
        self.durable.as_ref().map(|d| d.pending.first_offset())
    }
}

struct ClaimedDurable {
    pending: felix_storage::disk_log::PendingAppend,
    turn: felix_storage::CommitTurn<'static>,
    durable_start: Option<std::time::Instant>,
}

/// What an idempotent publish did: the batch's outcome, and whether that
/// outcome is from this call or from the batch's first arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotentOutcome {
    pub outcome: PublishOutcome,
    /// The batch had already been appended; nothing was written this time.
    pub duplicate: bool,
}

#[cfg(test)]
mod tests;
