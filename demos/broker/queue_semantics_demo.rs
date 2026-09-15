//! Queue semantics demo: what a consumer group guarantees, and what it costs.
//!
//! A stream and a queue are the same log read two ways. `subscribe` pushes every
//! record to every subscriber and forgets it. A **consumer group** hands each
//! record to one consumer, waits to be told it was handled, and hands it to
//! somebody else if it is not. That single difference is what this demo walks
//! through, in four acts:
//!
//! 1. **Work is distributed.** Two workers poll the same group and no job is
//!    handed to both.
//! 2. **A worker dies holding work.** Its claims lapse and another worker picks
//!    them up. Nothing is lost — and the demo shows the price, which is that a
//!    job handled but not acknowledged is handled *twice*. At-least-once is a
//!    guarantee about loss, not about duplicates.
//! 3. **A poison job.** One job always fails. It is retried up to
//!    `max_attempts`, then dead-lettered, and — the point of the whole
//!    mechanism — **the queue keeps draining past it**. Before there was an
//!    attempt bound, one bad record stalled a queue for ever.
//! 4. **The ledger**, so every claim above is a number rather than a story.
//!
//! ## Why there are no sleeps in here
//! `GroupReader::poll` takes the current time as an argument rather than reading
//! a clock. So the visibility timeout is driven forward explicitly, and the
//! demo's output is identical every run instead of depending on how loaded the
//! machine is. A demo that sometimes fails to show its own point is worse than
//! no demo.
//!
//! Run it with:
//!
//! ```text
//! cargo run --release -p broker --bin queue-semantics-demo
//! ```

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_broker::consumer_groups::ConsumerGroups;
use felix_broker::dead_letters::DeadLetters;
use felix_broker::group_reader::{Claimed, GroupKey, GroupReader};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const TENANT: &str = "t1";
const NAMESPACE: &str = "default";
const STREAM: &str = "jobs";
const GROUP: &str = "fulfilment";
const SHARD: u32 = 0;

/// Long enough that nothing lapses by accident, since the demo moves the clock
/// itself and never waits for one.
const VISIBILITY: Duration = Duration::from_secs(30);

/// Three attempts, then the record is somebody else's problem. Small so the
/// demo can show the whole retry sequence rather than a sample of it.
const MAX_ATTEMPTS: u32 = 3;

/// The job that never succeeds. Act 3 nacks this one every time it appears.
const POISON: &str = "job-06 (poison)";

fn log_config() -> LogConfig {
    LogConfig {
        // A group's cursor is only as durable as the log under it, and the
        // whole promise here is that a restart does not redeliver finished work.
        fsync_mode: FsyncMode::OnCommit,
        ..LogConfig::default()
    }
}

struct Queue {
    broker: Broker,
    reader: Arc<GroupReader>,
    key: GroupKey,
}

impl Queue {
    async fn open(dir: &std::path::Path) -> Result<Self> {
        let storage = DurableStorage::open(dir, log_config()).context("open durable storage")?;
        let groups = Arc::new(
            ConsumerGroups::open(dir.join("groups"), log_config()).context("open group cursors")?,
        );
        let dead = Arc::new(
            DeadLetters::open(dir.join("dead-letters"), log_config())
                .context("open dead letters")?,
        );

        let broker = Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_consumer_groups(groups, dead, VISIBILITY, MAX_ATTEMPTS);

        broker.register_tenant(TENANT).await?;
        broker.register_namespace(TENANT, NAMESPACE).await?;
        broker
            .register_stream(
                TENANT,
                NAMESPACE,
                STREAM,
                // A group's cursor is a projection over the stream's log, so
                // there has to be a log. A non-durable stream serves no groups.
                StreamMetadata {
                    durable: true,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await?;

        let reader = Arc::clone(
            broker
                .group_reader()
                .context("consumer groups were not wired up")?,
        );
        let key = GroupKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: SHARD,
            group: GROUP.to_string(),
        };
        Ok(Self {
            broker,
            reader,
            key,
        })
    }

    async fn publish(&self, job: &str) -> Result<()> {
        self.broker
            .publish(TENANT, NAMESPACE, STREAM, Bytes::from(job.to_string()))
            .await?;
        Ok(())
    }

    async fn poll(&self, max: usize, now: Instant) -> Result<Vec<Claimed>> {
        let storage = self
            .broker
            .durable_storage()
            .context("no durable storage")?;
        let log = storage.open_stream(TENANT, NAMESPACE, STREAM, SHARD)?;
        Ok(self.reader.poll(&self.key, &log, max, now).await?)
    }

    async fn ack(&self, offset: u64) -> Result<()> {
        Ok(self.reader.ack(&self.key, offset).await?)
    }

    async fn dead_lettered(&self) -> Result<Vec<u64>> {
        Ok(self.reader.dead_lettered(&self.key).await?)
    }
}

fn body(claimed: &Claimed) -> String {
    String::from_utf8_lossy(&claimed.payload).to_string()
}

fn step(title: &str) {
    println!("\n\x1b[1m{title}\x1b[0m");
    println!("{}", "─".repeat(title.len()));
}

/// Counts that back every claim the narration makes.
#[derive(Default)]
struct Ledger {
    published: usize,
    completed: usize,
    /// Deliveries that were not a record's first. This is the duplicate cost.
    redeliveries: usize,
    handled_twice: Vec<String>,
    /// Jobs each worker finished, to show the work actually spread.
    by_worker: HashMap<&'static str, usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().context("create temp dir")?;
    let queue = Queue::open(dir.path()).await?;
    let mut ledger = Ledger::default();

    // The clock the demo advances by hand. Everything below is deterministic
    // because nothing reads the real one.
    let mut now = Instant::now();

    println!("\x1b[1mFelix queue semantics\x1b[0m");
    println!("One log. `subscribe` reads it forward; a consumer group takes work from it.");

    // ---------------------------------------------------------------- act 1
    step("1. Five jobs, two workers, and no job done twice");

    // The poison job is published later, in act 3. Each act starts with the
    // queue in a state it fully accounts for, so no act can quietly consume
    // what the next one needs to demonstrate its point.
    for i in 1..=5 {
        queue.publish(&format!("job-{i:02}")).await?;
        ledger.published += 1;
    }
    println!("  published {} jobs to `{STREAM}`", ledger.published);

    let mut claimed_by: HashMap<u64, &'static str> = HashMap::new();
    for worker in ["worker-a", "worker-b", "worker-a"] {
        for claimed in &queue.poll(1, now).await? {
            println!(
                "  {worker:9} ← offset {:>2}  {}",
                claimed.offset,
                body(claimed)
            );
            if let Some(previous) = claimed_by.insert(claimed.offset, worker) {
                bail!(
                    "offset {} was handed to both {previous} and {worker}",
                    claimed.offset
                );
            }
            queue.ack(claimed.offset).await?;
            ledger.completed += 1;
            *ledger.by_worker.entry(worker).or_default() += 1;
        }
    }
    println!("\n  No offset appears twice: a claimed record is not handed to a second");
    println!("  consumer while the claim holds. That is the whole difference from");
    println!("  `subscribe`, which would have given every job to both workers.");

    // ---------------------------------------------------------------- act 2
    step("2. A worker dies holding work");

    let abandoned = queue.poll(4, now).await?;
    if abandoned.is_empty() {
        bail!("expected worker-b to claim the remaining jobs before dying");
    }
    for claimed in &abandoned {
        println!(
            "  worker-b  ← offset {:>2}  {}   … and then the process dies",
            claimed.offset,
            body(claimed)
        );
    }
    println!("\n  It handled them. It just never got to say so — a crash between");
    println!("  finishing the work and acknowledging it looks exactly like a crash");
    println!("  before starting.");

    // Every job is now either finished or claimed by the dead worker, so an
    // empty answer here means precisely "the live claims are being withheld"
    // and cannot be an artefact of this probe consuming something else.
    let too_soon = queue.poll(4, now).await?;
    if !too_soon.is_empty() {
        bail!(
            "a live claim was handed out again: offset {}",
            too_soon[0].offset
        );
    }
    println!(
        "\n  Polling again returns nothing: offsets {:?} are still claimed.",
        abandoned.iter().map(|c| c.offset).collect::<Vec<_>>()
    );

    now += VISIBILITY + Duration::from_secs(1);
    println!("  …the visibility timeout lapses ({VISIBILITY:?})…\n");

    let recovered = queue.poll(4, now).await?;
    if recovered.len() != abandoned.len() {
        bail!(
            "{} job(s) were abandoned but {} came back",
            abandoned.len(),
            recovered.len()
        );
    }
    for claimed in &recovered {
        println!(
            "  worker-a  ← offset {:>2}  {}   (attempt {})",
            claimed.offset,
            body(claimed),
            claimed.attempts
        );
        if claimed.attempts > 1 {
            ledger.redeliveries += 1;
            ledger.handled_twice.push(body(claimed));
        }
        queue.ack(claimed.offset).await?;
        ledger.completed += 1;
        *ledger.by_worker.entry("worker-a").or_default() += 1;
    }
    println!("\n  Nothing was lost. But look at the attempt counts: those jobs were");
    println!("  done by worker-b and done again by worker-a. \x1b[1mAt-least-once is a");
    println!("  promise about loss, not about duplicates\x1b[0m — a consumer has to be");
    println!("  idempotent, and `attempts > 1` is the signal it can key off.");

    // ---------------------------------------------------------------- act 3
    step("3. A job that always fails, and the jobs queued behind it");

    // Work keeps arriving. The poison job lands first, so the two behind it can
    // only run if the queue gets past it — which is the claim being tested.
    queue.publish(POISON).await?;
    ledger.published += 1;
    for i in 7..=8 {
        queue.publish(&format!("job-{i:02}")).await?;
        ledger.published += 1;
    }
    println!("  published `{POISON}`, then job-07 and job-08 behind it.");
    println!("  The poison job fails every time it is handled.\n");

    // `max = 1`, so each round returns the one record owed before any new one.
    // A nack redelivers immediately rather than waiting out the timeout, so the
    // whole retry sequence runs without the clock moving.
    let mut poison_offset = None;
    for _ in 0..MAX_ATTEMPTS {
        let batch = queue.poll(1, now).await?;
        let claimed = batch
            .first()
            .context("the poison job was owed and was not handed out")?;
        if body(claimed) != POISON {
            bail!(
                "expected the poison job to be owed first, got {}",
                body(claimed)
            );
        }
        poison_offset = Some(claimed.offset);
        println!(
            "  attempt {}/{}  offset {:>2}  → failed, handed back",
            claimed.attempts, MAX_ATTEMPTS, claimed.offset
        );
        queue.reader.nack(&queue.key, claimed.offset).await?;
    }
    let poison_offset = poison_offset.context("the poison job was never delivered")?;

    // Giving up happens on the *next* claim, so this poll is both the moment the
    // record is dead-lettered and the moment the queue moves on to what was
    // behind it. One call proves both halves.
    let after_poison = queue.poll(8, now).await?;

    let dead = queue.dead_lettered().await?;
    if !dead.contains(&poison_offset) {
        bail!("offset {poison_offset} exhausted its attempts but was not dead-lettered");
    }
    println!("\n  Dead-lettered at offset {poison_offset} after {MAX_ATTEMPTS} attempts.");
    println!("  The record itself is untouched — still in the log at that offset,");
    println!("  readable by an ordinary replay. A dead letter is a pointer, not a copy,");
    println!("  so nothing is duplicated and nothing is thrown away.\n");

    // The claim that actually matters: the queue is not stuck behind it.
    let mut drained = Vec::new();
    for claimed in &after_poison {
        println!(
            "  worker-a  ← offset {:>2}  {}",
            claimed.offset,
            body(claimed)
        );
        if claimed.attempts > 1 {
            ledger.redeliveries += 1;
        }
        queue.ack(claimed.offset).await?;
        ledger.completed += 1;
        drained.push(body(claimed));
        *ledger.by_worker.entry("worker-a").or_default() += 1;
    }
    if drained.len() != 2 {
        bail!("expected the 2 jobs behind the poison job to run, got {drained:?}");
    }
    println!(
        "\n  \x1b[1m{} job(s) queued behind it ran anyway.\x1b[0m Before there was an attempt",
        drained.len()
    );
    println!("  bound, the poison job would have been redelivered for ever and these");
    println!("  two would never have run — one bad record stalled the whole queue.");

    // ---------------------------------------------------------------- act 4
    step("4. The ledger");

    let dead = queue.dead_lettered().await?;
    println!("  published        {}", ledger.published);
    println!("  completed        {}", ledger.completed);
    println!("  dead-lettered    {}", dead.len());
    println!(
        "  redeliveries     {}  ← the cost of at-least-once",
        ledger.redeliveries
    );
    let mut workers: Vec<_> = ledger.by_worker.iter().collect();
    workers.sort();
    for (worker, count) in workers {
        println!("  {worker:14}   {count} job(s) finished");
    }

    if !ledger.handled_twice.is_empty() {
        println!("\n  handled twice: {}", ledger.handled_twice.join(", "));
    }

    // Every job is accounted for exactly once: finished, or given up on.
    if ledger.completed + dead.len() != ledger.published {
        bail!(
            "{} published, but {} completed + {} dead-lettered",
            ledger.published,
            ledger.completed,
            dead.len()
        );
    }
    println!(
        "\n  {} published = {} completed + {} dead-lettered. Every job is",
        ledger.published,
        ledger.completed,
        dead.len()
    );
    println!("  accounted for: finished, or explicitly given up on. Nothing is in limbo.");

    step("What this does not show");
    println!("  • One shard. A group is bound to the shard the caller names, so");
    println!("    consuming a multi-shard stream means polling each shard's group.");
    println!("  • One broker. A group's position and its dead-letter list are");
    println!("    replicated with the shard and survive a failover — the cluster");
    println!("    tests are where that is shown, not this single-broker demo.");
    println!("  • No competing brokers. Only the shard's leader serves its group,");
    println!("    which is what stops two brokers handing out the same record.");

    Ok(())
}
