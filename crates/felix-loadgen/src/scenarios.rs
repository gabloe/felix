//! The workloads, each against a remote cluster.
//!
//! Every scenario runs a warmup it discards, measures a fixed count, and
//! reports percentiles with spread — the local matrix discipline, at
//! distance. Publish payloads carry a 16-byte header (sequence, then this
//! process's monotonic nanos), so delivery latency is measured against one
//! clock: the load generator holds both ends of the pipe, which is the only
//! arrangement in which "publish to delivery" is a subtraction rather than a
//! clock-synchronisation problem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use felix_client::{Client, ClusterClient};
use felix_wire::AckMode;

use crate::stats::{Percentiles, Samples, emit_json, fmt_us};

pub(crate) struct Common {
    pub brokers: Vec<SocketAddr>,
    pub tenant: String,
    pub namespace: String,
    pub token: String,
    pub warmup: usize,
    pub total: usize,
    pub payload_bytes: usize,
    pub fanout: usize,
    pub batch: usize,
    pub concurrency: usize,
    pub environment: String,
    // Isolation probe: make the last `slow_subscribers` of the fanout dawdle
    // `slow_delay` per delivery, so a healthy subscriber (index 0, the sampled
    // one) and the publisher can be measured while others fall behind and drop.
    pub slow_subscribers: usize,
    pub slow_delay: Duration,
}

const HEADER: usize = 16;

fn payload(seq: u64, epoch: Instant, bytes: usize) -> Vec<u8> {
    let mut body = vec![0u8; HEADER.max(HEADER + bytes.saturating_sub(HEADER))];
    // The header rides *inside* the requested payload size when it fits, so a
    // "256-byte" case moves 256 bytes; only sizes below the header grow.
    let len = bytes.max(HEADER);
    body.truncate(len);
    body[0..8].copy_from_slice(&seq.to_be_bytes());
    body[8..16].copy_from_slice(&(epoch.elapsed().as_nanos() as u64).to_be_bytes());
    body
}

fn read_header(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() < HEADER {
        return None;
    }
    let seq = u64::from_be_bytes(payload[0..8].try_into().ok()?);
    let t0 = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    Some((seq, t0))
}

/// A momentary "the pipe was not ready" the instrument retries rather than
/// dies on, and *counts* rather than hides — a nonzero `publish_retries` in the
/// JSON is data about the cluster's readiness, not noise to bury:
///
/// - **routing convergence** — a non-owner ingress answers "stream not found"
///   while its control-plane routing snapshot is unsettled, so a forwarded
///   publish fails for a window;
/// - **client backpressure** — the publisher's bounded queue is momentarily
///   full because acks have not drained, which is the client telling the
///   caller to slow down, not a failure to deliver.
///
/// Neither is a completed round trip, so the retry is excluded from the
/// latency sample the way a warmup message is.
fn is_retriable_transient(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("stream not found")
        || text.contains("cannot be subscribed")
        || text.contains("queue full")
}

async fn cluster(common: &Common) -> Result<ClusterClient> {
    let config = crate::tls::client_config(&common.tenant, &common.token)?;
    ClusterClient::connect(&common.brokers, "localhost", config)
        .await
        .context("connect a cluster client")
}

async fn client(common: &Common, addr: SocketAddr) -> Result<Client> {
    let config = crate::tls::client_config(&common.tenant, &common.token)?;
    Client::connect(addr, "localhost", config)
        .await
        .with_context(|| format!("connect to {addr}"))
}

/// Publish/subscribe: `fanout` subscribers, one publisher, and both latencies
/// that matter — the acknowledgement round trip (batch 1) and the
/// publish-to-delivery path (always).
pub(crate) async fn pubsub(common: &Common, stream: &str, binary: bool) -> Result<()> {
    let epoch = Instant::now();
    let use_ack = common.batch <= 1 && !binary;

    let publisher_client = client(common, common.brokers[0]).await?;
    let publisher = publisher_client.publisher().await?;

    // Readiness pre-flight, *before* subscribers exist so its sentinels reach
    // nobody and pollute no count. A broker that does not yet hold the routing
    // snapshot for this stream cannot forward a publish to the owner and
    // answers "stream not found" — and the several brokers learn a fresh
    // stream registration through the control-plane watch at slightly
    // different times, so one success proves only that one path converged.
    // Require a sustained run of them, so every broker's snapshot has caught
    // up before the measured window opens: inside the run, a publish error is
    // an error, never a cold snapshot.
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    payload(u64::MAX, epoch, common.payload_bytes),
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(()) => consecutive += 1,
                Err(err) if Instant::now() < deadline => {
                    consecutive = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let _ = err;
                }
                Err(err) => return Err(err.context("cluster never became ready to publish")),
            }
        }
    }

    // Subscribers now, each following redirects to the shard's owner, and
    // each at the live tail — so the pre-flight sentinel above is already
    // behind them. Held for the whole run: dropping the client closes the
    // delivery stream.
    let subs_cluster = cluster(common).await?;
    let mut receivers = Vec::new();
    for _ in 0..common.fanout.max(1) {
        let (client, subscription) = subs_cluster
            .subscribe(&common.tenant, &common.namespace, stream)
            .await
            .context("subscribe")?;
        receivers.push((client, subscription));
    }

    let expected_per_sub = (common.warmup + common.total) as u64;
    let delivered = Arc::new(AtomicU64::new(0));
    let mut collectors = Vec::new();
    // The slow subscribers are the highest indices, so index 0 — the one whose
    // delivery latency is sampled — is always healthy.
    let fanout = common.fanout.max(1);
    let slow_from = fanout.saturating_sub(common.slow_subscribers);
    for (index, (client, mut subscription)) in receivers.into_iter().enumerate() {
        let delivered = Arc::clone(&delivered);
        let warmup = common.warmup as u64;
        let is_slow = common.slow_subscribers > 0 && index >= slow_from;
        let slow_delay = common.slow_delay;
        collectors.push(tokio::spawn(async move {
            // Sample delivery latency on the first subscriber only; count
            // deliveries on all. Sampling everywhere multiplies memory for a
            // curve that fanout does not change shape of.
            let mut samples = Samples::with_capacity(if index == 0 { 200_000 } else { 0 });
            let mut received = 0u64;
            let mut highest_seq = 0u64;
            while received < expected_per_sub {
                match tokio::time::timeout(Duration::from_secs(30), subscription.next_event()).await
                {
                    Ok(Ok(Some(event))) => {
                        received += 1;
                        delivered.fetch_add(1, Ordering::Relaxed);
                        if let Some((seq, t0)) = read_header(&event.payload) {
                            highest_seq = highest_seq.max(seq);
                            if index == 0 && seq >= warmup {
                                let now = epoch.elapsed().as_nanos() as u64;
                                samples.record(Duration::from_nanos(now.saturating_sub(t0)));
                            }
                        }
                        // A slow subscriber drains behind the publisher; under
                        // DropNew its queue overflows and the broker sheds its
                        // events, which is exactly the isolation being probed.
                        if is_slow && !slow_delay.is_zero() {
                            tokio::time::sleep(slow_delay).await;
                        }
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => break,
                    // Quiet too long: the run is over as far as this
                    // subscriber can tell. `unaccounted` reports the gap.
                    Err(_) => break,
                }
            }
            drop(client);
            (samples, received, highest_seq, is_slow)
        }));
    }

    // Publish. Ack latency is per-publish round trip; without acks the loop
    // paces itself only by the client's own backpressure.
    let mut ack_samples = Samples::with_capacity(common.total);
    let mut publish_retries = 0u64;
    let publish_started = Instant::now();
    let mut measured_started = None;
    let mut seq = 0u64;
    let total = common.warmup + common.total;
    while (seq as usize) < total {
        if seq as usize == common.warmup {
            measured_started = Some(Instant::now());
        }
        if common.batch <= 1 {
            let ack = if use_ack {
                AckMode::PerMessage
            } else {
                AckMode::None
            };
            // The latency recorded is the final, successful attempt: a routing
            // transient the instrument had to retry is an artifact of a
            // snapshot swap, not the system's steady-state round trip, so it
            // is excluded from the sample the way a warmup message is — and
            // counted, so the exclusion is visible.
            let started = loop {
                let body = payload(seq, epoch, common.payload_bytes);
                let at = Instant::now();
                match publisher
                    .publish(&common.tenant, &common.namespace, stream, body, ack)
                    .await
                {
                    Ok(()) => break at,
                    Err(err) if is_retriable_transient(&err) => {
                        publish_retries += 1;
                        if publish_retries > (total as u64).max(1) {
                            return Err(err.context("routing never stabilised"));
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(err) => return Err(err.context(format!("publish seq {seq}"))),
                }
            };
            if use_ack && seq as usize >= common.warmup {
                ack_samples.record(started.elapsed());
            }
            seq += 1;
        } else {
            let count = common.batch.min(total - seq as usize);
            loop {
                let batch: Vec<Vec<u8>> = (0..count)
                    .map(|i| payload(seq + i as u64, epoch, common.payload_bytes))
                    .collect();
                match publisher
                    .publish_batch(
                        &common.tenant,
                        &common.namespace,
                        stream,
                        batch,
                        AckMode::None,
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(err) if is_retriable_transient(&err) => {
                        publish_retries += 1;
                        if publish_retries > (total as u64).max(1) {
                            return Err(err.context("routing never stabilised"));
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(err) => {
                        return Err(err.context(format!("publish batch at seq {seq}")));
                    }
                }
            }
            seq += count as u64;
        }
    }
    let publish_elapsed = publish_started.elapsed();
    let measured_elapsed = measured_started
        .map(|at| at.elapsed())
        .unwrap_or(publish_elapsed);

    let mut delivery_samples = Samples::default();
    let mut received_total = 0u64;
    let mut healthy_received = 0u64;
    let mut slow_received = 0u64;
    for collector in collectors {
        let (samples, received, _highest, is_slow) = collector.await.context("collector")?;
        delivery_samples.merge(samples);
        received_total += received;
        if is_slow {
            slow_received += received;
        } else {
            healthy_received += received;
        }
    }
    let slow_count = common.slow_subscribers.min(fanout);
    let healthy_count = fanout - slow_count;

    let expected_total = expected_per_sub * common.fanout.max(1) as u64;
    let unaccounted = expected_total.saturating_sub(received_total);
    let publish_throughput = total as f64 / publish_elapsed.as_secs_f64();
    let measured_throughput = common.total as f64 / measured_elapsed.as_secs_f64();
    let delivered_throughput = received_total as f64 / publish_elapsed.as_secs_f64();
    let per_sub = delivered_throughput / common.fanout.max(1) as f64;

    let ack = if use_ack {
        Some(ack_samples.percentiles())
    } else {
        None
    };
    let delivery = if delivery_samples.is_empty() {
        None
    } else {
        Some(delivery_samples.percentiles())
    };
    // The latency the matrix files: acknowledgement round trip when the run
    // has one (the request-latency configuration), delivery latency otherwise.
    let headline = ack.or(delivery).unwrap_or(Percentiles {
        p50_us: 0,
        p99_us: 0,
        p999_us: 0,
        max_us: 0,
    });

    // The stdout shape scripts/perf already parses.
    println!(
        "Results (publish n = {}, sampled {}, received {}, unaccounted {}, payload {} bytes, fanout {}, batch {}, binary {})",
        total,
        common.total,
        received_total,
        unaccounted,
        common.payload_bytes,
        common.fanout,
        common.batch,
        binary,
    );
    println!("  delivered total = {received_total}");
    println!("  p50 = {}", fmt_us(headline.p50_us));
    println!("  p99 = {}", fmt_us(headline.p99_us));
    println!("  p999 = {}", fmt_us(headline.p999_us));
    println!("  throughput = {publish_throughput:.1} msg/s");
    println!("  effective throughput = {measured_throughput:.1} msg/s");
    println!("  delivered throughput = {delivered_throughput:.1} msg/s");
    println!("  delivered per-sub throughput = {per_sub:.1} msg/s");
    if common.slow_subscribers > 0 {
        println!(
            "  isolation: {healthy_count} healthy sub(s) got {healthy_received} ({}/sub of {expected_per_sub} expected); {slow_count} slow sub(s) got {slow_received} ({}/sub) — slow-delay {}ms",
            if healthy_count > 0 {
                healthy_received / healthy_count as u64
            } else {
                0
            },
            if slow_count > 0 {
                slow_received / slow_count as u64
            } else {
                0
            },
            common.slow_delay.as_millis(),
        );
    }
    if let (Some(ack), Some(delivery)) = (ack, delivery) {
        println!(
            "  delivery (publish -> subscriber): p50 = {}, p99 = {}, p999 = {} (ack latency above)",
            fmt_us(delivery.p50_us),
            fmt_us(delivery.p99_us),
            fmt_us(delivery.p999_us),
        );
        let _ = ack;
    }

    emit_json(&serde_json::json!({
        "scenario": "pubsub",
        "environment": common.environment,
        "stream": stream,
        "payload_bytes": common.payload_bytes,
        "fanout": common.fanout,
        "batch": common.batch,
        "binary": binary,
        "warmup": common.warmup,
        "total": common.total,
        "received": received_total,
        "unaccounted": unaccounted,
        "publish_retries": publish_retries,
        "publish_throughput_msg_s": publish_throughput,
        "effective_throughput_msg_s": measured_throughput,
        "delivered_throughput_msg_s": delivered_throughput,
        "slow_subscribers": common.slow_subscribers,
        "slow_delay_ms": common.slow_delay.as_millis() as u64,
        "healthy_subscribers": healthy_count,
        "healthy_received": healthy_received,
        "slow_received": slow_received,
        "healthy_received_per_sub": if healthy_count > 0 { healthy_received / healthy_count as u64 } else { 0 },
        "slow_received_per_sub": if slow_count > 0 { slow_received / slow_count as u64 } else { 0 },
        "expected_per_sub": expected_per_sub,
        "ack_latency_us": ack.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
            "samples": common.total,
        })),
        "delivery_latency_us": delivery.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
        })),
    }));
    Ok(())
}

/// Request/response scenarios share one engine: `concurrency` workers, each a
/// sequential loop of round trips against its own routed path.
async fn round_trips<F, Fut>(
    common: &Common,
    label: &str,
    op: Arc<F>,
) -> Result<(Percentiles, f64, u64)>
where
    F: Fn(Arc<Client>, u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let per_worker = (common.warmup + common.total).div_ceil(common.concurrency.max(1));
    let warmup_per_worker = common.warmup.div_ceil(common.concurrency.max(1));
    let started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..common.concurrency.max(1) {
        // One client per worker, spread across the brokers: the routed path —
        // possibly through a non-owner that forwards — is the deployment's
        // path, and measuring only the owner would flatter it.
        let addr = common.brokers[worker % common.brokers.len()];
        let client = Arc::new(client(common, addr).await?);
        let op = Arc::clone(&op);
        workers.push(tokio::spawn(async move {
            let mut samples = Samples::with_capacity(per_worker);
            for i in 0..per_worker {
                let key = (worker * per_worker + i) as u64;
                let at = Instant::now();
                op(Arc::clone(&client), key).await?;
                if i >= warmup_per_worker {
                    samples.record(at.elapsed());
                }
            }
            Ok::<Samples, anyhow::Error>(samples)
        }));
    }
    let mut all = Samples::default();
    for worker in workers {
        all.merge(worker.await.context("worker")??);
    }
    let elapsed = started.elapsed();
    let done = all.len() as u64;
    let throughput = (per_worker * common.concurrency.max(1)) as f64 / elapsed.as_secs_f64();
    let percentiles = all.percentiles();
    println!(
        "{label}: n = {done}, concurrency = {}, p50 = {}, p99 = {}, p999 = {}, max = {}, throughput = {throughput:.1} op/s",
        common.concurrency.max(1),
        fmt_us(percentiles.p50_us),
        fmt_us(percentiles.p99_us),
        fmt_us(percentiles.p999_us),
        fmt_us(percentiles.max_us),
    );
    Ok((percentiles, throughput, done))
}

/// Cache put then get, reported separately: a put pays the log append and a
/// get pays the index-plus-read, and folding them together hides both.
pub(crate) async fn cache(common: &Common, cache: &str) -> Result<()> {
    let value = bytes::Bytes::from(vec![0u8; common.payload_bytes]);
    let (tenant, namespace, name) = scope(common, cache);
    let put_value = value.clone();
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (put, put_tp, put_n) = round_trips(
        common,
        "cache put",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c, v) = (t.clone(), ns.clone(), c.clone(), put_value.clone());
            async move {
                client
                    .cache_put(&t, &ns, &c, &format!("k{key}"), v, None)
                    .await
            }
        }),
    )
    .await?;
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (get, get_tp, get_n) = round_trips(
        common,
        "cache get",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client.cache_get(&t, &ns, &c, &format!("k{key}")).await?;
                Ok(())
            }
        }),
    )
    .await?;

    emit_json(&serde_json::json!({
        "scenario": "cache",
        "environment": common.environment,
        "cache": name,
        "payload_bytes": common.payload_bytes,
        "concurrency": common.concurrency.max(1),
        "put": { "n": put_n, "throughput_op_s": put_tp,
                 "latency_us": { "p50": put.p50_us, "p99": put.p99_us, "p999": put.p999_us, "max": put.max_us } },
        "get": { "n": get_n, "throughput_op_s": get_tp,
                 "latency_us": { "p50": get.p50_us, "p99": get.p99_us, "p999": get.p999_us, "max": get.max_us } },
    }));
    Ok(())
}

/// Counter add then get. The add is the composed-semantics headline: one
/// round trip that both applies the delta and answers with the sum.
pub(crate) async fn counter(common: &Common, cache: &str) -> Result<()> {
    let (tenant, namespace, name) = scope(common, cache);
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (add, add_tp, add_n) = round_trips(
        common,
        "counter add",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client
                    .counter_add(&t, &ns, &c, &format!("c{}", key % 128), 1)
                    .await?;
                Ok(())
            }
        }),
    )
    .await?;
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (get, get_tp, get_n) = round_trips(
        common,
        "counter get",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client
                    .counter_get(&t, &ns, &c, &format!("c{}", key % 128))
                    .await?;
                Ok(())
            }
        }),
    )
    .await?;

    emit_json(&serde_json::json!({
        "scenario": "counter",
        "environment": common.environment,
        "cache": name,
        "concurrency": common.concurrency.max(1),
        "add": { "n": add_n, "throughput_op_s": add_tp,
                 "latency_us": { "p50": add.p50_us, "p99": add.p99_us, "p999": add.p999_us, "max": add.max_us } },
        "get": { "n": get_n, "throughput_op_s": get_tp,
                 "latency_us": { "p50": get.p50_us, "p99": get.p99_us, "p999": get.p999_us, "max": get.max_us } },
    }));
    Ok(())
}

/// Keyed watch: `fanout` watchers on one key, a writer putting `total` values
/// through it, and the latency from each put landing to each watcher seeing
/// it — the composed-semantics fanout claim, measured.
pub(crate) async fn watch(common: &Common, cache: &str) -> Result<()> {
    let epoch = Instant::now();
    let (tenant, namespace, name) = scope(common, cache);
    let key = "watched";

    // A watch is served by the owner and redirected elsewhere; follow by
    // trying each broker, exactly as a redirect-following client would.
    let mut watches = Vec::new();
    for _ in 0..common.fanout.max(1) {
        let mut opened = None;
        for addr in &common.brokers {
            let candidate = client(common, *addr).await?;
            match candidate
                .watch_cache(
                    &tenant,
                    &namespace,
                    &name,
                    felix_client::CacheWatchFilter::Key(key.to_string()),
                    None,
                )
                .await
            {
                Ok(watch) => {
                    opened = Some((candidate, watch));
                    break;
                }
                Err(err) if err.downcast_ref::<felix_client::NotLeaderError>().is_some() => {
                    continue;
                }
                Err(err) => return Err(err.context("open watch")),
            }
        }
        watches.push(opened.context("no broker served the watch")?);
    }

    let expected = (common.warmup + common.total) as u64;
    let mut collectors = Vec::new();
    for (index, (client, mut watch)) in watches.into_iter().enumerate() {
        let warmup = common.warmup as u64;
        collectors.push(tokio::spawn(async move {
            let mut samples = Samples::with_capacity(200_000);
            let mut received = 0u64;
            while received < expected {
                match tokio::time::timeout(Duration::from_secs(30), watch.recv()).await {
                    Ok(Some(felix_client::CacheWatchItem::Change(change))) => {
                        received += 1;
                        if let Some(value) = &change.value
                            && let Some((seq, t0)) = read_header(value)
                            && seq >= warmup
                        {
                            let now = epoch.elapsed().as_nanos() as u64;
                            samples.record(Duration::from_nanos(now.saturating_sub(t0)));
                        }
                    }
                    Ok(Some(felix_client::CacheWatchItem::Lagged { resume_from })) => {
                        bail!("watcher {index} lagged at offset {resume_from}: raise the pace or lower fanout");
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            drop(client);
            Ok::<(Samples, u64), anyhow::Error>((samples, received))
        }));
    }

    let writer = client(common, common.brokers[0]).await?;
    for seq in 0..expected {
        let body = payload(seq, epoch, common.payload_bytes);
        writer
            .cache_put(
                &tenant,
                &namespace,
                &name,
                key,
                bytes::Bytes::from(body),
                None,
            )
            .await
            .with_context(|| format!("put seq {seq}"))?;
    }

    let mut all = Samples::default();
    let mut received_total = 0u64;
    for collector in collectors {
        let (samples, received) = collector.await.context("watcher")??;
        all.merge(samples);
        received_total += received;
    }
    let expected_total = expected * common.fanout.max(1) as u64;
    let percentiles = all.percentiles();
    println!(
        "watch delivery: watchers = {}, puts = {expected}, delivered = {received_total}/{expected_total}, p50 = {}, p99 = {}, p999 = {}",
        common.fanout.max(1),
        fmt_us(percentiles.p50_us),
        fmt_us(percentiles.p99_us),
        fmt_us(percentiles.p999_us),
    );
    emit_json(&serde_json::json!({
        "scenario": "watch",
        "environment": common.environment,
        "cache": name,
        "watchers": common.fanout.max(1),
        "payload_bytes": common.payload_bytes,
        "puts": expected,
        "delivered": received_total,
        "expected": expected_total,
        "delivery_latency_us": { "p50": percentiles.p50_us, "p99": percentiles.p99_us,
                                  "p999": percentiles.p999_us, "max": percentiles.max_us },
    }));
    Ok(())
}

fn scope(common: &Common, cache: &str) -> (String, String, String) {
    (
        common.tenant.clone(),
        common.namespace.clone(),
        cache.to_string(),
    )
}

/// True when a group poll was refused because this broker does not lead the
/// shard — the redirect a consumer follows to find the owner.
fn is_not_leader(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<felix_client::NotLeaderError>().is_some() {
        return true;
    }
    let text = format!("{err:#}").to_lowercase();
    // A broker that does not lead a shard refuses a group poll and names the
    // owner: "group poll not served: shard 0 of <stream> is served by <node>".
    text.contains("not leader")
        || text.contains("not the leader")
        || text.contains("does not lead")
        || text.contains("not served")
        || text.contains("served by")
}

/// Fold one batch of delivered records into the latency sample and the seen-set.
/// Deduplicates by header sequence (a redelivery is counted, never re-measured)
/// and returns the highest offset in the batch, which is what a cumulative ack
/// finishes. A free function so the drain loop can borrow `samples`/`seen`
/// mutably without closure gymnastics.
fn account_group(
    records: &[felix_wire::GroupRecord],
    warmup: u64,
    epoch: Instant,
    samples: &mut Samples,
    seen: &mut std::collections::HashSet<u64>,
    redeliveries: &mut u64,
) -> u64 {
    let mut max_off = 0u64;
    for rec in records {
        if rec.attempts > 1 {
            *redeliveries += 1;
        }
        if let Some((seq, t0)) = read_header(&rec.payload)
            && seen.insert(seq)
            && seq >= warmup
        {
            let now = epoch.elapsed().as_nanos() as u64;
            samples.record(Duration::from_nanos(now.saturating_sub(t0)));
        }
        if rec.offset > max_off {
            max_off = rec.offset;
        }
    }
    max_off
}

/// Queue semantics: a consumer group draining a stream. Publish `warmup+total`
/// records into `stream`, then poll them through one consumer group and ack —
/// measuring enqueue-to-delivery latency (the record header's publish timestamp
/// against poll time, one clock) and the drain throughput. Redeliveries
/// (`attempts > 1`) are counted, not hidden. A cumulative ack of each batch's
/// highest offset finishes it, which is how a real drain settles a run of work.
pub(crate) async fn queue(common: &Common, stream: &str) -> Result<()> {
    let epoch = Instant::now();
    let group = "perf-cg";
    let shard = 0u32;
    let expected = (common.warmup + common.total) as u64;
    let warmup = common.warmup as u64;

    // Enqueue the whole run first (acked, so every record is durably in the log
    // before the drain we are measuring begins).
    let publisher_client = client(common, common.brokers[0]).await?;
    let publisher = publisher_client.publisher().await?;
    for seq in 0..expected {
        let body = payload(seq, epoch, common.payload_bytes);
        loop {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    body.clone(),
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(()) => break,
                Err(err) if is_retriable_transient(&err) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => return Err(err.context(format!("queue publish seq {seq}"))),
            }
        }
    }

    let mut samples = Samples::with_capacity(common.total);
    let mut seen = std::collections::HashSet::new();
    let mut redeliveries = 0u64;
    let started = Instant::now();

    // Only the shard leader serves its group; find it, and account for the
    // first batch it hands back — a poll leases records, so they must be acked.
    let mut consumer = None;
    for addr in &common.brokers {
        let candidate = client(common, *addr).await?;
        match candidate
            .group_poll_wait(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                512,
                Duration::from_secs(2),
            )
            .await
        {
            Ok(records) => {
                let max_off = account_group(
                    &records,
                    warmup,
                    epoch,
                    &mut samples,
                    &mut seen,
                    &mut redeliveries,
                );
                if !records.is_empty() {
                    candidate
                        .group_ack(
                            &common.tenant,
                            &common.namespace,
                            stream,
                            shard,
                            group,
                            max_off,
                        )
                        .await
                        .context("group ack")?;
                }
                consumer = Some(candidate);
                break;
            }
            Err(err) if is_not_leader(&err) => continue,
            Err(err) => return Err(err.context("group poll (owner probe)")),
        }
    }
    let consumer = consumer.context("no broker served the group")?;

    let mut idle = 0;
    while (seen.len() as u64) < expected {
        let records = consumer
            .group_poll_wait(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                512,
                Duration::from_secs(5),
            )
            .await
            .context("group poll")?;
        if records.is_empty() {
            idle += 1;
            if idle >= 3 {
                break; // nothing left after several bounded waits: drained
            }
            continue;
        }
        idle = 0;
        let max_off = account_group(
            &records,
            warmup,
            epoch,
            &mut samples,
            &mut seen,
            &mut redeliveries,
        );
        consumer
            .group_ack(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                max_off,
            )
            .await
            .context("group ack")?;
    }

    let elapsed = started.elapsed();
    let delivered = seen.len() as u64;
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        delivered as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let p = samples.percentiles();
    println!(
        "queue drain: delivered = {delivered}/{expected}, redeliveries = {redeliveries}, p50 = {}, p99 = {}, p999 = {}, throughput = {throughput:.1} msg/s",
        fmt_us(p.p50_us),
        fmt_us(p.p99_us),
        fmt_us(p.p999_us),
    );
    emit_json(&serde_json::json!({
        "scenario": "queue",
        "environment": common.environment,
        "stream": stream,
        "group": group,
        "payload_bytes": common.payload_bytes,
        "published": expected,
        "delivered": delivered,
        "redeliveries": redeliveries,
        "drain_throughput_msg_s": throughput,
        "delivery_latency_us": { "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us },
    }));
    Ok(())
}

/// Retained delivery (#349): the MQTT-style join-and-hold-roster. Seed a roster
/// of `total` retained keys, then open a retained watch and measure the
/// time-to-complete-state — how long a late joiner takes to receive the whole
/// roster before it is live. Run it at `--total` 100 / 1000 / 10000 to trace
/// the curve the design doc asks for. Each roster size gets its own key prefix,
/// so the retained replay is exactly the roster and nothing else in the cache.
pub(crate) async fn retained(common: &Common, cache: &str) -> Result<()> {
    let (tenant, namespace, name) = scope(common, cache);
    let roster = common.total.max(1);
    let prefix = format!("roster-{roster}/");

    // Seed the roster: one retained value per key.
    let writer = client(common, common.brokers[0]).await?;
    let value = bytes::Bytes::from(vec![0u8; common.payload_bytes.max(1)]);
    for i in 0..roster {
        writer
            .cache_put(
                &tenant,
                &namespace,
                &name,
                &format!("{prefix}{i}"),
                value.clone(),
                None,
            )
            .await
            .with_context(|| format!("seed roster key {i}"))?;
    }

    // Join: open a retained watch (owner serves; others redirect) and time how
    // long until the full roster has replayed.
    let mut opened = None;
    for addr in &common.brokers {
        let candidate = client(common, *addr).await?;
        match candidate
            .watch_cache_retained(
                &tenant,
                &namespace,
                &name,
                felix_client::CacheWatchFilter::Prefix(prefix.clone()),
            )
            .await
        {
            Ok(watch) => {
                opened = Some((candidate, watch));
                break;
            }
            Err(err) if err.downcast_ref::<felix_client::NotLeaderError>().is_some() => continue,
            Err(err) => return Err(err.context("open retained watch")),
        }
    }
    let (held, mut watch) = opened.context("no broker served the retained watch")?;

    let started = Instant::now();
    let mut received = 0u64;
    while received < roster as u64 {
        match tokio::time::timeout(Duration::from_secs(60), watch.recv()).await {
            Ok(Some(felix_client::CacheWatchItem::Change(_))) => received += 1,
            Ok(Some(felix_client::CacheWatchItem::Lagged { .. })) => continue,
            Ok(None) | Err(_) => break,
        }
    }
    let elapsed = started.elapsed();
    drop(held);

    let complete = received >= roster as u64;
    println!(
        "retained join: roster = {roster}, received = {received}/{roster}, time-to-complete-state = {}",
        fmt_us(elapsed.as_micros() as u64),
    );
    emit_json(&serde_json::json!({
        "scenario": "retained",
        "environment": common.environment,
        "cache": name,
        "roster": roster,
        "received": received,
        "complete": complete,
        "payload_bytes": common.payload_bytes,
        "time_to_complete_state_us": elapsed.as_micros() as u64,
    }));
    Ok(())
}

/// Aggregate ingest ceiling — the write throughput measured the way a
/// multi-partition system quotes it. `concurrency` publishers, each on its own
/// connection spread across the brokers, hammer `stream` (give it as many
/// shards as brokers, or more) with fire-and-forget **binary** batches and no
/// subscribers, so nothing on the delivery side can false-bottleneck the
/// number. Reports aggregate msg/s and MB/s, and the per-publisher figure so
/// scaling is visible. A single publisher on one shard is the least-parallel
/// configuration possible; this is the opposite, and it is what actually
/// stresses the brokers.
pub(crate) async fn ingest(common: &Common, stream: &str) -> Result<()> {
    let publishers = common.concurrency.max(1);
    let per = (common.total / publishers).max(1);
    let batch = common.batch.max(1);
    let payload_bytes = common.payload_bytes.max(1);

    // Readiness pre-flight on one connection, so a cold routing snapshot does
    // not land inside the measured window (same discipline as `pubsub`).
    {
        let pf = client(common, common.brokers[0]).await?;
        let publisher = pf.publisher().await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    vec![0u8; payload_bytes],
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(()) => consecutive += 1,
                Err(_) if Instant::now() < deadline => {
                    consecutive = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(err) => return Err(err.context("cluster never became ready to ingest")),
            }
        }
    }

    let started = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..publishers {
        // Each publisher owns a connection to a different broker in round-robin,
        // so the write load is spread across the fleet, not funnelled through
        // one ingress.
        let addr = common.brokers[p % common.brokers.len()];
        let tenant = common.tenant.clone();
        let namespace = common.namespace.clone();
        let token = common.token.clone();
        let stream = stream.to_string();
        tasks.push(tokio::spawn(async move {
            let config = crate::tls::client_config(&tenant, &token)?;
            let client = Client::connect(addr, "localhost", config)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            let publisher = client.publisher().await?;
            let template = vec![0u8; payload_bytes];
            let mut sent = 0usize;
            let mut retries = 0u64;
            while sent < per {
                let this = batch.min(per - sent);
                let payloads: Vec<Vec<u8>> = std::iter::repeat_with(|| template.clone())
                    .take(this)
                    .collect();
                match publisher
                    .publish_batch(&tenant, &namespace, &stream, payloads, AckMode::None)
                    .await
                {
                    Ok(()) => sent += this,
                    Err(err) if is_retriable_transient(&err) => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(err) => return Err(err.context("ingest publish")),
                }
            }
            Ok::<(usize, u64), anyhow::Error>((sent, retries))
        }));
    }

    let mut published = 0usize;
    let mut retries = 0u64;
    for task in tasks {
        let (sent, r) = task.await.context("publisher task")??;
        published += sent;
        retries += r;
    }
    let elapsed = started.elapsed();
    let msg_s = published as f64 / elapsed.as_secs_f64();
    let mb_s = msg_s * payload_bytes as f64 / 1_000_000.0;
    println!(
        "ingest: publishers = {publishers}, published = {published}, {msg_s:.0} msg/s, {mb_s:.1} MB/s, publish_retries = {retries}",
    );
    emit_json(&serde_json::json!({
        "scenario": "ingest",
        "environment": common.environment,
        "stream": stream,
        "publishers": publishers,
        "batch": batch,
        "payload_bytes": payload_bytes,
        "published": published,
        "publish_retries": retries,
        "throughput_msg_s": msg_s,
        "throughput_mb_s": mb_s,
        "per_publisher_msg_s": msg_s / publishers as f64,
    }));
    Ok(())
}
