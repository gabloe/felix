//! Latency demo binary for QUIC publish/subscribe.
//!
//! # Purpose
//! Runs a controlled pub/sub workload against an in-process broker and reports
//! latency percentiles and throughput for different payload sizes and fanout.
//!
//! # Notes
//! Intended for benchmarking and tuning; not part of production runtime.
use anyhow::{Context, Result};
use broker::timings as broker_timings;
use felix_broker::timings as broker_publish_timings;
use felix_broker::{Broker, StreamMetadata};
use felix_client::timings as client_timings;
use felix_client::{Client, ClientConfig, Publisher, Subscription};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

type DemoAuthResult = Result<(Arc<broker::auth::BrokerAuth>, Option<(String, String)>)>;

#[derive(Clone, Debug)]
struct DemoConfig {
    warmup: usize,
    total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    idle_ms: u64,
    pub_conns: usize,
    pub_streams_per_conn: usize,
    pub_sharding: Option<String>,
    pub_stream_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct DemoResult {
    publish_total: usize,
    sample_total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    received: usize,
    dropped: usize,
    delivered_total: usize,
    p50: Duration,
    p99: Duration,
    p999: Duration,
    throughput: f64,
    effective_throughput: f64,
    delivered_throughput: f64,
    delivered_per_sub_throughput: f64,
}

#[cfg(feature = "telemetry")]
#[derive(Clone, Copy, Debug)]
struct TimingSummary {
    client_pub_enqueue_wait_p50: Option<Duration>,
    client_pub_enqueue_wait_p99: Option<Duration>,
    client_pub_enqueue_wait_p999: Option<Duration>,
    client_encode_p50: Option<Duration>,
    client_encode_p99: Option<Duration>,
    client_binary_encode_p50: Option<Duration>,
    client_binary_encode_p99: Option<Duration>,
    client_binary_encode_p999: Option<Duration>,
    client_text_encode_p50: Option<Duration>,
    client_text_encode_p99: Option<Duration>,
    client_text_encode_p999: Option<Duration>,
    client_text_batch_build_p50: Option<Duration>,
    client_text_batch_build_p99: Option<Duration>,
    client_text_batch_build_p999: Option<Duration>,
    client_write_p50: Option<Duration>,
    client_write_p99: Option<Duration>,
    client_send_await_p50: Option<Duration>,
    client_send_await_p99: Option<Duration>,
    client_send_await_p999: Option<Duration>,
    client_sub_read_p50: Option<Duration>,
    client_sub_read_p99: Option<Duration>,
    client_sub_read_await_p50: Option<Duration>,
    client_sub_read_await_p99: Option<Duration>,
    client_sub_decode_p50: Option<Duration>,
    client_sub_decode_p99: Option<Duration>,
    client_sub_dispatch_p50: Option<Duration>,
    client_sub_dispatch_p99: Option<Duration>,
    client_sub_consumer_gap_p50: Option<Duration>,
    client_sub_consumer_gap_p99: Option<Duration>,
    client_sub_consumer_gap_p999: Option<Duration>,
    client_e2e_latency_p50: Option<Duration>,
    client_e2e_latency_p99: Option<Duration>,
    client_e2e_latency_p999: Option<Duration>,
    client_ack_read_p50: Option<Duration>,
    client_ack_read_p99: Option<Duration>,
    client_ack_decode_p50: Option<Duration>,
    client_ack_decode_p99: Option<Duration>,
    broker_decode_p50: Option<Duration>,
    broker_decode_p99: Option<Duration>,
    broker_fanout_p50: Option<Duration>,
    broker_fanout_p99: Option<Duration>,
    broker_ack_write_p50: Option<Duration>,
    broker_ack_write_p99: Option<Duration>,
    broker_quic_write_p50: Option<Duration>,
    broker_quic_write_p99: Option<Duration>,
    broker_sub_queue_wait_p50: Option<Duration>,
    broker_sub_queue_wait_p99: Option<Duration>,
    broker_sub_write_p50: Option<Duration>,
    broker_sub_write_p99: Option<Duration>,
    broker_sub_delivery_p50: Option<Duration>,
    broker_sub_delivery_p99: Option<Duration>,
    broker_sub_delivery_p999: Option<Duration>,
    broker_publish_lookup_p50: Option<Duration>,
    broker_publish_lookup_p99: Option<Duration>,
    broker_publish_append_p50: Option<Duration>,
    broker_publish_append_p99: Option<Duration>,
    broker_publish_send_p50: Option<Duration>,
    broker_publish_send_p99: Option<Duration>,
}

#[cfg(not(feature = "telemetry"))]
#[derive(Clone, Copy, Debug, Default)]
struct TimingSummary;

#[tokio::main]
async fn main() -> Result<()> {
    let (config, matrix, payloads, fanouts, all) = parse_args();
    println!("== Felix QUIC Pub/Sub Latency Demo ==");
    println!("Mode: QUIC broker protocol over felix-transport.");

    if all {
        run_all(config).await?;
    } else if matrix {
        for payload in payloads {
            for fanout in &fanouts {
                let mut case = config.clone();
                case.payload_bytes = payload;
                case.fanout = *fanout;
                let (result, summary) = run_case(case).await?;
                print_result(&result);
                print_timing_summary(summary);
            }
        }
    } else {
        let (result, summary) = run_case(config).await?;
        print_result(&result);
        print_timing_summary(summary);
    }
    Ok(())
}

fn parse_args() -> (DemoConfig, bool, Vec<usize>, Vec<usize>, bool) {
    let mut args = std::env::args().skip(1);
    let mut warmup = 1000;
    let mut total = 10000;
    let mut payload_bytes = 0;
    let mut fanout = 1;
    let mut batch_size = 1;
    let mut binary = false;
    let mut idle_ms = 2000;
    let mut pub_conns = 4;
    let mut pub_streams_per_conn = 2;
    let mut pub_sharding = None;
    let mut pub_stream_count = 1;
    let mut matrix = false;
    let mut all = true;
    let mut all_set = false;
    let mut payloads = None;
    let mut fanouts = None;
    let mut warmup_set = false;
    let mut total_set = false;
    let mut payload_set = false;
    let mut fanout_set = false;
    let mut batch_set = false;
    let mut idle_set = false;
    let mut binary_set = false;
    let mut pub_conns_set = false;
    let mut pub_streams_set = false;
    let mut pub_sharding_set = false;
    let mut pub_stream_count_set = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--matrix" => {
                matrix = true;
                all = false;
            }
            "--all" => {
                all = true;
                all_set = true;
            }
            "--binary" => {
                binary = true;
                binary_set = true;
            }
            "--warmup" => {
                if let Some(value) = args.next() {
                    warmup = value.parse().unwrap_or(warmup);
                    warmup_set = true;
                }
            }
            "--total" => {
                if let Some(value) = args.next() {
                    total = value.parse().unwrap_or(total);
                    total_set = true;
                }
            }
            "--payload" => {
                if let Some(value) = args.next() {
                    payload_bytes = value.parse().unwrap_or(payload_bytes);
                    payload_set = true;
                }
            }
            "--fanout" => {
                if let Some(value) = args.next() {
                    fanout = value.parse().unwrap_or(fanout);
                    fanout_set = true;
                }
            }
            "--batch" => {
                if let Some(value) = args.next() {
                    batch_size = value.parse().unwrap_or(batch_size);
                    batch_set = true;
                }
            }
            "--idle-ms" => {
                if let Some(value) = args.next() {
                    idle_ms = value.parse().unwrap_or(idle_ms);
                    idle_set = true;
                }
            }
            "--pub-conns" => {
                if let Some(value) = args.next() {
                    pub_conns = value.parse().unwrap_or(pub_conns);
                    pub_conns_set = true;
                }
            }
            "--pub-streams-per-conn" => {
                if let Some(value) = args.next() {
                    pub_streams_per_conn = value.parse().unwrap_or(pub_streams_per_conn);
                    pub_streams_set = true;
                }
            }
            "--pub-sharding" => {
                if let Some(value) = args.next() {
                    pub_sharding = Some(value);
                    pub_sharding_set = true;
                }
            }
            "--pub-stream-count" => {
                if let Some(value) = args.next() {
                    pub_stream_count = value.parse().unwrap_or(pub_stream_count);
                    pub_stream_count_set = true;
                }
            }
            "--payloads" => {
                payloads = args.next();
            }
            "--fanouts" => {
                fanouts = args.next();
            }
            _ => {}
        }
    }

    if matrix {
        if !warmup_set {
            warmup = 200;
        }
        if !total_set {
            total = 2000;
        }
    } else if !all_set
        && (warmup_set
            || total_set
            || payload_set
            || fanout_set
            || batch_set
            || idle_set
            || binary_set
            || pub_conns_set
            || pub_streams_set
            || pub_sharding_set
            || pub_stream_count_set)
    {
        all = false;
    }

    let payloads = payloads
        .as_deref()
        .map(parse_csv_usize)
        .unwrap_or_else(|| vec![0, 256, 1024, 4096]);
    let fanouts = fanouts
        .as_deref()
        .map(parse_csv_usize)
        .unwrap_or_else(|| vec![1, 10, 100]);

    (
        DemoConfig {
            warmup,
            total,
            payload_bytes,
            fanout,
            batch_size,
            binary,
            idle_ms,
            pub_conns,
            pub_streams_per_conn,
            pub_sharding,
            pub_stream_count,
        },
        matrix,
        payloads,
        fanouts,
        all,
    )
}

async fn run_case(config: DemoConfig) -> Result<(DemoResult, Option<TimingSummary>)> {
    unsafe {
        std::env::set_var("FELIX_FANOUT_BATCH", config.batch_size.to_string());
        std::env::set_var("FELIX_PUB_CONN_POOL", config.pub_conns.to_string());
        std::env::set_var(
            "FELIX_PUB_STREAMS_PER_CONN",
            config.pub_streams_per_conn.to_string(),
        );
        if let Some(ref sharding) = config.pub_sharding {
            std::env::set_var("FELIX_PUB_SHARDING", sharding);
        } else {
            std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        }
    }
    println!(
        "Warmup: {} messages | Measure: {} messages | payload {} bytes | fanout {} | batch {} | binary {} | pub conns {} | pub streams/conn {} | pub stream count {}",
        config.warmup,
        config.total,
        config.payload_bytes,
        config.fanout,
        config.batch_size,
        config.binary,
        config.pub_conns,
        config.pub_streams_per_conn,
        config.pub_stream_count
    );
    println!("{}", expectation_note(&config));
    let use_ack = config.batch_size <= 1 && !config.binary;
    let disable_timings = std::env::var("FELIX_DISABLE_TIMINGS")
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let collect_timings = (config.batch_size > 1 || use_ack) && !disable_timings;
    if collect_timings {
        let sample_every = std::env::var("FELIX_TIMING_SAMPLE_EVERY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1);
        client_timings::enable_collection(sample_every);
        broker_timings::enable_collection(sample_every);
        broker_publish_timings::enable_collection(sample_every);
    }
    client_timings::set_enabled(collect_timings);
    broker_timings::set_enabled(collect_timings);
    broker_publish_timings::set_enabled(collect_timings);

    let total_events = config.warmup + config.total;
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(total_events.saturating_add(1))?
            .with_log_capacity(total_events.saturating_add(1))?,
    );
    let event_queue_depth = total_events.max(1024);
    unsafe {
        std::env::set_var("FELIX_EVENT_QUEUE_DEPTH", event_queue_depth.to_string());
    }
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    for stream_index in 0..config.pub_stream_count.max(1) {
        broker
            .register_stream(
                "t1",
                "default",
                stream_name(stream_index, config.pub_stream_count).as_str(),
                StreamMetadata::default(),
            )
            .await?;
    }
    #[cfg(feature = "telemetry")]
    {
        broker::quic::reset_frame_counters();
        felix_client::reset_frame_counters();
    }
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let broker_config = broker::config::BrokerConfig::from_env()?;
    let (auth, auth_override) = resolve_demo_auth(&broker_config)?;
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
        auth,
    ));

    let client_config = apply_demo_auth(build_client_config(cert)?, auth_override);
    let client = Client::connect(addr, "localhost", client_config).await?;

    let delivered_total = Arc::new(AtomicUsize::new(0));
    let (primary_sub, drain_tasks) = setup_subscribers(
        &client,
        config.fanout,
        total_events,
        config.warmup,
        config.pub_stream_count,
        Arc::clone(&delivered_total),
    )
    .await?;

    let (warmup_tx, warmup_rx) = oneshot::channel();
    let idle_timeout = Duration::from_millis(config.idle_ms);
    let delivered_for_primary = Arc::clone(&delivered_total);
    let primary_total = per_stream_events(config.total, config.pub_stream_count.max(1), 0);
    let primary_warmup = per_stream_events(config.warmup, config.pub_stream_count.max(1), 0);
    let recv_task = tokio::spawn(async move {
        let mut results = Vec::with_capacity(primary_total);
        let mut seen = 0usize;
        let mut warmup_tx = Some(warmup_tx);
        if primary_warmup == 0
            && let Some(tx) = warmup_tx.take()
        {
            let _ = tx.send(());
        }
        let mut sub = primary_sub;
        while results.len() < primary_total {
            let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
            let event = match next {
                Ok(result) => result,
                Err(_) => break,
            };
            match event {
                Ok(Some(event)) => {
                    let sample = client_timings::should_sample();
                    let dispatch_start = sample.then(Instant::now);
                    if seen < primary_warmup {
                        seen += 1;
                        if seen == primary_warmup
                            && let Some(tx) = warmup_tx.take()
                        {
                            let _ = tx.send(());
                        }
                        continue;
                    }
                    let elapsed = decode_latency(event.payload.as_ref());
                    results.push(elapsed);
                    delivered_for_primary.fetch_add(1, Ordering::Relaxed);
                    if let Some(start) = dispatch_start {
                        let dispatch_ns = start.elapsed().as_nanos() as u64;
                        client_timings::record_sub_dispatch_ns(dispatch_ns);
                        metrics::histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        results
    });

    let publisher = client.publisher().await?;
    publish_batches(
        &publisher,
        config.payload_bytes,
        config.warmup,
        config.batch_size,
        config.binary,
        use_ack,
        config.pub_stream_count,
    )
    .await?;
    let _ = warmup_rx.await;

    let start = Instant::now();
    publish_batches(
        &publisher,
        config.payload_bytes,
        config.total,
        config.batch_size,
        config.binary,
        use_ack,
        config.pub_stream_count,
    )
    .await?;

    let mut latencies = recv_task.await.expect("join");
    let elapsed = start.elapsed();
    publisher.finish().await?;
    for mut task in drain_tasks {
        tokio::select! {
            _ = &mut task => {}
            _ = tokio::time::sleep(idle_timeout) => {
                task.abort();
            }
        }
    }
    server_task.abort();

    latencies.sort_unstable();
    let received = latencies.len();
    let summary = if collect_timings {
        let client_samples = client_timings::take_samples();
        let broker_samples = broker_timings::take_samples();
        let broker_publish_samples = broker_publish_timings::take_samples();
        Some(build_timing_summary(
            client_samples,
            broker_samples,
            broker_publish_samples,
        ))
    } else {
        None
    };
    let delivered_total = delivered_total.load(Ordering::Relaxed);
    let delivered_throughput = delivered_total as f64 / elapsed.as_secs_f64();
    let delivered_per_sub_throughput =
        (delivered_total as f64 / config.fanout.max(1) as f64) / elapsed.as_secs_f64();
    Ok((
        DemoResult {
            publish_total: config.total,
            sample_total: primary_total,
            payload_bytes: config.payload_bytes,
            fanout: config.fanout,
            batch_size: config.batch_size,
            binary: config.binary,
            received,
            dropped: primary_total.saturating_sub(received),
            delivered_total,
            p50: percentile(&latencies, 0.50),
            p99: percentile(&latencies, 0.99),
            p999: percentile(&latencies, 0.999),
            throughput: config.total as f64 / elapsed.as_secs_f64(),
            effective_throughput: received as f64 / elapsed.as_secs_f64(),
            delivered_throughput,
            delivered_per_sub_throughput,
        },
        summary,
    ))
}

async fn setup_subscribers(
    client: &Client,
    fanout: usize,
    total_events: usize,
    warmup: usize,
    stream_count: usize,
    delivered_total: Arc<AtomicUsize>,
) -> Result<(Subscription, Vec<tokio::task::JoinHandle<()>>)> {
    let mut drain_tasks = Vec::new();
    let mut primary_sub = None;
    let stream_count = stream_count.max(1);
    for stream_index in 0..stream_count {
        let per_stream_total = per_stream_events(total_events, stream_count, stream_index);
        let per_stream_warmup = per_stream_events(warmup, stream_count, stream_index);
        for _ in 0..fanout {
            let stream = stream_name(stream_index, stream_count);
            let sub = client.subscribe("t1", "default", stream.as_str()).await?;
            if primary_sub.is_none() {
                primary_sub = Some(sub);
            } else {
                let mut sub = sub;
                let idle_timeout = Duration::from_millis(2000);
                let delivered = Arc::clone(&delivered_total);
                drain_tasks.push(tokio::spawn(async move {
                    let mut remaining = per_stream_total;
                    let mut seen = 0usize;
                    while remaining > 0 {
                        let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
                        match next {
                            Ok(Ok(Some(_))) => {
                                remaining -= 1;
                                if seen >= per_stream_warmup {
                                    delivered.fetch_add(1, Ordering::Relaxed);
                                }
                                seen += 1;
                            }
                            Ok(Ok(None)) => break,
                            Ok(Err(_)) => break,
                            Err(_) => break,
                        }
                    }
                }));
            }
        }
    }
    Ok((primary_sub.expect("primary subscriber"), drain_tasks))
}

async fn publish_batches(
    publisher: &Publisher,
    payload_bytes: usize,
    total: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
    stream_count: usize,
) -> Result<()> {
    let mut remaining = total;
    let mut stream_index = 0usize;
    while remaining > 0 {
        let count = remaining.min(batch_size);
        publish_batch(
            publisher,
            payload_bytes,
            count,
            binary,
            use_ack,
            stream_index,
            stream_count,
        )
        .await?;
        stream_index = stream_index.wrapping_add(1);
        remaining -= count;
    }
    Ok(())
}

async fn publish_batch(
    publisher: &Publisher,
    payload_bytes: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
    stream_index: usize,
    stream_count: usize,
) -> Result<()> {
    let stream = stream_name(stream_index, stream_count);
    let payloads = (0..batch_size)
        .map(|_| encode_payload(payload_bytes))
        .collect::<Vec<_>>();
    if binary {
        return publisher
            .publish_batch_binary("t1", "default", stream.as_str(), &payloads)
            .await;
    }
    if batch_size == 1 {
        publisher
            .publish(
                "t1",
                "default",
                stream.as_str(),
                payloads[0].clone(),
                if use_ack {
                    AckMode::PerMessage
                } else {
                    AckMode::None
                },
            )
            .await?;
    } else {
        publisher
            .publish_batch(
                "t1",
                "default",
                stream.as_str(),
                payloads,
                if use_ack {
                    AckMode::PerBatch
                } else {
                    AckMode::None
                },
            )
            .await?;
    }
    Ok(())
}

fn stream_name(index: usize, stream_count: usize) -> String {
    if stream_count <= 1 {
        return "latency".to_string();
    }
    let index = index % stream_count;
    format!("latency-{index}")
}

fn per_stream_events(total: usize, stream_count: usize, stream_index: usize) -> usize {
    let base = total / stream_count;
    let extra = total % stream_count;
    base + usize::from(stream_index < extra)
}

fn print_result(result: &DemoResult) {
    println!(
        "Results (publish n = {}, sampled {}, received {}, dropped {}) payload={}B fanout={} batch={} binary={}:",
        result.publish_total,
        result.sample_total,
        result.received,
        result.dropped,
        result.payload_bytes,
        result.fanout,
        result.batch_size,
        result.binary
    );
    println!("  delivered total = {}", result.delivered_total);
    println!("  p50  = {}", format_duration(result.p50));
    println!("  p99  = {}", format_duration(result.p99));
    println!("  p999 = {}", format_duration(result.p999));
    println!("  throughput = {:.2} msg/s", result.throughput);
    println!(
        "  effective throughput = {:.2} msg/s",
        result.effective_throughput
    );
    println!(
        "  delivered throughput = {:.2} msg/s",
        result.delivered_throughput
    );
    println!(
        "  delivered per-sub throughput = {:.2} msg/s",
        result.delivered_per_sub_throughput
    );
}

#[cfg(feature = "telemetry")]
fn print_timing_summary(summary: Option<TimingSummary>) {
    let Some(summary) = summary else {
        return;
    };
    println!(
        "  timings: client_pub_enqueue_wait p50={} p99={} p999={} client_encode p50={} p99={} client_binary_encode p50={} p99={} p999={} client_text_encode p50={} p99={} p999={} client_text_batch_build p50={} p99={} p999={} client_write p50={} p99={} client_send_await p50={} p99={} p999={} client_sub_read p50={} p99={} client_sub_read_await p50={} p99={} client_sub_decode p50={} p99={} client_sub_dispatch p50={} p99={} client_sub_consumer_gap p50={} p99={} p999={} client_e2e_latency p50={} p99={} p999={} client_ack_read p50={} p99={} client_ack_decode p50={} p99={} broker_decode p50={} p99={} broker_ingress_enqueue p50={} p99={} broker_ack_write p50={} p99={} broker_quic_write p50={} p99={} broker_sub_queue_wait p50={} p99={} broker_sub_write p50={} p99={} broker_sub_delivery p50={} p99={} p999={} broker_publish_lookup p50={} p99={} broker_publish_append p50={} p99={} broker_publish_send p50={} p99={}",
        format_optional_duration(summary.client_pub_enqueue_wait_p50),
        format_optional_duration(summary.client_pub_enqueue_wait_p99),
        format_optional_duration(summary.client_pub_enqueue_wait_p999),
        format_optional_duration(summary.client_encode_p50),
        format_optional_duration(summary.client_encode_p99),
        format_optional_duration(summary.client_binary_encode_p50),
        format_optional_duration(summary.client_binary_encode_p99),
        format_optional_duration(summary.client_binary_encode_p999),
        format_optional_duration(summary.client_text_encode_p50),
        format_optional_duration(summary.client_text_encode_p99),
        format_optional_duration(summary.client_text_encode_p999),
        format_optional_duration(summary.client_text_batch_build_p50),
        format_optional_duration(summary.client_text_batch_build_p99),
        format_optional_duration(summary.client_text_batch_build_p999),
        format_optional_duration(summary.client_write_p50),
        format_optional_duration(summary.client_write_p99),
        format_optional_duration(summary.client_send_await_p50),
        format_optional_duration(summary.client_send_await_p99),
        format_optional_duration(summary.client_send_await_p999),
        format_optional_duration(summary.client_sub_read_p50),
        format_optional_duration(summary.client_sub_read_p99),
        format_optional_duration(summary.client_sub_read_await_p50),
        format_optional_duration(summary.client_sub_read_await_p99),
        format_optional_duration(summary.client_sub_decode_p50),
        format_optional_duration(summary.client_sub_decode_p99),
        format_optional_duration(summary.client_sub_dispatch_p50),
        format_optional_duration(summary.client_sub_dispatch_p99),
        format_optional_duration(summary.client_sub_consumer_gap_p50),
        format_optional_duration(summary.client_sub_consumer_gap_p99),
        format_optional_duration(summary.client_sub_consumer_gap_p999),
        format_optional_duration(summary.client_e2e_latency_p50),
        format_optional_duration(summary.client_e2e_latency_p99),
        format_optional_duration(summary.client_e2e_latency_p999),
        format_optional_duration(summary.client_ack_read_p50),
        format_optional_duration(summary.client_ack_read_p99),
        format_optional_duration(summary.client_ack_decode_p50),
        format_optional_duration(summary.client_ack_decode_p99),
        format_optional_duration(summary.broker_decode_p50),
        format_optional_duration(summary.broker_decode_p99),
        format_optional_duration(summary.broker_fanout_p50),
        format_optional_duration(summary.broker_fanout_p99),
        format_optional_duration(summary.broker_ack_write_p50),
        format_optional_duration(summary.broker_ack_write_p99),
        format_optional_duration(summary.broker_quic_write_p50),
        format_optional_duration(summary.broker_quic_write_p99),
        format_optional_duration(summary.broker_sub_queue_wait_p50),
        format_optional_duration(summary.broker_sub_queue_wait_p99),
        format_optional_duration(summary.broker_sub_write_p50),
        format_optional_duration(summary.broker_sub_write_p99),
        format_optional_duration(summary.broker_sub_delivery_p50),
        format_optional_duration(summary.broker_sub_delivery_p99),
        format_optional_duration(summary.broker_sub_delivery_p999),
        format_optional_duration(summary.broker_publish_lookup_p50),
        format_optional_duration(summary.broker_publish_lookup_p99),
        format_optional_duration(summary.broker_publish_append_p50),
        format_optional_duration(summary.broker_publish_append_p99),
        format_optional_duration(summary.broker_publish_send_p50),
        format_optional_duration(summary.broker_publish_send_p99),
    );
}

#[cfg(not(feature = "telemetry"))]
fn print_timing_summary(_summary: Option<TimingSummary>) {}

#[cfg(feature = "telemetry")]
fn print_frame_counters() -> (
    felix_client::FrameCountersSnapshot,
    broker::quic::FrameCountersSnapshot,
) {
    let broker = broker::quic::frame_counters_snapshot();
    let client = felix_client::frame_counters_snapshot();
    println!(
        "  frame_counters: client frames_in_ok={} frames_in_err={} frames_out_ok={} bytes_in={} bytes_out={} pub_out_ok={} pub_out_err={} pub_items_out_ok={} pub_items_out_err={} pub_batches_out_ok={} pub_batches_out_err={} sub_in_ok={} sub_items_in_ok={} sub_batches_in_ok={} ack_in_ok={} ack_items_in_ok={} binary_encode_reallocs={} text_encode_reallocs={}",
        client.frames_in_ok,
        client.frames_in_err,
        client.frames_out_ok,
        client.bytes_in,
        client.bytes_out,
        client.pub_frames_out_ok,
        client.pub_frames_out_err,
        client.pub_items_out_ok,
        client.pub_items_out_err,
        client.pub_batches_out_ok,
        client.pub_batches_out_err,
        client.sub_frames_in_ok,
        client.sub_items_in_ok,
        client.sub_batches_in_ok,
        client.ack_frames_in_ok,
        client.ack_items_in_ok,
        client.binary_encode_reallocs,
        client.text_encode_reallocs
    );
    println!(
        "  frame_counters: broker frames_in_ok={} frames_in_err={} frames_out_ok={} bytes_in={} bytes_out={} pub_in_ok={} pub_in_err={} pub_items_in_ok={} pub_items_in_err={} pub_batches_in_ok={} pub_batches_in_err={} ack_out_ok={} ack_items_out_ok={} sub_out_ok={} sub_items_out_ok={} sub_batches_out_ok={}",
        broker.frames_in_ok,
        broker.frames_in_err,
        broker.frames_out_ok,
        broker.bytes_in,
        broker.bytes_out,
        broker.pub_frames_in_ok,
        broker.pub_frames_in_err,
        broker.pub_items_in_ok,
        broker.pub_items_in_err,
        broker.pub_batches_in_ok,
        broker.pub_batches_in_err,
        broker.ack_frames_out_ok,
        broker.ack_items_out_ok,
        broker.sub_frames_out_ok,
        broker.sub_items_out_ok,
        broker.sub_batches_out_ok
    );
    (client, broker)
}

#[cfg(not(feature = "telemetry"))]
fn print_frame_counters() -> (
    felix_client::FrameCountersSnapshot,
    broker::quic::FrameCountersSnapshot,
) {
    println!("  frame_counters: telemetry disabled");
    (
        felix_client::frame_counters_snapshot(),
        broker::quic::frame_counters_snapshot(),
    )
}

#[cfg(feature = "telemetry")]
fn print_sanity_checks(
    config: &DemoConfig,
    result: &DemoResult,
    client: &felix_client::FrameCountersSnapshot,
    broker: &broker::quic::FrameCountersSnapshot,
) {
    let expected_items = config.warmup + config.total;
    let expected_batches = if config.batch_size == 0 {
        0
    } else {
        let warmup_batches = config.warmup.div_ceil(config.batch_size);
        let measure_batches = config.total.div_ceil(config.batch_size);
        warmup_batches + measure_batches
    };
    let expected_delivered = config.total * config.fanout;
    let use_ack = config.batch_size <= 1 && !config.binary;
    if client.pub_items_out_ok != expected_items as u64 {
        println!(
            "  sanity: pub_items_out_ok mismatch expected={} actual={}",
            expected_items, client.pub_items_out_ok
        );
    }
    if client.pub_batches_out_ok != expected_batches as u64 {
        println!(
            "  sanity: pub_batches_out_ok mismatch expected={} actual={}",
            expected_batches, client.pub_batches_out_ok
        );
    }
    if broker.pub_items_in_ok != client.pub_items_out_ok {
        println!(
            "  sanity: broker pub_items_in_ok mismatch client={} broker={}",
            client.pub_items_out_ok, broker.pub_items_in_ok
        );
    }
    if broker.pub_batches_in_ok != client.pub_batches_out_ok {
        println!(
            "  sanity: broker pub_batches_in_ok mismatch client={} broker={}",
            client.pub_batches_out_ok, broker.pub_batches_in_ok
        );
    }
    if result.delivered_total != expected_delivered {
        println!(
            "  sanity: delivered_total mismatch expected={} actual={}",
            expected_delivered, result.delivered_total
        );
    }
    if use_ack {
        if client.ack_items_in_ok == 0 {
            println!("  sanity: ack_items_in_ok expected > 0 (acks enabled)");
        }
    } else if client.ack_items_in_ok > 0 {
        println!(
            "  sanity: ack_items_in_ok expected 0 (acks disabled) actual={}",
            client.ack_items_in_ok
        );
    }
}

#[cfg(not(feature = "telemetry"))]
fn print_sanity_checks(
    _config: &DemoConfig,
    _result: &DemoResult,
    _client: &felix_client::FrameCountersSnapshot,
    _broker: &broker::quic::FrameCountersSnapshot,
) {
}

#[cfg(feature = "telemetry")]
fn build_timing_summary(
    client_samples: Option<client_timings::ClientTimingSamples>,
    broker_samples: Option<broker_timings::BrokerTimingSamples>,
    broker_publish_samples: Option<broker_publish_timings::BrokerPublishSamples>,
) -> TimingSummary {
    let (
        mut client_pub_enqueue_wait,
        mut client_encode,
        mut client_binary_encode,
        mut client_text_encode,
        mut client_text_batch_build,
        mut client_write,
        mut client_send_await,
        mut client_sub_read,
        mut client_sub_read_await,
        mut client_sub_decode,
        mut client_sub_dispatch,
        mut client_sub_consumer_gap,
        mut client_e2e_latency,
        mut client_ack_read,
        mut client_ack_decode,
    ) = client_samples.unwrap_or_else(|| {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    });
    let (
        mut broker_decode,
        mut broker_fanout,
        mut broker_ack,
        mut broker_quic_write,
        mut broker_sub_queue,
        mut broker_sub_write,
        mut broker_sub_delivery,
    ) = broker_samples.unwrap_or_else(|| {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    });
    let (mut broker_publish_lookup, mut broker_publish_append, mut broker_publish_send) =
        broker_publish_samples.unwrap_or_else(|| (Vec::new(), Vec::new(), Vec::new()));
    TimingSummary {
        client_pub_enqueue_wait_p50: percentile_ns(&mut client_pub_enqueue_wait, 0.50),
        client_pub_enqueue_wait_p99: percentile_ns(&mut client_pub_enqueue_wait, 0.99),
        client_pub_enqueue_wait_p999: percentile_ns(&mut client_pub_enqueue_wait, 0.999),
        client_encode_p50: percentile_ns(&mut client_encode, 0.50),
        client_encode_p99: percentile_ns(&mut client_encode, 0.99),
        client_binary_encode_p50: percentile_ns(&mut client_binary_encode, 0.50),
        client_binary_encode_p99: percentile_ns(&mut client_binary_encode, 0.99),
        client_binary_encode_p999: percentile_ns(&mut client_binary_encode, 0.999),
        client_text_encode_p50: percentile_ns(&mut client_text_encode, 0.50),
        client_text_encode_p99: percentile_ns(&mut client_text_encode, 0.99),
        client_text_encode_p999: percentile_ns(&mut client_text_encode, 0.999),
        client_text_batch_build_p50: percentile_ns(&mut client_text_batch_build, 0.50),
        client_text_batch_build_p99: percentile_ns(&mut client_text_batch_build, 0.99),
        client_text_batch_build_p999: percentile_ns(&mut client_text_batch_build, 0.999),
        client_write_p50: percentile_ns(&mut client_write, 0.50),
        client_write_p99: percentile_ns(&mut client_write, 0.99),
        client_send_await_p50: percentile_ns(&mut client_send_await, 0.50),
        client_send_await_p99: percentile_ns(&mut client_send_await, 0.99),
        client_send_await_p999: percentile_ns(&mut client_send_await, 0.999),
        client_sub_read_p50: percentile_ns(&mut client_sub_read, 0.50),
        client_sub_read_p99: percentile_ns(&mut client_sub_read, 0.99),
        client_sub_read_await_p50: percentile_ns(&mut client_sub_read_await, 0.50),
        client_sub_read_await_p99: percentile_ns(&mut client_sub_read_await, 0.99),
        client_sub_decode_p50: percentile_ns(&mut client_sub_decode, 0.50),
        client_sub_decode_p99: percentile_ns(&mut client_sub_decode, 0.99),
        client_sub_dispatch_p50: percentile_ns(&mut client_sub_dispatch, 0.50),
        client_sub_dispatch_p99: percentile_ns(&mut client_sub_dispatch, 0.99),
        client_sub_consumer_gap_p50: percentile_ns(&mut client_sub_consumer_gap, 0.50),
        client_sub_consumer_gap_p99: percentile_ns(&mut client_sub_consumer_gap, 0.99),
        client_sub_consumer_gap_p999: percentile_ns(&mut client_sub_consumer_gap, 0.999),
        client_e2e_latency_p50: percentile_ns(&mut client_e2e_latency, 0.50),
        client_e2e_latency_p99: percentile_ns(&mut client_e2e_latency, 0.99),
        client_e2e_latency_p999: percentile_ns(&mut client_e2e_latency, 0.999),
        client_ack_read_p50: percentile_ns(&mut client_ack_read, 0.50),
        client_ack_read_p99: percentile_ns(&mut client_ack_read, 0.99),
        client_ack_decode_p50: percentile_ns(&mut client_ack_decode, 0.50),
        client_ack_decode_p99: percentile_ns(&mut client_ack_decode, 0.99),
        broker_decode_p50: percentile_ns(&mut broker_decode, 0.50),
        broker_decode_p99: percentile_ns(&mut broker_decode, 0.99),
        broker_fanout_p50: percentile_ns(&mut broker_fanout, 0.50),
        broker_fanout_p99: percentile_ns(&mut broker_fanout, 0.99),
        broker_ack_write_p50: percentile_ns(&mut broker_ack, 0.50),
        broker_ack_write_p99: percentile_ns(&mut broker_ack, 0.99),
        broker_quic_write_p50: percentile_ns(&mut broker_quic_write, 0.50),
        broker_quic_write_p99: percentile_ns(&mut broker_quic_write, 0.99),
        broker_sub_queue_wait_p50: percentile_ns(&mut broker_sub_queue, 0.50),
        broker_sub_queue_wait_p99: percentile_ns(&mut broker_sub_queue, 0.99),
        broker_sub_write_p50: percentile_ns(&mut broker_sub_write, 0.50),
        broker_sub_write_p99: percentile_ns(&mut broker_sub_write, 0.99),
        broker_sub_delivery_p50: percentile_ns(&mut broker_sub_delivery, 0.50),
        broker_sub_delivery_p99: percentile_ns(&mut broker_sub_delivery, 0.99),
        broker_sub_delivery_p999: percentile_ns(&mut broker_sub_delivery, 0.999),
        broker_publish_lookup_p50: percentile_ns(&mut broker_publish_lookup, 0.50),
        broker_publish_lookup_p99: percentile_ns(&mut broker_publish_lookup, 0.99),
        broker_publish_append_p50: percentile_ns(&mut broker_publish_append, 0.50),
        broker_publish_append_p99: percentile_ns(&mut broker_publish_append, 0.99),
        broker_publish_send_p50: percentile_ns(&mut broker_publish_send, 0.50),
        broker_publish_send_p99: percentile_ns(&mut broker_publish_send, 0.99),
    }
}

#[cfg(not(feature = "telemetry"))]
fn build_timing_summary(
    _client_samples: Option<client_timings::ClientTimingSamples>,
    _broker_samples: Option<broker_timings::BrokerTimingSamples>,
    _broker_publish_samples: Option<broker_publish_timings::BrokerPublishSamples>,
) -> TimingSummary {
    TimingSummary
}

fn expectation_note(config: &DemoConfig) -> String {
    if config.batch_size <= 1 && !config.binary {
        "Expectation: latency-focused run; p999 <= 1 ms on localhost if not saturated.".to_string()
    } else {
        "Expectation: throughput-focused run; higher p999 is normal due to queueing.".to_string()
    }
}

async fn run_all(base: DemoConfig) -> Result<()> {
    println!("Running --all (default): latency-focused then throughput-focused.");
    let latency_payloads = [0, 256, 1024];
    let latency_fanouts = [1, 10];
    for payload in latency_payloads {
        for fanout in latency_fanouts {
            let mut case = base.clone();
            case.payload_bytes = payload;
            case.fanout = fanout;
            case.batch_size = 1;
            case.binary = base.binary;
            case.warmup = 200;
            case.total = 2000;
            let (result, summary) = run_case(case.clone()).await?;
            print_result(&result);
            print_timing_summary(summary);
            let (client_counters, broker_counters) = print_frame_counters();
            print_sanity_checks(&case, &result, &client_counters, &broker_counters);
        }
    }

    let throughput_payloads = [0, 1024, 4096];
    let throughput_fanouts = [1, 10];
    let binaries: &[bool] = if base.binary { &[true] } else { &[false, true] };
    for payload in throughput_payloads {
        for fanout in throughput_fanouts {
            for &binary in binaries {
                let mut case = base.clone();
                case.payload_bytes = payload;
                case.fanout = fanout;
                case.batch_size = 64;
                case.binary = binary;
                case.warmup = 200;
                case.total = 5000;
                let (result, summary) = run_case(case.clone()).await?;
                print_result(&result);
                print_timing_summary(summary);
                let (client_counters, broker_counters) = print_frame_counters();
                print_sanity_checks(&case, &result, &client_counters, &broker_counters);
            }
        }
    }
    let mut baseline = base.clone();
    baseline.payload_bytes = 256;
    baseline.fanout = 1;
    baseline.batch_size = 64;
    baseline.binary = base.binary;
    baseline.warmup = 200;
    baseline.total = 5000;
    baseline.pub_conns = 1;
    baseline.pub_streams_per_conn = 1;
    let (result, summary) = run_case(baseline.clone()).await?;
    print_result(&result);
    print_timing_summary(summary);

    let mut scaled = baseline;
    scaled.pub_conns = 4;
    scaled.pub_streams_per_conn = 2;
    let (result, summary) = run_case(scaled).await?;
    print_result(&result);
    print_timing_summary(summary);
    Ok(())
}

fn parse_csv_usize(input: &str) -> Vec<usize> {
    input
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .collect()
}

fn encode_payload(payload_bytes: usize) -> Vec<u8> {
    let timestamp = current_time_nanos();
    let mut buf = Vec::with_capacity(16 + payload_bytes);
    buf.extend_from_slice(&timestamp.to_be_bytes());
    if payload_bytes > 0 {
        buf.extend(std::iter::repeat_n(0u8, payload_bytes));
    }
    buf
}

fn decode_latency(bytes: &[u8]) -> Duration {
    let mut raw = [0u8; 16];
    raw.copy_from_slice(&bytes[..16]);
    let sent = u128::from_be_bytes(raw);
    let now = current_time_nanos();
    let delta = now.saturating_sub(sent);
    Duration::from_nanos(delta as u64)
}

fn current_time_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos()
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}

fn resolve_demo_auth(config: &broker::config::BrokerConfig) -> DemoAuthResult {
    if let Some(controlplane_url) = config.controlplane_url.clone() {
        return Ok((
            Arc::new(broker::auth::BrokerAuth::new(controlplane_url)),
            None,
        ));
    }

    let demo = broker::auth_demo::demo_auth_for_tenant("t1")?;
    Ok((demo.auth, Some((demo.tenant_id, demo.token))))
}

fn apply_demo_auth(
    mut config: ClientConfig,
    auth_override: Option<(String, String)>,
) -> ClientConfig {
    if let Some((tenant_id, token)) = auth_override {
        config.auth_tenant_id = Some(tenant_id);
        config.auth_token = Some(token);
    }
    config
}

fn percentile(values: &[Duration], p: f64) -> Duration {
    let len = values.len();
    if len == 0 {
        return Duration::from_nanos(0);
    }
    let idx = ((len - 1) as f64 * p).round() as usize;
    values[idx]
}

#[cfg(feature = "telemetry")]
fn percentile_ns(values: &mut [u64], p: f64) -> Option<Duration> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    Some(Duration::from_nanos(values[idx]))
}

#[cfg(feature = "telemetry")]
fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(format_duration)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_duration(duration: Duration) -> String {
    let micros = duration.as_micros();
    if micros >= 1000 {
        format!("{:.3} ms", micros as f64 / 1000.0)
    } else {
        format!("{micros} us")
    }
}
