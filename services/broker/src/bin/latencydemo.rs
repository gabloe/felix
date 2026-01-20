// Console demo that measures QUIC pub/sub latency distribution.
use anyhow::{Context, Result};
use broker::timings as broker_timings;
use felix_broker::timings as broker_publish_timings;
use felix_broker::{Broker, StreamMetadata};
use felix_client::timings as client_timings;
use felix_client::{Client, Publisher, Subscription};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
struct DemoConfig {
    warmup: usize,
    total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    idle_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct DemoResult {
    total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    received: usize,
    dropped: usize,
    p50: Duration,
    p99: Duration,
    p999: Duration,
    throughput: f64,
    effective_throughput: f64,
}

#[derive(Clone, Copy, Debug)]
struct TimingSummary {
    client_encode_p50: Option<Duration>,
    client_encode_p99: Option<Duration>,
    client_write_p50: Option<Duration>,
    client_write_p99: Option<Duration>,
    client_send_await_p50: Option<Duration>,
    client_send_await_p99: Option<Duration>,
    client_sub_read_p50: Option<Duration>,
    client_sub_read_p99: Option<Duration>,
    client_sub_decode_p50: Option<Duration>,
    client_sub_decode_p99: Option<Duration>,
    client_sub_dispatch_p50: Option<Duration>,
    client_sub_dispatch_p99: Option<Duration>,
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
    broker_publish_lookup_p50: Option<Duration>,
    broker_publish_lookup_p99: Option<Duration>,
    broker_publish_append_p50: Option<Duration>,
    broker_publish_append_p99: Option<Duration>,
    broker_publish_send_p50: Option<Duration>,
    broker_publish_send_p99: Option<Duration>,
}

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
                let mut case = config;
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
            || binary_set)
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
    }
    println!(
        "Warmup: {} messages | Measure: {} messages | payload {} bytes | fanout {} | batch {} | binary {}",
        config.warmup,
        config.total,
        config.payload_bytes,
        config.fanout,
        config.batch_size,
        config.binary
    );
    println!("{}", expectation_note(config));
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
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "latency", StreamMetadata::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let broker_config = broker::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;

    let (primary_sub, drain_tasks) =
        setup_subscribers(&client, config.fanout, total_events).await?;

    let (warmup_tx, warmup_rx) = oneshot::channel();
    let idle_timeout = Duration::from_millis(config.idle_ms);
    let recv_task = tokio::spawn(async move {
        let mut results = Vec::with_capacity(config.total);
        let mut seen = 0usize;
        let mut warmup_tx = Some(warmup_tx);
        if config.warmup == 0
            && let Some(tx) = warmup_tx.take()
        {
            let _ = tx.send(());
        }
        let mut sub = primary_sub;
        while results.len() < config.total {
            let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
            let event = match next {
                Ok(result) => result,
                Err(_) => break,
            };
            match event {
                Ok(Some(event)) => {
                    let sample = client_timings::should_sample();
                    let dispatch_start = sample.then(Instant::now);
                    if seen < config.warmup {
                        seen += 1;
                        if seen == config.warmup
                            && let Some(tx) = warmup_tx.take()
                        {
                            let _ = tx.send(());
                        }
                        continue;
                    }
                    let elapsed = decode_latency(event.payload.as_ref());
                    results.push(elapsed);
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
    )
    .await?;

    let mut latencies = recv_task.await.expect("join");
    let elapsed = start.elapsed();
    publisher.finish().await?;
    for task in drain_tasks {
        task.abort();
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
    Ok((
        DemoResult {
            total: config.total,
            payload_bytes: config.payload_bytes,
            fanout: config.fanout,
            batch_size: config.batch_size,
            binary: config.binary,
            received,
            dropped: config.total.saturating_sub(received),
            p50: percentile(&latencies, 0.50),
            p99: percentile(&latencies, 0.99),
            p999: percentile(&latencies, 0.999),
            throughput: config.total as f64 / elapsed.as_secs_f64(),
            effective_throughput: received as f64 / elapsed.as_secs_f64(),
        },
        summary,
    ))
}

async fn setup_subscribers(
    client: &Client,
    fanout: usize,
    total_events: usize,
) -> Result<(Subscription, Vec<tokio::task::JoinHandle<()>>)> {
    let mut drain_tasks = Vec::new();
    let mut primary_sub = None;
    for _ in 0..fanout {
        let sub = client.subscribe("t1", "default", "latency").await?;
        if primary_sub.is_none() {
            primary_sub = Some(sub);
        } else {
            let mut sub = sub;
            let idle_timeout = Duration::from_millis(2000);
            drain_tasks.push(tokio::spawn(async move {
                let mut remaining = total_events;
                while remaining > 0 {
                    let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
                    match next {
                        Ok(Ok(Some(_))) => {
                            remaining -= 1;
                        }
                        Ok(Ok(None)) => break,
                        Ok(Err(_)) => break,
                        Err(_) => break,
                    }
                }
            }));
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
) -> Result<()> {
    let mut remaining = total;
    while remaining > 0 {
        let count = remaining.min(batch_size);
        publish_batch(publisher, payload_bytes, count, binary, use_ack).await?;
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
) -> Result<()> {
    let payloads = (0..batch_size)
        .map(|_| encode_payload(payload_bytes))
        .collect::<Vec<_>>();
    if binary {
        return publisher
            .publish_batch_binary("t1", "default", "latency", &payloads)
            .await;
    }
    if batch_size == 1 {
        publisher
            .publish(
                "t1",
                "default",
                "latency",
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
                "latency",
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

fn print_result(result: &DemoResult) {
    println!(
        "Results (n = {}, received {}, dropped {}) payload={}B fanout={} batch={} binary={}:",
        result.total,
        result.received,
        result.dropped,
        result.payload_bytes,
        result.fanout,
        result.batch_size,
        result.binary
    );
    println!("  p50  = {}", format_duration(result.p50));
    println!("  p99  = {}", format_duration(result.p99));
    println!("  p999 = {}", format_duration(result.p999));
    println!("  throughput = {:.2} msg/s", result.throughput);
    println!(
        "  effective throughput = {:.2} msg/s",
        result.effective_throughput
    );
}

fn print_timing_summary(summary: Option<TimingSummary>) {
    let Some(summary) = summary else {
        return;
    };
    println!(
        "  timings: client_encode p50={} p99={} client_write p50={} p99={} client_send_await p50={} p99={} client_sub_read p50={} p99={} client_sub_decode p50={} p99={} client_sub_dispatch p50={} p99={} client_ack_read p50={} p99={} client_ack_decode p50={} p99={} broker_decode p50={} p99={} broker_ingress_enqueue p50={} p99={} broker_ack_write p50={} p99={} broker_quic_write p50={} p99={} broker_sub_queue_wait p50={} p99={} broker_sub_write p50={} p99={} broker_publish_lookup p50={} p99={} broker_publish_append p50={} p99={} broker_publish_send p50={} p99={}",
        format_optional_duration(summary.client_encode_p50),
        format_optional_duration(summary.client_encode_p99),
        format_optional_duration(summary.client_write_p50),
        format_optional_duration(summary.client_write_p99),
        format_optional_duration(summary.client_send_await_p50),
        format_optional_duration(summary.client_send_await_p99),
        format_optional_duration(summary.client_sub_read_p50),
        format_optional_duration(summary.client_sub_read_p99),
        format_optional_duration(summary.client_sub_decode_p50),
        format_optional_duration(summary.client_sub_decode_p99),
        format_optional_duration(summary.client_sub_dispatch_p50),
        format_optional_duration(summary.client_sub_dispatch_p99),
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
        format_optional_duration(summary.broker_publish_lookup_p50),
        format_optional_duration(summary.broker_publish_lookup_p99),
        format_optional_duration(summary.broker_publish_append_p50),
        format_optional_duration(summary.broker_publish_append_p99),
        format_optional_duration(summary.broker_publish_send_p50),
        format_optional_duration(summary.broker_publish_send_p99),
    );
}

fn build_timing_summary(
    client_samples: Option<client_timings::ClientTimingSamples>,
    broker_samples: Option<broker_timings::BrokerTimingSamples>,
    broker_publish_samples: Option<broker_publish_timings::BrokerPublishSamples>,
) -> TimingSummary {
    let (
        mut client_encode,
        mut client_write,
        mut client_send_await,
        mut client_sub_read,
        mut client_sub_decode,
        mut client_sub_dispatch,
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
        )
    });
    let (
        mut broker_decode,
        mut broker_fanout,
        mut broker_ack,
        mut broker_quic_write,
        mut broker_sub_queue,
        mut broker_sub_write,
    ) = broker_samples.unwrap_or_else(|| {
        (
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
        client_encode_p50: percentile_ns(&mut client_encode, 0.50),
        client_encode_p99: percentile_ns(&mut client_encode, 0.99),
        client_write_p50: percentile_ns(&mut client_write, 0.50),
        client_write_p99: percentile_ns(&mut client_write, 0.99),
        client_send_await_p50: percentile_ns(&mut client_send_await, 0.50),
        client_send_await_p99: percentile_ns(&mut client_send_await, 0.99),
        client_sub_read_p50: percentile_ns(&mut client_sub_read, 0.50),
        client_sub_read_p99: percentile_ns(&mut client_sub_read, 0.99),
        client_sub_decode_p50: percentile_ns(&mut client_sub_decode, 0.50),
        client_sub_decode_p99: percentile_ns(&mut client_sub_decode, 0.99),
        client_sub_dispatch_p50: percentile_ns(&mut client_sub_dispatch, 0.50),
        client_sub_dispatch_p99: percentile_ns(&mut client_sub_dispatch, 0.99),
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
        broker_publish_lookup_p50: percentile_ns(&mut broker_publish_lookup, 0.50),
        broker_publish_lookup_p99: percentile_ns(&mut broker_publish_lookup, 0.99),
        broker_publish_append_p50: percentile_ns(&mut broker_publish_append, 0.50),
        broker_publish_append_p99: percentile_ns(&mut broker_publish_append, 0.99),
        broker_publish_send_p50: percentile_ns(&mut broker_publish_send, 0.50),
        broker_publish_send_p99: percentile_ns(&mut broker_publish_send, 0.99),
    }
}

fn expectation_note(config: DemoConfig) -> String {
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
            let mut case = base;
            case.payload_bytes = payload;
            case.fanout = fanout;
            case.batch_size = 1;
            case.binary = base.binary;
            case.warmup = 200;
            case.total = 2000;
            let (result, summary) = run_case(case).await?;
            print_result(&result);
            print_timing_summary(summary);
        }
    }

    let throughput_payloads = [0, 1024, 4096];
    let throughput_fanouts = [1, 10];
    let binaries: &[bool] = if base.binary { &[true] } else { &[false, true] };
    for payload in throughput_payloads {
        for fanout in throughput_fanouts {
            for &binary in binaries {
                let mut case = base;
                case.payload_bytes = payload;
                case.fanout = fanout;
                case.batch_size = 64;
                case.binary = binary;
                case.warmup = 200;
                case.total = 5000;
                let (result, summary) = run_case(case).await?;
                print_result(&result);
                print_timing_summary(summary);
            }
        }
    }
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
    Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
}

fn percentile(values: &[Duration], p: f64) -> Duration {
    let len = values.len();
    if len == 0 {
        return Duration::from_nanos(0);
    }
    let idx = ((len - 1) as f64 * p).round() as usize;
    values[idx]
}

fn percentile_ns(values: &mut [u64], p: f64) -> Option<Duration> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    Some(Duration::from_nanos(values[idx]))
}

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
