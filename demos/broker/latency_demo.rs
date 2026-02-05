//! Latency demo binary for QUIC publish/subscribe.
//!
//! # Purpose
//! Runs a controlled pub/sub workload against an in-process broker and reports
//! latency percentiles and throughput for different payload sizes and fanout.
//!
//! # Notes
//! Intended for benchmarking and tuning; not part of production runtime.
//!
//! Throughput fairness experiments:
//! - `--pub-yield-every-batches N` yields publisher task every N batches.
//! - `--sub-dedicated-thread` runs the primary subscriber on a dedicated
//!   current-thread Tokio runtime to isolate scheduler contention.
use anyhow::{Context, Result};
use broker::timings as broker_timings;
use felix_broker::timings as broker_publish_timings;
use felix_broker::{Broker, StreamMetadata};
use felix_client::timings as client_timings;
use felix_client::{Client, ClientConfig, PublishSharding, Publisher, Subscription};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
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
    sub_conns: usize,
    sub_writer_lanes: Option<usize>,
    sub_lane_shard: Option<String>,
    sub_delivery_shaping: bool,
    pub_yield_every_batches: usize,
    sub_dedicated_thread: bool,
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
    client_sub_poll_gap_p50: Option<Duration>,
    client_sub_poll_gap_p99: Option<Duration>,
    client_sub_poll_gap_p999: Option<Duration>,
    client_sub_queue_wait_p50: Option<Duration>,
    client_sub_queue_wait_p99: Option<Duration>,
    client_sub_time_in_queue_p50: Option<Duration>,
    client_sub_time_in_queue_p99: Option<Duration>,
    client_sub_time_in_queue_p999: Option<Duration>,
    client_sub_runtime_gap_p50: Option<Duration>,
    client_sub_runtime_gap_p99: Option<Duration>,
    client_sub_runtime_gap_p999: Option<Duration>,
    client_sub_delivery_chan_wait_p50: Option<Duration>,
    client_sub_delivery_chan_wait_p99: Option<Duration>,
    client_sub_delivery_chan_wait_p999: Option<Duration>,
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
    broker_sub_prefix_build_p50: Option<Duration>,
    broker_sub_prefix_build_p99: Option<Duration>,
    broker_sub_write_p50: Option<Duration>,
    broker_sub_write_p99: Option<Duration>,
    broker_sub_write_await_p50: Option<Duration>,
    broker_sub_write_await_p99: Option<Duration>,
    broker_sub_delivery_p50: Option<Duration>,
    broker_sub_delivery_p99: Option<Duration>,
    broker_sub_delivery_p999: Option<Duration>,
    broker_publish_lookup_p50: Option<Duration>,
    broker_publish_lookup_p99: Option<Duration>,
    broker_publish_append_p50: Option<Duration>,
    broker_publish_append_p99: Option<Duration>,
    broker_publish_fanout_p50: Option<Duration>,
    broker_publish_fanout_p99: Option<Duration>,
    broker_publish_enqueue_p50: Option<Duration>,
    broker_publish_enqueue_p99: Option<Duration>,
    broker_publish_send_p50: Option<Duration>,
    broker_publish_send_p99: Option<Duration>,
}

#[cfg(not(feature = "telemetry"))]
#[derive(Clone, Copy, Debug, Default)]
struct TimingSummary;

async fn run_demo(
    config: DemoConfig,
    matrix: bool,
    payloads: Vec<usize>,
    fanouts: Vec<usize>,
    all: bool,
) -> Result<()> {
    println!("== Felix QUIC Pub/Sub Latency Demo ==");
    println!("Mode: QUIC broker protocol over felix-transport.");
    println!(
        "Note: subscriber/event delivery is always binary; publish_fastpath toggles publish encoding path."
    );

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

#[cfg(feature = "telemetry")]
async fn run_smoke(mut base: DemoConfig) -> Result<()> {
    base.warmup = 200;
    base.total = 2000;
    base.payload_bytes = 1024;
    base.fanout = 10;
    base.batch_size = 64;
    base.binary = false;
    base.pub_conns = 4;
    base.pub_streams_per_conn = 2;
    base.pub_stream_count = 1;

    let mut baseline = base.clone();
    baseline.sub_dedicated_thread = false;
    let (baseline_result, baseline_summary) = run_case(baseline).await?;
    let baseline_summary = baseline_summary.ok_or_else(|| {
        anyhow::anyhow!("smoke mode requires telemetry timing summary for baseline run")
    })?;

    let mut dedicated = base;
    dedicated.sub_dedicated_thread = true;
    let (dedicated_result, dedicated_summary) = run_case(dedicated).await?;
    let dedicated_summary = dedicated_summary.ok_or_else(|| {
        anyhow::anyhow!("smoke mode requires telemetry timing summary for dedicated run")
    })?;

    println!(
        "Smoke summary: baseline p99={} dedicated p99={} | baseline sub_read_await_p99={} dedicated sub_read_await_p99={} | baseline sub_dispatch_p99={} dedicated sub_dispatch_p99={}",
        format_duration(baseline_result.p99),
        format_duration(dedicated_result.p99),
        baseline_summary
            .client_sub_read_await_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        dedicated_summary
            .client_sub_read_await_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        baseline_summary
            .client_sub_dispatch_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        dedicated_summary
            .client_sub_dispatch_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
    );

    let baseline_read = baseline_summary
        .client_sub_read_await_p99
        .unwrap_or(Duration::MAX);
    let dedicated_read = dedicated_summary
        .client_sub_read_await_p99
        .unwrap_or(Duration::MAX);
    let dedicated_dispatch = dedicated_summary
        .client_sub_dispatch_p99
        .unwrap_or(Duration::MAX);

    let threshold = Duration::from_millis(50);
    if dedicated_read > threshold || dedicated_dispatch > threshold {
        anyhow::bail!(
            "smoke failed: dedicated subscriber tails too high (read_await_p99={}, dispatch_p99={})",
            format_duration(dedicated_read),
            format_duration(dedicated_dispatch),
        );
    }
    if dedicated_read > baseline_read.mul_f64(1.10) {
        anyhow::bail!(
            "smoke failed: dedicated subscriber did not improve read_await_p99 enough (baseline={}, dedicated={})",
            format_duration(baseline_read),
            format_duration(dedicated_read),
        );
    }
    Ok(())
}

#[cfg(not(feature = "telemetry"))]
async fn run_smoke(_base: DemoConfig) -> Result<()> {
    anyhow::bail!("--smoke requires telemetry feature");
}

#[tokio::main]
async fn main() -> Result<()> {
    let (config, matrix, payloads, fanouts, all, smoke, compare_sub_delivery_shaping) =
        parse_args();
    if compare_sub_delivery_shaping {
        run_compare_sub_delivery_shaping(config).await
    } else if smoke {
        run_smoke(config).await
    } else {
        run_demo(config, matrix, payloads, fanouts, all).await
    }
}

fn parse_args_from<I>(mut args: I) -> (DemoConfig, bool, Vec<usize>, Vec<usize>, bool, bool, bool)
where
    I: Iterator<Item = String>,
{
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
    let mut sub_conns = std::env::var("FELIX_SUB_EGRESS_CONNS")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_CONNS").ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let mut sub_writer_lanes = None;
    let mut sub_lane_shard = None;
    let mut pub_yield_every_batches = 0usize;
    let mut sub_dedicated_thread = std::env::var("FELIX_SUB_DEDICATED_THREAD")
        .ok()
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let mut sub_delivery_shaping = std::env::var("FELIX_SUB_DELIVERY_SHAPING")
        .ok()
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let mut smoke = false;
    let mut compare_sub_delivery_shaping = false;
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
    let mut sub_conns_set = false;
    let mut sub_writer_lanes_set = false;
    let mut sub_lane_shard_set = false;
    let mut pub_yield_every_batches_set = false;
    let mut sub_dedicated_thread_set = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
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
            "--sub-writer-lanes" => {
                if let Some(value) = args.next() {
                    sub_writer_lanes = value.parse().ok();
                    sub_writer_lanes_set = true;
                }
            }
            "--sub-conns" => {
                if let Some(value) = args.next() {
                    sub_conns = value.parse().unwrap_or(sub_conns);
                    sub_conns_set = true;
                }
            }
            "--sub-lane-shard" => {
                if let Some(value) = args.next() {
                    sub_lane_shard = Some(value);
                    sub_lane_shard_set = true;
                }
            }
            "--pub-yield-every-batches" => {
                if let Some(value) = args.next() {
                    pub_yield_every_batches = value.parse().unwrap_or(pub_yield_every_batches);
                    pub_yield_every_batches_set = true;
                }
            }
            "--sub-dedicated-thread" => {
                sub_dedicated_thread = true;
                sub_dedicated_thread_set = true;
            }
            "--sub-shared-thread" => {
                sub_dedicated_thread = false;
                sub_dedicated_thread_set = true;
            }
            "--smoke" => {
                smoke = true;
                all = false;
            }
            "--sub-delivery-shaping-on" => {
                sub_delivery_shaping = true;
            }
            "--sub-delivery-shaping-off" => {
                sub_delivery_shaping = false;
            }
            "--compare-sub-delivery-shaping" => {
                compare_sub_delivery_shaping = true;
                all = false;
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
            || pub_stream_count_set
            || sub_conns_set
            || sub_writer_lanes_set
            || sub_lane_shard_set
            || pub_yield_every_batches_set
            || sub_dedicated_thread_set)
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
            sub_conns,
            sub_writer_lanes,
            sub_lane_shard,
            sub_delivery_shaping,
            pub_yield_every_batches,
            sub_dedicated_thread,
        },
        matrix,
        payloads,
        fanouts,
        all,
        smoke,
        compare_sub_delivery_shaping,
    )
}

fn parse_args() -> (DemoConfig, bool, Vec<usize>, Vec<usize>, bool, bool, bool) {
    parse_args_from(std::env::args().skip(1))
}

fn print_usage() {
    println!("Felix latency-demo");
    println!("  --warmup <N> --total <N> --payload <bytes> --fanout <N> --batch <N>");
    println!("  --pub-conns <N> --pub-streams-per-conn <N> --pub-stream-count <N>");
    println!("  --pub-yield-every-batches <N>  Yield publisher loop every N batches (0=off)");
    println!("  --sub-dedicated-thread         Force dedicated subscriber thread (default ON)");
    println!("  --sub-shared-thread            Disable dedicated subscriber thread");
    println!("  --smoke                        Run throughput A/B sanity check");
    println!("  --sub-delivery-shaping-on      Enable broker sub delivery shaping (default ON)");
    println!("  --sub-delivery-shaping-off     Disable broker sub delivery shaping");
    println!("  --compare-sub-delivery-shaping Run current config twice (OFF then ON) and compare");
    println!("  --sub-writer-lanes <N> --sub-conns <N> --sub-lane-shard <mode>");
    println!("  --matrix --payloads <csv> --fanouts <csv> --all --binary");
    println!();
    println!(
        "Throughput starvation repro:\n  cargo run --release -p broker --bin latency-demo --all-features -- --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1"
    );
    println!("Fairness A/B:\n  ... --pub-yield-every-batches 1\n  ... --sub-dedicated-thread");
}

fn effective_sub_lanes_hint(config: &DemoConfig) -> usize {
    if let Some(lanes) = config.sub_writer_lanes {
        return lanes.max(1);
    }
    std::env::var("FELIX_SUB_EGRESS_LANES")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_WRITER_LANES").ok())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

async fn run_case(config: DemoConfig) -> Result<(DemoResult, Option<TimingSummary>)> {
    let sub_lanes_hint = effective_sub_lanes_hint(&config);
    let sub_stream_mode = std::env::var("FELIX_SUB_STREAM_MODE")
        .ok()
        .unwrap_or_else(|| "per_subscriber".to_string());
    let sub_queue_bound = std::env::var("FELIX_SUB_QUEUE_BOUND")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_DEPTH").ok())
        .unwrap_or_else(|| "8192".to_string());
    println!(
        "Warmup: {} messages | Measure: {} messages | payload {} bytes | fanout {} | batch {} | publish_fastpath {} | pub conns {} | pub streams/conn {} | pub stream count {} | sub lanes {} | sub shard {} | sub conns {} | sub stream mode {} | sub queue bound {} | pub yield every {} batches | sub dedicated thread {} | sub delivery shaping {}",
        config.warmup,
        config.total,
        config.payload_bytes,
        config.fanout,
        config.batch_size,
        config.binary,
        config.pub_conns,
        config.pub_streams_per_conn,
        config.pub_stream_count,
        sub_lanes_hint,
        config.sub_lane_shard.as_deref().unwrap_or("default"),
        config.sub_conns.max(1),
        sub_stream_mode,
        sub_queue_bound,
        config.pub_yield_every_batches,
        config.sub_dedicated_thread,
        config.sub_delivery_shaping
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
    let mut broker_config = broker::config::BrokerConfig::from_env()?;
    broker_config.fanout_batch_size = config.batch_size.max(1);
    // Tune subscriber egress by workload profile:
    // - latency profile (batch=1): favor immediate flush and stable ordering
    // - throughput profile (batch>1): favor coalescing and parallel lanes
    if config.batch_size <= 1 {
        broker_config.subscriber_lane_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_flush_max_items = 1;
        broker_config.subscriber_flush_max_delay_us = 0;
        broker_config.subscriber_max_bytes_per_write = 64 * 1024;
        broker_config.subscriber_single_writer_per_conn = true;
    } else {
        broker_config.subscriber_lane_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_flush_max_items = 64;
        broker_config.subscriber_flush_max_delay_us = 200;
        broker_config.subscriber_max_bytes_per_write = 256 * 1024;
        broker_config.subscriber_single_writer_per_conn = false;
    }
    if config.sub_delivery_shaping {
        broker_config.event_batch_max_events = broker_config.event_batch_max_events.max(64);
        broker_config.event_batch_max_bytes = 64 * 1024;
        broker_config.event_batch_max_delay_us = 250;
        broker_config.subscriber_max_bytes_per_write = 64 * 1024;
    } else {
        broker_config.event_batch_max_events = 1;
        broker_config.event_batch_max_bytes = 1024;
        broker_config.event_batch_max_delay_us = 0;
    }
    if let Some(lanes) = config.sub_writer_lanes {
        broker_config.subscriber_writer_lanes = lanes.max(1);
    }
    if let Ok(raw) = std::env::var("FELIX_SUB_EGRESS_LANES")
        && let Ok(expected) = raw.parse::<usize>()
    {
        let active = broker_config.subscriber_writer_lanes.max(1);
        if expected > 0 && active != expected {
            return Err(anyhow::anyhow!(
                "FELIX_SUB_EGRESS_LANES set to {} but active lanes is {}",
                expected,
                active
            ));
        }
    }
    if let Some(shard) = config.sub_lane_shard.as_deref() {
        broker_config.subscriber_lane_shard = match shard {
            "auto" => broker::config::SubscriberLaneShard::Auto,
            "subscriber_id_hash" => broker::config::SubscriberLaneShard::SubscriberIdHash,
            "connection_id_hash" => broker::config::SubscriberLaneShard::ConnectionIdHash,
            "round_robin_pin" => broker::config::SubscriberLaneShard::RoundRobinPin,
            _ => broker_config.subscriber_lane_shard,
        };
    }
    let (auth, auth_override) = resolve_demo_auth(&broker_config)?;
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
        auth,
    ));

    let mut client_config = apply_demo_auth(build_client_config(cert)?, auth_override);
    client_config.publish_conn_pool = config.pub_conns.max(1);
    client_config.publish_streams_per_conn = config.pub_streams_per_conn.max(1);
    if let Some(sharding) = config.pub_sharding.as_deref().and_then(parse_sharding_mode) {
        client_config.publish_sharding = sharding;
    }
    let client = Client::connect(addr, "localhost", client_config.clone()).await?;
    let mut sub_clients = Vec::with_capacity(config.sub_conns.max(1));
    for _ in 0..config.sub_conns.max(1) {
        sub_clients.push(Client::connect(addr, "localhost", client_config.clone()).await?);
    }

    let delivered_total = Arc::new(AtomicUsize::new(0));
    let (primary_sub, drain_tasks) = setup_subscribers(
        &sub_clients,
        config.fanout,
        total_events,
        config.warmup,
        config.pub_stream_count,
        Arc::clone(&delivered_total),
        config.sub_dedicated_thread,
    )
    .await?;

    let idle_timeout = Duration::from_millis(config.idle_ms);
    let primary_total = per_stream_events(config.total, config.pub_stream_count.max(1), 0);
    let primary_warmup = per_stream_events(config.warmup, config.pub_stream_count.max(1), 0);
    let mut dedicated_handle = if config.sub_dedicated_thread {
        Some(spawn_dedicated_primary_subscriber(
            addr,
            client_config.clone(),
            idle_timeout,
            primary_warmup,
            primary_total,
            config.pub_stream_count.max(1),
            Arc::clone(&delivered_total),
        ))
    } else {
        None
    };
    let mut warmup_rx = None;
    let recv_task = if let Some(mut sub) = primary_sub {
        let (warmup_tx, local_warmup_rx) = oneshot::channel();
        warmup_rx = Some(local_warmup_rx);
        let delivered_for_primary = Arc::clone(&delivered_total);
        Some(tokio::spawn(async move {
            let mut results = Vec::with_capacity(primary_total);
            let mut seen = 0usize;
            let mut warmup_tx = Some(warmup_tx);
            if primary_warmup == 0
                && let Some(tx) = warmup_tx.take()
            {
                let _ = tx.send(());
            }
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
                    Err(err) => {
                        return Err(anyhow::anyhow!("primary subscriber failed: {err}"));
                    }
                }
            }
            Ok::<Vec<Duration>, anyhow::Error>(results)
        }))
    } else {
        None
    };
    if let Some(handle) = &mut dedicated_handle {
        handle.await_ready().await?;
    }
    let dedicated_recv_task = if let Some(handle) = &mut dedicated_handle {
        let mut rx = handle
            .delivery_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated delivery receiver missing"))?;
        Some(tokio::spawn(async move {
            let mut results = Vec::with_capacity(primary_total);
            while results.len() < primary_total {
                let next = tokio::time::timeout(idle_timeout, rx.recv()).await;
                match next {
                    Ok(Some(latency)) => results.push(latency),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            results
        }))
    } else {
        None
    };

    let publisher = client.publisher().await?;
    publish_batches(
        &publisher,
        &PublishBatchConfig {
            payload_bytes: config.payload_bytes,
            total: config.warmup,
            batch_size: config.batch_size,
            binary: config.binary,
            use_ack,
            stream_count: config.pub_stream_count,
            pub_yield_every_batches: config.pub_yield_every_batches,
        },
    )
    .await?;
    if let Some(handle) = &mut dedicated_handle {
        handle.await_warmup().await
    } else if let Some(local_warmup_rx) = warmup_rx {
        local_warmup_rx
            .await
            .map_err(|_| anyhow::anyhow!("warmup channel closed"))
    } else {
        Ok(())
    }?;

    let start = Instant::now();
    publish_batches(
        &publisher,
        &PublishBatchConfig {
            payload_bytes: config.payload_bytes,
            total: config.total,
            batch_size: config.batch_size,
            binary: config.binary,
            use_ack,
            stream_count: config.pub_stream_count,
            pub_yield_every_batches: config.pub_yield_every_batches,
        },
    )
    .await?;
    let elapsed = start.elapsed();

    let mut latencies = if let Some(task) = recv_task {
        task.await.expect("join")?
    } else if let Some(task) = dedicated_recv_task {
        task.await.expect("join")
    } else if let Some(handle) = dedicated_handle.take() {
        handle.finish().await?;
        Vec::new()
    } else {
        Vec::new()
    };
    if let Some(handle) = dedicated_handle.take() {
        handle.finish().await?;
    }
    let expected_delivered_total = config.total * config.fanout;
    let delivery_wait_timeout = Duration::from_millis(config.idle_ms.max(2000) * 5);
    let _ = tokio::time::timeout(delivery_wait_timeout, async {
        while delivered_total.load(Ordering::Relaxed) < expected_delivered_total {
            tokio::task::yield_now().await;
        }
    })
    .await;
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

async fn run_compare_sub_delivery_shaping(mut base: DemoConfig) -> Result<()> {
    base.sub_delivery_shaping = false;
    let (off_result, off_summary) = run_case(base.clone()).await?;
    base.sub_delivery_shaping = true;
    let (on_result, on_summary) = run_case(base).await?;

    #[cfg(feature = "telemetry")]
    {
        let off = off_summary.ok_or_else(|| anyhow::anyhow!("missing timing summary (OFF run)"))?;
        let on = on_summary.ok_or_else(|| anyhow::anyhow!("missing timing summary (ON run)"))?;
        println!(
            "Shaping compare: OFF p99={} p999={} read_await_p99={} write_await_p99={} | ON p99={} p999={} read_await_p99={} write_await_p99={}",
            format_duration(off_result.p99),
            format_duration(off_result.p999),
            format_optional_duration(off.client_sub_read_await_p99),
            format_optional_duration(off.broker_sub_write_await_p99),
            format_duration(on_result.p99),
            format_duration(on_result.p999),
            format_optional_duration(on.client_sub_read_await_p99),
            format_optional_duration(on.broker_sub_write_await_p99),
        );
    }
    #[cfg(not(feature = "telemetry"))]
    {
        let _ = (off_summary, on_summary);
        println!(
            "Shaping compare: OFF p99={} p999={} | ON p99={} p999={}",
            format_duration(off_result.p99),
            format_duration(off_result.p999),
            format_duration(on_result.p99),
            format_duration(on_result.p999),
        );
    }
    Ok(())
}

async fn setup_subscribers(
    clients: &[Client],
    fanout: usize,
    total_events: usize,
    warmup: usize,
    stream_count: usize,
    delivered_total: Arc<AtomicUsize>,
    reserve_primary_slot: bool,
) -> Result<(Option<Subscription>, Vec<tokio::task::JoinHandle<()>>)> {
    let mut drain_tasks = Vec::new();
    let mut primary_sub = None;
    let mut primary_slot_reserved = reserve_primary_slot;
    let assign_primary = !reserve_primary_slot;
    let stream_count = stream_count.max(1);
    if clients.is_empty() {
        anyhow::bail!("no subscriber clients configured");
    }
    let mut next_client = 0usize;
    for stream_index in 0..stream_count {
        let per_stream_total = per_stream_events(total_events, stream_count, stream_index);
        let per_stream_warmup = per_stream_events(warmup, stream_count, stream_index);
        for _ in 0..fanout {
            let stream = stream_name(stream_index, stream_count);
            let client = &clients[next_client % clients.len()];
            next_client = next_client.wrapping_add(1);
            if stream_index == 0 && primary_slot_reserved {
                primary_slot_reserved = false;
                continue;
            }
            let sub = client.subscribe("t1", "default", stream.as_str()).await?;
            if assign_primary && primary_sub.is_none() {
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
    Ok((primary_sub, drain_tasks))
}

async fn publish_batches(publisher: &Publisher, config: &PublishBatchConfig) -> Result<()> {
    let mut remaining = config.total;
    let mut stream_index = 0usize;
    let mut sent_batches = 0usize;
    while remaining > 0 {
        let count = remaining.min(config.batch_size);
        publish_batch(
            publisher,
            config.payload_bytes,
            count,
            config.binary,
            config.use_ack,
            stream_index,
            config.stream_count,
        )
        .await?;
        stream_index = stream_index.wrapping_add(1);
        remaining -= count;
        sent_batches = sent_batches.wrapping_add(1);
        if config.pub_yield_every_batches > 0
            && sent_batches.is_multiple_of(config.pub_yield_every_batches)
        {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

struct PublishBatchConfig {
    payload_bytes: usize,
    total: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
    stream_count: usize,
    pub_yield_every_batches: usize,
}

struct DedicatedPrimaryHandle {
    ready_rx: Option<oneshot::Receiver<Result<(), String>>>,
    warmup_rx: Option<oneshot::Receiver<()>>,
    error_rx: oneshot::Receiver<Result<(), String>>,
    delivery_rx: Option<tokio::sync::mpsc::Receiver<Duration>>,
    join: Option<thread::JoinHandle<()>>,
}

impl DedicatedPrimaryHandle {
    async fn await_ready(&mut self) -> Result<()> {
        let ready_rx = self
            .ready_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated subscriber ready already awaited"))?;
        let ready = ready_rx
            .await
            .map_err(|_| anyhow::anyhow!("dedicated subscriber ready channel closed"))?;
        ready.map_err(anyhow::Error::msg)
    }

    async fn await_warmup(&mut self) -> Result<()> {
        let warmup_rx = self
            .warmup_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated subscriber warmup already awaited"))?;
        warmup_rx
            .await
            .map_err(|_| anyhow::anyhow!("dedicated subscriber warmup channel closed"))
    }

    async fn finish(mut self) -> Result<()> {
        let recv = self
            .error_rx
            .await
            .map_err(|_| anyhow::anyhow!("dedicated subscriber completion channel closed"))?;
        if let Some(join) = self.join.take() {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = join.join();
            })
            .await;
        }
        recv.map_err(anyhow::Error::msg)
    }
}

fn spawn_dedicated_primary_subscriber(
    addr: std::net::SocketAddr,
    client_config: ClientConfig,
    idle_timeout: Duration,
    primary_warmup: usize,
    primary_total: usize,
    stream_count: usize,
    delivered_total: Arc<AtomicUsize>,
) -> DedicatedPrimaryHandle {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (warmup_ready_tx, warmup_ready_rx) = oneshot::channel();
    let (error_tx, error_rx) = oneshot::channel();
    let chan_capacity = std::env::var("FELIX_SUB_DEDICATED_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4096)
        .max(1);
    let (delivery_tx, delivery_rx) = tokio::sync::mpsc::channel(chan_capacity);
    let join = thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                let _ = error_tx.send(Err(format!("build dedicated runtime failed: {err}")));
                return;
            }
        };
        let result = runtime.block_on(async move {
            let mut warmup_ready_tx = Some(warmup_ready_tx);
            let delivery_tx = delivery_tx;
            let client = match Client::connect(addr, "localhost", client_config).await {
                Ok(client) => client,
                Err(err) => {
                    let _ =
                        ready_tx.send(Err(format!("dedicated subscriber connect failed: {err}")));
                    return Err(format!("dedicated subscriber connect failed: {err}"));
                }
            };
            let mut sub = match client
                .subscribe("t1", "default", stream_name(0, stream_count).as_str())
                .await
            {
                Ok(sub) => sub,
                Err(err) => {
                    let _ =
                        ready_tx.send(Err(format!("dedicated subscriber subscribe failed: {err}")));
                    return Err(format!("dedicated subscriber subscribe failed: {err}"));
                }
            };
            let _ = ready_tx.send(Ok(()));
            let mut seen = 0usize;
            let mut last_runtime_tick = Instant::now();
            if primary_warmup == 0
                && let Some(tx) = warmup_ready_tx.take()
            {
                let _ = tx.send(());
            }
            while seen < primary_warmup + primary_total {
                let runtime_gap_ns = last_runtime_tick.elapsed().as_nanos() as u64;
                last_runtime_tick = Instant::now();
                client_timings::record_sub_runtime_gap_ns(runtime_gap_ns);
                metrics::histogram!("client_sub_runtime_gap_ns").record(runtime_gap_ns as f64);
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
                                && let Some(tx) = warmup_ready_tx.take()
                            {
                                let _ = tx.send(());
                            }
                            continue;
                        }
                        let latency = decode_latency(event.payload.as_ref());
                        let enqueue_start = Instant::now();
                        if delivery_tx.send(latency).await.is_err() {
                            break;
                        }
                        seen += 1;
                        let wait_ns = enqueue_start.elapsed().as_nanos() as u64;
                        client_timings::record_sub_delivery_chan_wait_ns(wait_ns);
                        metrics::histogram!("client_sub_delivery_chan_wait_ns")
                            .record(wait_ns as f64);
                        if wait_ns > 0 {
                            metrics::counter!(
                                "felix_client_sub_delivery_channel_backpressure_total"
                            )
                            .increment(1);
                        }
                        metrics::gauge!("felix_client_sub_delivery_channel_depth")
                            .set((chan_capacity.saturating_sub(delivery_tx.capacity())) as f64);
                        delivered_total.fetch_add(1, Ordering::Relaxed);
                        if let Some(start) = dispatch_start {
                            let dispatch_ns = start.elapsed().as_nanos() as u64;
                            client_timings::record_sub_dispatch_ns(dispatch_ns);
                            metrics::histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        return Err(format!("dedicated subscriber receive failed: {err}"));
                    }
                }
            }
            Ok(())
        });
        let _ = error_tx.send(result);
    });
    DedicatedPrimaryHandle {
        ready_rx: Some(ready_rx),
        warmup_rx: Some(warmup_ready_rx),
        error_rx,
        delivery_rx: Some(delivery_rx),
        join: Some(join),
    }
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
        "Results (publish n = {}, sampled {}, received {}, dropped {}) payload={}B fanout={} batch={} publish_fastpath={}:",
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
    if result.received == 0 {
        println!("  p50  = n/a (no received samples)");
        println!("  p99  = n/a (no received samples)");
        println!("  p999 = n/a (no received samples)");
    } else {
        println!("  p50  = {}", format_duration(result.p50));
        println!("  p99  = {}", format_duration(result.p99));
        println!("  p999 = {}", format_duration(result.p999));
    }
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
        "  timings: client_pub_enqueue_wait p50={} p99={} p999={} client_encode p50={} p99={} client_binary_encode p50={} p99={} p999={} client_text_encode p50={} p99={} p999={} client_text_batch_build p50={} p99={} p999={} client_write p50={} p99={} client_send_await p50={} p99={} p999={} client_sub_read p50={} p99={} client_sub_read_await p50={} p99={} client_sub_poll_gap p50={} p99={} p999={} client_sub_queue_wait p50={} p99={} client_sub_time_in_queue p50={} p99={} p999={} client_sub_runtime_gap p50={} p99={} p999={} client_sub_delivery_chan_wait p50={} p99={} p999={} client_sub_decode p50={} p99={} client_sub_dispatch p50={} p99={} client_sub_consumer_gap p50={} p99={} p999={} client_e2e_latency p50={} p99={} p999={} client_ack_read p50={} p99={} client_ack_decode p50={} p99={} broker_decode p50={} p99={} broker_ingress_enqueue p50={} p99={} broker_ack_write p50={} p99={} broker_quic_write p50={} p99={} broker_sub_queue_wait p50={} p99={} broker_sub_prefix_build p50={} p99={} broker_sub_write p50={} p99={} broker_sub_write_await p50={} p99={} broker_sub_delivery p50={} p99={} p999={} broker_publish_lookup p50={} p99={} broker_publish_append p50={} p99={} broker_publish_fanout p50={} p99={} broker_publish_enqueue p50={} p99={} broker_publish_send p50={} p99={}",
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
        format_optional_duration(summary.client_sub_poll_gap_p50),
        format_optional_duration(summary.client_sub_poll_gap_p99),
        format_optional_duration(summary.client_sub_poll_gap_p999),
        format_optional_duration(summary.client_sub_queue_wait_p50),
        format_optional_duration(summary.client_sub_queue_wait_p99),
        format_optional_duration(summary.client_sub_time_in_queue_p50),
        format_optional_duration(summary.client_sub_time_in_queue_p99),
        format_optional_duration(summary.client_sub_time_in_queue_p999),
        format_optional_duration(summary.client_sub_runtime_gap_p50),
        format_optional_duration(summary.client_sub_runtime_gap_p99),
        format_optional_duration(summary.client_sub_runtime_gap_p999),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p50),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p99),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p999),
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
        format_optional_duration(summary.broker_sub_prefix_build_p50),
        format_optional_duration(summary.broker_sub_prefix_build_p99),
        format_optional_duration(summary.broker_sub_write_p50),
        format_optional_duration(summary.broker_sub_write_p99),
        format_optional_duration(summary.broker_sub_write_await_p50),
        format_optional_duration(summary.broker_sub_write_await_p99),
        format_optional_duration(summary.broker_sub_delivery_p50),
        format_optional_duration(summary.broker_sub_delivery_p99),
        format_optional_duration(summary.broker_sub_delivery_p999),
        format_optional_duration(summary.broker_publish_lookup_p50),
        format_optional_duration(summary.broker_publish_lookup_p99),
        format_optional_duration(summary.broker_publish_append_p50),
        format_optional_duration(summary.broker_publish_append_p99),
        format_optional_duration(summary.broker_publish_fanout_p50),
        format_optional_duration(summary.broker_publish_fanout_p99),
        format_optional_duration(summary.broker_publish_enqueue_p50),
        format_optional_duration(summary.broker_publish_enqueue_p99),
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
    println!(
        "  sub_delivery_counters: broker_batches_out={} broker_items_out={} client_batches_in={} client_items_in={}",
        broker.sub_batches_out_ok,
        broker.sub_items_out_ok,
        client.sub_batches_in_ok,
        client.sub_items_in_ok
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
    if client.pub_batches_out_ok > 0 && client.pub_frames_out_ok > 0 {
        let writes_per_batch = client.pub_frames_out_ok as f64 / client.pub_batches_out_ok as f64;
        let avg_bytes_per_write = client.bytes_out as f64 / client.pub_frames_out_ok as f64;
        println!(
            "  publish_write_shape: writes_per_batch={:.2} avg_bytes_per_write={:.1}",
            writes_per_batch, avg_bytes_per_write
        );
    }
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
        mut client_sub_queue_wait,
        mut client_sub_decode,
        mut client_sub_dispatch,
        mut client_sub_consumer_gap,
        mut client_sub_poll_gap,
        mut client_sub_time_in_queue,
        mut client_sub_runtime_gap,
        mut client_sub_delivery_chan_wait,
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
        mut broker_sub_prefix,
        mut broker_sub_write,
        mut broker_sub_write_await,
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
            Vec::new(),
            Vec::new(),
        )
    });
    let (
        mut broker_publish_lookup,
        mut broker_publish_append,
        mut broker_publish_fanout,
        mut broker_publish_enqueue,
        mut broker_publish_send,
    ) = broker_publish_samples
        .unwrap_or_else(|| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
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
        client_sub_poll_gap_p50: percentile_ns(&mut client_sub_poll_gap, 0.50),
        client_sub_poll_gap_p99: percentile_ns(&mut client_sub_poll_gap, 0.99),
        client_sub_poll_gap_p999: percentile_ns(&mut client_sub_poll_gap, 0.999),
        client_sub_queue_wait_p50: percentile_ns(&mut client_sub_queue_wait, 0.50),
        client_sub_queue_wait_p99: percentile_ns(&mut client_sub_queue_wait, 0.99),
        client_sub_time_in_queue_p50: percentile_ns(&mut client_sub_time_in_queue, 0.50),
        client_sub_time_in_queue_p99: percentile_ns(&mut client_sub_time_in_queue, 0.99),
        client_sub_time_in_queue_p999: percentile_ns(&mut client_sub_time_in_queue, 0.999),
        client_sub_runtime_gap_p50: percentile_ns(&mut client_sub_runtime_gap, 0.50),
        client_sub_runtime_gap_p99: percentile_ns(&mut client_sub_runtime_gap, 0.99),
        client_sub_runtime_gap_p999: percentile_ns(&mut client_sub_runtime_gap, 0.999),
        client_sub_delivery_chan_wait_p50: percentile_ns(&mut client_sub_delivery_chan_wait, 0.50),
        client_sub_delivery_chan_wait_p99: percentile_ns(&mut client_sub_delivery_chan_wait, 0.99),
        client_sub_delivery_chan_wait_p999: percentile_ns(
            &mut client_sub_delivery_chan_wait,
            0.999,
        ),
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
        broker_sub_prefix_build_p50: percentile_ns(&mut broker_sub_prefix, 0.50),
        broker_sub_prefix_build_p99: percentile_ns(&mut broker_sub_prefix, 0.99),
        broker_sub_write_p50: percentile_ns(&mut broker_sub_write, 0.50),
        broker_sub_write_p99: percentile_ns(&mut broker_sub_write, 0.99),
        broker_sub_write_await_p50: percentile_ns(&mut broker_sub_write_await, 0.50),
        broker_sub_write_await_p99: percentile_ns(&mut broker_sub_write_await, 0.99),
        broker_sub_delivery_p50: percentile_ns(&mut broker_sub_delivery, 0.50),
        broker_sub_delivery_p99: percentile_ns(&mut broker_sub_delivery, 0.99),
        broker_sub_delivery_p999: percentile_ns(&mut broker_sub_delivery, 0.999),
        broker_publish_lookup_p50: percentile_ns(&mut broker_publish_lookup, 0.50),
        broker_publish_lookup_p99: percentile_ns(&mut broker_publish_lookup, 0.99),
        broker_publish_append_p50: percentile_ns(&mut broker_publish_append, 0.50),
        broker_publish_append_p99: percentile_ns(&mut broker_publish_append, 0.99),
        broker_publish_fanout_p50: percentile_ns(&mut broker_publish_fanout, 0.50),
        broker_publish_fanout_p99: percentile_ns(&mut broker_publish_fanout, 0.99),
        broker_publish_enqueue_p50: percentile_ns(&mut broker_publish_enqueue, 0.50),
        broker_publish_enqueue_p99: percentile_ns(&mut broker_publish_enqueue, 0.99),
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
    let fast = std::env::var("FELIX_LATENCY_DEMO_FAST")
        .ok()
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    run_all_with_mode(base, fast).await
}

async fn run_all_with_mode(base: DemoConfig, fast: bool) -> Result<()> {
    println!("Running --all (default): latency-focused then throughput-focused.");
    let latency_payloads: &[usize] = if fast { &[0] } else { &[0, 256, 1024] };
    let latency_fanouts: &[usize] = if fast { &[1] } else { &[1, 10] };
    for &payload in latency_payloads {
        for &fanout in latency_fanouts {
            let mut case = base.clone();
            case.payload_bytes = payload;
            case.fanout = fanout;
            case.batch_size = 1;
            case.binary = base.binary;
            case.warmup = if fast { 1 } else { 200 };
            case.total = if fast { 5 } else { 2000 };
            let (result, summary) = run_case(case.clone()).await?;
            print_result(&result);
            print_timing_summary(summary);
            let (client_counters, broker_counters) = print_frame_counters();
            print_sanity_checks(&case, &result, &client_counters, &broker_counters);
        }
    }

    let throughput_payloads: &[usize] = if fast { &[0] } else { &[0, 1024, 4096] };
    let throughput_fanouts: &[usize] = if fast { &[1] } else { &[1, 10] };
    let binaries: &[bool] = if base.binary { &[true] } else { &[false, true] };
    for &payload in throughput_payloads {
        for &fanout in throughput_fanouts {
            for &binary in binaries {
                let mut case = base.clone();
                case.payload_bytes = payload;
                case.fanout = fanout;
                case.batch_size = 64;
                case.binary = binary;
                case.warmup = if fast { 1 } else { 200 };
                case.total = if fast { 5 } else { 5000 };
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
    baseline.warmup = if fast { 1 } else { 200 };
    baseline.total = if fast { 5 } else { 5000 };
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

fn parse_sharding_mode(value: &str) -> Option<PublishSharding> {
    match value {
        "rr" => Some(PublishSharding::RoundRobin),
        "hash_stream" => Some(PublishSharding::HashStream),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn latency_demo_end_to_end() -> Result<()> {
        let config = DemoConfig {
            warmup: 1,
            total: 5,
            payload_bytes: 8,
            fanout: 1,
            batch_size: 1,
            binary: false,
            idle_ms: 10,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
        };
        tokio::time::timeout(
            Duration::from_secs(15),
            run_demo(config, false, vec![8], vec![1], false),
        )
        .await
        .context("latency demo timeout")?
    }

    #[tokio::test]
    async fn latency_demo_matrix_small() -> Result<()> {
        let config = DemoConfig {
            warmup: 0,
            total: 2,
            payload_bytes: 0,
            fanout: 1,
            batch_size: 1,
            binary: false,
            idle_ms: 5,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: Some("rr".to_string()),
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
        };
        tokio::time::timeout(
            Duration::from_secs(15),
            run_demo(config, true, vec![0], vec![1], false),
        )
        .await
        .context("latency demo matrix timeout")?
    }

    #[test]
    fn latency_demo_parse_args_and_helpers() {
        let args = vec![
            "--matrix".to_string(),
            "--warmup".to_string(),
            "2".to_string(),
            "--total".to_string(),
            "4".to_string(),
            "--payload".to_string(),
            "8".to_string(),
            "--fanout".to_string(),
            "2".to_string(),
            "--batch".to_string(),
            "2".to_string(),
            "--idle-ms".to_string(),
            "5".to_string(),
            "--pub-conns".to_string(),
            "1".to_string(),
            "--pub-streams-per-conn".to_string(),
            "1".to_string(),
            "--pub-sharding".to_string(),
            "hash".to_string(),
            "--pub-stream-count".to_string(),
            "2".to_string(),
            "--sub-writer-lanes".to_string(),
            "8".to_string(),
            "--sub-lane-shard".to_string(),
            "connection_id_hash".to_string(),
            "--pub-yield-every-batches".to_string(),
            "3".to_string(),
            "--sub-dedicated-thread".to_string(),
            "--payloads".to_string(),
            "1,2".to_string(),
            "--fanouts".to_string(),
            "1,3".to_string(),
        ];
        let (config, matrix, payloads, fanouts, all, smoke, compare) =
            parse_args_from(args.into_iter());
        assert!(matrix);
        assert_eq!(payloads, vec![1, 2]);
        assert_eq!(fanouts, vec![1, 3]);
        assert!(!all);
        assert!(!smoke);
        assert!(!compare);
        assert_eq!(config.sub_writer_lanes, Some(8));
        assert_eq!(config.sub_lane_shard.as_deref(), Some("connection_id_hash"));
        assert_eq!(config.pub_yield_every_batches, 3);
        assert!(config.sub_dedicated_thread);

        assert_eq!(parse_csv_usize("1,2,3"), vec![1, 2, 3]);
        let payload = encode_payload(4);
        let encoded = current_time_nanos();
        assert!(encoded > 0);
        let _decoded = decode_latency(&payload);
        assert_eq!(format_duration(Duration::from_micros(5)), "5 us");
        let _ = percentile(&[Duration::from_micros(1)], 0.5);
        assert_eq!(stream_name(0, 1), "latency");
        assert_eq!(stream_name(3, 2), "latency-1");
        assert_eq!(per_stream_events(5, 2, 0), 3);
        assert_eq!(per_stream_events(5, 2, 1), 2);
        assert!(
            expectation_note(&DemoConfig {
                warmup: 1,
                total: 1,
                payload_bytes: 0,
                fanout: 1,
                batch_size: 1,
                binary: false,
                idle_ms: 1,
                pub_conns: 1,
                pub_streams_per_conn: 1,
                pub_sharding: None,
                pub_stream_count: 1,
                sub_conns: 1,
                sub_writer_lanes: None,
                sub_lane_shard: None,
                sub_delivery_shaping: true,
                pub_yield_every_batches: 0,
                sub_dedicated_thread: false,
            })
            .contains("latency-focused")
        );
        assert!(
            expectation_note(&DemoConfig {
                warmup: 1,
                total: 1,
                payload_bytes: 0,
                fanout: 1,
                batch_size: 2,
                binary: false,
                idle_ms: 1,
                pub_conns: 1,
                pub_streams_per_conn: 1,
                pub_sharding: None,
                pub_stream_count: 1,
                sub_conns: 1,
                sub_writer_lanes: None,
                sub_lane_shard: None,
                sub_delivery_shaping: true,
                pub_yield_every_batches: 0,
                sub_dedicated_thread: false,
            })
            .contains("throughput-focused")
        );
        assert_eq!(parse_sharding_mode("rr"), Some(PublishSharding::RoundRobin));
        assert_eq!(
            parse_sharding_mode("hash_stream"),
            Some(PublishSharding::HashStream)
        );
        assert_eq!(parse_sharding_mode("nope"), None);
        #[cfg(feature = "telemetry")]
        {
            let mut values = vec![100u64, 200, 300];
            let _ = percentile_ns(&mut values, 0.5);
        }
        #[cfg(feature = "telemetry")]
        {
            let _ = format_optional_duration(None);
        }
    }

    #[tokio::test]
    async fn latency_demo_run_all_fast() -> Result<()> {
        let base = DemoConfig {
            warmup: 1,
            total: 5,
            payload_bytes: 0,
            fanout: 1,
            batch_size: 1,
            binary: false,
            idle_ms: 5,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
        };
        tokio::time::timeout(Duration::from_secs(20), run_all_with_mode(base, true))
            .await
            .context("latency demo fast run_all timeout")?
    }

    #[tokio::test]
    async fn latency_demo_smoke_flags() -> Result<()> {
        let config = DemoConfig {
            warmup: 1,
            total: 4,
            payload_bytes: 8,
            fanout: 1,
            batch_size: 2,
            binary: false,
            idle_ms: 10,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: Some(2),
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 1,
            sub_dedicated_thread: true,
        };
        tokio::time::timeout(
            Duration::from_secs(20),
            run_demo(config, false, vec![8], vec![1], false),
        )
        .await
        .context("latency demo smoke flags timeout")?
    }
}
