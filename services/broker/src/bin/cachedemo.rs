// Console demo that exercises cache Put/Get over QUIC using felix-wire frames.
use anyhow::{Context, Result};
use broker::quic;
use broker::timings as broker_timings;
use bytes::Bytes;
use felix_broker::{Broker, CacheMetadata};
use felix_client::Client;
use felix_client::timings as client_timings;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    println!("== Felix QUIC Cache Demo ==");
    println!("Goal: benchmark cache Put/Get over QUIC (not pub/sub).");

    println!("Step 1/4: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", CacheMetadata)
        .await?;
    let config = broker::config::BrokerConfig::from_env()?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        transport,
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker, config.clone()));

    println!("Step 2/4: connecting QUIC client.");
    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;

    println!("Step 3/4: running cache benchmarks.");
    let mut bench = BenchConfig::from_env();
    if config.disable_timings {
        bench.collect_timings = false;
    }
    println!(
        "Config: warmup={} samples={} payloads={:?} ttl_ms={:?} concurrency={} keys={} ops={:?} timings={}",
        bench.warmup,
        bench.samples,
        bench.payload_sizes,
        bench.ttl_ms,
        bench.concurrency,
        bench.key_count,
        bench.ops,
        bench.collect_timings
    );
    if bench.collect_timings {
        client_timings::enable_collection(1);
        broker_timings::enable_collection(1);
    }
    for payload_size in &bench.payload_sizes {
        run_payload_suite(&client, *payload_size, &bench).await?;
    }

    println!("Step 4/4: TTL sanity check.");
    run_ttl_check(&client).await?;
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

struct BenchConfig {
    warmup: usize,
    samples: usize,
    payload_sizes: Vec<usize>,
    ttl_ms: Option<u64>,
    concurrency: usize,
    key_count: usize,
    ops: Vec<BenchOp>,
    collect_timings: bool,
    validate_each: bool,
    conn_stats: bool,
}

impl BenchConfig {
    fn from_env() -> Self {
        let warmup = std::env::var("FELIX_CACHE_BENCH_WARMUP")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(200);
        let samples = std::env::var("FELIX_CACHE_BENCH_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(2000);
        let payload_sizes = std::env::var("FELIX_CACHE_BENCH_PAYLOADS")
            .ok()
            .and_then(|value| parse_payloads(&value))
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![0, 64, 256, 1024, 4096]);
        let ttl_ms = std::env::var("FELIX_CACHE_BENCH_TTL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0);
        let concurrency = std::env::var("FELIX_CACHE_BENCH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);
        let key_count = std::env::var("FELIX_CACHE_BENCH_KEYS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1024);
        let ops = std::env::var("FELIX_CACHE_BENCH_OPS")
            .ok()
            .and_then(|value| parse_ops(&value))
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![BenchOp::Put, BenchOp::GetHit, BenchOp::GetMiss]);
        let collect_timings = std::env::var("FELIX_CACHE_BENCH_TIMINGS")
            .ok()
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let validate_each = std::env::var("FELIX_CACHE_BENCH_VALIDATE_EACH")
            .ok()
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let conn_stats = std::env::var("FELIX_CACHE_BENCH_CONN_STATS")
            .ok()
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            warmup,
            samples,
            payload_sizes,
            ttl_ms,
            concurrency,
            key_count,
            ops,
            collect_timings,
            validate_each,
            conn_stats,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchOp {
    Put,
    GetHit,
    GetMiss,
}

fn parse_ops(value: &str) -> Option<Vec<BenchOp>> {
    let mut ops = Vec::new();
    for part in value.split(',') {
        let op = match part.trim() {
            "put" => BenchOp::Put,
            "get_hit" => BenchOp::GetHit,
            "get_miss" => BenchOp::GetMiss,
            _ => return None,
        };
        ops.push(op);
    }
    Some(ops)
}

fn parse_payloads(value: &str) -> Option<Vec<usize>> {
    let mut values = Vec::new();
    for part in value.split(',') {
        let size = part.trim().parse::<usize>().ok()?;
        values.push(size);
    }
    Some(values)
}

struct BenchStats {
    count: usize,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    avg_us: f64,
    throughput_ops: f64,
}

impl BenchStats {
    fn from_samples(samples: &[Duration], elapsed: Duration) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut values = samples
            .iter()
            .map(|value| value.as_secs_f64() * 1_000_000.0)
            .collect::<Vec<_>>();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = values.len();
        let avg_us = values.iter().sum::<f64>() / count as f64;
        let p50_us = percentile_us(&values, 0.50);
        let p99_us = percentile_us(&values, 0.99);
        let p999_us = percentile_us(&values, 0.999);
        let throughput_ops = count as f64 / elapsed.as_secs_f64();
        Some(Self {
            count,
            p50_us,
            p99_us,
            p999_us,
            avg_us,
            throughput_ops,
        })
    }
}

fn percentile_us(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = (percentile * values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values[index]
}

fn percentile_ns_to_us(values: &mut [u64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_unstable();
    let rank = (percentile * values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values[index] as f64 / 1_000.0
}

async fn run_payload_suite(
    client: &Client,
    payload_size: usize,
    bench: &BenchConfig,
) -> Result<()> {
    println!();
    println!("Payload size: {} bytes", payload_size);
    let ttl = bench.ttl_ms;

    prepopulate_hits(client, payload_size, bench.key_count, ttl).await?;
    validate_hit_payload(client, payload_size).await?;

    if bench.ops.contains(&BenchOp::Put) {
        let put_stats = bench_puts(client, payload_size, bench, ttl).await?;
        print_stats("put", &put_stats);
        if bench.collect_timings {
            print_cache_timings(
                "put",
                client_timings::take_cache_samples(),
                broker_timings::take_cache_samples(),
            );
        }
    }

    if bench.ops.contains(&BenchOp::GetHit) {
        let hit_stats = bench_get_hits(client, payload_size, bench).await?;
        print_stats("get_hit", &hit_stats);
        if bench.collect_timings {
            print_cache_timings(
                "get_hit",
                client_timings::take_cache_samples(),
                broker_timings::take_cache_samples(),
            );
        }
    }

    if bench.ops.contains(&BenchOp::GetMiss) {
        let miss_stats = bench_get_misses(client, payload_size, bench).await?;
        print_stats("get_miss", &miss_stats);
        if bench.collect_timings {
            print_cache_timings(
                "get_miss",
                client_timings::take_cache_samples(),
                broker_timings::take_cache_samples(),
            );
        }
    }
    if bench.conn_stats {
        println!("cache_conn_counts={:?}", client.cache_conn_counts());
    }
    Ok(())
}

async fn prepopulate_hits(
    client: &Client,
    payload_size: usize,
    count: usize,
    ttl_ms: Option<u64>,
) -> Result<()> {
    for key_index in 0..count {
        let key = format!("hit-{payload_size}-{key_index}");
        let payload = make_payload(payload_size, key_index, 0);
        client
            .cache_put("t1", "default", "primary", &key, payload, ttl_ms)
            .await?;
    }
    Ok(())
}

async fn validate_hit_payload(client: &Client, payload_size: usize) -> Result<()> {
    let key = format!("hit-{payload_size}-0");
    let value = client
        .cache_get("t1", "default", "primary", &key)
        .await?
        .context("expected cache hit for validation")?;
    let expected = make_payload(payload_size, 0, 0);
    if value.len() != payload_size {
        return Err(anyhow::anyhow!(
            "cache hit payload length mismatch: expected {payload_size} got {}",
            value.len()
        ));
    }
    if value != expected {
        return Err(anyhow::anyhow!(
            "cache hit payload mismatch for payload_size={payload_size}"
        ));
    }
    println!("Validation: get_hit value_len={} bytes", value.len());
    Ok(())
}

fn make_payload(payload_size: usize, key_index: usize, op_index: usize) -> Bytes {
    if payload_size == 0 {
        return Bytes::new();
    }
    let mut value = vec![0u8; payload_size];
    let mut state = (payload_size as u64)
        ^ ((key_index as u64) << 32)
        ^ (op_index as u64)
        ^ 0x9e37_79b9_7f4a_7c15;
    for byte in &mut value {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        *byte = (state & 0xFF) as u8;
    }
    Bytes::from(value)
}

async fn bench_puts(
    client: &Client,
    payload_size: usize,
    bench: &BenchConfig,
    ttl_ms: Option<u64>,
) -> Result<BenchStats> {
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
        let _ = client_timings::take_cache_samples();
        let _ = broker_timings::take_cache_samples();
    }
    run_ops(
        bench.warmup,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "put-warm",
        false,
        &|op_index, key_index, key| async move {
            client
                .cache_put(
                    "t1",
                    "default",
                    "primary",
                    &key,
                    make_payload(payload_size, key_index, op_index),
                    ttl_ms,
                )
                .await
        },
    )
    .await?;

    if bench.collect_timings {
        client_timings::set_enabled(true);
        broker_timings::set_enabled(true);
    }
    let (samples, elapsed) = run_ops(
        bench.samples,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "put",
        true,
        &|op_index, key_index, key| async move {
            client
                .cache_put(
                    "t1",
                    "default",
                    "primary",
                    &key,
                    make_payload(payload_size, key_index, op_index),
                    ttl_ms,
                )
                .await
        },
    )
    .await?;
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
    }
    BenchStats::from_samples(&samples, elapsed).context("put stats")
}

async fn bench_get_hits(
    client: &Client,
    payload_size: usize,
    bench: &BenchConfig,
) -> Result<BenchStats> {
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
        let _ = client_timings::take_cache_samples();
        let _ = broker_timings::take_cache_samples();
    }
    run_ops(
        bench.warmup,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "hit",
        false,
        &|_, _, key| async move {
            let _ = client.cache_get("t1", "default", "primary", &key).await?;
            Ok(())
        },
    )
    .await?;
    if bench.collect_timings {
        client_timings::set_enabled(true);
        broker_timings::set_enabled(true);
    }
    let (durations, elapsed) = run_ops(
        bench.samples,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "hit",
        true,
        &|_, key_index, key| async move {
            let value = client.cache_get("t1", "default", "primary", &key).await?;
            if bench.validate_each {
                let validate_start = Instant::now();
                let expected = make_payload(payload_size, key_index, 0);
                let value = value.context("expected cache hit")?;
                if value != expected {
                    return Err(anyhow::anyhow!("cache hit validation failed"));
                }
                let validate_ns = validate_start.elapsed().as_nanos() as u64;
                client_timings::record_cache_validate_ns(validate_ns);
            }
            Ok(())
        },
    )
    .await?;
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
    }
    BenchStats::from_samples(&durations, elapsed).context("get hit stats")
}

async fn bench_get_misses(
    client: &Client,
    payload_size: usize,
    bench: &BenchConfig,
) -> Result<BenchStats> {
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
        let _ = client_timings::take_cache_samples();
        let _ = broker_timings::take_cache_samples();
    }
    run_ops(
        bench.warmup,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "miss-warm",
        false,
        &|_, _, key| async move {
            let _ = client.cache_get("t1", "default", "primary", &key).await?;
            Ok(())
        },
    )
    .await?;
    if bench.collect_timings {
        client_timings::set_enabled(true);
        broker_timings::set_enabled(true);
    }
    let (durations, elapsed) = run_ops(
        bench.samples,
        bench.concurrency,
        bench.key_count,
        payload_size,
        "miss",
        true,
        &|_, _, key| async move {
            let _ = client.cache_get("t1", "default", "primary", &key).await?;
            Ok(())
        },
    )
    .await?;
    if bench.collect_timings {
        client_timings::set_enabled(false);
        broker_timings::set_enabled(false);
    }
    BenchStats::from_samples(&durations, elapsed).context("get miss stats")
}

async fn run_ops<F, Fut>(
    total: usize,
    concurrency: usize,
    key_count: usize,
    payload_size: usize,
    prefix: &str,
    record: bool,
    op: &F,
) -> Result<(Vec<Duration>, Duration)>
where
    F: Fn(usize, usize, String) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    if total == 0 {
        return Ok((Vec::new(), Duration::from_secs(0)));
    }
    let mut in_flight = FuturesUnordered::new();
    let mut durations = if record {
        Vec::with_capacity(total)
    } else {
        Vec::new()
    };
    let start = Instant::now();
    let mut launched = 0usize;
    let mut completed = 0usize;
    let limit = concurrency.max(1);
    while completed < total {
        while launched < total && in_flight.len() < limit {
            let op_index = launched;
            let key_index = launched % key_count.max(1);
            let key = format!("{prefix}-{payload_size}-{key_index}");
            let fut = async move {
                let op_start = Instant::now();
                op(op_index, key_index, key).await?;
                Ok::<_, anyhow::Error>(op_start.elapsed())
            };
            in_flight.push(fut);
            launched += 1;
        }
        if let Some(result) = in_flight.next().await {
            let elapsed = result?;
            if record {
                durations.push(elapsed);
            }
            completed += 1;
        }
    }
    Ok((durations, start.elapsed()))
}

fn print_cache_timings(
    op: &str,
    client_samples: Option<client_timings::ClientCacheTimingSamples>,
    broker_samples: Option<broker_timings::BrokerCacheTimingSamples>,
) {
    let (
        mut client_encode,
        mut client_open_stream,
        mut client_write,
        mut client_finish,
        mut client_read_wait,
        mut client_read_drain,
        mut client_decode,
        mut client_validate,
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
        mut broker_read,
        mut broker_decode,
        mut broker_lookup,
        mut broker_insert,
        mut broker_encode,
        mut broker_write,
        mut broker_finish,
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

    println!(
        "timings[{op}] client_encode p50={}us p99={}us client_open_stream p50={}us p99={}us client_write p50={}us p99={}us client_finish p50={}us p99={}us client_read_wait p50={}us p99={}us p999={}us client_read_drain p50={}us p99={}us p999={}us client_decode p50={}us p99={}us client_validate p50={}us p99={}us",
        percentile_ns_to_us(&mut client_encode, 0.50),
        percentile_ns_to_us(&mut client_encode, 0.99),
        percentile_ns_to_us(&mut client_open_stream, 0.50),
        percentile_ns_to_us(&mut client_open_stream, 0.99),
        percentile_ns_to_us(&mut client_write, 0.50),
        percentile_ns_to_us(&mut client_write, 0.99),
        percentile_ns_to_us(&mut client_finish, 0.50),
        percentile_ns_to_us(&mut client_finish, 0.99),
        percentile_ns_to_us(&mut client_read_wait, 0.50),
        percentile_ns_to_us(&mut client_read_wait, 0.99),
        percentile_ns_to_us(&mut client_read_wait, 0.999),
        percentile_ns_to_us(&mut client_read_drain, 0.50),
        percentile_ns_to_us(&mut client_read_drain, 0.99),
        percentile_ns_to_us(&mut client_read_drain, 0.999),
        percentile_ns_to_us(&mut client_decode, 0.50),
        percentile_ns_to_us(&mut client_decode, 0.99),
        percentile_ns_to_us(&mut client_validate, 0.50),
        percentile_ns_to_us(&mut client_validate, 0.99),
    );
    println!(
        "timings[{op}] broker_read p50={}us p99={}us broker_decode p50={}us p99={}us broker_cache_lookup p50={}us p99={}us p999={}us broker_cache_insert p50={}us p99={}us broker_encode p50={}us p99={}us broker_write p50={}us p99={}us broker_finish p50={}us p99={}us",
        percentile_ns_to_us(&mut broker_read, 0.50),
        percentile_ns_to_us(&mut broker_read, 0.99),
        percentile_ns_to_us(&mut broker_decode, 0.50),
        percentile_ns_to_us(&mut broker_decode, 0.99),
        percentile_ns_to_us(&mut broker_lookup, 0.50),
        percentile_ns_to_us(&mut broker_lookup, 0.99),
        percentile_ns_to_us(&mut broker_lookup, 0.999),
        percentile_ns_to_us(&mut broker_insert, 0.50),
        percentile_ns_to_us(&mut broker_insert, 0.99),
        percentile_ns_to_us(&mut broker_encode, 0.50),
        percentile_ns_to_us(&mut broker_encode, 0.99),
        percentile_ns_to_us(&mut broker_write, 0.50),
        percentile_ns_to_us(&mut broker_write, 0.99),
        percentile_ns_to_us(&mut broker_finish, 0.50),
        percentile_ns_to_us(&mut broker_finish, 0.99),
    );
}

fn print_stats(label: &str, stats: &BenchStats) {
    println!(
        "{label}: n={} p50={:.2}us p99={:.2}us p999={:.2}us avg={:.2}us throughput={:.2} ops/s",
        stats.count, stats.p50_us, stats.p99_us, stats.p999_us, stats.avg_us, stats.throughput_ops
    );
}

async fn run_ttl_check(client: &Client) -> Result<()> {
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "demo-key",
            Bytes::from_static(b"cached"),
            Some(500),
        )
        .await?;
    let get_response = client
        .cache_get("t1", "default", "primary", "demo-key")
        .await?;
    println!(
        "Cache get response: {}",
        format_cache_response(&get_response)
    );
    tokio::time::sleep(Duration::from_millis(650)).await;
    let expired_response = client
        .cache_get("t1", "default", "primary", "demo-key")
        .await?;
    println!(
        "Cache get after TTL: {}",
        format_cache_response(&expired_response)
    );
    Ok(())
}

fn format_cache_response(response: &Option<Bytes>) -> String {
    match response {
        Some(bytes) => format!(
            "CacheValue {{ value: {:?} }}",
            String::from_utf8_lossy(bytes)
        ),
        None => "CacheValue { value: None }".to_string(),
    }
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
