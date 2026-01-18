// Console demo that measures QUIC pub/sub latency distribution.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::{Broker, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
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
                let result = run_case(case).await?;
                print_result(&result);
            }
        }
    } else {
        let result = run_case(config).await?;
        print_result(&result);
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
    let mut payloads = None;
    let mut fanouts = None;
    let mut warmup_set = false;
    let mut total_set = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--matrix" => {
                matrix = true;
                all = false;
            }
            "--all" => all = true,
            "--binary" => binary = true,
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
                }
            }
            "--fanout" => {
                if let Some(value) = args.next() {
                    fanout = value.parse().unwrap_or(fanout);
                }
            }
            "--batch" => {
                if let Some(value) = args.next() {
                    batch_size = value.parse().unwrap_or(batch_size);
                }
            }
            "--idle-ms" => {
                if let Some(value) = args.next() {
                    idle_ms = value.parse().unwrap_or(idle_ms);
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

async fn run_case(config: DemoConfig) -> Result<DemoResult> {
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

    let total_events = config.warmup + config.total;
    let broker = Arc::new(
        Broker::new(EphemeralCache::new())
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
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = Arc::new(client.connect(addr, "localhost").await?);

    let (primary_recv, drain_tasks) =
        setup_subscribers(Arc::clone(&connection), config.fanout, total_events).await?;

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
        let mut recv = primary_recv;
        while results.len() < config.total {
            let next = tokio::time::timeout(idle_timeout, quic::read_message(&mut recv)).await;
            let message = match next {
                Ok(result) => result,
                Err(_) => break,
            };
            match message {
                Ok(Some(Message::Event { payload, .. })) => {
                    if seen < config.warmup {
                        seen += 1;
                        if seen == config.warmup
                            && let Some(tx) = warmup_tx.take()
                        {
                            let _ = tx.send(());
                        }
                        continue;
                    }
                    let elapsed = decode_latency(&payload);
                    results.push(elapsed);
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        results
    });

    let use_ack = config.batch_size <= 1 && !config.binary;
    let (mut pub_send, mut pub_recv) = if config.binary {
        let send = connection.open_uni().await?;
        (send, None)
    } else {
        let (send, recv) = connection.open_bi().await?;
        (send, Some(recv))
    };
    publish_batches(
        &mut pub_send,
        pub_recv.as_mut(),
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
        &mut pub_send,
        pub_recv.as_mut(),
        config.payload_bytes,
        config.total,
        config.batch_size,
        config.binary,
        use_ack,
    )
    .await?;

    let mut latencies = recv_task.await.expect("join");
    let elapsed = start.elapsed();
    pub_send.finish()?;
    if let Some(recv) = pub_recv.as_mut() {
        let _ = recv.read_to_end(usize::MAX).await;
    }
    drop(connection);
    for task in drain_tasks {
        task.abort();
    }
    server_task.abort();

    latencies.sort_unstable();
    let received = latencies.len();
    Ok(DemoResult {
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
    })
}

async fn setup_subscribers(
    connection: Arc<felix_transport::QuicConnection>,
    fanout: usize,
    total_events: usize,
) -> Result<(quinn::RecvStream, Vec<tokio::task::JoinHandle<()>>)> {
    let mut drain_tasks = Vec::new();
    let mut primary_recv = None;
    for _ in 0..fanout {
        let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
        quic::write_message(
            &mut sub_send,
            Message::Subscribe {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "latency".to_string(),
            },
        )
        .await?;
        sub_send.finish()?;
        let response = quic::read_message(&mut sub_recv).await?;
        if response != Some(Message::Ok) {
            return Err(anyhow::anyhow!("subscribe failed: {response:?}"));
        }
        if primary_recv.is_none() {
            primary_recv = Some(sub_recv);
        } else {
            let mut recv = sub_recv;
            let idle_timeout = Duration::from_millis(2000);
            drain_tasks.push(tokio::spawn(async move {
                let mut remaining = total_events;
                while remaining > 0 {
                    let next =
                        tokio::time::timeout(idle_timeout, quic::read_message(&mut recv)).await;
                    match next {
                        Ok(Ok(Some(Message::Event { .. }))) => {
                            remaining -= 1;
                        }
                        Ok(Ok(_)) => continue,
                        Ok(Err(_)) => break,
                        Err(_) => break,
                    }
                }
            }));
        }
    }
    Ok((primary_recv.expect("primary subscriber"), drain_tasks))
}

async fn publish_batches(
    send: &mut quinn::SendStream,
    mut recv: Option<&mut quinn::RecvStream>,
    payload_bytes: usize,
    total: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
) -> Result<()> {
    let mut remaining = total;
    while remaining > 0 {
        let count = remaining.min(batch_size);
        publish_batch(
            send,
            recv.as_deref_mut(),
            payload_bytes,
            count,
            binary,
            use_ack,
        )
        .await?;
        remaining -= count;
    }
    Ok(())
}

async fn publish_batch(
    send: &mut quinn::SendStream,
    recv: Option<&mut quinn::RecvStream>,
    payload_bytes: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
) -> Result<()> {
    let payloads = (0..batch_size)
        .map(|_| encode_payload(payload_bytes))
        .collect::<Vec<_>>();
    if binary {
        let frame =
            felix_wire::binary::encode_publish_batch("t1", "default", "latency", &payloads)?;
        send.write_all(&frame.encode()).await?;
        return Ok(());
    }
    if batch_size == 1 {
        quic::write_message(
            send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "latency".to_string(),
                payload: payloads[0].clone(),
                ack: Some(if use_ack {
                    AckMode::PerMessage
                } else {
                    AckMode::None
                }),
            },
        )
        .await?;
        if use_ack
            && let Some(recv) = recv
            && quic::read_message(recv).await? != Some(Message::Ok)
        {
            return Err(anyhow::anyhow!("publish ack failed"));
        }
    } else {
        quic::write_message(
            send,
            Message::PublishBatch {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "latency".to_string(),
                payloads,
                ack: Some(if use_ack {
                    AckMode::PerBatch
                } else {
                    AckMode::None
                }),
            },
        )
        .await?;
        if use_ack
            && let Some(recv) = recv
            && quic::read_message(recv).await? != Some(Message::Ok)
        {
            return Err(anyhow::anyhow!("publish batch ack failed"));
        }
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
            case.binary = false;
            case.warmup = 200;
            case.total = 2000;
            let result = run_case(case).await?;
            print_result(&result);
        }
    }

    let throughput_payloads = [0, 1024, 4096];
    let throughput_fanouts = [1, 10];
    for payload in throughput_payloads {
        for fanout in throughput_fanouts {
            for binary in [false, true] {
                let mut case = base;
                case.payload_bytes = payload;
                case.fanout = fanout;
                case.batch_size = 64;
                case.binary = binary;
                case.warmup = 200;
                case.total = 5000;
                let result = run_case(case).await?;
                print_result(&result);
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

fn format_duration(duration: Duration) -> String {
    let micros = duration.as_micros();
    if micros >= 1000 {
        format!("{:.3} ms", micros as f64 / 1000.0)
    } else {
        format!("{micros} us")
    }
}
