// Console demo that measures QUIC pub/sub latency distribution.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::Message;
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<()> {
    let (warmup, total, payload_bytes) = parse_args();
    println!("== Felix QUIC Pub/Sub Latency Demo ==");
    println!("Mode: QUIC broker protocol over felix-transport.");
    println!("Warmup: {warmup} messages");
    println!("Measure: {total} messages, payload {payload_bytes} bytes");
    println!("Target: p999 <= 1 ms (local baseline)");

    let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
    let connection = client.connect(addr, "localhost").await?;

    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            stream: "latency".to_string(),
        },
    )
    .await?;
    sub_send.finish()?;
    let response = quic::read_message(&mut sub_recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow::anyhow!("subscribe failed: {response:?}"));
    }

    for _ in 0..warmup {
        publish(&connection, payload_bytes).await?;
    }
    for _ in 0..warmup {
        let _ = quic::read_message(&mut sub_recv).await?;
    }

    let recv_task = tokio::spawn(async move {
        let mut results = Vec::with_capacity(total);
        for _ in 0..total {
            match quic::read_message(&mut sub_recv).await {
                Ok(Some(Message::Event { payload, .. })) => {
                    let elapsed = decode_latency(&payload);
                    results.push(elapsed);
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        results
    });

    let start = Instant::now();
    for _ in 0..total {
        publish(&connection, payload_bytes).await?;
    }

    let mut latencies = recv_task.await.expect("join");
    let elapsed = start.elapsed();
    drop(connection);
    server_task.abort();

    latencies.sort_unstable();
    println!("Results (n = {total}):");
    println!("  p50  = {}", format_duration(percentile(&latencies, 0.50)));
    println!("  p99  = {}", format_duration(percentile(&latencies, 0.99)));
    println!(
        "  p999 = {}",
        format_duration(percentile(&latencies, 0.999))
    );
    println!(
        "  throughput = {:.2} msg/s",
        total as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

fn parse_args() -> (usize, usize, usize) {
    let mut args = std::env::args().skip(1);
    let warmup = args
        .next()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let total = args
        .next()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10000);
    let payload_bytes = args
        .next()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    (warmup, total, payload_bytes)
}

async fn publish(connection: &felix_transport::QuicConnection, payload_bytes: usize) -> Result<()> {
    let payload = encode_payload(payload_bytes);
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Publish {
            stream: "latency".to_string(),
            payload: payload.to_vec(),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow::anyhow!("publish failed: {response:?}"));
    }
    Ok(())
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
