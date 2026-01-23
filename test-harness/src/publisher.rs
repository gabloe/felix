// Test harness publisher: publishes messages to the broker at a configured rate.
use anyhow::{Context, Result};
use clap::Parser;
use felix_client::{Client, ClientConfig};
use felix_test_harness::tls;
use felix_wire::AckMode;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "publisher")]
#[command(about = "Test harness publisher for Felix pub/sub")]
struct Args {
    /// Broker address (host:port)
    #[arg(long, default_value = "broker:5000")]
    broker: String,

    /// Tenant ID
    #[arg(long, default_value = "test-tenant")]
    tenant: String,

    /// Namespace
    #[arg(long, default_value = "default")]
    namespace: String,

    /// Stream/topic name
    #[arg(long, default_value = "test-stream")]
    stream: String,

    /// Message payload size in bytes
    #[arg(long, default_value = "1024")]
    payload_size: usize,

    /// Messages per second rate (0 = unlimited)
    #[arg(long, default_value = "100")]
    rate: u64,

    /// Total number of messages to publish (0 = unlimited)
    #[arg(long, default_value = "0")]
    count: u64,

    /// Publisher ID for logging
    #[arg(long, default_value = "pub-1")]
    id: String,

    /// Disable certificate validation (for self-signed certs)
    #[arg(long, default_value = "true")]
    insecure: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install default crypto provider for rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(
        id = %args.id,
        broker = %args.broker,
        tenant = %args.tenant,
        namespace = %args.namespace,
        stream = %args.stream,
        payload_size = args.payload_size,
        rate = args.rate,
        count = args.count,
        "Starting publisher"
    );

    // Build client config with optional insecure mode
    let quinn_config = if args.insecure {
        tls::build_insecure_client_config()?
    } else {
        tls::build_client_config()?
    };

    let client_config = ClientConfig::from_env_or_yaml(quinn_config, None)?;

    // Connect to broker
    let addr = args.broker.parse::<std::net::SocketAddr>()
        .context("invalid broker address")?;
    let host = args.broker.split(':')
        .next()
        .context("invalid broker address format")?;
    
    info!(id = %args.id, addr = %addr, "Connecting to broker");
    let client = Client::connect(addr, host, client_config)
        .await
        .context("connect to broker")?;
    info!(id = %args.id, "Connected to broker");

    // Get publisher handle
    let publisher = client.publisher().await?;

    // Create payload
    let payload = vec![b'x'; args.payload_size];

    // Calculate delay between messages if rate is specified
    let delay = if args.rate > 0 {
        Some(Duration::from_micros(1_000_000 / args.rate))
    } else {
        None
    };

    let start_time = Instant::now();
    let mut published = 0u64;
    let mut errors = 0u64;

    loop {
        if args.count > 0 && published >= args.count {
            break;
        }

        let result = publisher
            .publish(
                &args.tenant,
                &args.namespace,
                &args.stream,
                payload.clone(),
                AckMode::None,
            )
            .await;

        match result {
            Ok(_) => {
                published += 1;
                if published % 1000 == 0 {
                    let elapsed = start_time.elapsed();
                    let rate = published as f64 / elapsed.as_secs_f64();
                    info!(
                        id = %args.id,
                        published = published,
                        errors = errors,
                        rate = format!("{:.2}", rate),
                        "Publishing progress"
                    );
                }
            }
            Err(e) => {
                errors += 1;
                if errors % 100 == 0 {
                    error!(id = %args.id, error = %e, errors = errors, "Publish error");
                }
            }
        }

        if let Some(delay) = delay {
            sleep(delay).await;
        }
    }

    let elapsed = start_time.elapsed();
    let rate = published as f64 / elapsed.as_secs_f64();
    info!(
        id = %args.id,
        published = published,
        errors = errors,
        elapsed = format!("{:.2}s", elapsed.as_secs_f64()),
        rate = format!("{:.2} msg/s", rate),
        "Publisher completed"
    );

    Ok(())
}
