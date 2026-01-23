// Test harness publisher: publishes messages to the broker at a configured rate.
use anyhow::{Context, Result};
use clap::Parser;
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rustls::RootCertStore;
use std::sync::Arc;
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
        build_insecure_client_config()?
    } else {
        build_client_config()?
    };

    let client_config = ClientConfig::from_env_or_yaml(quinn_config, None)?;

    // Connect to broker
    let (host, port_str) = args.broker.split_once(':').context("invalid broker address")?;
    let port: u16 = port_str.parse().context("invalid port")?;
    let addr = format!("{}:{}", host, port);
    
    info!(id = %args.id, addr = %addr, "Connecting to broker");
    let client = Client::connect(addr.parse()?, host, client_config)
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

fn build_insecure_client_config() -> Result<QuinnClientConfig> {
    // Dangerous: accepts any certificate (for testing with self-signed certs)
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
        .with_no_client_auth();
    Ok(QuinnClientConfig::new(Arc::new(crypto)))
}

fn build_client_config() -> Result<QuinnClientConfig> {
    let roots = RootCertStore::empty();
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

// Dangerous: certificate verifier that accepts any certificate
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer,
        _intermediates: &[rustls::pki_types::CertificateDer],
        _server_name: &rustls::pki_types::ServerName,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
