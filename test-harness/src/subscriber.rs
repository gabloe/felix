// Test harness subscriber: receives messages from the broker.
use anyhow::{Context, Result};
use clap::Parser;
use felix_client::{Client, ClientConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rustls::RootCertStore;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "subscriber")]
#[command(about = "Test harness subscriber for Felix pub/sub")]
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

    /// Subscriber ID for logging
    #[arg(long, default_value = "sub-1")]
    id: String,

    /// Timeout for receiving messages in seconds (0 = no timeout)
    #[arg(long, default_value = "60")]
    timeout: u64,

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
        "Starting subscriber"
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

    // Subscribe
    info!(id = %args.id, "Subscribing to stream");
    let mut subscription = client
        .subscribe(&args.tenant, &args.namespace, &args.stream)
        .await
        .context("subscribe to stream")?;
    info!(id = %args.id, "Subscribed to stream");

    let start_time = Instant::now();
    let mut received = 0u64;
    let mut errors = 0u64;
    let timeout_duration = if args.timeout > 0 {
        Some(Duration::from_secs(args.timeout))
    } else {
        None
    };

    loop {
        let result = if let Some(timeout_dur) = timeout_duration {
            timeout(timeout_dur, subscription.next_event()).await
        } else {
            Ok(subscription.next_event().await)
        };

        match result {
            Ok(Ok(Some(event))) => {
                received += 1;
                if received % 1000 == 0 {
                    let elapsed = start_time.elapsed();
                    let rate = received as f64 / elapsed.as_secs_f64();
                    info!(
                        id = %args.id,
                        received = received,
                        errors = errors,
                        rate = format!("{:.2}", rate),
                        payload_size = event.payload.len(),
                        "Receiving progress"
                    );
                }
            }
            Ok(Ok(None)) => {
                info!(id = %args.id, "Stream closed by broker");
                break;
            }
            Ok(Err(e)) => {
                errors += 1;
                if errors % 100 == 0 {
                    error!(id = %args.id, error = %e, errors = errors, "Receive error");
                }
            }
            Err(_) => {
                warn!(id = %args.id, "Timeout waiting for event, stopping");
                break;
            }
        }
    }

    let elapsed = start_time.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        received as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    info!(
        id = %args.id,
        received = received,
        errors = errors,
        elapsed = format!("{:.2}s", elapsed.as_secs_f64()),
        rate = format!("{:.2} msg/s", rate),
        "Subscriber completed"
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
