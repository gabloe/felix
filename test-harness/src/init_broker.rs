// Broker initialization script: registers tenant, namespace, and stream for testing.
use anyhow::{Context, Result};
use clap::Parser;
use felix_client::{Client, ClientConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rustls::RootCertStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "init-broker")]
#[command(about = "Initialize broker with test tenant, namespace, and stream")]
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

    /// Retry attempts for broker connection
    #[arg(long, default_value = "30")]
    retry_attempts: u32,

    /// Retry delay in seconds
    #[arg(long, default_value = "2")]
    retry_delay: u64,

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
        broker = %args.broker,
        tenant = %args.tenant,
        namespace = %args.namespace,
        stream = %args.stream,
        "Initializing broker"
    );

    // Build client config with optional insecure mode
    let quinn_config = if args.insecure {
        build_insecure_client_config()?
    } else {
        build_client_config()?
    };

    let client_config = ClientConfig::from_env_or_yaml(quinn_config, None)?;

    // Parse broker address
    let addr = args.broker.parse::<std::net::SocketAddr>()
        .context("invalid broker address")?;
    let host = args.broker.split(':')
        .next()
        .context("invalid broker address format")?;

    // Retry connection to broker
    let mut client = None;
    for attempt in 1..=args.retry_attempts {
        info!(attempt = attempt, "Attempting to connect to broker");
        match Client::connect(addr, host, client_config.clone()).await {
            Ok(c) => {
                client = Some(c);
                info!("Connected to broker");
                break;
            }
            Err(e) => {
                if attempt < args.retry_attempts {
                    error!(error = %e, "Failed to connect, retrying...");
                    sleep(Duration::from_secs(args.retry_delay)).await;
                } else {
                    return Err(e).context("failed to connect to broker after all retries");
                }
            }
        }
    }

    let _client = client.unwrap();

    // Note: The broker automatically accepts any tenant/namespace/stream on first use
    // in the current implementation. This init script is a placeholder for future
    // versions that may require explicit registration via a control plane API.
    
    info!("Broker initialization complete");
    info!(
        tenant = %args.tenant,
        namespace = %args.namespace,
        stream = %args.stream,
        "Test environment is ready"
    );

    Ok(())
}

fn build_insecure_client_config() -> Result<QuinnClientConfig> {
    // Dangerous: accepts any certificate (for testing with self-signed certs)
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
        .with_no_client_auth();
    Ok(QuinnClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    )))
}

fn build_client_config() -> Result<QuinnClientConfig> {
    let roots = RootCertStore::empty();
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

// Dangerous: certificate verifier that accepts any certificate
#[derive(Debug)]
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
