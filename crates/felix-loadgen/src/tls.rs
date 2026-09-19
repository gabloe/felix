//! Client TLS for a cluster whose brokers present self-signed certificates.
//!
//! Brokers generate a certificate at startup and publish it nowhere, so a
//! remote client has nothing to verify against — the same gap the cluster
//! harness documents, with the same answer until a real PKI exists (#127
//! territory). The perf suite runs inside a private VNet the deployer owns,
//! which is what makes accepting any certificate tolerable *here*; the flag
//! that enables it is named for what it does, and there is no quiet default.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_client::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

pub(crate) fn client_config(tenant_id: &str, token: &str) -> Result<ClientConfig> {
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .context("client protocol versions")?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnyBroker))
    .with_no_client_auth();
    // The client-facing role negotiates no ALPN; setting one would aim this at
    // the internal listener, which refuses it.
    tls.alpn_protocols.clear();

    let quinn = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("client crypto")?,
    ));
    // Env-aware, not `optimized_defaults`: the instrument has to be able to
    // sweep the client's own knobs. Built on the defaults, so an unset
    // environment measures exactly what a default client does -- but
    // FELIX_PUB_CONN_POOL, FELIX_PUB_SHARDING and FELIX_PUBLISH_INFLIGHT_BYTES
    // silently did nothing here before, which made three runs of a perf
    // session measure the configuration they were meant to be varying (#553).
    let mut config =
        ClientConfig::from_env_or_yaml(quinn, None).context("build the loadgen's client config")?;
    config.auth_tenant_id = Some(tenant_id.to_string());
    config.auth_token = Some(token.to_string());
    Ok(config)
}

#[derive(Debug)]
struct AcceptAnyBroker;

impl ServerCertVerifier for AcceptAnyBroker {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::client_config;

    /// **The instrument must respond to the knobs it documents.**
    ///
    /// `optimized_defaults` ignores the environment, so every `FELIX_PUB_*`
    /// variable was inert here and a run that set one measured the default
    /// instead -- silently, which is how a perf session drew two conclusions
    /// from configurations it had never actually run (#553).
    #[test]
    fn the_client_config_reads_the_environment() {
        // Serialised by the env mutation, so both assertions live in one test.
        unsafe { std::env::remove_var("FELIX_PUB_CONN_POOL") };
        let default_pool = client_config("t1", "token")
            .expect("config")
            .publish_conn_pool;

        unsafe { std::env::set_var("FELIX_PUB_CONN_POOL", "17") };
        let configured = client_config("t1", "token").expect("config");
        unsafe { std::env::remove_var("FELIX_PUB_CONN_POOL") };

        assert_ne!(
            default_pool, 17,
            "pick a probe value the default is not, or this proves nothing"
        );
        assert_eq!(
            configured.publish_conn_pool, 17,
            "FELIX_PUB_CONN_POOL did not reach the client"
        );
        // An unset environment must still measure a default client, so every
        // number taken before this change stays comparable.
        assert_eq!(
            client_config("t1", "token")
                .expect("config")
                .publish_conn_pool,
            default_pool
        );
    }
}
