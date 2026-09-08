//! TLS for the internal listener and the peer client.
//!
//! **Peer connections are encrypted but not authenticated yet.** Brokers
//! generate a self-signed certificate at startup and the peer client accepts
//! any certificate, because there is no way for one broker to learn another's
//! today. mTLS between brokers is M8.1 (#136); until it lands, the internal
//! listener must be on a network only brokers can reach.
//!
//! What is enforced now is *role separation*: both ends negotiate
//! [`INTERNAL_ALPN`], and the listener rejects a connection that settled on
//! anything else. The client-facing endpoint uses no ALPN, so a client that
//! dials the internal port is refused before it can send a frame.
use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::{ClientConfig, ServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use super::config::INTERNAL_ALPN;

/// Server name the peer client presents. Certificates are not verified, so this
/// only has to be a valid DNS name both ends agree on.
pub const INTERNAL_SERVER_NAME: &str = "felix-internal";

/// Build the internal listener's TLS config.
pub fn server_config() -> Result<ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec![INTERNAL_SERVER_NAME.to_string()])
        .context("generate internal certificate")?;
    let cert_der = cert.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .context("internal server protocol versions")?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key.into())
        .context("internal server certificate")?;
    tls.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];

    let tls =
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).context("internal server crypto")?;
    Ok(ServerConfig::with_crypto(Arc::new(tls)))
}

/// Build the peer client's TLS config.
pub fn client_config() -> Result<ClientConfig> {
    let mut tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .context("internal client protocol versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyPeer))
        .with_no_client_auth();
    tls.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];

    let tls =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("internal client crypto")?;
    Ok(ClientConfig::new(Arc::new(tls)))
}

/// The crypto provider both internal endpoints use.
///
/// Named explicitly rather than left to `CryptoProvider::get_default`: both
/// `ring` and `aws-lc-rs` are in the dependency graph, so there is no unambiguous
/// process default, and this matches the provider quinn's own config helpers
/// pick for the client-facing endpoints.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Accepts any server certificate.
///
/// Deliberate and temporary: see the module docs. Named for what it does so it
/// cannot be mistaken for verification at a call site.
#[derive(Debug)]
struct AcceptAnyPeer;

impl ServerCertVerifier for AcceptAnyPeer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
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
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
