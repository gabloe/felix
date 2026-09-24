//! TLS for the internal listener and the peer client.
//!
//! Two modes, chosen by configuration:
//!
//! - **mTLS**, when `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA` are set. Both
//!   ends present a certificate and each verifies the other's chains to the
//!   configured CA. The certificate's DNS name is the broker's identity: a
//!   dialler verifies the listener's certificate against the node id it meant
//!   to reach, and a listener checks the name a peer claims in `Hello` against
//!   the certificate it presented. Nothing is granted on a claimed name.
//! - **Unauthenticated**, when none is set. Brokers generate a self-signed
//!   certificate at startup and the peer client accepts any certificate. The
//!   link is encrypted and nothing more; startup says so, and the internal
//!   listener must then be on a network only brokers can reach.
//!
//! Rotation is a file swap. The certificate and key are re-read on a timer
//! and installed for the *next* handshake; connections already up keep the
//! identity they were made with, so a rolling rotation never drops healthy
//! traffic. The CA bundle is read once: trust roots change at a restart.
//!
//! What both modes enforce is *role separation*: both ends negotiate
//! [`INTERNAL_ALPN`], and the listener rejects a connection that settled on
//! anything else. The client-facing endpoint uses no ALPN, so a client that
//! dials the internal port is refused before it can send a frame.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use quinn::{ClientConfig, ServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, SignatureScheme};

use super::config::{INTERNAL_ALPN, PeerTlsConfig};

/// Server name the peer client presents when certificates are not verified.
/// Only has to be a valid DNS name both ends agree on.
pub(super) const INTERNAL_SERVER_NAME: &str = "felix-internal";

/// How often the certificate and key files are checked for a rotation.
const RELOAD_INTERVAL: Duration = Duration::from_secs(30);

/// This broker's peer identity and the roots it trusts.
///
/// One value shared by the listener and the dialler, so a rotation reaches
/// both at once.
pub struct PeerTls {
    paths: PeerTlsConfig,
    roots: Arc<rustls::RootCertStore>,
    identity: Arc<Identity>,
}

impl std::fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Paths, never material.
        f.debug_struct("PeerTls")
            .field("paths", &self.paths)
            .finish()
    }
}

impl PeerTls {
    /// Load the CA bundle and this broker's certificate and key.
    ///
    /// Fails at startup rather than at the first connection: a listener that
    /// comes up with unreadable key material is a misconfiguration, not a
    /// runtime condition to retry.
    pub fn load(paths: &PeerTlsConfig) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in load_pem_certs(&paths.ca_path)
            .with_context(|| format!("read FELIX_INTERNAL_TLS_CA {}", paths.ca_path))?
        {
            roots.add(cert).context("add internal CA root")?;
        }
        let identity = Arc::new(Identity {
            current: ArcSwap::from_pointee(load_identity(paths)?),
        });
        Ok(Self {
            paths: paths.clone(),
            roots: Arc::new(roots),
            identity,
        })
    }

    /// Re-read the certificate and key. A file that is mid-write or missing
    /// leaves the current identity in place; the next check tries again.
    pub fn reload(&self) -> Result<bool> {
        let fresh = load_identity(&self.paths)?;
        let changed = fresh.cert != self.identity.current.load().cert;
        if changed {
            self.identity.current.store(Arc::new(fresh));
        }
        Ok(changed)
    }

    /// Check the files on an interval until `shutdown`.
    pub fn spawn_reload(
        self: Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(RELOAD_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = ticker.tick() => match self.reload() {
                        Ok(true) => tracing::info!(
                            cert = %self.paths.cert_path,
                            "internal TLS certificate rotated; new peer connections use it",
                        ),
                        Ok(false) => {}
                        Err(err) => tracing::warn!(
                            error = %err,
                            "could not reload the internal TLS certificate; keeping the current one",
                        ),
                    },
                }
            }
        })
    }

    /// The name `peer_certs` is valid for must be `node_id`.
    ///
    /// The chain was verified at the handshake; this is the binding between
    /// what the certificate says and what the peer *claims*, so a broker
    /// holding a valid certificate for one name cannot speak as another.
    pub fn verify_identity(
        &self,
        peer_certs: Option<&[CertificateDer<'_>]>,
        node_id: &str,
    ) -> std::result::Result<(), String> {
        let leaf = peer_certs
            .and_then(|certs| certs.first())
            .ok_or_else(|| "the peer presented no certificate".to_string())?;
        let name = ServerName::try_from(node_id)
            .map_err(|_| format!("{node_id} is not a name a certificate can carry"))?;
        let cert = webpki::EndEntityCert::try_from(leaf)
            .map_err(|err| format!("the peer's certificate does not parse: {err}"))?;
        cert.verify_is_valid_for_subject_name(&name)
            .map_err(|_| format!("the peer's certificate is not issued to {node_id}"))
    }
}

/// The certificate and key currently presented, swapped whole on rotation.
struct Identity {
    current: ArcSwap<Loaded>,
}

struct Loaded {
    cert: Vec<CertificateDer<'static>>,
    key: Arc<CertifiedKey>,
}

impl rustls::server::ResolvesServerCert for Identity {
    fn resolve(&self, _hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.load().key))
    }
}

impl rustls::client::ResolvesClientCert for Identity {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.load().key))
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Identity(<redacted>)")
    }
}

/// Build the internal listener's TLS config.
pub(super) fn server_config(tls: Option<&PeerTls>) -> Result<ServerConfig> {
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .context("internal server protocol versions")?;
    let mut config = match tls {
        Some(tls) => {
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::clone(&tls.roots),
                provider(),
            )
            .build()
            .context("build internal client verifier")?;
            let mut config = builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(
                    Arc::clone(&tls.identity) as Arc<dyn rustls::server::ResolvesServerCert>
                );
            // No resumption: a resumed session carries the client identity of
            // the session it resumes, which is how a rotated -- or revoked --
            // certificate would go on being accepted. Peer connections are
            // few and long-lived, so a full handshake each time costs nothing
            // worth having.
            config.send_tls13_tickets = 0;
            config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
            config
        }
        None => {
            let cert = rcgen::generate_simple_self_signed(vec![INTERNAL_SERVER_NAME.to_string()])
                .context("generate internal certificate")?;
            let cert_der = cert.cert.der().clone();
            let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
            builder
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key.into())
                .context("internal server certificate")?
        }
    };
    config.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];

    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(config)
        .context("internal server crypto")?;
    Ok(ServerConfig::with_crypto(Arc::new(crypto)))
}

/// Build the peer client's TLS config.
pub(super) fn client_config(tls: Option<&PeerTls>) -> Result<ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .context("internal client protocol versions")?;
    let mut config = match tls {
        Some(tls) => {
            let mut config = builder
                .with_root_certificates(Arc::clone(&tls.roots))
                .with_client_cert_resolver(
                    Arc::clone(&tls.identity) as Arc<dyn rustls::client::ResolvesClientCert>
                );
            // See `server_config`: every connection presents the certificate
            // currently on disk, never one remembered from an earlier session.
            config.resumption = rustls::client::Resumption::disabled();
            config
        }
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyPeer))
            .with_no_client_auth(),
    };
    config.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(config)
        .context("internal client crypto")?;
    Ok(ClientConfig::new(Arc::new(crypto)))
}

fn load_identity(paths: &PeerTlsConfig) -> Result<Loaded> {
    let cert = load_pem_certs(&paths.cert_path)
        .with_context(|| format!("read FELIX_INTERNAL_TLS_CERT {}", paths.cert_path))?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_file(&paths.key_path)
        .with_context(|| format!("read FELIX_INTERNAL_TLS_KEY {}", paths.key_path))?;
    let key = CertifiedKey::from_der(cert.clone(), key, &provider())
        .context("the internal TLS key does not match its certificate")?;
    Ok(Loaded {
        cert,
        key: Arc::new(key),
    })
}

fn load_pem_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)?.collect::<std::result::Result<Vec<_>, _>>()?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path}");
    Ok(certs)
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
/// Only in the unauthenticated mode. Named for what it does so it cannot be
/// mistaken for verification at a call site.
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
