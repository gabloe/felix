//! mTLS termination for the bootstrap listener.
//!
//! The bootstrap API hands out admin-equivalent power, and a shared token is
//! one factor. When [`crate::config::BootstrapTlsConfig`] is set, the listener
//! refuses the TLS handshake itself to any client that does not present a
//! certificate signed by the configured CA — an unauthenticated caller never
//! reaches the router, so there is no request to mis-handle.
//!
//! This is the control plane's half of the mTLS story; M8 extends the same
//! certificate model to broker-to-broker traffic. The material loading here
//! (`load_server_config`) is deliberately plain PEM-file plumbing so that work
//! can reuse it rather than invent a second format.
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_util::sync::CancellationToken;

use crate::config::BootstrapTlsConfig;

/// Build the listener's rustls config: serve `cert_path`/`key_path`, require a
/// client certificate signed by `client_ca_path`.
///
/// Fails at startup rather than at the first connection — a bootstrap listener
/// that comes up with unreadable key material is a misconfiguration, not a
/// runtime condition to retry.
pub fn load_server_config(tls: &BootstrapTlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    // Named explicitly: more than one rustls crypto provider is linked into
    // this binary, so there is no unambiguous process default to rely on.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    let certs = load_pem_certs(&tls.cert_path)
        .with_context(|| format!("read bootstrap TLS certificate {}", tls.cert_path))?;
    let key = {
        let pem = std::fs::read(&tls.key_path)
            .with_context(|| format!("read bootstrap TLS key {}", tls.key_path))?;
        rustls_pemfile::private_key(&mut pem.as_slice())
            .context("parse bootstrap TLS key")?
            .context("bootstrap TLS key file holds no private key")?
    };

    let mut roots = rustls::RootCertStore::empty();
    for cert in load_pem_certs(&tls.client_ca_path)
        .with_context(|| format!("read bootstrap client CA {}", tls.client_ca_path))?
    {
        roots.add(cert).context("add bootstrap client CA root")?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .context("build bootstrap client verifier")?;

    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("bootstrap TLS protocol versions")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("bootstrap TLS certificate/key")?;
    Ok(Arc::new(config))
}

fn load_pem_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = std::fs::read(path)?;
    let certs = rustls_pemfile::certs(&mut pem.as_slice()).collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path}");
    Ok(certs)
}

/// Serve `router` over mTLS until `shutdown` fires, then let accepted
/// connections finish.
///
/// A failed handshake ends that connection and nothing else: the listener's
/// job during a probe or scan is to keep serving legitimate clients.
pub async fn serve_mtls(
    listener: tokio::net::TcpListener,
    router: Router,
    tls: Arc<rustls::ServerConfig>,
    shutdown: CancellationToken,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // Transient accept errors (EMFILE, resets) starve the
                        // loop if it exits; log and go on accepting.
                        tracing::warn!(error = %err, "bootstrap accept failed");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let app = router.clone();
                let shutdown = shutdown.clone();
                connections.spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(tls_stream) => tls_stream,
                        Err(err) => {
                            // The refusal doing its job: no accepted client
                            // certificate, no connection.
                            tracing::debug!(%peer, error = %err, "bootstrap TLS handshake refused");
                            return;
                        }
                    };
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let builder =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                    let conn = builder.serve_connection(TokioIo::new(tls_stream), service);
                    tokio::pin!(conn);
                    let result = tokio::select! {
                        result = conn.as_mut() => result,
                        _ = shutdown.cancelled() => {
                            // Keep-alive connections idle between requests are
                            // the common case; without this nudge each would
                            // hold the drain until its peer's idle timeout.
                            conn.as_mut().graceful_shutdown();
                            conn.as_mut().await
                        }
                    };
                    if let Err(err) = result {
                        tracing::debug!(%peer, error = %err, "bootstrap connection ended with error");
                    }
                });
            }
        }
    }

    // In-flight requests get to finish; the caller's drain budget bounds how
    // long this is allowed to take before the whole task is aborted.
    while connections.join_next().await.is_some() {}
}
