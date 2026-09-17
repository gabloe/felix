//! TLS for the client side of a QUIC connection.
//!
//! QUIC has no unencrypted mode, so every Python client needs a trust
//! decision. Two are offered and no third: the platform trust store (what a
//! broker with a real certificate needs) or an explicit CA file (what a
//! self-signed development broker needs). There is deliberately no
//! "skip verification" switch — it is the one setting that silently turns a
//! secure deployment insecure, and a CA file covers the development case
//! without it.
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use quinn::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

pub(crate) fn client_config(ca_file: Option<&str>) -> PyResult<ClientConfig> {
    match ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            let mut file = std::io::BufReader::new(std::fs::File::open(path).map_err(|err| {
                PyValueError::new_err(format!("could not read ca_file {path:?}: {err}"))
            })?);
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut file)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    PyValueError::new_err(format!("ca_file {path:?} is not valid PEM: {err}"))
                })?;
            if certs.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "ca_file {path:?} contained no certificates"
                )));
            }
            for cert in certs {
                roots.add(cert).map_err(|err| {
                    PyValueError::new_err(format!(
                        "ca_file {path:?} holds an unusable certificate: {err}"
                    ))
                })?;
            }
            ClientConfig::with_root_certificates(Arc::new(roots)).map_err(|err| {
                PyValueError::new_err(format!("could not build a TLS config: {err}"))
            })
        }
        None => ClientConfig::try_with_platform_verifier().map_err(|err| {
            PyValueError::new_err(format!(
                "could not use the platform trust store: {err}. \
                 Pass ca_file= to trust a specific CA instead."
            ))
        }),
    }
}
