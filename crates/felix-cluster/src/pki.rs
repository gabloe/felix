//! Certificates for the brokers a cluster spawns.
//!
//! Every cluster test runs under broker-to-broker mTLS, so the property the
//! tests prove is the deployed one. One CA per cluster, on disk under the data
//! root the way a deployment mounts it; one certificate per broker, issued to
//! its node id, regenerated from the same CA whenever that broker is spawned
//! again so a restart keeps its identity.
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Paths a broker is told about, in the shape of its `FELIX_INTERNAL_TLS_*`.
pub struct PeerCert {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

/// Issue `node_id`'s certificate under `root/pki`, creating the CA on first
/// use and reusing it afterwards.
pub fn issue(root: &Path, node_id: &str) -> Result<PeerCert> {
    let dir = root.join("pki");
    std::fs::create_dir_all(&dir).context("create pki dir")?;
    let ca = load_or_create_ca(&dir)?;

    let key = rcgen::KeyPair::generate().context("broker key")?;
    let params =
        rcgen::CertificateParams::new(vec![node_id.to_string()]).context("broker cert params")?;
    let cert = params.signed_by(&key, &ca).context("sign broker cert")?;
    let cert_path = dir.join(format!("{node_id}.pem"));
    let key_path = dir.join(format!("{node_id}.key.pem"));
    std::fs::write(&cert_path, cert.pem()).context("write broker cert")?;
    std::fs::write(&key_path, key.serialize_pem()).context("write broker key")?;
    Ok(PeerCert {
        cert: cert_path,
        key: key_path,
        ca: dir.join("ca.pem"),
    })
}

fn load_or_create_ca(dir: &Path) -> Result<rcgen::Issuer<'static, rcgen::KeyPair>> {
    let cert_path = dir.join("ca.pem");
    let key_path = dir.join("ca.key.pem");
    if cert_path.exists() && key_path.exists() {
        let key =
            rcgen::KeyPair::from_pem(&std::fs::read_to_string(&key_path).context("read ca key")?)
                .context("parse ca key")?;
        return rcgen::Issuer::from_ca_cert_pem(
            &std::fs::read_to_string(&cert_path).context("read ca cert")?,
            key,
        )
        .context("parse ca cert");
    }
    let key = rcgen::KeyPair::generate().context("ca key")?;
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).context("ca params")?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key).context("self-sign ca")?;
    std::fs::write(&cert_path, cert.pem()).context("write ca cert")?;
    std::fs::write(&key_path, key.serialize_pem()).context("write ca key")?;
    Ok(rcgen::Issuer::new(params, key))
}
