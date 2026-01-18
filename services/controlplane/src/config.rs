use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;

// Control plane configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub bind_addr: SocketAddr,
    pub metrics_bind: SocketAddr,
    pub region_id: String,
}

#[derive(Debug, Deserialize)]
struct ControlPlaneConfigOverride {
    bind_addr: Option<String>,
    metrics_bind: Option<String>,
    region_id: Option<String>,
}

impl ControlPlaneConfig {
    pub fn from_env() -> Result<Self> {
        let metrics_bind = std::env::var("FELIX_CP_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_CP_METRICS_BIND")?;
        let bind_addr = std::env::var("FELIX_CP_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
            .parse()
            .with_context(|| "parse FELIX_CP_BIND")?;
        let region_id = std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string());
        Ok(Self {
            bind_addr,
            metrics_bind,
            region_id,
        })
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        if let Ok(path) = std::env::var("FELIX_CP_CONFIG") {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read FELIX_CP_CONFIG: {path}"))?;
            let override_cfg: ControlPlaneConfigOverride = serde_yaml::from_str(&contents)
                .with_context(|| "parse control plane config yaml")?;
            if let Some(value) = override_cfg.bind_addr {
                config.bind_addr = value.parse().with_context(|| "parse bind_addr")?;
            }
            if let Some(value) = override_cfg.metrics_bind {
                config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
            }
            if let Some(value) = override_cfg.region_id {
                config.region_id = value;
            }
        }
        Ok(config)
    }
}
