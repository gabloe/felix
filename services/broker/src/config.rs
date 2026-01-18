use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;

// Broker service configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub quic_bind: SocketAddr,
    pub metrics_bind: SocketAddr,
    pub controlplane_url: Option<String>,
    pub controlplane_sync_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BrokerConfigOverride {
    quic_bind: Option<String>,
    metrics_bind: Option<String>,
    controlplane_url: Option<String>,
    controlplane_sync_interval_ms: Option<u64>,
}

impl BrokerConfig {
    pub fn from_env() -> Result<Self> {
        let metrics_bind = std::env::var("FELIX_BROKER_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_BROKER_METRICS_BIND")?;
        let quic_bind = std::env::var("FELIX_QUIC_BIND")
            .unwrap_or_else(|_| "0.0.0.0:5000".to_string())
            .parse()
            .with_context(|| "parse FELIX_QUIC_BIND")?;
        let controlplane_url = std::env::var("FELIX_CP_URL").ok();
        let controlplane_sync_interval_ms = std::env::var("FELIX_CP_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2000);
        Ok(Self {
            quic_bind,
            metrics_bind,
            controlplane_url,
            controlplane_sync_interval_ms,
        })
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        if let Ok(path) = std::env::var("FELIX_BROKER_CONFIG") {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read FELIX_BROKER_CONFIG: {path}"))?;
            let override_cfg: BrokerConfigOverride =
                serde_yaml::from_str(&contents).with_context(|| "parse broker config yaml")?;
            if let Some(value) = override_cfg.quic_bind {
                config.quic_bind = value.parse().with_context(|| "parse quic_bind")?;
            }
            if let Some(value) = override_cfg.metrics_bind {
                config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
            }
            if let Some(value) = override_cfg.controlplane_url {
                config.controlplane_url = Some(value);
            }
            if let Some(value) = override_cfg.controlplane_sync_interval_ms {
                config.controlplane_sync_interval_ms = value;
            }
        }
        Ok(config)
    }
}
