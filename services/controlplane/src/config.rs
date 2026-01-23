use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;

// Control plane configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    // HTTPS bind address for the control plane API.
    pub bind_addr: SocketAddr,
    // Metrics HTTP bind address.
    pub metrics_bind: SocketAddr,
    // Region identifier used for multi-region awareness.
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
        // Environment variables provide defaults for local development.
        let metrics_bind = std::env::var("FELIX_CP_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_CP_METRICS_BIND")?;
        let bind_addr = std::env::var("FELIX_CP_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
            .parse()
            .with_context(|| "parse FELIX_CP_BIND")?;
        // Region defaults to "local" when unset.
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
            // YAML overrides allow ops-friendly config files.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use tempfile::TempDir;
    use serial_test::serial;

    // Helper to clear all Felix env vars
    fn clear_felix_env() {
        for (key, _) in env::vars() {
            if key.starts_with("FELIX_") {
                unsafe {
                    env::remove_var(key);
                }
            }
        }
    }

    #[serial]
    #[test]
    fn from_env_uses_defaults() {
        clear_felix_env();
        let config = ControlPlaneConfig::from_env().expect("from_env");
        assert_eq!(config.bind_addr.to_string(), "0.0.0.0:8443");
        assert_eq!(config.metrics_bind.to_string(), "0.0.0.0:8080");
        assert_eq!(config.region_id, "local");
    }

    #[serial]
    #[test]
    fn from_env_respects_env_vars() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_CP_BIND", "127.0.0.1:9443");
            env::set_var("FELIX_CP_METRICS_BIND", "127.0.0.1:9090");
            env::set_var("FELIX_REGION_ID", "us-west-2");
        }

        let config = ControlPlaneConfig::from_env().expect("from_env");
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:9443");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9090");
        assert_eq!(config.region_id, "us-west-2");

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_rejects_invalid_socket_addr() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_CP_BIND", "not-a-valid-address");
        }
        let result = ControlPlaneConfig::from_env();
        assert!(result.is_err());
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_no_file_uses_defaults() {
        clear_felix_env();
        let config = ControlPlaneConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.bind_addr.to_string(), "0.0.0.0:8443");
        assert_eq!(config.region_id, "local");
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_file_not_found_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let nonexistent = tmpdir.path().join("nonexistent.yml");
        unsafe {
            env::set_var("FELIX_CP_CONFIG", nonexistent.to_str().unwrap());
        }
        let result = ControlPlaneConfig::from_env_or_yaml();
        assert!(result.is_err());
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_overrides_with_valid_yaml() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(
            &config_path,
            r#"
bind_addr: "127.0.0.1:7443"
metrics_bind: "127.0.0.1:7070"
region_id: "eu-central-1"
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_CP_CONFIG", config_path.to_str().unwrap());
        }

        let config = ControlPlaneConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:7443");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:7070");
        assert_eq!(config.region_id, "eu-central-1");

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_invalid_yaml_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("bad.yml");
        fs::write(&config_path, "this is not: valid: yaml:").unwrap();
        unsafe {
            env::set_var("FELIX_CP_CONFIG", config_path.to_str().unwrap());
        }

        let result = ControlPlaneConfig::from_env_or_yaml();
        assert!(result.is_err());

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_invalid_socket_in_yaml_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(&config_path, "bind_addr: \"not-a-socket\"").unwrap();
        unsafe {
            env::set_var("FELIX_CP_CONFIG", config_path.to_str().unwrap());
        }

        let result = ControlPlaneConfig::from_env_or_yaml();
        assert!(result.is_err());

        clear_felix_env();
    }
}
