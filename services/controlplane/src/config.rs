use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;

pub const DEFAULT_CHANGES_LIMIT: u64 = 1000;
pub const DEFAULT_CHANGE_RETENTION_MAX_ROWS: i64 = 10_000;
const DEFAULT_PG_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_ACQUIRE_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    Memory,
    Postgres,
}

impl FromStr for StorageBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "memory" => Ok(StorageBackend::Memory),
            "postgres" => Ok(StorageBackend::Postgres),
            other => Err(anyhow!("invalid storage backend: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub url: String,
    pub max_connections: u32,
    pub connect_timeout_ms: u64,
    pub acquire_timeout_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: DEFAULT_PG_MAX_CONNECTIONS,
            connect_timeout_ms: DEFAULT_PG_CONNECT_TIMEOUT_MS,
            acquire_timeout_ms: DEFAULT_PG_ACQUIRE_TIMEOUT_MS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub bind_addr: SocketAddr,
    pub metrics_bind: SocketAddr,
    pub region_id: String,
    pub storage: StorageBackend,
    pub postgres: Option<PostgresConfig>,
    pub changes_limit: u64,
    pub change_retention_max_rows: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ControlPlaneConfigOverride {
    bind_addr: Option<String>,
    metrics_bind: Option<String>,
    region_id: Option<String>,
    storage: Option<StorageOverride>,
    postgres: Option<PostgresOverride>,
    changes_limit: Option<u64>,
    change_retention_max_rows: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct StorageOverride {
    backend: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PostgresOverride {
    url: Option<String>,
    max_connections: Option<u32>,
    connect_timeout_ms: Option<u64>,
    acquire_timeout_ms: Option<u64>,
}

impl ControlPlaneConfig {
    pub fn from_env() -> Result<Self> {
        let metrics_bind = std::env::var("FELIX_CONTROLPLANE_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_METRICS_BIND")?;
        let bind_addr = std::env::var("FELIX_CONTROLPLANE_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_BIND")?;
        let region_id = std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string());

        let mut storage = std::env::var("FELIX_CONTROLPLANE_STORAGE_BACKEND")
            .ok()
            .and_then(|v| StorageBackend::from_str(&v).ok())
            .unwrap_or(StorageBackend::Memory);

        let changes_limit = std::env::var("FELIX_CONTROLPLANE_CHANGES_LIMIT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CHANGES_LIMIT);
        let change_retention_max_rows =
            std::env::var("FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .or(Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS));

        let pg_url = std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok();
        let mut postgres = None;
        if let Some(url) = pg_url {
            postgres = Some(PostgresConfig {
                url,
                max_connections: std::env::var("FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(DEFAULT_PG_MAX_CONNECTIONS),
                connect_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_CONNECT_TIMEOUT_MS),
                acquire_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_ACQUIRE_TIMEOUT_MS),
            });
            if matches!(storage, StorageBackend::Memory) {
                storage = StorageBackend::Postgres;
            }
        }

        let config = Self {
            bind_addr,
            metrics_bind,
            region_id,
            storage,
            postgres,
            changes_limit,
            change_retention_max_rows,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        if let Ok(path) = std::env::var("FELIX_CONTROLPLANE_CONFIG") {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read FELIX_CONTROLPLANE_CONFIG: {path}"))?;
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
            if let Some(value) = override_cfg.changes_limit {
                config.changes_limit = value;
            }
            if let Some(value) = override_cfg.change_retention_max_rows {
                config.change_retention_max_rows = Some(value);
            }
            if let Some(storage_override) = override_cfg.storage
                && let Some(backend) = storage_override.backend
            {
                config.storage = StorageBackend::from_str(&backend)?;
            }
            if let Some(pg_override) = override_cfg.postgres {
                let mut pg_cfg = config.postgres.unwrap_or_default();
                if let Some(url) = pg_override.url {
                    pg_cfg.url = url;
                }
                if let Some(max) = pg_override.max_connections {
                    pg_cfg.max_connections = max;
                }
                if let Some(timeout) = pg_override.connect_timeout_ms {
                    pg_cfg.connect_timeout_ms = timeout;
                }
                if let Some(timeout) = pg_override.acquire_timeout_ms {
                    pg_cfg.acquire_timeout_ms = timeout;
                }
                config.postgres = Some(pg_cfg);
                if matches!(config.storage, StorageBackend::Memory) {
                    config.storage = StorageBackend::Postgres;
                }
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if matches!(self.storage, StorageBackend::Postgres) && self.postgres.is_none() {
            return Err(anyhow!(
                "postgres backend requested but FELIX_CONTROLPLANE_POSTGRES_URL / postgres.url is not set"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    fn clear_felix_env() {
        for (key, _) in env::vars() {
            if key.starts_with("FELIX_") || key == "DATABASE_URL" {
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
        assert_eq!(config.changes_limit, DEFAULT_CHANGES_LIMIT);
        assert!(matches!(config.storage, StorageBackend::Memory));
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_respects_env_vars() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_CONTROLPLANE_BIND", "127.0.0.1:9443");
            env::set_var("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:9090");
            env::set_var("FELIX_REGION_ID", "us-west-2");
            env::set_var("FELIX_CONTROLPLANE_CHANGES_LIMIT", "5000");
        }

        let config = ControlPlaneConfig::from_env().expect("from_env");
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:9443");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9090");
        assert_eq!(config.region_id, "us-west-2");
        assert_eq!(config.changes_limit, 5000);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_rejects_invalid_socket_addr() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_CONTROLPLANE_BIND", "not-a-valid-address");
        }
        let result = ControlPlaneConfig::from_env();
        assert!(result.is_err());
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_activates_postgres_when_url_present() {
        clear_felix_env();
        unsafe {
            env::set_var(
                "FELIX_CONTROLPLANE_POSTGRES_URL",
                "postgres://localhost/test",
            );
        }
        let config = ControlPlaneConfig::from_env().expect("from_env");
        assert!(matches!(config.storage, StorageBackend::Postgres));
        assert!(config.postgres.is_some());
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
            env::set_var("FELIX_CONTROLPLANE_CONFIG", nonexistent.to_str().unwrap());
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
changes_limit: 2000
storage:
  backend: "memory"
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
        }

        let config = ControlPlaneConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:7443");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:7070");
        assert_eq!(config.region_id, "eu-central-1");
        assert_eq!(config.changes_limit, 2000);

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
            env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
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
            env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
        }

        let result = ControlPlaneConfig::from_env_or_yaml();
        assert!(result.is_err());

        clear_felix_env();
    }
}
