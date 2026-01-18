// Shared data types and small helpers used across crates.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("config error: {0}")]
    Config(String),
}

pub mod ids {
    // Strongly typed IDs to avoid mixing namespaces at compile time.
    use super::{Error, Result};
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::str::FromStr;
    use uuid::Uuid;

    macro_rules! id_type {
        ($name:ident) => {
            #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
            pub struct $name(Uuid);

            impl $name {
                // Generate a new random ID for this namespace.
                pub fn new() -> Self {
                    Self(Uuid::new_v4())
                }

                // Wrap an existing UUID when decoding from storage.
                pub fn from_uuid(uuid: Uuid) -> Self {
                    Self(uuid)
                }

                // Expose the underlying UUID for interoperability.
                pub fn as_uuid(&self) -> Uuid {
                    self.0
                }
            }

            impl Default for $name {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.0)
                }
            }

            impl FromStr for $name {
                type Err = Error;

                fn from_str(input: &str) -> Result<Self> {
                    // Preserve the original input for clearer error messages.
                    let uuid =
                        Uuid::parse_str(input).map_err(|_| Error::InvalidId(input.into()))?;
                    Ok(Self(uuid))
                }
            }
        };
    }

    id_type!(RegionId);
    id_type!(TenantId);
    id_type!(NamespaceId);
    id_type!(StreamId);
    id_type!(TopicId);
    id_type!(ShardId);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    pub max_message_bytes: usize,
    pub max_inflight: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        // Defaults are conservative for local/dev usage.
        Self {
            max_message_bytes: 1024 * 1024,
            max_inflight: 10_000,
        }
    }
}

/// Node configuration values shared across components.
///
/// ```
/// use felix_common::{ids::RegionId, NodeConfig};
///
/// let region = RegionId::new();
/// let config = NodeConfig::new(region, "127.0.0.1:9000", "/tmp/felix");
/// assert_eq!(config.listen_addr, "127.0.0.1:9000");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: Uuid,
    pub region: ids::RegionId,
    pub listen_addr: String,
    pub data_dir: String,
    pub limits: LimitsConfig,
}

impl NodeConfig {
    pub fn new(
        region: ids::RegionId,
        listen_addr: impl Into<String>,
        data_dir: impl Into<String>,
    ) -> Self {
        // Use a new node ID so multiple nodes can run on one machine.
        Self {
            node_id: Uuid::new_v4(),
            region,
            listen_addr: listen_addr.into(),
            data_dir: data_dir.into(),
            limits: LimitsConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, NodeConfig, ids::RegionId};
    use std::str::FromStr;

    #[test]
    fn region_id_round_trip() {
        // IDs should serialize and parse without loss.
        let region = RegionId::new();
        let parsed = RegionId::from_str(&region.to_string()).expect("parse");
        assert_eq!(region, parsed);
    }

    #[test]
    fn region_id_rejects_invalid_input() {
        let err = RegionId::from_str("not-a-uuid").expect_err("invalid");
        assert!(matches!(err, Error::InvalidId(s) if s == "not-a-uuid"));
    }

    #[test]
    fn node_config_new_sets_fields() {
        let region = RegionId::new();
        let config = NodeConfig::new(region, "127.0.0.1:9000", "/tmp/felix");
        assert_eq!(config.region, region);
        assert_eq!(config.listen_addr, "127.0.0.1:9000");
        assert_eq!(config.data_dir, "/tmp/felix");
        assert!(config.limits.max_message_bytes > 0);
        assert!(config.limits.max_inflight > 0);
    }
}
