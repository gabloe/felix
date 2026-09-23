// Publish stream sharding policies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishSharding {
    RoundRobin,
    HashStream,
}

impl PublishSharding {
    pub(crate) fn from_env() -> Option<Self> {
        let value = std::env::var("FELIX_PUB_SHARDING").ok()?;
        match value.as_str() {
            "rr" => Some(Self::RoundRobin),
            "hash_stream" => Some(Self::HashStream),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PublishSharding;
    use serial_test::serial;

    fn clear_env() {
        unsafe {
            std::env::remove_var("FELIX_PUB_SHARDING");
        }
    }

    #[test]
    #[serial]
    fn from_env_missing_returns_none() {
        clear_env();
        assert!(PublishSharding::from_env().is_none());
    }

    #[test]
    #[serial]
    fn from_env_rr_returns_round_robin() {
        clear_env();
        unsafe {
            std::env::set_var("FELIX_PUB_SHARDING", "rr");
        }
        assert_eq!(
            PublishSharding::from_env(),
            Some(PublishSharding::RoundRobin)
        );
        clear_env();
    }

    #[test]
    #[serial]
    fn from_env_hash_stream_returns_hash_stream() {
        clear_env();
        unsafe {
            std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        }
        assert_eq!(
            PublishSharding::from_env(),
            Some(PublishSharding::HashStream)
        );
        clear_env();
    }

    #[test]
    #[serial]
    fn from_env_invalid_returns_none() {
        clear_env();
        unsafe {
            std::env::set_var("FELIX_PUB_SHARDING", "invalid");
        }
        assert!(PublishSharding::from_env().is_none());
        clear_env();
    }
}
