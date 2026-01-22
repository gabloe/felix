// Publish stream sharding policies.
#[derive(Clone, Copy, Debug)]
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
