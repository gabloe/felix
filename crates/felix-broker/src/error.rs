// Broker error type shared across the registry, stream, and subscription modules.

pub type Result<T> = std::result::Result<T, BrokerError>;

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("topic capacity too large")]
    CapacityTooLarge,
    #[error("cursor too old (oldest {oldest}, requested {requested})")]
    CursorTooOld { oldest: u64, requested: u64 },
    #[error("stream not found: tenant={tenant_id} namespace={namespace} stream={stream}")]
    StreamNotFound {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    #[error("stream handle {0} is no longer active")]
    StreamHandleInactive(u64),
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("namespace not found: tenant={tenant_id} namespace={namespace}")]
    NamespaceNotFound {
        tenant_id: String,
        namespace: String,
    },
    /// Durable storage refused or failed a write. A publish that hits this is
    /// never acknowledged: the record is not on disk, so claiming otherwise
    /// would be the one failure mode durability exists to prevent.
    #[error("durable storage error: {0}")]
    Storage(String),
}
