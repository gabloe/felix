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
    #[error(
        "changing stream durability requires removal and recreation: tenant={tenant_id} namespace={namespace} stream={stream} current={current} requested={requested}"
    )]
    DurabilityChangeRequiresRecreate {
        tenant_id: String,
        namespace: String,
        stream: String,
        current: bool,
        requested: bool,
    },
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("namespace not found: tenant={tenant_id} namespace={namespace}")]
    NamespaceNotFound {
        tenant_id: String,
        namespace: String,
    },
    /// Historical replay was requested for a stream that keeps no history on
    /// disk. Its records live only in the bounded in-memory replay ring, which
    /// is reachable through `subscribe_with_cursor`.
    #[error("stream {tenant_id}/{namespace}/{stream} is not durable and has no persisted history")]
    StreamNotDurable {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    /// The stream asks for durability but this broker has none configured.
    ///
    /// Separate from [`BrokerError::Storage`] because the two want opposite
    /// handling: this is a static misconfiguration that will not resolve on
    /// retry and affects only the stream naming it, while a storage failure
    /// means the disk is in an unknown state and must never be shrugged off.
    #[error(
        "stream {tenant_id}/{namespace}/{stream} is marked durable but this broker has no durable storage configured"
    )]
    DurableStorageNotConfigured {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    /// Durable storage refused or failed a write, or a log failed to recover.
    /// A publish that hits this is never acknowledged: the record is not on
    /// disk, so claiming otherwise would be the one failure mode durability
    /// exists to prevent.
    #[error("durable storage error: {0}")]
    Storage(String),
}
