//! Control-plane data model module.
//!
//! # Purpose
//! Re-exports the core tenant/namespace/stream/cache models and change payloads
//! used by the API and store layers.
mod cache;
mod namespace;
mod stream;
mod tenant;

pub use cache::{Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest};
pub use namespace::{Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey};
pub use stream::{
    ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, Stream, StreamChange, StreamChangeOp,
    StreamKey, StreamKind, StreamPatchRequest,
};
pub use tenant::{Tenant, TenantChange, TenantChangeOp};
