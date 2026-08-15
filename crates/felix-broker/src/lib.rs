// In-process pub/sub broker with a tiny cache hook.
// The broker enforces tenant/namespace/stream existence via local registries
// that are kept in sync by the control plane watcher.
//
// Module layout:
// - `telemetry`: cfg-gated sampling shims and the `t_histogram!` macro.
// - `error` / `config`: shared error type, capacity defaults, queue policy.
// - `keys`: map keys plus their borrowed lookup twins.
// - `delivery`: shared delivery batches and queue-depth accounting.
// - `commit_order`: one authoritative publish order per durable stream.
// - `stream_state`: per-stream subscriber registry, snapshot, and replay log.
// - `durable`: disk-backed logs for streams registered with `durable: true`.
// - `subscription`: subscriber-facing receive handles.
// - `broker` / `registry`: the `Broker` aggregate and its two impl blocks.
//
// Everything public is re-exported at the crate root; downstream crates and the
// docs site address these types as `felix_broker::<Name>`.

// Declared first so the `t_histogram!` macro is in scope for every module below.
#[macro_use]
mod telemetry;

mod broker;
mod commit_order;
mod config;
mod delivery;
pub mod durable;
mod error;
mod keys;
mod registry;
mod stream_state;
mod subscription;

pub mod timings;

pub use broker::{
    Broker, CacheMetadata, HistoryRange, ResumedSubscription, StartPosition, StreamHandle,
    StreamMetadata,
};
pub use config::SubQueuePolicy;
pub use delivery::DeliveryEnvelope;
pub use durable::{DurableStorage, StreamLog};
pub use error::{BrokerError, Result};
pub use keys::{CacheKey, NamespaceKey, StreamKey};
pub use stream_state::Cursor;
pub use subscription::{Subscription, SubscriptionGuard, SubscriptionReceiver};

#[cfg(test)]
mod tests;
