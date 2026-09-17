// Client-side modules for publish, cache, subscription, and routing.
#![allow(clippy::module_inception)]
pub(crate) mod cache;
pub(crate) mod cache_watch;
pub(crate) mod client;
pub(crate) mod cluster;
pub(crate) mod event_router;
#[cfg(feature = "in-process")]
pub(crate) mod inprocess;
pub(crate) mod publisher;
pub(crate) mod sharded;
pub(crate) mod sharding;
pub(crate) mod subscription;
