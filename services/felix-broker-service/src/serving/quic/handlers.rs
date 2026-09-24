//! Per-message work for the client protocol, one module per operation family.

pub mod cache_watch;
pub mod publish;
pub(crate) mod redirect;
pub mod subscribe;
