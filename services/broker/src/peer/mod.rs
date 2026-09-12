//! The broker-internal transport: how brokers reach each other.
//!
//! [`PeerServer`] accepts peer connections; [`PeerPool`] makes them. Both speak
//! the protocol in `felix_wire::internal`, on their own listener, with their own
//! ALPN and their own configuration — see `docs/internal-protocol.md` for the
//! wire contract and the module docs here for what the transport guarantees.
//!
//! Forwarding is not wired to the publish path yet; that is M4.3 (#106). What
//! exists here is the transport it will use, complete on its own terms: a
//! request sent through the pool always terminates, and an unhealthy peer cannot
//! consume this broker.
pub mod codec;
pub mod config;
pub mod dispatch;
pub mod forward;
pub mod handler;
pub mod metrics;
pub mod pool;
pub mod replica;
pub mod server;
mod tls;

mod partition;
pub use config::PeerTransportConfig;
pub use dispatch::BrokerPeerHandler;
pub use forward::{
    CacheRequest, ForwardError, ForwardKey, ForwardTarget, PeerRequester, forward_cache_op,
    forward_publish,
};
pub use handler::ForwardingHandler;
pub use partition::PartitionInjector;
pub use pool::{PeerError, PeerPool};
pub use replica::ReplicaHandler;
pub use server::{PeerRequestHandler, PeerServer, UnavailableHandler};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
