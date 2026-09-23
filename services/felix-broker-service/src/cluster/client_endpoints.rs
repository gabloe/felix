//! Where a client may connect, as this broker last understood the cluster.
//!
//! Kept apart from the node catalog the router uses. That catalog is addressed
//! to other brokers -- it names the internal listener, and it carries placement
//! and liveness detail that is the cluster's business. This is the subset a
//! tenant's client is entitled to and can act on: an identity, and somewhere to
//! dial.
//!
//! Refreshed by the same loop that refreshes the catalog, and read on the
//! control stream, so it is an `ArcSwap`: one writer on a timer, a reader per
//! `Topology` request, and no reason for either to wait on the other.
use arc_swap::ArcSwap;
use felix_wire::BrokerEndpoint;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct ClientEndpoints {
    endpoints: ArcSwap<Vec<BrokerEndpoint>>,
}

impl ClientEndpoints {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the list wholesale.
    ///
    /// Whole-list rather than merged: a broker that has stopped advertising a
    /// client address, or stopped being eligible, has to leave the answer, and
    /// merging would keep handing clients an address the cluster no longer
    /// stands behind.
    pub fn publish(&self, endpoints: Vec<BrokerEndpoint>) {
        self.endpoints.store(Arc::new(endpoints));
    }

    pub fn snapshot(&self) -> Arc<Vec<BrokerEndpoint>> {
        self.endpoints.load_full()
    }
}

#[cfg(test)]
mod tests;
