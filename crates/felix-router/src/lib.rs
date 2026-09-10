//! Routing: which node serves a shard, and whether we are allowed to reach it.
//!
//! Two independent questions, and keeping them separate is the point.
//! [`ShardRouter`] answers *where* a shard lives, from assignments the control
//! plane published. [`RegionRouter`] answers *whether* traffic may cross from
//! here to there. A route is only usable when both agree, and folding them
//! together would make a placement decision look like a policy decision.
pub mod shard;

pub use shard::{
    NodeRef, ReplicaRole, Resolution, RoutingTable, ShardKey, ShardRouter, Unavailable,
};

use felix_common::ids::RegionId;
use std::collections::HashSet;
use std::hash::Hash;

/// Region router with optional bridge allowlist.
///
/// Generic over how a region is named. It defaults to [`RegionId`], which is
/// what configuration uses, and the shard router instantiates it over `String`
/// because a node advertises its region by name -- the two vocabularies are
/// real, and one allowlist serving both beats two allowlists drifting apart.
///
/// ```
/// use felix_common::ids::RegionId;
/// use felix_router::RegionRouter;
///
/// let local = RegionId::new();
/// let remote = RegionId::new();
/// let mut router = RegionRouter::new(local);
/// assert!(!router.can_route(&local, &remote));
/// router.allow_bridge(local, remote);
/// assert!(router.can_route(&local, &remote));
/// ```
#[derive(Debug, Clone)]
pub struct RegionRouter<R = RegionId> {
    // Local region we always allow.
    local_region: R,
    // Explicit bridge rules for cross-region traffic.
    allowed_bridges: HashSet<(R, R)>,
}

impl<R: Eq + Hash + Clone> RegionRouter<R> {
    // Start with only the local region allowed.
    pub fn new(local_region: R) -> Self {
        Self {
            local_region,
            allowed_bridges: HashSet::new(),
        }
    }

    pub fn local_region(&self) -> &R {
        &self.local_region
    }

    pub fn allow_bridge(&mut self, source: R, dest: R) {
        // Store a single directional rule; caller can add reverse if needed.
        self.allowed_bridges.insert((source, dest));
    }

    pub fn can_route(&self, source: &R, dest: &R) -> bool {
        // Local routes are always allowed; otherwise check the allowlist.
        if source == dest {
            return true;
        }
        self.allowed_bridges
            .contains(&(source.clone(), dest.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_route_allowed() {
        // Same-region traffic should pass.
        let region = RegionId::new();
        let router = RegionRouter::new(region);
        assert!(router.can_route(&region, &region));
    }

    #[test]
    fn cross_region_denied_by_default() {
        // No bridges means cross-region traffic is denied.
        let source = RegionId::new();
        let dest = RegionId::new();
        let router = RegionRouter::new(source);
        assert!(!router.can_route(&source, &dest));
    }

    #[test]
    fn cross_region_allowed_with_bridge() {
        // Explicit bridge should enable routing.
        let source = RegionId::new();
        let dest = RegionId::new();
        let mut router = RegionRouter::new(source);
        router.allow_bridge(source, dest);
        assert!(router.can_route(&source, &dest));
    }

    #[test]
    fn bridge_is_directional() {
        let source = RegionId::new();
        let dest = RegionId::new();
        let mut router = RegionRouter::new(source);
        router.allow_bridge(source, dest);
        assert!(!router.can_route(&dest, &source));
    }

    #[test]
    fn local_region_accessor_returns_seed() {
        let region = RegionId::new();
        let router = RegionRouter::new(region);
        assert_eq!(*router.local_region(), region);
    }
}
