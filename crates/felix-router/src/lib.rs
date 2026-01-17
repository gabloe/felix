// Region routing rules with an allowlist of bridges.
use felix_common::ids::RegionId;
use std::collections::HashSet;

/// Region router with optional bridge allowlist.
///
/// ```
/// use felix_common::ids::RegionId;
/// use felix_router::RegionRouter;
///
/// let local = RegionId::new();
/// let remote = RegionId::new();
/// let mut router = RegionRouter::new(local);
/// assert!(!router.can_route(local, remote));
/// router.allow_bridge(local, remote);
/// assert!(router.can_route(local, remote));
/// ```
#[derive(Debug, Clone)]
pub struct RegionRouter {
    // Local region we always allow.
    local_region: RegionId,
    // Explicit bridge rules for cross-region traffic.
    allowed_bridges: HashSet<(RegionId, RegionId)>,
}

impl RegionRouter {
    // Start with only the local region allowed.
    pub fn new(local_region: RegionId) -> Self {
        Self {
            local_region,
            allowed_bridges: HashSet::new(),
        }
    }

    pub fn local_region(&self) -> RegionId {
        self.local_region
    }

    pub fn allow_bridge(&mut self, source: RegionId, dest: RegionId) {
        // Store a single directional rule; caller can add reverse if needed.
        self.allowed_bridges.insert((source, dest));
    }

    pub fn can_route(&self, source: RegionId, dest: RegionId) -> bool {
        // Local routes are always allowed; otherwise check the allowlist.
        if source == dest {
            return true;
        }
        self.allowed_bridges.contains(&(source, dest))
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
        assert!(router.can_route(region, region));
    }

    #[test]
    fn cross_region_denied_by_default() {
        // No bridges means cross-region traffic is denied.
        let source = RegionId::new();
        let dest = RegionId::new();
        let router = RegionRouter::new(source);
        assert!(!router.can_route(source, dest));
    }

    #[test]
    fn cross_region_allowed_with_bridge() {
        // Explicit bridge should enable routing.
        let source = RegionId::new();
        let dest = RegionId::new();
        let mut router = RegionRouter::new(source);
        router.allow_bridge(source, dest);
        assert!(router.can_route(source, dest));
    }

    #[test]
    fn bridge_is_directional() {
        let source = RegionId::new();
        let dest = RegionId::new();
        let mut router = RegionRouter::new(source);
        router.allow_bridge(source, dest);
        assert!(!router.can_route(dest, source));
    }
}
