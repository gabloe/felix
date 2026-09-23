//! Region policy: whether traffic may cross from one region to another.

use std::collections::HashSet;
use std::hash::Hash;

use felix_common::ids::RegionId;

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
    /// A router that allows only traffic within `local_region`.
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
mod tests;
