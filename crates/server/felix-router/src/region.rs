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

    /// A router that allows traffic within `local_region` and along each
    /// `(source, dest)` bridge.
    pub fn with_bridges(local_region: R, bridges: impl IntoIterator<Item = (R, R)>) -> Self {
        Self {
            local_region,
            allowed_bridges: bridges.into_iter().collect(),
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

impl<R: Eq + Hash> PartialEq for RegionRouter<R> {
    fn eq(&self, other: &Self) -> bool {
        self.local_region == other.local_region && self.allowed_bridges == other.allowed_bridges
    }
}

impl<R: Eq + Hash> Eq for RegionRouter<R> {}

/// Read a bridge allowlist written as comma-separated `source>dest` pairs,
/// such as `eu-west-1>us-east-1,us-east-1>eu-west-1`.
///
/// Each pair allows one direction only; a two-way bridge is two pairs. Blank
/// input is no bridges. Anything malformed is an error rather than skipped: a
/// dropped pair is a region cut off, and a misspelled one is a region opened
/// that nobody meant to open.
pub fn parse_bridges(spec: &str) -> Result<Vec<(String, String)>, BridgeSpecError> {
    spec.split(',')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (source, dest) = pair
                .split_once('>')
                .ok_or_else(|| BridgeSpecError(pair.to_string()))?;
            let (source, dest) = (source.trim(), dest.trim());
            if source.is_empty() || dest.is_empty() || dest.contains('>') {
                return Err(BridgeSpecError(pair.to_string()));
            }
            Ok((source.to_string(), dest.to_string()))
        })
        .collect()
}

/// A bridge that is not a `source>dest` pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSpecError(String);

impl std::fmt::Display for BridgeSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "region bridge {:?} is not a `source>dest` pair of region names",
            self.0
        )
    }
}

impl std::error::Error for BridgeSpecError {}

#[cfg(test)]
mod tests;
