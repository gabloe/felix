//! How subscriber delivery is spread across writer lanes and streams.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberLaneShard {
    // Prefer connection-aware routing when a connection id is known, else fallback to subscriber id.
    Auto,
    SubscriberIdHash,
    ConnectionIdHash,
    // Assign once at subscribe time and keep lane pinned (ordering-safe RR variant).
    RoundRobinPin,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubStreamMode {
    PerSubscriber,
    HashedPool,
}

impl SubStreamMode {
    pub(super) fn parse_env(value: &str) -> Option<Self> {
        match value {
            "per_subscriber" => Some(Self::PerSubscriber),
            "hashed_pool" => Some(Self::HashedPool),
            _ => None,
        }
    }
}

impl SubscriberLaneShard {
    pub(super) fn parse_env(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(Self::Auto),
            "subscriber_id_hash" => Some(Self::SubscriberIdHash),
            "connection_id_hash" => Some(Self::ConnectionIdHash),
            "round_robin_pin" => Some(Self::RoundRobinPin),
            _ => None,
        }
    }
}
