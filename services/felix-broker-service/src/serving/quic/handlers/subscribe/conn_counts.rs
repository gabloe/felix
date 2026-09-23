// Per-connection active-subscriber bookkeeping behind the `felix_sub_active_connections` gauge.

use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

pub(super) static ACTIVE_SUB_CONN_COUNTS: OnceLock<DashMap<u64, usize>> = OnceLock::new();

pub(super) fn hash64(value: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub(super) fn connection_subscriber_register(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let map = ACTIVE_SUB_CONN_COUNTS.get_or_init(DashMap::new);
    let new_count = if let Some(mut entry) = map.get_mut(&connection_id) {
        *entry += 1;
        *entry
    } else {
        map.insert(connection_id, 1);
        1
    };
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
    metrics::gauge!("felix_sub_connection_subscribers", "connection_id" => connection_id.to_string())
        .set(new_count as f64);
}

pub(super) fn connection_subscriber_unregister(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let Some(map) = ACTIVE_SUB_CONN_COUNTS.get() else {
        return;
    };
    if let Some(mut entry) = map.get_mut(&connection_id) {
        if *entry > 1 {
            *entry -= 1;
            metrics::gauge!(
                "felix_sub_connection_subscribers",
                "connection_id" => connection_id.to_string()
            )
            .set(*entry as f64);
        } else {
            drop(entry);
            map.remove(&connection_id);
            metrics::gauge!(
                "felix_sub_connection_subscribers",
                "connection_id" => connection_id.to_string()
            )
            .set(0.0);
        }
    }
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
}
