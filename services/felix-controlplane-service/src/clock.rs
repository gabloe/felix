//! The process wall clock, in the milliseconds the metadata model stores.

/// Wall-clock milliseconds since the Unix epoch.
///
/// Clamped at zero so a clock behind the epoch cannot panic the caller.
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
