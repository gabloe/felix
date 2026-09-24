//! Helpers shared by the integration tests that run as binaries of their own.
//!
//! Each test binary compiles its own copy of this module, and no single
//! binary uses every helper — so per-target dead-code analysis is noise here.
#![allow(dead_code)]

pub(crate) async fn read_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

/// A schema name no other caller will produce.
///
/// `pid + timestamp` is not enough. Tests in one binary run on parallel
/// threads, so the pid is identical, and two threads starting together can read
/// the same timestamp whenever the clock's granularity is coarser than the gap
/// between them — which is how two tests came to ask for the same schema and
/// the second was refused:
///
/// ```text
/// duplicate key value violates unique constraint "pg_namespace_nspname_index"
/// Key (nspname)=(felix_migrate_31654_1789668951690256729) already exists
/// ```
///
/// `CREATE SCHEMA IF NOT EXISTS` does not save it: two concurrent creates of
/// the same name race in Postgres and one gets exactly that error, so the
/// uniqueness has to be real rather than papered over at the call site.
///
/// The counter is what makes it real. The pid separates concurrent test
/// binaries, the timestamp keeps a name readable and tells runs apart, and the
/// counter guarantees that two calls in one process differ however close
/// together they are.
pub(crate) fn unique_schema(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed),
    )
}

// No `#[cfg(test)]`: this module is compiled into integration test binaries,
// which are already test crates, so the gate would remove these entirely.
mod tests {
    use super::*;

    /// The condition that broke it: many names asked for at once, from one
    /// process. A timestamp alone repeats here whenever the clock is coarser
    /// than the gap between two calls.
    #[test]
    fn names_are_unique_within_one_process() {
        let names: std::collections::HashSet<String> =
            (0..2_000).map(|_| unique_schema("felix_test")).collect();
        assert_eq!(
            names.len(),
            2_000,
            "two calls produced the same schema name, which Postgres refuses \
             with a duplicate-key error rather than reusing",
        );
    }

    /// And across threads, which is how tests in one binary actually run.
    #[test]
    fn names_are_unique_across_threads() {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..500)
                        .map(|_| unique_schema("felix_test"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let names: std::collections::HashSet<String> = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("thread"))
            .collect();
        assert_eq!(names.len(), 4_000);
    }

    #[test]
    fn a_name_is_a_usable_identifier() {
        let name = unique_schema("felix_test");
        assert!(name.starts_with("felix_test_"));
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "a schema name has to be quotable without escaping: {name}",
        );
    }
}
