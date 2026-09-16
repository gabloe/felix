//! RBAC: Casbin enforcement, permission expansion, and policy storage types.
pub mod authorize;
pub mod enforcer;
pub mod permissions;
pub mod policy_store;

/// The Casbin model, embedded at compile time so every service and test runs
/// the same one.
pub const MODEL_CONF: &str = include_str!("rbac/model.conf");
