//! RBAC module wiring and shared constants.
//!
//! # Purpose
//! Exposes RBAC enforcer helpers, permission expansion, and policy store types.
pub mod enforcer;
pub mod permissions;
pub mod policy_store;

// Embed the Casbin model so deployments don't need a separate config file.
pub const MODEL_CONF: &str = include_str!("rbac/model.conf");
