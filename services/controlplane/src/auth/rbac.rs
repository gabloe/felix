//! RBAC module wiring and shared constants.
//!
//! # Purpose and responsibility
//! Aggregates RBAC enforcement helpers, permission expansion logic, and policy
//! storage types used by the control-plane.
//!
//! # Where it fits in Felix
//! These modules power authorization decisions for tenant-scoped APIs and
//! determine which permissions are embedded in Felix tokens.
//!
//! # Key invariants and assumptions
//! - Casbin model configuration is embedded and must stay in sync with policy.
//! - Policy and grouping rules are interpreted with `keyMatch2` semantics.
//!
//! # Security considerations
//! - RBAC rules directly control access; treat policy changes as privileged.
//! - The embedded model must not be modified without updating tests.
pub mod authorize;
pub mod enforcer;
pub mod permissions;
pub mod policy_store;

/// Embedded Casbin model configuration for RBAC enforcement.
///
/// # What it does
/// Stores the RBAC model as a compile-time string to avoid external files.
///
/// # Why it exists
/// Keeps deployment simple and ensures model consistency across services/tests.
///
/// # Invariants
/// - Must align with the policy shape stored in the control-plane store.
pub const MODEL_CONF: &str = include_str!("rbac/model.conf");
