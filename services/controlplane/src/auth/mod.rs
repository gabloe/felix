//! Control-plane authentication and authorization modules.
//!
//! # Purpose and responsibility
//! Houses the control-plane's authentication (OIDC, Felix tokens) and
//! authorization (RBAC) building blocks and HTTP handlers.
//!
//! # Where it fits in Felix
//! The control-plane uses these modules to authenticate callers, mint Felix
//! tokens, expose JWKS for brokers, and enforce tenant-scoped policy.
//!
//! # Key invariants and assumptions
//! - Felix tokens are EdDSA/Ed25519 only and scoped to a tenant.
//! - JWKS endpoints expose public keys only.
//! - RBAC policy follows Casbin model semantics.
//!
//! # Security considerations
//! - Private signing keys must remain within the control-plane trust boundary.
//! - Admin endpoints must verify tenant admin permissions.
pub mod admin;
pub mod exchange;
pub mod felix_token;
pub mod idp_registry;
pub mod jwks;
pub mod keys;
pub mod oidc;
pub mod principal;
pub mod rbac;
