//! Control-plane authentication and authorization modules.
//!
//! # Purpose
//! Groups OIDC validation, token minting/verification, JWKS handling, and
//! RBAC enforcement utilities.
pub mod admin;
pub mod exchange;
pub mod felix_token;
pub mod idp_registry;
pub mod jwks;
pub mod keys;
pub mod oidc;
pub mod principal;
pub mod rbac;
