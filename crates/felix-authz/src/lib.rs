//! Felix authn/authz primitives shared by control-plane and broker services.
//!
//! # Purpose
//! Centralizes the authz model (Casbin), permission matching, and token/JWKS
//! helpers used across services. This crate provides the shared types and
//! helpers so the control-plane and broker agree on policy shape and validation.
//!
//! # Structure
//! Most functionality lives in focused modules and is re-exported here to keep
//! downstream code small and consistent.

mod action;
mod casbin_model;
mod errors;
mod jwks;
mod matcher;
mod permission;
mod resource;
mod token;
mod types;

pub use action::Action;
pub use casbin_model::{casbin_model, casbin_model_string};
pub use errors::{AuthzError, AuthzResult};
pub use jwks::{Jwk, Jwks, KeyUse};
pub use matcher::{PermissionMatcher, wildcard_match};
pub use permission::{Permission, PermissionPattern};
pub use resource::{
    cache_resource, namespace_resource, stream_resource, tenant_resource, tenant_wildcard,
};
pub use token::{
    FelixClaims, FelixTokenIssuer, FelixTokenVerifier, TenantKeyMaterial, TenantKeyStore,
    TenantSigningKey, TenantVerificationKey,
};
pub use types::{CacheScope, Namespace, StreamName, TenantId};
