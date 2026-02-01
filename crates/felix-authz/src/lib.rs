//! Felix authn/authz primitives shared by control-plane and broker services.
//!
//! # Purpose
//! Centralizes the authorization model (Casbin), permission matching, and
//! token/JWKS helpers used across services.
//!
//! # How it fits
//! Control-plane services mint and publish tokens/JWKS, while brokers verify
//! tokens and enforce permissions using shared types from this crate.
//!
//! # Key invariants
//! - Felix tokens are EdDSA/Ed25519 only; RSA/HS algorithms are rejected.
//! - Permission strings follow the `action:resource` pattern with wildcards.
//!
//! # Important configuration
//! - Issuer/audience values must be consistent between token issuer and verifier.
//! - JWKS endpoints must publish only public key material.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Action, Permission};
//!
//! let perm = Permission::new(Action::StreamPublish, "stream:tenant-a/ns/stream");
//! assert!(perm.as_string().contains("stream.publish"));
//! ```
//!
//! # Common pitfalls
//! - Skipping permission validation allows malformed patterns into policy stores.
//! - Mixing issuer/audience across services causes token verification failures.
//!
//! # Future work
//! - Consolidate token and JWKS caching across services for fewer cache invalidations.

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
    FelixClaims, FelixTokenIssuer, FelixTokenVerifier, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore, TenantSigningKey, TenantVerificationKey,
};
pub use types::{CacheScope, Namespace, StreamName, TenantId};
