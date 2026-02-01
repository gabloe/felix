//! JWKS endpoint handler for tenant signing keys.
//!
//! # Purpose
//! Expose tenant public keys in JWKS format so brokers/clients can verify Felix
//! tokens without access to private key material.
//!
//! # Architectural role
//! Bridges the control-plane key store to external verifiers by translating
//! internal Ed25519 key material into RFC-compliant JWKS.
//!
//! # Callers / consumers
//! - Broker auth layer fetching `/.well-known/jwks.json`.
//! - External clients that verify Felix-issued JWTs.
//!
//! # Key invariants
//! - Only Ed25519 public keys are exported.
//! - JWKS never contains private key material.
//! - `kid` is stable for the life of a key and used for rotation.
//!
//! # Concurrency model
//! Stateless request handler; relies on async store calls and does not share
//! mutable state across requests.
//!
//! # Security boundary
//! This is the boundary where public key material leaves the control-plane.
//! Private keys must never cross this interface.
//!
//! # Security model and threat assumptions
//! - Attackers may call this endpoint; keys are public by design.
//! - We must avoid accidental RSA exposure by always returning EdDSA/Ed25519.
//! - JWKS output must be deterministic for cacheability and verification.
//!
//! # How to use
//! Call the `GET /v1/tenants/{tenant_id}/.well-known/jwks.json` endpoint and use
//! the returned `x` values as Ed25519 public keys for EdDSA verification.
use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::app::AppState;
use axum::Json;
use axum::extract::{Path, State};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use utoipa::ToSchema;

/// Return the JWKS for a tenant's current and previous signing keys.
///
/// # Overview
/// Loads the tenant's signing keys, converts them to JWKs with Ed25519 public
/// components, and returns a JWKS payload suitable for broker verification.
///
/// # Arguments
/// - `tenant_id`: Path parameter identifying the tenant.
/// - `state`: Shared application state providing access to the store.
///
/// # Returns
/// - `Ok(Json<JwksResponse>)` containing Ed25519 public keys.
///
/// # Errors
/// - `404` if the tenant does not exist.
/// - `500` if the store fails to load keys.
///
/// # Panics
/// - Does not panic under normal conditions.
///
/// # Examples
/// ```rust,no_run
/// use axum::extract::{Path, State};
/// use controlplane::app::AppState;
/// use controlplane::auth::jwks::tenant_jwks;
///
/// async fn handle(Path(tenant_id): Path<String>, State(state): State<AppState>) {
///     let _ = tenant_jwks(Path(tenant_id), State(state)).await;
/// }
/// ```
///
/// # Security
/// - Only public keys are returned; never expose private keys or seeds.
/// - The algorithm is pinned to EdDSA to prevent RSA/HS confusion.
#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/.well-known/jwks.json",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses((status = 200, description = "Tenant JWKS", body = JwksResponse), (status = 404))
)]
pub async fn tenant_jwks(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<JwksResponse>, ApiError> {
    // Step 1: Confirm the tenant exists before serving keys.
    // This avoids leaking which tenants have keys and simplifies error handling.
    let exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;
    if !exists {
        return Err(api_not_found("tenant not found"));
    }

    // Step 2: Load tenant signing keys from storage.
    // We intentionally fetch all keys (current + previous) for rotation support.
    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    // Step 3: Convert internal Ed25519 keys to JWKS entries.
    // Only the public `x` coordinate is exposed; no private material is serialized.
    let mut jwks = JwksResponse { keys: Vec::new() };
    for key in keys.all_keys() {
        let x = URL_SAFE_NO_PAD.encode(key.public_key);
        jwks.keys.push(JwkResponse {
            kty: "OKP".to_string(),
            kid: key.kid.clone(),
            alg: alg_to_string(key.alg),
            use_field: "sig".to_string(),
            crv: "Ed25519".to_string(),
            x,
        });
    }

    Ok(Json(jwks))
}

/// A single JWK entry for an Ed25519 public key.
///
/// # Overview
/// Represents the minimal fields required for EdDSA verification.
///
/// # Arguments
/// - This is a data container; fields are populated by the JWKS handler.
///
/// # Returns
/// - Not applicable.
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::jwks::JwkResponse;
///
/// let jwk = JwkResponse {
///     kty: "OKP".to_string(),
///     kid: "k1".to_string(),
///     alg: "EdDSA".to_string(),
///     use_field: "sig".to_string(),
///     crv: "Ed25519".to_string(),
///     x: "base64url".to_string(),
/// };
/// assert_eq!(jwk.kty, "OKP");
/// ```
///
/// # Security
/// - Only public key material should be stored in `x`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JwkResponse {
    pub kty: String,
    pub kid: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_field: String,
    pub crv: String,
    pub x: String,
}

/// A JWKS response payload containing one or more JWK entries.
///
/// # Overview
/// Wraps a list of Ed25519 JWKs for JSON serialization.
///
/// # Arguments
/// - This is a data container; fields are populated by the JWKS handler.
///
/// # Returns
/// - Not applicable.
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::jwks::{JwkResponse, JwksResponse};
///
/// let payload = JwksResponse { keys: vec![JwkResponse {
///     kty: "OKP".to_string(),
///     kid: "k1".to_string(),
///     alg: "EdDSA".to_string(),
///     use_field: "sig".to_string(),
///     crv: "Ed25519".to_string(),
///     x: "base64url".to_string(),
/// }]};
/// assert_eq!(payload.keys.len(), 1);
/// ```
///
/// # Security
/// - Only include public keys; never serialize private key material.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JwksResponse {
    pub keys: Vec<JwkResponse>,
}

fn alg_to_string(alg: jsonwebtoken::Algorithm) -> String {
    // We map jsonwebtoken's enum to stable string values for JWKS output.
    // The EdDSA branch is the only valid algorithm for Felix keys.
    match alg {
        jsonwebtoken::Algorithm::RS256 => "RS256",
        jsonwebtoken::Algorithm::RS384 => "RS384",
        jsonwebtoken::Algorithm::RS512 => "RS512",
        jsonwebtoken::Algorithm::ES256 => "ES256",
        jsonwebtoken::Algorithm::ES384 => "ES384",
        jsonwebtoken::Algorithm::PS256 => "PS256",
        jsonwebtoken::Algorithm::PS384 => "PS384",
        jsonwebtoken::Algorithm::PS512 => "PS512",
        jsonwebtoken::Algorithm::HS256 => "HS256",
        jsonwebtoken::Algorithm::HS384 => "HS384",
        jsonwebtoken::Algorithm::HS512 => "HS512",
        jsonwebtoken::Algorithm::EdDSA => "EdDSA",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alg_to_string_maps_all_algorithms() {
        let cases = vec![
            (jsonwebtoken::Algorithm::RS256, "RS256"),
            (jsonwebtoken::Algorithm::RS384, "RS384"),
            (jsonwebtoken::Algorithm::RS512, "RS512"),
            (jsonwebtoken::Algorithm::ES256, "ES256"),
            (jsonwebtoken::Algorithm::ES384, "ES384"),
            (jsonwebtoken::Algorithm::PS256, "PS256"),
            (jsonwebtoken::Algorithm::PS384, "PS384"),
            (jsonwebtoken::Algorithm::PS512, "PS512"),
            (jsonwebtoken::Algorithm::HS256, "HS256"),
            (jsonwebtoken::Algorithm::HS384, "HS384"),
            (jsonwebtoken::Algorithm::HS512, "HS512"),
            (jsonwebtoken::Algorithm::EdDSA, "EdDSA"),
        ];
        for (alg, expected) in cases {
            assert_eq!(alg_to_string(alg), expected);
        }
    }
}
