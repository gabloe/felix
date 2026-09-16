//! The tenant JWKS endpoint: `GET /v1/tenants/{tenant_id}/.well-known/jwks.json`.
//!
//! This is where public key material leaves the control plane so brokers and
//! clients can verify Felix tokens. Only Ed25519 public keys are exported —
//! private material never crosses this interface — and both current and
//! previous keys are served so verification keeps working through rotation.
use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::app::AppState;
use axum::Json;
use axum::extract::{Path, State};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use utoipa::ToSchema;

/// Serve the JWKS for a tenant's current and previous signing keys.
///
/// # Errors
/// `404` when the tenant does not exist, `500` when the store fails.
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
    let exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;
    if !exists {
        return Err(api_not_found("tenant not found"));
    }

    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    // Only the public `x` coordinate is exposed.
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

/// One Ed25519 public key in JWK form.
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

/// The JWKS payload.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JwksResponse {
    pub keys: Vec<JwkResponse>,
}

fn alg_to_string(alg: jsonwebtoken::Algorithm) -> String {
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
