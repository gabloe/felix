//! JWKS endpoint handler for tenant signing keys.
//!
//! # Purpose
//! Exposes tenant public keys in JWKS format so brokers/clients can verify tokens.
use crate::api::error::{ApiError, api_internal, api_internal_message, api_not_found};
use crate::app::AppState;
use axum::Json;
use axum::extract::{Path, State};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rsa::RsaPublicKey;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::traits::PublicKeyParts;
use serde::Serialize;
use utoipa::ToSchema;

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

    let mut jwks = JwksResponse { keys: Vec::new() };
    for key in keys.all_keys() {
        let pem = std::str::from_utf8(&key.public_key_pem)
            .map_err(|_| api_internal_message("invalid public key"))?;
        let public_key = RsaPublicKey::from_pkcs1_pem(pem)
            .map_err(|_| api_internal_message("invalid public key"))?;

        let n = URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
        let e = URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());
        jwks.keys.push(JwkResponse {
            kty: "RSA".to_string(),
            kid: key.kid,
            alg: alg_to_string(key.alg),
            use_field: "sig".to_string(),
            n,
            e,
        });
    }

    Ok(Json(jwks))
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JwkResponse {
    pub kty: String,
    pub kid: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_field: String,
    pub n: String,
    pub e: String,
}

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
