//! Auth admin endpoints for managing IdP issuers and RBAC policy.
//!
//! # Purpose
//! Provides privileged endpoints to update issuer configuration and policy
//! groupings for a tenant, guarded by admin tokens.
use crate::api::ensure_tenant_exists;
use crate::api::error::{ApiError, api_forbidden, api_internal, api_unauthorized};
use crate::app::AppState;
use crate::auth::felix_token::verify_token;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use casbin::function_map::key_match2;
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct PolicyRequest {
    pub subject: String,
    pub object: String,
    pub action: String,
}

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct GroupingRequest {
    pub user: String,
    pub role: String,
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/idp-issuers",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = IdpIssuerConfig,
    responses((status = 204), (status = 404))
)]
pub async fn upsert_idp_issuer(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<IdpIssuerConfig>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    state
        .store
        .upsert_idp_issuer(&tenant_id, body)
        .await
        .map_err(|err| api_internal("failed to upsert issuer", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/idp-issuers/{issuer}",
    tag = "auth",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("issuer" = String, Path, description = "Issuer"))
    ,
    responses((status = 204), (status = 404))
)]
pub async fn delete_idp_issuer(
    Path((tenant_id, issuer)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    state
        .store
        .delete_idp_issuer(&tenant_id, &issuer)
        .await
        .map_err(|err| api_internal("failed to delete issuer", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/rbac/policies",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = PolicyRequest,
    responses((status = 204), (status = 404))
)]
pub async fn add_policy(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PolicyRequest>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    state
        .store
        .add_rbac_policy(
            &tenant_id,
            PolicyRule {
                subject: body.subject,
                object: body.object,
                action: body.action,
            },
        )
        .await
        .map_err(|err| api_internal("failed to add policy", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/rbac/groupings",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = GroupingRequest,
    responses((status = 204), (status = 404))
)]
pub async fn add_grouping(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<GroupingRequest>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    state
        .store
        .add_rbac_grouping(
            &tenant_id,
            GroupingRule {
                user: body.user,
                role: body.role,
            },
        )
        .await
        .map_err(|err| api_internal("failed to add grouping", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn require_tenant_admin(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    let bearer = extract_bearer(headers).ok_or_else(|| api_unauthorized("missing bearer token"))?;
    let keys = state
        .store
        .get_tenant_signing_keys(tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;
    let claims = verify_token(&keys, bearer, 5).map_err(|_| api_unauthorized("invalid token"))?;
    if claims.tid != tenant_id {
        return Err(api_forbidden("tenant mismatch"));
    }
    let required = format!("tenant:{tenant_id}");
    let allowed = claims.perms.iter().any(|perm| {
        let Some((action, object)) = perm.split_once(':') else {
            return false;
        };
        action == "tenant.admin" && key_match2(object, &required)
    });
    if !allowed {
        return Err(api_forbidden("missing tenant.admin"));
    }
    Ok(())
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(axum::http::header::AUTHORIZATION)?;
    let value = value.to_str().ok()?;
    value.strip_prefix("Bearer ")
}
