//! Bootstrap API handlers.
//!
//! # Purpose
//! Implements the initial tenant bootstrap flow that seeds auth configuration,
//! RBAC policies, and signing keys for a newly created tenant.
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_internal_message, api_not_enabled, api_unauthorized,
    api_validation_error,
};
use crate::app::AppState;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::Tenant;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use casbin::function_map::key_match2;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema, Clone)]
pub struct BootstrapInitializeRequest {
    pub display_name: String,
    pub idp_issuers: Vec<IdpIssuerConfig>,
    pub initial_admin_principals: Vec<String>,
    #[serde(default)]
    pub policies: Vec<PolicyRule>,
    #[serde(default)]
    pub groupings: Vec<GroupingRule>,
}

#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct BootstrapInitializeResponse {
    pub tenant_id: String,
    pub kid: String,
    pub jwks_url: String,
    pub status: String,
}

#[utoipa::path(
    post,
    path = "/internal/bootstrap/tenants/{tenant_id}/initialize",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = BootstrapInitializeRequest,
    responses(
        (status = 200, description = "Bootstrap initialized", body = BootstrapInitializeResponse),
        (status = 400, description = "Validation error"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not enabled"),
        (status = 409, description = "Already initialized")
    )
)]
pub async fn initialize(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<BootstrapInitializeRequest>,
) -> Result<Json<BootstrapInitializeResponse>, ApiError> {
    if !state.bootstrap_enabled {
        return Err(api_not_enabled("bootstrap not enabled"));
    }

    ensure_bootstrap_authorized(&state, &headers)?;

    if body.display_name.trim().is_empty() {
        return Err(api_validation_error("display_name is required"));
    }
    if body.initial_admin_principals.is_empty() {
        return Err(api_validation_error(
            "initial_admin_principals must not be empty",
        ));
    }
    for issuer in &body.idp_issuers {
        if issuer.issuer.trim().is_empty() {
            return Err(api_validation_error("issuer must not be empty"));
        }
    }

    let tenant_exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;

    if tenant_exists {
        let bootstrapped = state
            .store
            .tenant_auth_is_bootstrapped(&tenant_id)
            .await
            .map_err(|err| api_internal("failed to check bootstrap state", &err))?;
        if bootstrapped {
            return Err(api_conflict(
                "already_initialized",
                "tenant already initialized",
            ));
        }
    } else {
        state
            .store
            .create_tenant(Tenant {
                tenant_id: tenant_id.clone(),
                display_name: body.display_name.clone(),
            })
            .await
            .map_err(|err| api_internal("failed to create tenant", &err))?;
    }

    let keys = match state.store.ensure_signing_key_current(&tenant_id).await {
        Ok(keys) => keys,
        Err(err) => return Err(api_internal("failed to ensure signing keys", &err)),
    };

    for issuer in &body.idp_issuers {
        state
            .store
            .upsert_idp_issuer(&tenant_id, issuer.clone())
            .await
            .map_err(|err| api_internal("failed to upsert issuer", &err))?;
    }

    let mut policies = body.policies.clone();
    let required = format!("tenant:{tenant_id}");
    if !policies.iter().any(|policy| {
        policy.subject == "role:tenant-admin"
            && policy.action == "tenant.admin"
            && key_match2(&policy.object, &required)
    }) {
        policies.push(PolicyRule {
            subject: "role:tenant-admin".to_string(),
            object: "tenant:*".to_string(),
            action: "tenant.admin".to_string(),
        });
    }

    let mut groupings = body.groupings.clone();
    for principal in &body.initial_admin_principals {
        let grouping = GroupingRule {
            user: principal.clone(),
            role: "role:tenant-admin".to_string(),
        };
        if !groupings.contains(&grouping) {
            groupings.push(grouping);
        }
    }

    state
        .store
        .seed_rbac_policies_and_groupings(&tenant_id, policies, groupings)
        .await
        .map_err(|err| api_internal("failed to seed rbac", &err))?;

    state
        .store
        .set_tenant_auth_bootstrapped(&tenant_id, true)
        .await
        .map_err(|err| api_internal("failed to update bootstrap state", &err))?;

    Ok(Json(BootstrapInitializeResponse {
        tenant_id: tenant_id.clone(),
        kid: keys.current.kid,
        jwks_url: format!("/v1/tenants/{tenant_id}/.well-known/jwks.json"),
        status: "initialized".to_string(),
    }))
}

fn ensure_bootstrap_authorized(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let token = match headers.get("X-Felix-Bootstrap-Token") {
        Some(value) => value
            .to_str()
            .map_err(|_| api_unauthorized("invalid bootstrap token"))?,
        None => return Err(api_unauthorized("missing bootstrap token")),
    };

    let expected = state
        .bootstrap_token
        .as_ref()
        .ok_or_else(|| api_internal_message("bootstrap token missing"))?;

    if !constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        return Err(api_unauthorized("invalid bootstrap token"));
    }

    // TODO: add optional mTLS validation hook for bootstrap requests.
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}
