//! Token exchange endpoint handler.
//!
//! # Purpose
//! Validates OIDC tokens, evaluates RBAC permissions, and mints Felix tokens.
use crate::api::error::{
    ApiError, api_forbidden, api_internal, api_internal_message, api_unauthorized,
};
use crate::app::AppState;
use crate::auth::felix_token::mint_token;
use crate::auth::oidc::OidcError;
use crate::auth::principal;
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::permissions::effective_permissions;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use casbin::function_map::key_match2;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema, Clone, Default)]
pub struct TokenExchangeRequest {
    pub requested: Option<Vec<String>>,
    pub resources: Option<Vec<String>>,
}

#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct TokenExchangeResponse {
    pub felix_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/token/exchange",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = TokenExchangeRequest,
    responses(
        (status = 200, description = "Exchange token", body = TokenExchangeResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden")
    )
)]
pub async fn exchange_token(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Option<Json<TokenExchangeRequest>>,
) -> Result<Json<TokenExchangeResponse>, ApiError> {
    let bearer =
        extract_bearer(&headers).ok_or_else(|| api_unauthorized("missing bearer token"))?;

    let tenant_exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;
    if !tenant_exists {
        return Err(api_forbidden("tenant not allowed"));
    }

    let issuers = state
        .store
        .list_idp_issuers(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load issuers", &err))?;
    if issuers.is_empty() {
        return Err(api_forbidden("no issuers configured"));
    }

    let validated = match state.oidc_validator.validate(bearer, &issuers).await {
        Ok(token) => token,
        Err(OidcError::IssuerNotAllowed) => return Err(api_forbidden("issuer not allowed")),
        Err(_) => return Err(api_unauthorized("invalid token")),
    };

    let principal = principal::from_claims(&validated.issuer, &validated.subject, validated.groups);

    let policies = state
        .store
        .list_rbac_policies(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load policies", &err))?;
    let groupings = state
        .store
        .list_rbac_groupings(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load groupings", &err))?;

    let enforcer = build_enforcer(&policies, &groupings, &tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to build rbac enforcer");
            api_internal_message("failed to build enforcer")
        })?;

    let mut perms = effective_permissions(&enforcer, &principal.principal_id, &tenant_id);

    if let Some(request) = body.map(|Json(value)| value) {
        perms = filter_permissions(perms, &request);
    }

    if perms.is_empty() {
        return Err(api_forbidden("no permissions"));
    }

    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    let ttl = Duration::from_secs(900);
    let felix_token = mint_token(&keys, &tenant_id, &principal.principal_id, perms, ttl)
        .map_err(|_| api_internal_message("failed to mint token"))?;

    Ok(Json(TokenExchangeResponse {
        felix_token,
        expires_in: ttl.as_secs(),
        token_type: "Bearer".to_string(),
    }))
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(axum::http::header::AUTHORIZATION)?;
    let value = value.to_str().ok()?;
    value.strip_prefix("Bearer ")
}

fn filter_permissions(perms: Vec<String>, request: &TokenExchangeRequest) -> Vec<String> {
    let requested_actions = request.requested.as_ref().map(|actions| {
        actions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<String>>()
    });
    let resources = request.resources.as_ref();

    perms
        .into_iter()
        .filter(|perm| {
            let Some((action, object)) = perm.split_once(':') else {
                return false;
            };
            if let Some(actions) = &requested_actions
                && !actions.contains(action)
            {
                return false;
            }
            if let Some(resources) = resources
                && !resources.iter().any(|hint| key_match2(hint, object))
            {
                return false;
            }
            true
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_by_requested_actions() {
        let perms = vec![
            "stream.publish:stream:payments/*".to_string(),
            "stream.subscribe:stream:payments/*".to_string(),
        ];
        let request = TokenExchangeRequest {
            requested: Some(vec!["stream.publish".to_string()]),
            resources: None,
        };
        let filtered = filter_permissions(perms, &request);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], "stream.publish:stream:payments/*");
    }
}
