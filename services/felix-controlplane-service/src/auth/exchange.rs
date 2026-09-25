//! Token exchange: `POST /v1/tenants/{tenant_id}/token/exchange`.
//!
//! This is the boundary between external IdP identity and Felix authorization.
//! An upstream OIDC token is validated against the tenant's configured
//! issuers, RBAC decides the effective permissions, and the result is a Felix
//! EdDSA token for the broker. The request body can narrow those permissions
//! but can never widen them.
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use casbin::function_map::key_match2;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{
    ApiError, api_forbidden, api_internal, api_internal_message, api_unauthorized,
};
use crate::auth::felix_token::mint_token;
use crate::auth::oidc::OidcError;
use crate::auth::principal;
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::permissions::effective_permissions;
use crate::auth::rbac::policy_store::GroupingRule;

/// Optional narrowing filter: keep only these actions and/or resources out of
/// what RBAC already granted. Never widens scope.
#[derive(Debug, Deserialize, ToSchema, Clone, Default)]
pub struct TokenExchangeRequest {
    pub requested: Option<Vec<String>>,
    pub resources: Option<Vec<String>>,
}

/// The minted Felix bearer token plus expiry. Treat `felix_token` as a secret.
#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct TokenExchangeResponse {
    pub felix_token: String,
    pub expires_in: u64,
    pub token_type: String,
    /// Presented to `/token/refresh` for a new access token, without another
    /// IdP round trip. Single-use: refreshing mints its replacement.
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}

/// Exchange an upstream IdP token for a Felix EdDSA token.
///
/// # Errors
/// `401` for a missing or invalid bearer token, `403` when the issuer is not
/// allowed or no permissions remain, `500` for store failures.
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

    // Forbidden (not 404) for unknown tenants, so callers can't probe which
    // tenants exist.
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
        // Same ambiguity as a missing signing key on the bearer path: a tenant
        // with no issuers and a tenant this instance has not learned about yet
        // look identical from here. "No issuers configured" sends an operator
        // to the tenant's IdP settings, which are fine (#601).
        return Err(match state.readiness.check().await {
            Ok(()) => api_forbidden("no issuers configured"),
            Err(reason) => crate::auth::bearer::cannot_verify(&reason.to_string()),
        });
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
    let mut groupings = state
        .store
        .list_rbac_groupings(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load groupings", &err))?;
    add_group_claim_groupings(&mut groupings, &principal.principal_id, &principal.groups);

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

    // A token with no permissions is useless and usually masks a
    // misconfiguration; reject instead.
    if perms.is_empty() {
        return Err(api_forbidden("no permissions"));
    }

    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    let ttl = access_token_ttl();
    let felix_token = mint_token(&keys, &tenant_id, &principal.principal_id, perms, ttl)
        .map_err(|_| api_internal_message("failed to mint token"))?;

    // The refresh token is what makes the short access TTL above workable for
    // anything long-running. Its group claims are recorded rather than its
    // permissions: a refresh re-runs RBAC, so a grant removed later stops
    // working without waiting for a re-exchange.
    let refresh_ttl = crate::auth::refresh::refresh_ttl();
    let (record, refresh_secret) = crate::auth::refresh::issue(
        &tenant_id,
        &principal.principal_id,
        principal.groups.clone(),
        None,
        crate::auth::refresh::now_secs(),
        refresh_ttl,
    );
    state
        .store
        .insert_refresh_token(record)
        .await
        .map_err(|err| api_internal("failed to store refresh token", &err))?;
    metrics::counter!("felix_refresh_tokens_issued_total", "via" => "exchange").increment(1);

    Ok(Json(TokenExchangeResponse {
        felix_token,
        expires_in: ttl.as_secs(),
        token_type: "Bearer".to_string(),
        refresh_token: refresh_secret,
        refresh_expires_in: refresh_ttl.as_secs(),
    }))
}

/// How long a minted access token is good for.
///
/// Short by default (900s) to limit blast radius if one leaks. Refresh is what
/// keeps a long-running caller authenticated, so raising this is a tuning knob
/// rather than the way to stay up — see `FELIX_REFRESH_TOKEN_TTL_SECONDS`.
pub fn access_token_ttl() -> Duration {
    Duration::from_secs(
        std::env::var("FELIX_EXCHANGE_TOKEN_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(900),
    )
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

    // Intersection only: a permission survives if it was already granted AND
    // the request asked for it.
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

// Group claims from the IdP become ephemeral Casbin groupings for this
// request only; they are never persisted.
pub(crate) fn add_group_claim_groupings(
    groupings: &mut Vec<GroupingRule>,
    principal_id: &str,
    groups: &[String],
) {
    for group in groups {
        let membership = GroupingRule {
            user: principal_id.to_string(),
            role: group_subject(group),
        };
        if !groupings.contains(&membership) {
            groupings.push(membership);
        }
    }
}

// Always prefixed, even when the value already starts with `group:`: an IdP
// group named `group:ops` is a different group from `ops`.
fn group_subject(group: &str) -> String {
    format!("group:{group}")
}

#[cfg(test)]
mod tests;
