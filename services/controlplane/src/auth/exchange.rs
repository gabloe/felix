//! Token exchange endpoint handler.
//!
//! # Purpose
//! Validate upstream OIDC tokens, evaluate RBAC permissions, and mint Felix
//! EdDSA tokens for broker access.
//!
//! # Architectural role
//! This module is the control-plane boundary that turns IdP identity into a
//! Felix-scoped authorization token for the broker.
//!
//! # Callers / consumers
//! - HTTP clients invoking `/v1/tenants/{tenant_id}/token/exchange`.
//! - Brokers and SDKs that need a Felix token after authenticating to an IdP.
//!
//! # Key invariants
//! - Upstream IdP tokens must be validated before any Felix token is issued.
//! - Felix tokens are always EdDSA and include `iss`, `aud`, and `tid` claims.
//! - Permissions are never widened by the exchange request.
//!
//! # Concurrency model
//! Stateless request handler; relies on async store calls and per-request data.
//!
//! # Security boundary
//! This endpoint is the boundary between external IdP tokens and internal Felix
//! authorization. It must enforce issuer, audience, and RBAC constraints.
//!
//! # Security model and threat assumptions
//! - Attackers may present arbitrary bearer tokens; we only accept validated IdP
//!   tokens from configured issuers.
//! - The exchange request may attempt to reduce scope, but never increase it.
//! - Felix tokens must remain EdDSA to avoid RSA fallback in downstream services.
//!
//! # How to use
//! POST a bearer IdP token to `/v1/tenants/{tenant_id}/token/exchange` and use
//! the returned `felix_token` for broker authentication.
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

/// Request body for token exchange, optionally narrowing the permissions.
///
/// # Overview
/// Allows callers to request a subset of permissions and/or resources that are
/// already granted by RBAC, without expanding scope.
///
/// # Arguments
/// - `requested`: Optional list of action names (e.g. `stream.publish`).
/// - `resources`: Optional list of resource patterns to further narrow access.
///
/// # Returns
/// - Not applicable (data container).
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::exchange::TokenExchangeRequest;
///
/// let req = TokenExchangeRequest {
///     requested: Some(vec!["stream.publish".to_string()]),
///     resources: Some(vec!["stream:payments/*".to_string()]),
/// };
/// assert!(req.requested.is_some());
/// ```
///
/// # Security
/// - Inputs are treated as a narrowing filter; they must never widen scope.
#[derive(Debug, Deserialize, ToSchema, Clone, Default)]
pub struct TokenExchangeRequest {
    pub requested: Option<Vec<String>>,
    pub resources: Option<Vec<String>>,
}

/// Response body for token exchange.
///
/// # Overview
/// Returns a Felix-issued bearer token and expiration metadata.
///
/// # Arguments
/// - Not applicable (data container).
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
/// use controlplane::auth::exchange::TokenExchangeResponse;
///
/// let resp = TokenExchangeResponse {
///     felix_token: "token".to_string(),
///     expires_in: 900,
///     token_type: "Bearer".to_string(),
/// };
/// assert_eq!(resp.token_type, "Bearer");
/// ```
///
/// # Security
/// - `felix_token` is a bearer token and must be treated as a secret in logs.
#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct TokenExchangeResponse {
    pub felix_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

/// Exchange an upstream IdP token for a Felix EdDSA token.
///
/// # Overview
/// Validates the incoming bearer token against configured issuers, computes
/// effective permissions via RBAC, optionally narrows them, and mints a Felix
/// token scoped to the tenant.
///
/// # Arguments
/// - `tenant_id`: Path parameter identifying the tenant.
/// - `state`: Shared application state providing store access and OIDC validator.
/// - `headers`: Incoming HTTP headers containing the bearer token.
/// - `body`: Optional request body to narrow permissions.
///
/// # Returns
/// - `Ok(Json<TokenExchangeResponse>)` with a Felix bearer token.
///
/// # Errors
/// - `401` if the bearer token is missing or invalid.
/// - `403` if the issuer is not allowed or no permissions are granted.
/// - `500` for store errors or internal failures.
///
/// # Panics
/// - Does not panic under normal conditions.
///
/// # Examples
/// ```rust,no_run
/// use axum::extract::{Path, State};
/// use axum::http::HeaderMap;
/// use axum::Json;
/// use controlplane::app::AppState;
/// use controlplane::auth::exchange::{exchange_token, TokenExchangeRequest};
///
/// async fn handler(
///     Path(tenant_id): Path<String>,
///     State(state): State<AppState>,
///     headers: HeaderMap,
/// ) {
///     let _ = exchange_token(Path(tenant_id), State(state), headers, None).await;
/// }
/// ```
///
/// # Security
/// - Upstream tokens are validated before issuing any Felix token.
/// - Issuer/audience validation and RBAC enforcement are mandatory.
/// - Felix tokens are always EdDSA; no RSA fallback is permitted.
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
    // Step 1: Extract the bearer token early so we can fail fast.
    // This avoids doing any store work for unauthenticated requests.
    let bearer =
        extract_bearer(&headers).ok_or_else(|| api_unauthorized("missing bearer token"))?;

    // Step 2: Ensure the tenant exists to prevent issuer probing.
    // We return forbidden for non-existent tenants to avoid disclosure.
    let tenant_exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;
    if !tenant_exists {
        return Err(api_forbidden("tenant not allowed"));
    }

    // Step 3: Load allowed issuers for this tenant.
    // Without configured issuers, we must reject any token exchange.
    let issuers = state
        .store
        .list_idp_issuers(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load issuers", &err))?;
    if issuers.is_empty() {
        return Err(api_forbidden("no issuers configured"));
    }

    // Step 4: Validate the upstream token against configured issuers.
    // This uses the OIDC allowlist (ES256 by default; RS*/PS* behind `oidc-rsa`).
    let validated = match state.oidc_validator.validate(bearer, &issuers).await {
        Ok(token) => token,
        Err(OidcError::IssuerNotAllowed) => return Err(api_forbidden("issuer not allowed")),
        Err(_) => return Err(api_unauthorized("invalid token")),
    };

    // Step 5: Map token claims to a principal identifier.
    // This is stable across issuers and used for RBAC evaluation.
    let principal = principal::from_claims(&validated.issuer, &validated.subject, validated.groups);

    // Step 6: Load RBAC policies and groupings for evaluation.
    // We keep these local to the request to avoid stale permission reads.
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

    // Step 7: Build the RBAC enforcer with explicit tenant scoping.
    // This guarantees we only evaluate policies within the tenant boundary.
    let enforcer = build_enforcer(&policies, &groupings, &tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to build rbac enforcer");
            api_internal_message("failed to build enforcer")
        })?;

    // Step 8: Compute effective permissions for the principal.
    // This list is authoritative and should never be widened by the request.
    let mut perms = effective_permissions(&enforcer, &principal.principal_id, &tenant_id);

    // Step 9: Optionally filter permissions based on the request body.
    // This is a narrowing operation only; widen attempts are ignored.
    if let Some(request) = body.map(|Json(value)| value) {
        perms = filter_permissions(perms, &request);
    }

    // Step 10: Enforce that some permissions remain.
    // Issuing a token with empty permissions is not useful and may mask errors.
    if perms.is_empty() {
        return Err(api_forbidden("no permissions"));
    }

    // Step 11: Fetch the tenant signing keys for Felix token minting.
    // This uses Ed25519 key material; we must not allow RSA here.
    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    // Step 12: Mint a short-lived Felix token for broker use.
    // TTL is intentionally short to limit blast radius if leaked.
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
    // We only accept standard `Authorization: Bearer <token>` format.
    // Any other scheme must be rejected to avoid ambiguity.
    let value = headers.get(axum::http::header::AUTHORIZATION)?;
    let value = value.to_str().ok()?;
    value.strip_prefix("Bearer ")
}

fn filter_permissions(perms: Vec<String>, request: &TokenExchangeRequest) -> Vec<String> {
    // Step 1: Normalize requested actions into a set for O(1) lookups.
    // This prevents quadratic scans when filtering large permission lists.
    let requested_actions = request.requested.as_ref().map(|actions| {
        actions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<String>>()
    });
    let resources = request.resources.as_ref();

    // Step 2: Retain only permissions that match requested actions and resources.
    // This narrows scope; it never expands beyond the RBAC-derived permissions.
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
    use crate::api::types::{FeatureFlags, Region};
    use crate::app::AppState;
    use crate::auth::idp_registry::IdpIssuerConfig;
    use crate::config::{DEFAULT_CHANGE_RETENTION_MAX_ROWS, DEFAULT_CHANGES_LIMIT};
    use crate::store::memory::InMemoryStore;
    use crate::store::{AuthStore, ControlPlaneStore, StoreConfig};
    use anyhow::Result;
    use axum::http::HeaderValue;
    use axum::http::header::AUTHORIZATION;
    use base64::Engine;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn filters_by_requested_actions() {
        // This test prevents a regression where requested actions are ignored,
        // which would unintentionally broaden permissions.
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

    fn test_state(store: Arc<InMemoryStore>) -> AppState {
        AppState {
            region: Region {
                region_id: "local".to_string(),
                display_name: "Local".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: false,
                tiered_storage: false,
                bridges: false,
            },
            store,
            oidc_validator: crate::auth::oidc::UpstreamOidcValidator::default(),
            bootstrap_enabled: false,
            bootstrap_token: None,
        }
    }

    fn store_config() -> StoreConfig {
        StoreConfig {
            changes_limit: DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS),
        }
    }

    fn bearer_header(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let value = format!("Bearer {token}");
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&value).expect("auth header"),
        );
        headers
    }

    fn unsigned_es256_token(issuer: &str, kid: &str) -> String {
        let header = json!({
            "alg": "ES256",
            "kid": kid,
            "typ": "JWT"
        });
        let payload = json!({
            "iss": issuer
        });
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&header).expect("header json"));
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).expect("payload json"));
        format!("{header_b64}.{payload_b64}.sig")
    }

    #[tokio::test]
    async fn exchange_token_rejects_missing_bearer() {
        let store = Arc::new(InMemoryStore::new(store_config()));
        let state = test_state(store);
        let headers = HeaderMap::new();
        let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
            .await
            .expect_err("missing bearer");
        assert_eq!(err.status, axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn exchange_token_rejects_unknown_tenant() {
        let store = Arc::new(InMemoryStore::new(store_config()));
        let state = test_state(store);
        let token = unsigned_es256_token("https://issuer.example", "kid1");
        let headers = bearer_header(&token);
        let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
            .await
            .expect_err("unknown tenant");
        assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
        assert!(err.body.message.contains("tenant not allowed"));
    }

    #[tokio::test]
    async fn exchange_token_rejects_when_no_issuers_configured() -> Result<()> {
        let store = Arc::new(InMemoryStore::new(store_config()));
        store
            .create_tenant(crate::model::Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant".to_string(),
            })
            .await?;
        let state = test_state(store);
        let token = unsigned_es256_token("https://issuer.example", "kid1");
        let headers = bearer_header(&token);
        let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
            .await
            .expect_err("no issuers");
        assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
        assert!(err.body.message.contains("no issuers configured"));
        Ok(())
    }

    #[tokio::test]
    async fn exchange_token_rejects_issuer_not_allowed() -> Result<()> {
        let store = Arc::new(InMemoryStore::new(store_config()));
        store
            .create_tenant(crate::model::Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant".to_string(),
            })
            .await?;
        store
            .upsert_idp_issuer(
                "t1",
                IdpIssuerConfig {
                    issuer: "https://issuer.allowed".to_string(),
                    audiences: vec!["aud".to_string()],
                    discovery_url: None,
                    jwks_url: None,
                    claim_mappings: crate::auth::idp_registry::ClaimMappings::default(),
                },
            )
            .await?;
        let state = test_state(store);
        let token = unsigned_es256_token("https://issuer.denied", "kid1");
        let headers = bearer_header(&token);
        let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
            .await
            .expect_err("issuer not allowed");
        assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
        assert!(err.body.message.contains("issuer not allowed"));
        Ok(())
    }
}
