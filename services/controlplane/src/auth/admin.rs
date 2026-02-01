//! Admin-only endpoints for IdP issuer config and RBAC policy management.
//!
//! # Purpose and responsibility
//! Exposes privileged APIs that mutate identity provider issuer configuration
//! and RBAC policy/groupings for a tenant.
//!
//! # Where it fits in Felix
//! These handlers sit behind the control-plane API and are invoked by operator
//! tooling or bootstrap flows. Changes here directly affect authorization
//! decisions in the broker and control-plane.
//!
//! # Key invariants and assumptions
//! - Each request is scoped to a specific tenant via the path parameter.
//! - A valid Felix admin token is required for all mutations.
//! - Policy action/object pairs must align with Casbin matching semantics.
//!
//! # Security considerations
//! - This is a privileged surface: mistakes can grant or revoke access.
//! - Tokens are verified with tenant signing keys and must match tenant scope.
//! - Never log or echo bearer tokens or sensitive issuer configuration.
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

/// Request body for adding or removing an RBAC policy rule.
///
/// # What it does
/// Captures the subject/object/action tuple for Casbin policy storage.
///
/// # Why it exists
/// Provides a stable, explicit API contract for policy mutations.
///
/// # Invariants
/// - `action` must be a known Felix action string (e.g. `tenant.admin`).
/// - `object` must be compatible with `keyMatch2` patterns.
///
/// # Errors
/// - Validation is enforced in `add_policy`/`delete_policy`; invalid values
///   return authorization or validation errors.
///
/// # Example
/// ```rust
/// use controlplane::auth::admin::PolicyRequest;
///
/// let req = PolicyRequest {
///     subject: "role:tenant_admin".to_string(),
///     object: "tenant:*".to_string(),
///     action: "tenant.admin".to_string(),
/// };
/// ```
#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct PolicyRequest {
    pub subject: String,
    pub object: String,
    pub action: String,
}

/// Request body for adding or removing an RBAC grouping rule.
///
/// # What it does
/// Associates a subject (user) with a role in the tenant's RBAC graph.
///
/// # Why it exists
/// Keeps role assignment API payloads explicit and auditable.
///
/// # Invariants
/// - `user` and `role` must be non-empty identifiers.
///
/// # Errors
/// - Validation is enforced in `add_grouping`/`delete_grouping`.
///
/// # Example
/// ```rust
/// use controlplane::auth::admin::GroupingRequest;
///
/// let req = GroupingRequest {
///     user: "user:alice".to_string(),
///     role: "role:tenant_admin".to_string(),
/// };
/// ```
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
/// Upsert an IdP issuer configuration for a tenant.
///
/// # What it does
/// Creates or updates an issuer configuration used for OIDC token validation.
///
/// # Why it exists
/// Allows operators to manage trusted identity providers without redeploys.
///
/// # Invariants
/// - Caller must be authorized with `tenant.admin`.
/// - `tenant_id` must refer to an existing tenant.
///
/// # Errors
/// - Returns 401/403 for missing or invalid admin tokens.
/// - Returns 404 if the tenant does not exist.
/// - Returns 500 on store failures.
///
/// # Example
/// ```rust,no_run
/// use axum::extract::{Path, State};
/// use axum::Json;
/// use controlplane::app::AppState;
/// use controlplane::auth::admin::upsert_idp_issuer;
/// use controlplane::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
///
/// async fn handler(Path(tid): Path<String>, State(state): State<AppState>) {
///     let cfg = IdpIssuerConfig {
///         issuer: "https://issuer".to_string(),
///         audiences: vec!["client-id".to_string()],
///         discovery_url: None,
///         jwks_url: Some("https://issuer/.well-known/jwks.json".to_string()),
///         claim_mappings: ClaimMappings::default(),
///     };
///     let _ = upsert_idp_issuer(Path(tid), State(state), Default::default(), Json(cfg)).await;
/// }
/// ```
pub async fn upsert_idp_issuer(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<IdpIssuerConfig>,
) -> Result<StatusCode, ApiError> {
    // Ensure tenant existence before mutating issuer configuration.
    ensure_tenant_exists(&state, &tenant_id).await?;
    // Enforce tenant-admin authorization for issuer updates.
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    // Persist issuer configuration; affects OIDC token validation immediately.
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
/// Delete an IdP issuer configuration for a tenant.
///
/// # What it does
/// Removes an issuer so tokens from it are no longer accepted.
///
/// # Why it exists
/// Supports rotation and revocation of identity providers.
///
/// # Invariants
/// - Caller must be authorized with `tenant.admin`.
/// - `tenant_id` must refer to an existing tenant.
///
/// # Errors
/// - Returns 401/403 for missing or invalid admin tokens.
/// - Returns 404 if the tenant does not exist.
/// - Returns 500 on store failures.
pub async fn delete_idp_issuer(
    Path((tenant_id, issuer)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    // Ensure tenant existence before mutating issuer configuration.
    ensure_tenant_exists(&state, &tenant_id).await?;
    // Enforce tenant-admin authorization for issuer deletion.
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    // Remove issuer configuration; tokens from this issuer will be rejected.
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
/// Add an RBAC policy rule for a tenant.
///
/// # What it does
/// Inserts a Casbin policy tuple (`subject`, `object`, `action`) into storage.
///
/// # Why it exists
/// Provides a privileged mechanism to evolve access control rules.
///
/// # Invariants
/// - Caller must be authorized with `tenant.admin`.
/// - Policy values must align with Casbin matchers.
///
/// # Errors
/// - Returns 401/403 for missing or invalid admin tokens.
/// - Returns 404 if the tenant does not exist.
/// - Returns 500 on store failures.
pub async fn add_policy(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PolicyRequest>,
) -> Result<StatusCode, ApiError> {
    // Ensure tenant exists before policy mutation.
    ensure_tenant_exists(&state, &tenant_id).await?;
    // Enforce tenant-admin authorization for policy updates.
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    // Store policy rule; authorization decisions will use it immediately.
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
/// Add an RBAC grouping rule for a tenant.
///
/// # What it does
/// Associates a subject with a role, enabling inherited permissions.
///
/// # Why it exists
/// Allows administrators to manage role assignments dynamically.
///
/// # Invariants
/// - Caller must be authorized with `tenant.admin`.
/// - Grouping rules must be well-formed identifiers.
///
/// # Errors
/// - Returns 401/403 for missing or invalid admin tokens.
/// - Returns 404 if the tenant does not exist.
/// - Returns 500 on store failures.
pub async fn add_grouping(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<GroupingRequest>,
) -> Result<StatusCode, ApiError> {
    // Ensure tenant exists before grouping mutation.
    ensure_tenant_exists(&state, &tenant_id).await?;
    // Enforce tenant-admin authorization for role assignment.
    require_tenant_admin(&state, &tenant_id, &headers).await?;
    // Store grouping rule; RBAC graph is updated for subsequent checks.
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
    // Step 1: Extract bearer token from Authorization header.
    let bearer = extract_bearer(headers).ok_or_else(|| api_unauthorized("missing bearer token"))?;
    // Step 2: Load tenant signing keys to verify the admin token.
    let keys = state
        .store
        .get_tenant_signing_keys(tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;
    // Step 3: Verify token signature and tenant scoping.
    let claims =
        verify_token(&keys, tenant_id, bearer, 5).map_err(|_| api_unauthorized("invalid token"))?;
    // Step 4: Enforce tenant claim match to prevent cross-tenant admin access.
    if claims.tid != tenant_id {
        return Err(api_forbidden("tenant mismatch"));
    }
    // Step 5: Require tenant.admin permission on the tenant resource.
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
