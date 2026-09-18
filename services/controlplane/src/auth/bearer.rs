//! Bearer-token checks shared by every authenticated control-plane handler.
//!
//! Two shapes of check, one verification path:
//!
//! - **Cluster-scoped.** The object is `cluster:*`, which no tenant owns, so
//!   the token's own `tid` picks the signing keys. Membership and the metadata
//!   feeds the brokers consume live here, and so does the tenant catalog.
//! - **Tenant-scoped.** The path names the tenant; its keys verify the token
//!   and its `tid` has to match. Everything under `/v1/tenants/{tenant_id}`.
//!
//! In both, a permission that does not parse is skipped rather than trusted,
//! so a malformed entry can never widen what a token allows.
use crate::api::error::{ApiError, api_forbidden, api_internal, api_unauthorized};
use crate::app::AppState;
use crate::auth::felix_token::{FelixClaims, verify_token};
use crate::auth::rbac::authorize::{
    ParsedObject, ParsedPermission, object_within_scope, parse_permission,
};
use crate::store::StoreError;
use axum::http::HeaderMap;

/// Clock skew tolerated when checking `exp`, in seconds.
const LEEWAY_SECS: u64 = 5;

pub(crate) fn extract_bearer(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| api_unauthorized("missing bearer token"))
}

/// Verify a token against the keys of the tenant it names, and return both.
///
/// For endpoints outside the tenant hierarchy. The `tid` claim only selects
/// which keys to check against -- the signature is what grants trust, exactly
/// as `kid` selects a key without conferring one.
pub(crate) async fn verified_claims(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, FelixClaims), ApiError> {
    let bearer = extract_bearer(headers)?;
    let tenant_id = unverified_tenant(bearer)?;
    let claims = verify_against(state, &tenant_id, bearer).await?;
    Ok((tenant_id, claims))
}

/// Verify a token against `tenant_id`'s keys and require that it was minted
/// for that tenant.
///
/// A tenant with no keys answers 401, not 404: whether a tenant exists is not
/// something an unauthenticated caller gets to learn by asking.
pub(crate) async fn tenant_claims(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<FelixClaims, ApiError> {
    let bearer = extract_bearer(headers)?;
    let claims = verify_against(state, tenant_id, bearer).await?;
    if claims.tid != tenant_id {
        return Err(api_forbidden("tenant mismatch"));
    }
    Ok(claims)
}

/// The caller's parsed permissions within `tenant_id`.
pub(crate) async fn tenant_permissions(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<Vec<ParsedPermission>, ApiError> {
    let claims = tenant_claims(state, tenant_id, headers).await?;
    Ok(claims
        .perms
        .iter()
        .filter_map(|perm| parse_permission(perm, tenant_id).ok())
        .collect())
}

/// The objects the caller holds `action` over within `tenant_id`.
///
/// 403 when there are none, so a listing endpoint can filter to these without
/// a separate "may list at all" check.
pub(crate) async fn tenant_scopes_for(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
) -> Result<Vec<ParsedObject>, ApiError> {
    let scopes: Vec<ParsedObject> = tenant_permissions(state, tenant_id, headers)
        .await?
        .into_iter()
        .filter(|perm| perm.action == action)
        .map(|perm| perm.object)
        .collect();
    if scopes.is_empty() {
        return Err(api_forbidden("missing required permission"));
    }
    Ok(scopes)
}

/// Require `action` over one object inside `tenant_id`.
pub(crate) async fn require_tenant_action(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
    target: &ParsedObject,
) -> Result<(), ApiError> {
    let scopes = tenant_scopes_for(state, tenant_id, headers, action).await?;
    if scopes
        .iter()
        .any(|scope| object_within_scope(scope, target))
    {
        Ok(())
    } else {
        Err(api_forbidden("insufficient scope"))
    }
}

/// Require `action` over `cluster:*`.
///
/// No tenant scope contains the cluster, so a tenant admin cannot write this
/// rule for themselves; it reaches a token only when an operator who already
/// holds cluster scope grants it.
pub(crate) async fn require_cluster_action(
    state: &AppState,
    headers: &HeaderMap,
    action: &str,
) -> Result<(), ApiError> {
    let (tenant_id, claims) = verified_claims(state, headers).await?;
    let allowed = claims.perms.iter().any(|perm| {
        // Parsed against the token's own tenant; a cluster object ignores it.
        matches!(
            parse_permission(perm, &tenant_id),
            Ok(parsed) if parsed.action == action && parsed.object == ParsedObject::Cluster
        )
    });
    if allowed {
        Ok(())
    } else {
        Err(api_forbidden(&format!(
            "missing {action}:cluster:* permission"
        )))
    }
}

async fn verify_against(
    state: &AppState,
    tenant_id: &str,
    bearer: &str,
) -> Result<FelixClaims, ApiError> {
    let keys = match state.store.get_tenant_signing_keys(tenant_id).await {
        Ok(keys) => keys,
        // No keys means nothing could have signed this token.
        Err(StoreError::NotFound(_)) => return Err(api_unauthorized("invalid token")),
        Err(ref err) => return Err(api_internal("failed to load signing keys", err)),
    };
    verify_token(&keys, tenant_id, bearer, LEEWAY_SECS)
        .map_err(|_| api_unauthorized("invalid token"))
}

/// Read `tid` from an unverified token, only to choose a verification key.
fn unverified_tenant(token: &str) -> Result<String, ApiError> {
    use base64::Engine as _;

    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| api_unauthorized("malformed token"))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| api_unauthorized("malformed token"))?;
    let claims: serde_json::Value =
        serde_json::from_slice(&decoded).map_err(|_| api_unauthorized("malformed token"))?;

    claims
        .get("tid")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| api_unauthorized("token has no tenant claim"))
}
