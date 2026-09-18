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
//!
//! Every refusal goes through [`refused`], so a credential that was presented
//! and turned away is counted and logged: an unauthorized attempt is something
//! an operator gets to see, not only something the caller gets told.
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

/// Counts every credential refusal. `reason` is one of a closed set, so the
/// label stays bounded.
pub(crate) const AUTH_REJECTED_TOTAL: &str = "felix_controlplane_auth_rejected_total";

/// Why a credential was turned away.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Refusal {
    /// No `Authorization: Bearer` at all.
    MissingToken,
    /// Not a JWT, or one without a tenant claim.
    MalformedToken,
    /// Did not verify against the tenant's keys, or the tenant has none.
    InvalidToken,
    /// Verified, but for a different tenant than the one addressed.
    TenantMismatch,
    /// Verified, but without the permission the request needs.
    Forbidden,
}

impl Refusal {
    fn label(self) -> &'static str {
        match self {
            Self::MissingToken => "missing_token",
            Self::MalformedToken => "malformed_token",
            Self::InvalidToken => "invalid_token",
            Self::TenantMismatch => "tenant_mismatch",
            Self::Forbidden => "forbidden",
        }
    }
}

/// Turn a credential away: count it, log it, and answer.
///
/// Never the token, never the key. What is logged is the reason and the
/// message the caller also sees.
pub(crate) fn refused(reason: Refusal, message: &str) -> ApiError {
    metrics::counter!(AUTH_REJECTED_TOTAL, "reason" => reason.label()).increment(1);
    tracing::info!(
        reason = reason.label(),
        message,
        "refused a control-plane credential"
    );
    match reason {
        Refusal::MissingToken | Refusal::MalformedToken | Refusal::InvalidToken => {
            api_unauthorized(message)
        }
        Refusal::TenantMismatch | Refusal::Forbidden => api_forbidden(message),
    }
}

pub(crate) fn extract_bearer(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| refused(Refusal::MissingToken, "missing bearer token"))
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
        return Err(refused(Refusal::TenantMismatch, "tenant mismatch"));
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
        return Err(refused(Refusal::Forbidden, "missing required permission"));
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
        Err(refused(Refusal::Forbidden, "insufficient scope"))
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
        Err(refused(
            Refusal::Forbidden,
            &format!("missing {action}:cluster:* permission"),
        ))
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
        Err(StoreError::NotFound(_)) => {
            return Err(refused(Refusal::InvalidToken, "invalid token"));
        }
        Err(ref err) => return Err(api_internal("failed to load signing keys", err)),
    };
    verify_token(&keys, tenant_id, bearer, LEEWAY_SECS)
        .map_err(|_| refused(Refusal::InvalidToken, "invalid token"))
}

/// Read `tid` from an unverified token, only to choose a verification key.
fn unverified_tenant(token: &str) -> Result<String, ApiError> {
    use base64::Engine as _;

    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| refused(Refusal::MalformedToken, "malformed token"))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| refused(Refusal::MalformedToken, "malformed token"))?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded)
        .map_err(|_| refused(Refusal::MalformedToken, "malformed token"))?;

    claims
        .get("tid")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| refused(Refusal::MalformedToken, "token has no tenant claim"))
}
