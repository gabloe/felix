//! Control-plane storage interfaces and shared types.
//!
//! # Purpose
//! Defines the `ControlPlaneStore` trait, error types, and shared snapshot/change
//! structs used by both in-memory and Postgres backends.
//!
//! # Notes
//! Store implementors should preserve change ordering and enforce conflict/not-found
//! semantics consistently with these trait contracts.
use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant, TenantChange,
};
use async_trait::async_trait;
use thiserror::Error;

pub mod memory;
pub mod postgres;

#[cfg(test)]
mod postgres_tests;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub changes_limit: u64,
    pub change_retention_max_rows: Option<i64>,
}

impl StoreConfig {
    pub fn change_window(&self) -> usize {
        // Clamp to at least changes_limit; the DB retention may be higher but never lower.
        self.change_retention_max_rows
            .unwrap_or(self.changes_limit as i64)
            .max(self.changes_limit as i64) as usize
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot<T> {
    pub items: Vec<T>,
    pub next_seq: u64,
}

#[derive(Debug, Clone)]
pub struct ChangeSet<T> {
    pub items: Vec<T>,
    pub next_seq: u64,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[async_trait]
pub trait ControlPlaneStore: Send + Sync {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>>;
    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant>;
    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()>;
    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>>;
    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>>;

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>>;
    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace>;
    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()>;
    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>>;
    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>>;

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>>;
    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream>;
    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream>;
    async fn patch_stream(&self, key: &StreamKey, patch: StreamPatchRequest)
    -> StoreResult<Stream>;
    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()>;
    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>>;
    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>>;

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>>;
    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache>;
    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache>;
    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache>;
    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()>;
    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>>;
    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>>;

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool>;
    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool>;

    async fn health_check(&self) -> StoreResult<()>;
    fn is_durable(&self) -> bool;
    fn backend_name(&self) -> &'static str;
}

#[async_trait]
pub trait AuthStore: Send + Sync {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>>;
    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()>;
    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()>;

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>>;
    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>>;
    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()>;
    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()>;

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys>;
    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()>;

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool>;
    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()>;

    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys>;
    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()>;
}

pub trait ControlPlaneAuthStore: ControlPlaneStore + AuthStore {}

impl<T> ControlPlaneAuthStore for T where T: ControlPlaneStore + AuthStore {}
