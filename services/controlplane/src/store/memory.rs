//! In-memory implementation of the control-plane store.
//!
//! # Purpose
//! This store implements the `ControlPlaneStore` trait entirely in memory using `HashMap`s guarded
//! by `tokio::sync::RwLock`. It exists for:
//! - local development and tests (no external dependencies)
//! - deployments where durability is not required
//! - as a fallback when a durable backend (e.g., Postgres) is not configured
//!
//! # Durability and consistency
//! - **Not durable**: all state is lost on process restart.
//! - **Single-process consistency**: operations are consistent within one process. We use write locks
//!   for mutations and read locks for reads.
//! - **No multi-node coordination**: multiple controlplane instances each have independent state.
//!
//! # Change streams
//! The controlplane exposes two ways to consume state:
//! 1) A full **snapshot** of current state.
//! 2) An incremental **change stream** since a sequence number (`seq`).
//!
//! This store maintains per-entity in-memory change logs with a bounded retention window
//! (`StoreConfig::change_window`). When the window overflows, old changes are evicted.
//! Consumers that fall behind past the window must re-bootstrap via snapshot.
//!
//! # Performance characteristics
//! - Reads are cheap and concurrent (many readers).
//! - Writes are serialized per map/log (write lock per structure).
//! - Deletes cascade by scanning keys to find dependents; this is acceptable for small in-memory
//!   dev workloads but would be inefficient at very large scale.
//!
//! # Metrics
//! This store updates a small set of gauges/counters to keep observability behavior consistent with
//! durable backends.
use super::{
    AuthStore, ChangeSet, ControlPlaneStore, Snapshot, StoreConfig, StoreError, StoreResult,
};
use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, Namespace, NamespaceChange,
    NamespaceChangeOp, NamespaceKey, Stream, StreamChange, StreamChangeOp, StreamKey,
    StreamPatchRequest, Tenant, TenantChange, TenantChangeOp,
};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Bounded, in-memory append-only log of changes for a single entity type.
///
/// The log is keyed by a monotonically increasing `seq` that is assigned by this process.
/// - `record()` assigns the next sequence number, appends the change, and evicts older items when
///   the configured capacity is exceeded.
/// - Eviction means a consumer may miss changes if it polls too slowly; in that case it must
///   re-bootstrap by calling the corresponding `*_snapshot()` API.
///
/// This structure is intentionally simple. Unlike the Postgres backend:
/// - There is no transactional coupling between authoritative state and the change log.
/// - Atomicity is achieved by sequencing operations under locks.
#[derive(Debug)]
struct ChangeLog<T> {
    next_seq: u64,
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> ChangeLog<T> {
    fn new(capacity: usize) -> Self {
        // Pre-allocate the deque to the configured capacity to reduce reallocations.
        Self {
            next_seq: 0,
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    fn record(&mut self, item: impl FnOnce(u64) -> T) -> u64 {
        // Assign a strictly increasing sequence number for this change stream.
        // Consumers use `since` checkpoints to resume from a known position.
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(item(seq));
        // Enforce a fixed retention window: keep only the most recent `capacity` changes.
        while self.items.len() > self.capacity {
            self.items.pop_front();
        }
        seq
    }
}

/// In-memory control-plane store.
///
/// ## Data structures
/// - Authoritative state is stored in `HashMap`s.
/// - Change streams are stored in `ChangeLog`s.
///
/// All maps/logs are wrapped in `Arc<RwLock<...>>` so:
/// - the store can be cloned and shared across async request handlers
/// - reads can proceed concurrently
/// - writes are serialized to preserve invariants
///
/// ## Cascading deletes
/// Deleting a tenant/namespace removes dependent namespaces/streams/caches by scanning keys.
/// This keeps behavior correct and predictable for dev/test usage.
/// Durable backends should implement cascades via SQL constraints/transactions.
pub struct InMemoryStore {
    /// Store-level configuration (change window, paging limit, etc.).
    config: StoreConfig,
    /// Authoritative tenant objects keyed by `tenant_id`.
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    /// Authoritative namespaces keyed by `(tenant_id, namespace)`.
    namespaces: Arc<RwLock<HashMap<NamespaceKey, Namespace>>>,
    /// Authoritative streams keyed by `(tenant_id, namespace, stream)`.
    streams: Arc<RwLock<HashMap<StreamKey, Stream>>>,
    /// Authoritative caches keyed by `(tenant_id, namespace, cache)`.
    caches: Arc<RwLock<HashMap<CacheKey, Cache>>>,
    /// Bounded change log for tenant changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    tenant_changes: Arc<RwLock<ChangeLog<TenantChange>>>,
    /// Bounded change log for namespace changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    namespace_changes: Arc<RwLock<ChangeLog<NamespaceChange>>>,
    /// Bounded change log for stream changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    stream_changes: Arc<RwLock<ChangeLog<StreamChange>>>,
    /// Bounded change log for cache changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    cache_changes: Arc<RwLock<ChangeLog<CacheChange>>>,
    /// Per-tenant identity provider issuer configs.
    idp_issuers: Arc<RwLock<HashMap<String, Vec<IdpIssuerConfig>>>>,
    /// Per-tenant signing keys for Felix tokens.
    tenant_signing_keys: Arc<RwLock<HashMap<String, TenantSigningKeys>>>,
    /// Per-tenant RBAC policy rules.
    rbac_policies: Arc<RwLock<HashMap<String, Vec<PolicyRule>>>>,
    /// Per-tenant RBAC group bindings.
    rbac_groupings: Arc<RwLock<HashMap<String, Vec<GroupingRule>>>>,
    /// Per-tenant auth bootstrap completion.
    auth_bootstrapped: Arc<RwLock<HashMap<String, bool>>>,
}

impl InMemoryStore {
    pub fn new(config: StoreConfig) -> Self {
        let capacity = config.change_window();
        // The change window is a retention bound for incremental sync consumers.
        // Once capacity is exceeded, oldest changes are evicted.
        Self {
            config,
            tenants: Arc::new(RwLock::new(HashMap::new())),
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            caches: Arc::new(RwLock::new(HashMap::new())),
            tenant_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            namespace_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            stream_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            cache_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            idp_issuers: Arc::new(RwLock::new(HashMap::new())),
            tenant_signing_keys: Arc::new(RwLock::new(HashMap::new())),
            rbac_policies: Arc::new(RwLock::new(HashMap::new())),
            rbac_groupings: Arc::new(RwLock::new(HashMap::new())),
            auth_bootstrapped: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn limit(&self) -> usize {
        // Max number of changes returned per `*_changes()` call.
        // This prevents unbounded responses and keeps polling predictable.
        self.config.changes_limit as usize
    }
}

#[async_trait]
impl ControlPlaneStore for InMemoryStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        Ok(self.tenants.read().await.values().cloned().collect())
    }

    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant> {
        // Create is a pure in-memory upsert with conflict detection.
        // We also append a change-log entry so watchers can incrementally sync.
        let mut tenants = self.tenants.write().await;
        if tenants.contains_key(&tenant.tenant_id) {
            return Err(StoreError::Conflict("tenant exists".into()));
        }
        tenants.insert(tenant.tenant_id.clone(), tenant.clone());
        self.tenant_changes
            .write()
            .await
            .record(|seq| TenantChange {
                seq,
                op: TenantChangeOp::Created,
                tenant_id: tenant.tenant_id.clone(),
                tenant: Some(tenant.clone()),
            });
        Ok(tenant)
    }

    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        let mut tenants = self.tenants.write().await;
        if tenants.remove(tenant_id).is_none() {
            return Err(StoreError::NotFound("tenant".into()));
        }
        drop(tenants);
        self.idp_issuers.write().await.remove(tenant_id);
        self.tenant_signing_keys.write().await.remove(tenant_id);
        self.rbac_policies.write().await.remove(tenant_id);
        self.rbac_groupings.write().await.remove(tenant_id);
        self.auth_bootstrapped.write().await.remove(tenant_id);
        // Cascading delete: remove dependent namespaces, streams, and caches.
        // We emit delete changes for dependents so incremental consumers can evict their caches.
        let mut namespaces = self.namespaces.write().await;
        let ns_keys: Vec<_> = namespaces
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        for key in &ns_keys {
            if let Some(ns) = namespaces.remove(key) {
                self.namespace_changes
                    .write()
                    .await
                    .record(|seq| NamespaceChange {
                        seq,
                        op: NamespaceChangeOp::Deleted,
                        key: key.clone(),
                        namespace: Some(ns),
                    });
            }
        }
        drop(namespaces);

        let mut streams = self.streams.write().await;
        let stream_keys: Vec<_> = streams
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        for key in &stream_keys {
            if let Some(stream) = streams.remove(key) {
                self.stream_changes
                    .write()
                    .await
                    .record(|seq| StreamChange {
                        seq,
                        op: StreamChangeOp::Deleted,
                        key: key.clone(),
                        stream: Some(stream),
                    });
            }
        }
        metrics::gauge!("felix_streams_total").set(streams.len() as f64);
        drop(streams);

        let mut caches = self.caches.write().await;
        let cache_keys: Vec<_> = caches
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        for key in &cache_keys {
            if let Some(cache) = caches.remove(key) {
                self.cache_changes.write().await.record(|seq| CacheChange {
                    seq,
                    op: CacheChangeOp::Deleted,
                    key: key.clone(),
                    cache: Some(cache),
                });
            }
        }
        metrics::gauge!("felix_caches_total").set(caches.len() as f64);

        self.tenant_changes
            .write()
            .await
            .record(|seq| TenantChange {
                seq,
                op: TenantChangeOp::Deleted,
                tenant_id: tenant_id.to_string(),
                tenant: None,
            });
        Ok(())
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
        let items = self.tenants.read().await.values().cloned().collect();
        let next_seq = self.tenant_changes.read().await.next_seq;
        Ok(Snapshot { items, next_seq })
    }

    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        // We filter by `seq >= since` (inclusive) and apply a page limit.
        // If the caller's `since` is older than the retained window, it will receive a partial
        // history and should fall back to `*_snapshot()` to re-bootstrap.
        let guard = self.tenant_changes.read().await;
        let items = guard
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: guard.next_seq,
        })
    }

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        let items = self
            .namespaces
            .read()
            .await
            .values()
            .filter(|ns| ns.tenant_id == tenant_id)
            .cloned()
            .collect();
        Ok(items)
    }

    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace> {
        // Namespaces are scoped to a tenant; we reject creation if the parent tenant doesn't exist.
        if !self.tenant_exists(&namespace.tenant_id).await? {
            return Err(StoreError::NotFound("tenant".into()));
        }
        let key = NamespaceKey {
            tenant_id: namespace.tenant_id.clone(),
            namespace: namespace.namespace.clone(),
        };
        let mut namespaces = self.namespaces.write().await;
        if namespaces.contains_key(&key) {
            return Err(StoreError::Conflict("namespace exists".into()));
        }
        namespaces.insert(key.clone(), namespace.clone());
        self.namespace_changes
            .write()
            .await
            .record(|seq| NamespaceChange {
                seq,
                op: NamespaceChangeOp::Created,
                key,
                namespace: Some(namespace.clone()),
            });
        Ok(namespace)
    }

    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()> {
        if !self.tenant_exists(&key.tenant_id).await? {
            return Err(StoreError::NotFound("tenant".into()));
        }
        let mut namespaces = self.namespaces.write().await;
        let removed = namespaces.remove(key);
        drop(namespaces);
        if removed.is_none() {
            return Err(StoreError::NotFound("namespace".into()));
        }
        self.namespace_changes
            .write()
            .await
            .record(|seq| NamespaceChange {
                seq,
                op: NamespaceChangeOp::Deleted,
                key: key.clone(),
                namespace: None,
            });
        // Cascading delete: removing a namespace also removes its streams and caches.
        let mut streams = self.streams.write().await;
        let stream_keys: Vec<_> = streams
            .keys()
            .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
            .cloned()
            .collect();
        for stream_key in &stream_keys {
            if let Some(stream) = streams.remove(stream_key) {
                self.stream_changes
                    .write()
                    .await
                    .record(|seq| StreamChange {
                        seq,
                        op: StreamChangeOp::Deleted,
                        key: stream_key.clone(),
                        stream: Some(stream),
                    });
            }
        }
        metrics::gauge!("felix_streams_total").set(streams.len() as f64);

        let mut caches = self.caches.write().await;
        let cache_keys: Vec<_> = caches
            .keys()
            .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
            .cloned()
            .collect();
        for cache_key in &cache_keys {
            if let Some(cache) = caches.remove(cache_key) {
                self.cache_changes.write().await.record(|seq| CacheChange {
                    seq,
                    op: CacheChangeOp::Deleted,
                    key: cache_key.clone(),
                    cache: Some(cache),
                });
            }
        }
        metrics::gauge!("felix_caches_total").set(caches.len() as f64);
        Ok(())
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
        let items = self.namespaces.read().await.values().cloned().collect();
        let next_seq = self.namespace_changes.read().await.next_seq;
        Ok(Snapshot { items, next_seq })
    }

    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        // We filter by `seq >= since` (inclusive) and apply a page limit.
        // If the caller's `since` is older than the retained window, it will receive a partial
        // history and should fall back to `*_snapshot()` to re-bootstrap.
        let guard = self.namespace_changes.read().await;
        let items = guard
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: guard.next_seq,
        })
    }

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>> {
        let items = self
            .streams
            .read()
            .await
            .values()
            .filter(|stream| stream.tenant_id == tenant_id && stream.namespace == namespace)
            .cloned()
            .collect();
        Ok(items)
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        self.streams
            .read()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound("stream".into()))
    }

    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream> {
        // Streams are scoped to a namespace; we reject creation if the parent namespace doesn't exist.
        if !self
            .namespace_exists(&NamespaceKey {
                tenant_id: stream.tenant_id.clone(),
                namespace: stream.namespace.clone(),
            })
            .await?
        {
            return Err(StoreError::NotFound("namespace".into()));
        }
        let key = StreamKey {
            tenant_id: stream.tenant_id.clone(),
            namespace: stream.namespace.clone(),
            stream: stream.stream.clone(),
        };
        let mut streams = self.streams.write().await;
        if streams.contains_key(&key) {
            return Err(StoreError::Conflict("stream exists".into()));
        }
        streams.insert(key.clone(), stream.clone());
        self.stream_changes
            .write()
            .await
            .record(|seq| StreamChange {
                seq,
                op: StreamChangeOp::Created,
                key,
                stream: Some(stream.clone()),
            });
        metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
        metrics::gauge!("felix_streams_total").set(streams.len() as f64);
        Ok(stream)
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        let mut streams = self.streams.write().await;
        let stream = streams
            .get_mut(key)
            .ok_or_else(|| StoreError::NotFound("stream".into()))?;
        if let Some(retention) = patch.retention {
            stream.retention = retention;
        }
        if let Some(consistency) = patch.consistency {
            stream.consistency = consistency;
        }
        if let Some(delivery) = patch.delivery {
            stream.delivery = delivery;
        }
        if let Some(durable) = patch.durable {
            stream.durable = durable;
        }
        // After applying the patch, we emit an `Updated` change so watchers can reconcile.
        let updated = stream.clone();
        self.stream_changes
            .write()
            .await
            .record(|seq| StreamChange {
                seq,
                op: StreamChangeOp::Updated,
                key: key.clone(),
                stream: Some(updated.clone()),
            });
        metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
        Ok(updated)
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        let mut streams = self.streams.write().await;
        let removed = streams.remove(key);
        if removed.is_none() {
            return Err(StoreError::NotFound("stream".into()));
        }
        self.stream_changes
            .write()
            .await
            .record(|seq| StreamChange {
                seq,
                op: StreamChangeOp::Deleted,
                key: key.clone(),
                stream: None,
            });
        metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
        metrics::gauge!("felix_streams_total").set(streams.len() as f64);
        Ok(())
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
        let items = self.streams.read().await.values().cloned().collect();
        let next_seq = self.stream_changes.read().await.next_seq;
        Ok(Snapshot { items, next_seq })
    }

    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        // We filter by `seq >= since` (inclusive) and apply a page limit.
        // If the caller's `since` is older than the retained window, it will receive a partial
        // history and should fall back to `*_snapshot()` to re-bootstrap.
        let guard = self.stream_changes.read().await;
        let items = guard
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: guard.next_seq,
        })
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        let items = self
            .caches
            .read()
            .await
            .values()
            .filter(|cache| cache.tenant_id == tenant_id && cache.namespace == namespace)
            .cloned()
            .collect();
        Ok(items)
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        self.caches
            .read()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound("cache".into()))
    }

    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache> {
        // Caches are scoped to a namespace; we reject creation if the parent namespace doesn't exist.
        if !self
            .namespace_exists(&NamespaceKey {
                tenant_id: cache.tenant_id.clone(),
                namespace: cache.namespace.clone(),
            })
            .await?
        {
            return Err(StoreError::NotFound("namespace".into()));
        }
        let key = CacheKey {
            tenant_id: cache.tenant_id.clone(),
            namespace: cache.namespace.clone(),
            cache: cache.cache.clone(),
        };
        let mut caches = self.caches.write().await;
        if caches.contains_key(&key) {
            return Err(StoreError::Conflict("cache exists".into()));
        }
        caches.insert(key.clone(), cache.clone());
        self.cache_changes.write().await.record(|seq| CacheChange {
            seq,
            op: CacheChangeOp::Created,
            key,
            cache: Some(cache.clone()),
        });
        metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        metrics::gauge!("felix_caches_total").set(caches.len() as f64);
        Ok(cache)
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        let mut caches = self.caches.write().await;
        let cache = caches
            .get_mut(key)
            .ok_or_else(|| StoreError::NotFound("cache".into()))?;
        if let Some(display_name) = patch.display_name {
            cache.display_name = display_name;
        }
        // After applying the patch, we emit an `Updated` change so watchers can reconcile.
        let updated = cache.clone();
        self.cache_changes.write().await.record(|seq| CacheChange {
            seq,
            op: CacheChangeOp::Updated,
            key: key.clone(),
            cache: Some(updated.clone()),
        });
        metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
        Ok(updated)
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        let mut caches = self.caches.write().await;
        let removed = caches.remove(key);
        if removed.is_none() {
            return Err(StoreError::NotFound("cache".into()));
        }
        self.cache_changes.write().await.record(|seq| CacheChange {
            seq,
            op: CacheChangeOp::Deleted,
            key: key.clone(),
            cache: None,
        });
        metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
        metrics::gauge!("felix_caches_total").set(caches.len() as f64);
        Ok(())
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
        let items = self.caches.read().await.values().cloned().collect();
        let next_seq = self.cache_changes.read().await.next_seq;
        Ok(Snapshot { items, next_seq })
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        // We filter by `seq >= since` (inclusive) and apply a page limit.
        // If the caller's `since` is older than the retained window, it will receive a partial
        // history and should fall back to `*_snapshot()` to re-bootstrap.
        let guard = self.cache_changes.read().await;
        let items = guard
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: guard.next_seq,
        })
    }

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        Ok(self.tenants.read().await.contains_key(tenant_id))
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        Ok(self.namespaces.read().await.contains_key(key))
    }

    async fn health_check(&self) -> StoreResult<()> {
        // In-memory backend is always "healthy" if the process is running.
        // Durable backends should probe connectivity (e.g., a simple SELECT).
        Ok(())
    }

    /// Whether this backend provides persistence across restarts.
    ///
    /// In-memory store does not persist state and is therefore not durable.
    fn is_durable(&self) -> bool {
        false
    }

    /// Human-readable backend identifier used in logs/metrics/diagnostics.
    fn backend_name(&self) -> &'static str {
        "memory"
    }
}

#[async_trait]
impl AuthStore for InMemoryStore {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        Ok(self
            .idp_issuers
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()> {
        let mut issuers = self.idp_issuers.write().await;
        let entries = issuers.entry(tenant_id.to_string()).or_default();
        if let Some(existing) = entries.iter_mut().find(|item| item.issuer == issuer.issuer) {
            *existing = issuer;
        } else {
            entries.push(issuer);
        }
        Ok(())
    }

    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()> {
        let mut issuers = self.idp_issuers.write().await;
        if let Some(entries) = issuers.get_mut(tenant_id) {
            entries.retain(|item| item.issuer != issuer);
        }
        Ok(())
    }

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        Ok(self
            .rbac_policies
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        Ok(self
            .rbac_groupings
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        self.rbac_policies
            .write()
            .await
            .entry(tenant_id.to_string())
            .or_default()
            .push(policy);
        Ok(())
    }

    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()> {
        self.rbac_groupings
            .write()
            .await
            .entry(tenant_id.to_string())
            .or_default()
            .push(grouping);
        Ok(())
    }

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        self.tenant_signing_keys
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound("signing keys".into()))
    }

    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        self.tenant_signing_keys
            .write()
            .await
            .insert(tenant_id.to_string(), keys);
        crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
        Ok(())
    }

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool> {
        Ok(self
            .auth_bootstrapped
            .read()
            .await
            .get(tenant_id)
            .copied()
            .unwrap_or(false))
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()> {
        self.auth_bootstrapped
            .write()
            .await
            .insert(tenant_id.to_string(), bootstrapped);
        Ok(())
    }

    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        if let Some(keys) = self
            .tenant_signing_keys
            .read()
            .await
            .get(tenant_id)
            .cloned()
        {
            return Ok(keys);
        }
        let keys = crate::auth::keys::generate_signing_keys().map_err(StoreError::Unexpected)?;
        self.tenant_signing_keys
            .write()
            .await
            .insert(tenant_id.to_string(), keys.clone());
        crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
        Ok(keys)
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        {
            let mut existing = self.rbac_policies.write().await;
            let entry = existing.entry(tenant_id.to_string()).or_default();
            for policy in policies {
                if !entry.contains(&policy) {
                    entry.push(policy);
                }
            }
        }
        {
            let mut existing = self.rbac_groupings.write().await;
            let entry = existing.entry(tenant_id.to_string()).or_default();
            for grouping in groupings {
                if !entry.contains(&grouping) {
                    entry.push(grouping);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, StreamKind};

    fn store_with_limits(changes_limit: u64, retention: i64) -> InMemoryStore {
        InMemoryStore::new(StoreConfig {
            changes_limit,
            change_retention_max_rows: Some(retention),
        })
    }

    #[tokio::test]
    async fn tenant_conflict_and_change_window() {
        let store = store_with_limits(1, 1);
        store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            })
            .await
            .expect("tenant");

        let err = store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One Duplicate".to_string(),
            })
            .await
            .expect_err("conflict");
        assert!(matches!(err, StoreError::Conflict(_)));

        store
            .create_tenant(Tenant {
                tenant_id: "t2".to_string(),
                display_name: "Tenant Two".to_string(),
            })
            .await
            .expect("tenant");

        let changes = store.tenant_changes(0).await.expect("changes");
        assert_eq!(changes.items.len(), 1);
        assert_eq!(changes.items[0].tenant_id, "t2");
        assert_eq!(changes.next_seq, 2);
    }

    #[tokio::test]
    async fn namespace_stream_cache_errors_and_cascades() {
        let store = store_with_limits(10, 10);

        let err = store
            .create_namespace(Namespace {
                tenant_id: "missing".to_string(),
                namespace: "default".to_string(),
                display_name: "Default".to_string(),
            })
            .await
            .expect_err("missing tenant");
        assert!(matches!(err, StoreError::NotFound(_)));

        store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            })
            .await
            .expect("tenant");

        store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                display_name: "Default".to_string(),
            })
            .await
            .expect("namespace");

        let err = store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                display_name: "Default Duplicate".to_string(),
            })
            .await
            .expect_err("namespace conflict");
        assert!(matches!(err, StoreError::Conflict(_)));

        let err = store
            .get_stream(&StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            })
            .await
            .expect_err("stream missing");
        assert!(matches!(err, StoreError::NotFound(_)));

        store
            .create_stream(Stream {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
                kind: StreamKind::Stream,
                shards: 1,
                retention: RetentionPolicy {
                    max_age_seconds: Some(3600),
                    max_size_bytes: None,
                },
                consistency: ConsistencyLevel::Leader,
                delivery: DeliveryGuarantee::AtLeastOnce,
                durable: false,
            })
            .await
            .expect("stream");

        store
            .create_cache(Cache {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                display_name: "Primary".to_string(),
            })
            .await
            .expect("cache");

        store
            .delete_namespace(&NamespaceKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
            })
            .await
            .expect("delete namespace");

        let stream_err = store
            .get_stream(&StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            })
            .await
            .expect_err("stream deleted");
        assert!(matches!(stream_err, StoreError::NotFound(_)));

        let cache_err = store
            .get_cache(&CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
            })
            .await
            .expect_err("cache deleted");
        assert!(matches!(cache_err, StoreError::NotFound(_)));

        let changes = store.stream_changes(0).await.expect("stream changes");
        assert!(
            changes
                .items
                .iter()
                .any(|item| matches!(item.op, StreamChangeOp::Deleted))
        );
        let cache_changes = store.cache_changes(0).await.expect("cache changes");
        assert!(
            cache_changes
                .items
                .iter()
                .any(|item| matches!(item.op, CacheChangeOp::Deleted))
        );
    }

    #[tokio::test]
    async fn backend_health_and_identity() {
        let store = store_with_limits(10, 10);
        store.health_check().await.expect("health");
        assert!(!store.is_durable());
        assert_eq!(store.backend_name(), "memory");
    }
}
