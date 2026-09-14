//! In-memory implementation of the control-plane store.
//!
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
    NamespaceChangeOp, NamespaceKey, Node, NodeChange, NodeChangeOp, NodeLifecycle,
    NodePatchRequest, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp, ShardKey,
    ShardKind, Stream, StreamChange, StreamChangeOp, StreamKey, StreamPatchRequest, Tenant,
    TenantChange, TenantChangeOp,
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

/// A model rejection is the caller's fault, so it surfaces as a conflict
/// rather than an internal error.
fn invalid_node(err: crate::model::NodeValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

fn invalid_shard(err: crate::model::ShardValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

/// Sort key giving a stable stream-then-shard order.
fn shard_order(key: &ShardKey) -> (&str, &str, &str, u32) {
    (&key.tenant_id, &key.namespace, &key.stream, key.shard)
}

fn invalid_transition(from: NodeLifecycle, to: NodeLifecycle) -> StoreError {
    invalid_node(crate::model::NodeValidationError::UnsupportedTransition { from, to })
}

/// Node records and their change log under one lock.
///
/// Kept together so a snapshot cannot observe a record set and a `next_seq`
/// that disagree.
#[derive(Debug)]
struct NodeState {
    records: HashMap<String, Node>,
    changes: ChangeLog<NodeChange>,
}

impl NodeState {
    fn record(&mut self, op: NodeChangeOp, node_id: &str, node: Option<Node>) {
        self.changes.record(|seq| NodeChange {
            seq,
            op,
            node_id: node_id.to_string(),
            node,
        });
    }
}

/// Shard assignments and their change log under one lock.
#[derive(Debug)]
struct ShardState {
    records: HashMap<ShardKey, ShardAssignment>,
    changes: ChangeLog<ShardAssignmentChange>,
}

impl ShardState {
    fn record(
        &mut self,
        op: ShardAssignmentChangeOp,
        key: &ShardKey,
        assignment: Option<ShardAssignment>,
    ) {
        self.changes.record(|seq| ShardAssignmentChange {
            seq,
            op,
            key: key.clone(),
            assignment,
        });
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
    /// Broker membership keyed by `node_id`, with its change log.
    ///
    /// One lock over both, unlike the entities above: a snapshot has to read
    /// the records and the log position as one value, or a change committed
    /// between the two reads is missed by every consumer that resumes from it.
    nodes: Arc<RwLock<NodeState>>,
    /// Shard ownership and its change log, under one lock for the same reason
    /// as `nodes`: a snapshot must read records and log position as one value.
    shards: Arc<RwLock<ShardState>>,
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
    /// Serializes `bootstrap_tenant_auth`, standing in for the row lock the
    /// Postgres backend takes. Held across the whole operation; nothing else
    /// acquires it, so it cannot deadlock against the field locks above.
    bootstrap_serial: Arc<tokio::sync::Mutex<()>>,
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
            nodes: Arc::new(RwLock::new(NodeState {
                records: HashMap::new(),
                changes: ChangeLog::new(capacity),
            })),
            shards: Arc::new(RwLock::new(ShardState {
                records: HashMap::new(),
                changes: ChangeLog::new(capacity),
            })),
            tenant_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            namespace_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            stream_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            cache_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            idp_issuers: Arc::new(RwLock::new(HashMap::new())),
            tenant_signing_keys: Arc::new(RwLock::new(HashMap::new())),
            rbac_policies: Arc::new(RwLock::new(HashMap::new())),
            rbac_groupings: Arc::new(RwLock::new(HashMap::new())),
            auth_bootstrapped: Arc::new(RwLock::new(HashMap::new())),
            bootstrap_serial: Arc::new(tokio::sync::Mutex::new(())),
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
        let mut ns_keys: Vec<_> = namespaces
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        // Sorted before publishing: cascade events take sequence numbers in
        // this order, and a Raft replica applying the same command must
        // assign the same seq to the same event — HashMap order would not.
        ns_keys.sort_by(|a, b| (&a.tenant_id, &a.namespace).cmp(&(&b.tenant_id, &b.namespace)));
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
        let mut stream_keys: Vec<_> = streams
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        stream_keys.sort_by(|a, b| {
            (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
        });
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
        let mut cache_keys: Vec<_> = caches
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();
        cache_keys.sort_by(|a, b| {
            (&a.tenant_id, &a.namespace, &a.cache).cmp(&(&b.tenant_id, &b.namespace, &b.cache))
        });
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
        let mut stream_keys: Vec<_> = streams
            .keys()
            .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
            .cloned()
            .collect();
        // Sorted for the same reason as the tenant cascade: identical seq
        // assignment on every Raft replica.
        stream_keys.sort_by(|a, b| {
            (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
        });
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
        let mut cache_keys: Vec<_> = caches
            .keys()
            .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
            .cloned()
            .collect();
        cache_keys.sort_by(|a, b| {
            (&a.tenant_id, &a.namespace, &a.cache).cmp(&(&b.tenant_id, &b.namespace, &b.cache))
        });
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

    async fn register_node(&self, node: Node) -> StoreResult<Node> {
        node.validate().map_err(invalid_node)?;
        let mut state = self.nodes.write().await;

        if let Some((holder, _)) = state.records.iter().find(|(id, existing)| {
            existing.spec.advertise_addr == node.spec.advertise_addr && *id != &node.node_id
        }) {
            return Err(StoreError::Conflict(format!(
                "advertise_addr {} is already registered to node {holder}",
                node.spec.advertise_addr
            )));
        }

        let stored = match state.records.get(&node.node_id) {
            Some(existing) => {
                if !existing
                    .status
                    .lifecycle
                    .can_transition_to(node.status.lifecycle)
                {
                    return Err(invalid_transition(
                        existing.status.lifecycle,
                        node.status.lifecycle,
                    ));
                }
                Node {
                    status: crate::model::NodeStatus {
                        // The identity outlives the process, so its first
                        // registration is what dates it.
                        registered_at_millis: existing.status.registered_at_millis,
                        incarnation: existing.status.incarnation + 1,
                        ..node.status
                    },
                    ..node
                }
            }
            None => Node {
                status: crate::model::NodeStatus {
                    incarnation: 0,
                    ..node.status
                },
                ..node
            },
        };

        state.records.insert(stored.node_id.clone(), stored.clone());
        state.record(
            NodeChangeOp::Registered,
            &stored.node_id,
            Some(stored.clone()),
        );
        metrics::counter!("felix_node_changes_total", "op" => "registered").increment(1);
        Ok(stored)
    }

    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        self.nodes
            .read()
            .await
            .records
            .get(node_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound("node".into()))
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        let mut items: Vec<Node> = self.nodes.read().await.records.values().cloned().collect();
        items.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(items)
    }

    async fn patch_node(&self, node_id: &str, patch: NodePatchRequest) -> StoreResult<Node> {
        let mut state = self.nodes.write().await;
        let existing = state
            .records
            .get(node_id)
            .ok_or_else(|| StoreError::NotFound("node".into()))?;
        let patched = patch.apply(existing).map_err(invalid_node)?;

        if let Some((holder, _)) = state.records.iter().find(|(id, other)| {
            other.spec.advertise_addr == patched.spec.advertise_addr && *id != node_id
        }) {
            return Err(StoreError::Conflict(format!(
                "advertise_addr {} is already registered to node {holder}",
                patched.spec.advertise_addr
            )));
        }

        state.records.insert(node_id.to_string(), patched.clone());
        state.record(NodeChangeOp::Updated, node_id, Some(patched.clone()));
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        Ok(patched)
    }

    async fn delete_node(&self, node_id: &str) -> StoreResult<()> {
        // Refused rather than cascaded: deleting the assignment would erase the
        // only record of where that shard's data lives.
        let led = self
            .shards
            .read()
            .await
            .records
            .values()
            .filter(|assignment| assignment.leader == node_id)
            .count();
        if led > 0 {
            return Err(StoreError::Conflict(format!(
                "node {node_id} still leads {led} shard(s); reassign them first"
            )));
        }

        let mut state = self.nodes.write().await;
        if state.records.remove(node_id).is_none() {
            return Err(StoreError::NotFound("node".into()));
        }
        state.record(NodeChangeOp::Deregistered, node_id, None);
        metrics::counter!("felix_node_changes_total", "op" => "deregistered").increment(1);
        Ok(())
    }

    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        let mut state = self.nodes.write().await;
        let node = state
            .records
            .get_mut(node_id)
            .ok_or_else(|| StoreError::NotFound("node".into()))?;
        if incarnation < node.status.incarnation {
            return Err(StoreError::Conflict(format!(
                "heartbeat for incarnation {incarnation} of {node_id}, which is now at {}",
                node.status.incarnation
            )));
        }
        // Never moves backwards: heartbeats from two connections can arrive out
        // of order, and the newest observation is the one that matters.
        node.status.last_heartbeat_at_millis = node.status.last_heartbeat_at_millis.max(at_millis);
        Ok(node.clone())
    }

    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        let mut state = self.nodes.write().await;
        let mut stale: Vec<String> = state
            .records
            .values()
            .filter(|node| {
                matches!(
                    node.status.lifecycle,
                    NodeLifecycle::Live | NodeLifecycle::Draining
                ) && node.status.last_heartbeat_at_millis < expiry_before_millis
            })
            .map(|node| node.node_id.clone())
            .collect();
        // Sorted before publishing, not after returning: each expiry takes a
        // change-log seq here, and a Raft replica applying this command must
        // hand the same node the same seq — HashMap order would not.
        stale.sort();

        let mut expired = Vec::with_capacity(stale.len());
        for node_id in stale {
            let node = state.records.get_mut(&node_id).expect("just listed");
            crate::membership_metrics::record_transition(
                node.status.lifecycle,
                NodeLifecycle::Down,
            );
            node.status.lifecycle = NodeLifecycle::Down;
            let moved = node.clone();
            state.record(NodeChangeOp::Updated, &node_id, Some(moved.clone()));
            metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
            expired.push(moved);
        }
        Ok(expired)
    }

    async fn set_node_lifecycle(
        &self,
        node_id: &str,
        lifecycle: NodeLifecycle,
    ) -> StoreResult<Option<Node>> {
        let mut state = self.nodes.write().await;
        let node = state
            .records
            .get_mut(node_id)
            .ok_or_else(|| StoreError::NotFound("node".into()))?;
        if node.status.lifecycle == lifecycle {
            return Ok(None);
        }
        if !node.status.lifecycle.can_transition_to(lifecycle) {
            return Err(invalid_transition(node.status.lifecycle, lifecycle));
        }
        let previous = node.status.lifecycle;
        node.status.lifecycle = lifecycle;
        let updated = node.clone();
        crate::membership_metrics::record_transition(previous, lifecycle);
        state.record(NodeChangeOp::Updated, node_id, Some(updated.clone()));
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        Ok(Some(updated))
    }

    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>> {
        // One guard, so `items` and `next_seq` describe the same instant.
        let state = self.nodes.read().await;
        let mut items: Vec<Node> = state.records.values().cloned().collect();
        items.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Snapshot {
            items,
            next_seq: state.changes.next_seq,
        })
    }

    async fn node_changes(&self, since: u64) -> StoreResult<ChangeSet<NodeChange>> {
        let state = self.nodes.read().await;
        let items = state
            .changes
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: state.changes.next_seq,
        })
    }

    async fn put_shard_assignment(
        &self,
        assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        assignment.validate().map_err(invalid_shard)?;

        // The stream or cache bounds the shard number, and it has to exist at
        // all. Which of the two is decided by the key's kind, not by looking in
        // both: a cache and a stream may share a name, and falling back from one
        // to the other would let a shard of the wrong thing validate.
        let shards = match assignment.key.kind {
            ShardKind::Stream => self
                .streams
                .read()
                .await
                .get(&StreamKey {
                    tenant_id: assignment.key.tenant_id.clone(),
                    namespace: assignment.key.namespace.clone(),
                    stream: assignment.key.stream.clone(),
                })
                .map(|stream| stream.shards)
                .ok_or_else(|| StoreError::NotFound("stream".into()))?,
            ShardKind::Cache => self
                .caches
                .read()
                .await
                .get(&CacheKey {
                    tenant_id: assignment.key.tenant_id.clone(),
                    namespace: assignment.key.namespace.clone(),
                    cache: assignment.key.stream.clone(),
                })
                .map(|cache| cache.shards)
                .ok_or_else(|| StoreError::NotFound("cache".into()))?,
        };
        assignment.validate_within(shards).map_err(invalid_shard)?;

        // Checked here rather than by a foreign key: the node reference has none
        // deliberately, so that deleting a node cannot cascade an assignment away.
        {
            let nodes = self.nodes.read().await;
            for node_id in assignment.nodes() {
                if !nodes.records.contains_key(node_id) {
                    return Err(StoreError::NotFound(format!("node {node_id}")));
                }
            }
        }

        let mut state = self.shards.write().await;
        let (op, generation) = match state.records.get(&assignment.key) {
            Some(existing) => {
                if !existing.state.can_transition_to(assignment.state) {
                    return Err(invalid_shard(
                        crate::model::ShardValidationError::UnsupportedTransition {
                            from: existing.state,
                            to: assignment.state,
                        },
                    ));
                }
                (
                    ShardAssignmentChangeOp::Updated,
                    existing.generation.saturating_add(1),
                )
            }
            None => (ShardAssignmentChangeOp::Assigned, 0),
        };

        // Store-owned, so a caller cannot pin a generation and make its own
        // stale report look current.
        let stored = ShardAssignment {
            generation,
            ..assignment
        };
        state.records.insert(stored.key.clone(), stored.clone());
        state.record(op, &stored.key, Some(stored.clone()));
        metrics::counter!("felix_shard_assignment_changes_total", "op" => match op {
            ShardAssignmentChangeOp::Assigned => "assigned",
            ShardAssignmentChangeOp::Updated => "updated",
            ShardAssignmentChangeOp::Unassigned => "unassigned",
        })
        .increment(1);
        Ok(stored)
    }

    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment> {
        self.shards
            .read()
            .await
            .records
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        let mut items: Vec<ShardAssignment> =
            self.shards.read().await.records.values().cloned().collect();
        items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
        Ok(items)
    }

    async fn list_shard_assignments_for_node(
        &self,
        node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>> {
        let mut items: Vec<ShardAssignment> = self
            .shards
            .read()
            .await
            .records
            .values()
            .filter(|assignment| assignment.leader == node_id)
            .cloned()
            .collect();
        items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
        Ok(items)
    }

    async fn delete_shard_assignment(&self, key: &ShardKey) -> StoreResult<()> {
        let mut state = self.shards.write().await;
        if state.records.remove(key).is_none() {
            return Err(StoreError::NotFound("shard assignment".into()));
        }
        state.record(ShardAssignmentChangeOp::Unassigned, key, None);
        metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned")
            .increment(1);
        Ok(())
    }

    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>> {
        let state = self.shards.read().await;
        let mut items: Vec<ShardAssignment> = state.records.values().cloned().collect();
        items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
        Ok(Snapshot {
            items,
            next_seq: state.changes.next_seq,
        })
    }

    async fn shard_assignment_changes(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
        let state = self.shards.read().await;
        let items = state
            .changes
            .items
            .iter()
            .filter(|item| item.seq >= since)
            .take(self.limit())
            .cloned()
            .collect();
        Ok(ChangeSet {
            items,
            next_seq: state.changes.next_seq,
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

    async fn bootstrap_tenant_auth(
        &self,
        tenant_id: &str,
        seed: crate::store::TenantAuthSeed,
    ) -> StoreResult<TenantSigningKeys> {
        let _serial = self.bootstrap_serial.lock().await;

        if !self.tenants.read().await.contains_key(tenant_id) {
            return Err(StoreError::NotFound("tenant".into()));
        }
        if self
            .auth_bootstrapped
            .read()
            .await
            .get(tenant_id)
            .copied()
            .unwrap_or(false)
        {
            return Err(StoreError::Conflict("tenant already initialized".into()));
        }

        // Install the caller's keys only when none exist: the seed's keys are
        // the propose-time randomness, and existing keys always win so a
        // replayed or raced bootstrap cannot rotate a tenant's keys.
        let keys = match self.get_tenant_signing_keys(tenant_id).await {
            Ok(existing) => existing,
            Err(StoreError::NotFound(_)) => {
                self.set_tenant_signing_keys(tenant_id, seed.signing_keys.clone())
                    .await?;
                seed.signing_keys.clone()
            }
            Err(err) => return Err(err),
        };
        for issuer in seed.issuers {
            self.upsert_idp_issuer(tenant_id, issuer).await?;
        }
        self.seed_rbac_policies_and_groupings(tenant_id, seed.policies, seed.groupings)
            .await?;
        // Last, so a failure above leaves the tenant retryable rather than
        // half-initialized and claimed.
        self.auth_bootstrapped
            .write()
            .await
            .insert(tenant_id.to_string(), true);
        Ok(keys)
    }
}

/// One change stream, exported: position and retained window, but not
/// capacity — that is configuration, and every instance applies its own.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportedLog<T> {
    next_seq: u64,
    items: Vec<T>,
}

impl<T> ExportedLog<T> {
    fn from_log(log: &ChangeLog<T>) -> Self
    where
        T: Clone,
    {
        Self {
            next_seq: log.next_seq,
            items: log.items.iter().cloned().collect(),
        }
    }

    fn into_log(self, capacity: usize) -> ChangeLog<T> {
        ChangeLog {
            next_seq: self.next_seq,
            capacity,
            items: self.items.into(),
        }
    }
}

/// The whole store as one serializable value — the Raft state machine's
/// snapshot format.
///
/// Every map is exported as a **sorted** vector: two replicas that applied
/// the same command log must serialize byte-identical state, and HashMap
/// iteration order is the one thing in this store that would differ between
/// them. Change logs come with their sequence positions, so a restored
/// store keeps answering `changes(since)` exactly as the original —
/// including the resnapshot signals a stale `since` triggers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportedState {
    v: u16,
    tenants: Vec<(String, Tenant)>,
    namespaces: Vec<(NamespaceKey, Namespace)>,
    streams: Vec<(StreamKey, Stream)>,
    caches: Vec<(CacheKey, Cache)>,
    nodes: Vec<(String, Node)>,
    node_changes: ExportedLog<NodeChange>,
    shards: Vec<(ShardKey, ShardAssignment)>,
    shard_changes: ExportedLog<ShardAssignmentChange>,
    tenant_changes: ExportedLog<TenantChange>,
    namespace_changes: ExportedLog<NamespaceChange>,
    stream_changes: ExportedLog<StreamChange>,
    cache_changes: ExportedLog<CacheChange>,
    idp_issuers: Vec<(String, Vec<IdpIssuerConfig>)>,
    tenant_signing_keys: Vec<(String, TenantSigningKeys)>,
    rbac_policies: Vec<(String, Vec<PolicyRule>)>,
    rbac_groupings: Vec<(String, Vec<GroupingRule>)>,
    auth_bootstrapped: Vec<(String, bool)>,
}

impl ExportedState {
    /// What the operator is about to move, for the tool's own output —
    /// the cheap sanity check before and after a cutover.
    pub fn summary(&self) -> String {
        format!(
            "{} tenants, {} namespaces, {} streams, {} caches, {} nodes, {} shard assignments",
            self.tenants.len(),
            self.namespaces.len(),
            self.streams.len(),
            self.caches.len(),
            self.nodes.len(),
            self.shards.len(),
        )
    }
}

/// The exported snapshot format version. Bump on shape changes; an import
/// refuses a newer version rather than misreading it.
const EXPORTED_STATE_VERSION: u16 = 1;

fn sorted_by_string_key<V: Clone>(map: &HashMap<String, V>) -> Vec<(String, V)> {
    let mut entries: Vec<(String, V)> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

impl InMemoryStore {
    /// Serialize the entire store, deterministically.
    pub async fn export_state(&self) -> ExportedState {
        let mut namespaces: Vec<(NamespaceKey, Namespace)> = self
            .namespaces
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        namespaces.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace).cmp(&(&b.0.tenant_id, &b.0.namespace))
        });

        let mut streams: Vec<(StreamKey, Stream)> = self
            .streams
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        streams.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace, &a.0.stream).cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                &b.0.stream,
            ))
        });

        let mut caches: Vec<(CacheKey, Cache)> = self
            .caches
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        caches.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace, &a.0.cache).cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                &b.0.cache,
            ))
        });

        let (nodes, node_changes) = {
            let state = self.nodes.read().await;
            (
                sorted_by_string_key(&state.records),
                ExportedLog::from_log(&state.changes),
            )
        };

        let (shards, shard_changes) = {
            let state = self.shards.read().await;
            let mut records: Vec<(ShardKey, ShardAssignment)> = state
                .records
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            records.sort_by(|a, b| {
                (
                    &a.0.tenant_id,
                    &a.0.namespace,
                    a.0.kind,
                    &a.0.stream,
                    a.0.shard,
                )
                    .cmp(&(
                        &b.0.tenant_id,
                        &b.0.namespace,
                        b.0.kind,
                        &b.0.stream,
                        b.0.shard,
                    ))
            });
            (records, ExportedLog::from_log(&state.changes))
        };

        ExportedState {
            v: EXPORTED_STATE_VERSION,
            tenants: sorted_by_string_key(&*self.tenants.read().await),
            namespaces,
            streams,
            caches,
            nodes,
            node_changes,
            shards,
            shard_changes,
            tenant_changes: ExportedLog::from_log(&*self.tenant_changes.read().await),
            namespace_changes: ExportedLog::from_log(&*self.namespace_changes.read().await),
            stream_changes: ExportedLog::from_log(&*self.stream_changes.read().await),
            cache_changes: ExportedLog::from_log(&*self.cache_changes.read().await),
            idp_issuers: sorted_by_string_key(&*self.idp_issuers.read().await),
            tenant_signing_keys: sorted_by_string_key(&*self.tenant_signing_keys.read().await),
            rbac_policies: sorted_by_string_key(&*self.rbac_policies.read().await),
            rbac_groupings: sorted_by_string_key(&*self.rbac_groupings.read().await),
            auth_bootstrapped: sorted_by_string_key(&*self.auth_bootstrapped.read().await),
        }
    }

    /// Replace the entire store with a previously exported state.
    ///
    /// Sequence numbers and retained change windows come back exactly, so a
    /// consumer polling `changes(since)` across a restore sees the same
    /// answers — including the same "your checkpoint is too old, resnapshot"
    /// signals — as it would have from the original.
    pub async fn import_state(&self, state: ExportedState) -> StoreResult<()> {
        if state.v > EXPORTED_STATE_VERSION {
            return Err(StoreError::Unexpected(anyhow::anyhow!(
                "exported state version {} is newer than this build's {}",
                state.v,
                EXPORTED_STATE_VERSION
            )));
        }
        let capacity = self.config.change_window();

        *self.tenants.write().await = state.tenants.into_iter().collect();
        *self.namespaces.write().await = state.namespaces.into_iter().collect();
        *self.streams.write().await = state.streams.into_iter().collect();
        *self.caches.write().await = state.caches.into_iter().collect();
        *self.nodes.write().await = NodeState {
            records: state.nodes.into_iter().collect(),
            changes: state.node_changes.into_log(capacity),
        };
        *self.shards.write().await = ShardState {
            records: state.shards.into_iter().collect(),
            changes: state.shard_changes.into_log(capacity),
        };
        *self.tenant_changes.write().await = state.tenant_changes.into_log(capacity);
        *self.namespace_changes.write().await = state.namespace_changes.into_log(capacity);
        *self.stream_changes.write().await = state.stream_changes.into_log(capacity);
        *self.cache_changes.write().await = state.cache_changes.into_log(capacity);
        *self.idp_issuers.write().await = state.idp_issuers.into_iter().collect();
        *self.tenant_signing_keys.write().await = state.tenant_signing_keys.into_iter().collect();
        *self.rbac_policies.write().await = state.rbac_policies.into_iter().collect();
        *self.rbac_groupings.write().await = state.rbac_groupings.into_iter().collect();
        *self.auth_bootstrapped.write().await = state.auth_bootstrapped.into_iter().collect();
        // The derived-key cache may hold keys the imported state replaced —
        // an --overwrite restore in a live process would otherwise keep
        // verifying tokens against a world that no longer exists.
        for tenant_id in self.tenant_signing_keys.read().await.keys() {
            crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
        }
        Ok(())
    }

    /// Whether this store has never held anything — no records and no
    /// change-feed history. The guard an import checks before replacing
    /// everything: a store with any past has consumers whose checkpoints an
    /// accidental import would silently invalidate.
    pub async fn is_unused(&self) -> bool {
        self.tenants.read().await.is_empty()
            && self.namespaces.read().await.is_empty()
            && self.streams.read().await.is_empty()
            && self.caches.read().await.is_empty()
            && self.nodes.read().await.records.is_empty()
            && self.shards.read().await.records.is_empty()
            && self.tenant_changes.read().await.next_seq == 0
            && self.namespace_changes.read().await.next_seq == 0
            && self.stream_changes.read().await.next_seq == 0
            && self.cache_changes.read().await.next_seq == 0
            && self.nodes.read().await.changes.next_seq == 0
            && self.shards.read().await.changes.next_seq == 0
    }
}

/// Export any store — Postgres included — through the traits it already
/// implements, into the Raft state machine's snapshot format.
///
/// This is the migration's whole read side, and it deliberately reuses the
/// snapshot endpoints (`*_snapshot()` returns records **and** the feed
/// position as one value) so the exported change feeds carry the source's
/// sequence high-water marks with **empty retained windows**. A broker whose
/// checkpoint equals the head continues without noticing; one behind the
/// head gets the ordinary "your checkpoint predates the window, resnapshot"
/// signal — the at-most-one-resnapshot cost the migration accepts instead
/// of dragging Postgres's change rows along.
///
/// Consistency is the caller's job: run this only against a store whose
/// writes are frozen (the cutover ceremony's first step), because the reads
/// span many calls.
pub async fn export_state_from(
    store: &(dyn crate::store::ControlPlaneAuthStore + Send + Sync),
) -> StoreResult<ExportedState> {
    let tenant_snapshot = store.tenant_snapshot().await?;
    let namespace_snapshot = store.namespace_snapshot().await?;
    let stream_snapshot = store.stream_snapshot().await?;
    let cache_snapshot = store.cache_snapshot().await?;
    let node_snapshot = store.node_snapshot().await?;
    let shard_snapshot = store.shard_assignment_snapshot().await?;

    let mut tenants: Vec<(String, Tenant)> = tenant_snapshot
        .items
        .into_iter()
        .map(|tenant| (tenant.tenant_id.clone(), tenant))
        .collect();
    tenants.sort_by(|a, b| a.0.cmp(&b.0));

    let mut idp_issuers = Vec::new();
    let mut tenant_signing_keys = Vec::new();
    let mut rbac_policies = Vec::new();
    let mut rbac_groupings = Vec::new();
    let mut auth_bootstrapped = Vec::new();
    for (tenant_id, _) in &tenants {
        let issuers = store.list_idp_issuers(tenant_id).await?;
        if !issuers.is_empty() {
            idp_issuers.push((tenant_id.clone(), issuers));
        }
        match store.get_tenant_signing_keys(tenant_id).await {
            Ok(keys) => tenant_signing_keys.push((tenant_id.clone(), keys)),
            Err(StoreError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        let policies = store.list_rbac_policies(tenant_id).await?;
        if !policies.is_empty() {
            rbac_policies.push((tenant_id.clone(), policies));
        }
        let groupings = store.list_rbac_groupings(tenant_id).await?;
        if !groupings.is_empty() {
            rbac_groupings.push((tenant_id.clone(), groupings));
        }
        if store.tenant_auth_is_bootstrapped(tenant_id).await? {
            auth_bootstrapped.push((tenant_id.clone(), true));
        }
    }

    let mut namespaces: Vec<(NamespaceKey, Namespace)> = namespace_snapshot
        .items
        .into_iter()
        .map(|namespace| {
            (
                NamespaceKey {
                    tenant_id: namespace.tenant_id.clone(),
                    namespace: namespace.namespace.clone(),
                },
                namespace,
            )
        })
        .collect();
    namespaces
        .sort_by(|a, b| (&a.0.tenant_id, &a.0.namespace).cmp(&(&b.0.tenant_id, &b.0.namespace)));

    let mut streams: Vec<(StreamKey, Stream)> = stream_snapshot
        .items
        .into_iter()
        .map(|stream| {
            (
                StreamKey {
                    tenant_id: stream.tenant_id.clone(),
                    namespace: stream.namespace.clone(),
                    stream: stream.stream.clone(),
                },
                stream,
            )
        })
        .collect();
    streams.sort_by(|a, b| {
        (&a.0.tenant_id, &a.0.namespace, &a.0.stream).cmp(&(
            &b.0.tenant_id,
            &b.0.namespace,
            &b.0.stream,
        ))
    });

    let mut caches: Vec<(CacheKey, Cache)> = cache_snapshot
        .items
        .into_iter()
        .map(|cache| {
            (
                CacheKey {
                    tenant_id: cache.tenant_id.clone(),
                    namespace: cache.namespace.clone(),
                    cache: cache.cache.clone(),
                },
                cache,
            )
        })
        .collect();
    caches.sort_by(|a, b| {
        (&a.0.tenant_id, &a.0.namespace, &a.0.cache).cmp(&(
            &b.0.tenant_id,
            &b.0.namespace,
            &b.0.cache,
        ))
    });

    let mut nodes: Vec<(String, Node)> = node_snapshot
        .items
        .into_iter()
        .map(|node| (node.node_id.clone(), node))
        .collect();
    nodes.sort_by(|a, b| a.0.cmp(&b.0));

    let mut shards: Vec<(ShardKey, ShardAssignment)> = shard_snapshot
        .items
        .into_iter()
        .map(|assignment| (assignment.key.clone(), assignment))
        .collect();
    shards.sort_by(|a, b| {
        (
            &a.0.tenant_id,
            &a.0.namespace,
            a.0.kind,
            &a.0.stream,
            a.0.shard,
        )
            .cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                b.0.kind,
                &b.0.stream,
                b.0.shard,
            ))
    });

    fn empty_log_at<T>(next_seq: u64) -> ExportedLog<T> {
        ExportedLog {
            next_seq,
            items: Vec::new(),
        }
    }

    Ok(ExportedState {
        v: EXPORTED_STATE_VERSION,
        tenants,
        namespaces,
        streams,
        caches,
        nodes,
        node_changes: empty_log_at(node_snapshot.next_seq),
        shards,
        shard_changes: empty_log_at(shard_snapshot.next_seq),
        tenant_changes: empty_log_at(tenant_snapshot.next_seq),
        namespace_changes: empty_log_at(namespace_snapshot.next_seq),
        stream_changes: empty_log_at(stream_snapshot.next_seq),
        cache_changes: empty_log_at(cache_snapshot.next_seq),
        idp_issuers,
        tenant_signing_keys,
        rbac_policies,
        rbac_groupings,
        auth_bootstrapped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, StreamKind};

    /// The same suite Postgres runs, so parity is enforced rather than assumed.
    #[tokio::test]
    async fn satisfies_the_node_store_contract() {
        let store = std::sync::Arc::new(store_with_limits(100, 1000));
        crate::store::node_contract::run_node_contract(store.clone()).await;
        crate::store::node_contract::run_node_concurrency_contract(store).await;
    }

    /// The same suite Postgres runs.
    #[tokio::test]
    async fn satisfies_the_shard_store_contract() {
        let store = std::sync::Arc::new(store_with_limits(100, 1000));
        crate::store::shard_contract::run_shard_contract(store.clone()).await;
        crate::store::shard_contract::run_shard_concurrency_contract(store).await;
    }

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
                replication_factor: 1,
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
                shards: 1,
                replication_factor: 1,
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
