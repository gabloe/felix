//! A cluster member leading one shard of each kind through the real shard
//! lifecycle, so its write fence is open exactly as a running broker's is.
//!
//! [`Leader::fence_move`] closes it the way a move does, and leaves the
//! servable set the ingress router reads untouched. That is the window every
//! write-fence test is about: admission still says the shard is served here,
//! and only the fence can refuse the write.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::TempDir;

use crate::shards::lifecycle::{Action, ShardLifecycle};
use crate::shards::routing::{IngressRouter, routing_table_from};
use crate::shards::watch::ShardAssignment;
use crate::shards::{ShardKey, ShardKind};

pub(crate) const TENANT: &str = "t1";
pub(crate) const NAMESPACE: &str = "ns";
/// A durable stream, whose publishes take the split claim-then-complete path.
pub(crate) const DURABLE: &str = "orders";
/// An ephemeral stream, whose publishes complete inline.
pub(crate) const EPHEMERAL: &str = "events";
pub(crate) const CACHE: &str = "sessions";
pub(crate) const GENERATION: u64 = 1;
const NODE: &str = "broker-a";

pub(crate) struct Leader {
    pub(crate) broker: Arc<Broker>,
    pub(crate) ingress: Arc<IngressRouter>,
    pub(crate) router: Arc<ShardRouter>,
    pub(crate) lifecycle: ShardLifecycle,
    _dir: TempDir,
}

impl Leader {
    pub(crate) async fn start() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let root = dir.path();
        let broker = Broker::new(Box::new(
            LogCache::open(root.join("caches"), config.clone()).expect("cache"),
        ))
        .with_durable_storage(
            felix_broker::DurableStorage::open(root.join("streams"), config.clone())
                .expect("streams"),
        )
        .with_consumer_groups(
            Arc::new(
                felix_broker::ConsumerGroups::open(root.join("groups"), config.clone())
                    .expect("groups"),
            ),
            Arc::new(
                felix_broker::DeadLetters::open(root.join("dead-letters"), config.clone())
                    .expect("dead letters"),
            ),
            Duration::from_secs(30),
            3,
        )
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(root.join("counters"), config).expect("counters"),
        ));
        broker.register_tenant(TENANT).await.expect("tenant");
        broker
            .register_namespace(TENANT, NAMESPACE)
            .await
            .expect("namespace");
        for (stream, durable) in [(DURABLE, true), (EPHEMERAL, false)] {
            broker
                .register_stream(
                    TENANT,
                    NAMESPACE,
                    stream,
                    StreamMetadata {
                        durable,
                        ..Default::default()
                    },
                )
                .await
                .expect("stream");
        }
        broker
            .register_cache(TENANT, NAMESPACE, CACHE, CacheMetadata::default())
            .await
            .expect("cache");

        let keys = [stream_key(DURABLE), stream_key(EPHEMERAL), cache_key()];
        let assignments: HashMap<ShardKey, ShardAssignment> = keys
            .iter()
            .map(|key| (key.clone(), assignment(key, "active")))
            .collect();
        let nodes: HashMap<String, NodeRef> = [(
            NODE.to_string(),
            NodeRef {
                node_id: NODE.to_string(),
                advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 7001)),
                region: "us-west-2".to_string(),
                live: true,
            },
        )]
        .into_iter()
        .collect();
        let router = Arc::new(ShardRouter::new(
            NODE,
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        router.publish(routing_table_from(&assignments, &nodes), &nodes);

        let mut lifecycle = ShardLifecycle::new(NODE);
        for (key, assignment) in &assignments {
            lifecycle.observe(key, Some(assignment));
            lifecycle.opened(key, GENERATION);
        }
        let ingress = Arc::new(IngressRouter::new(
            Arc::clone(&router),
            Arc::clone(lifecycle.fence()),
        ));
        ingress.publish_servable(lifecycle.servable());

        Self {
            broker: Arc::new(broker),
            ingress,
            router,
            lifecycle,
            _dir: dir,
        }
    }

    /// Where `stream`'s shard 0 would write next: how many records it holds.
    pub(crate) async fn tail(&self, stream: &str) -> u64 {
        self.broker
            .cursor_tail(TENANT, NAMESPACE, stream, 0)
            .await
            .expect("tail")
            .next_seq()
    }

    /// What a move does to the old leader: the same generation, now draining.
    pub(crate) fn fence_move(&mut self, key: &ShardKey) {
        let action = self
            .lifecycle
            .observe(key, Some(&assignment(key, "draining")));
        assert!(
            matches!(action, Action::Release { .. }),
            "expected a release, got {action:?}"
        );
    }
}

pub(crate) fn stream_key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

pub(crate) fn cache_key() -> ShardKey {
    ShardKey {
        kind: ShardKind::Cache,
        ..stream_key(CACHE)
    }
}

fn assignment(key: &ShardKey, state: &str) -> ShardAssignment {
    ShardAssignment {
        key: key.clone(),
        leader: NODE.to_string(),
        replicas: Vec::new(),
        generation: GENERATION,
        state: state.to_string(),
    }
}
