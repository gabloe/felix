// In-process broker client wrapper.
use anyhow::Result;
use bytes::Bytes;
use felix_broker::{Broker, Subscription};
use std::sync::Arc;

/// Client wrapper that talks to an in-process broker.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_client::InProcessClient;
/// use felix_storage::EphemeralCache;
/// use std::sync::Arc;
///
/// let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
/// let client = InProcessClient::new(broker.clone());
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     broker
///         .register_tenant("t1")
///         .await
///         .expect("tenant");
///     broker
///         .register_namespace("t1", "default")
///         .await
///         .expect("namespace");
///     broker
///         .register_stream("t1", "default", "updates", Default::default())
///         .await
///         .expect("register");
///     let mut sub = client.subscribe("t1", "default", "updates").await.expect("subscribe");
///     client
///         .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"payload"));
/// });
/// ```
#[derive(Clone)]
pub struct InProcessClient {
    broker: Arc<Broker>,
}

impl InProcessClient {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        self.broker
            .publish(tenant_id, namespace, stream, payload)
            .await
            .map_err(Into::into)
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        self.broker
            .subscribe(tenant_id, namespace, stream, 0)
            .await
            .map_err(Into::into)
    }
}
