//! The Kafka listener: accepting connections, TLS, and answering
//! `felix_kafka`'s questions about this cluster.
//!
//! The protocol work is in the `felix-kafka` crate. This module is what only
//! the broker service knows: how tokens are verified, who leads each shard,
//! and where every broker's Kafka listener is.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use felix_authz::AuthzError;
use felix_broker::Broker;
use felix_kafka::{Cluster, Endpoint, KafkaService, Placement, Principal, Settings, ShardRef};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::client_endpoints::ClientEndpoints;
use crate::config::KafkaListenerConfig;
use crate::serving::auth::BrokerAuth;
use crate::shards::routing::{Dispatch, IngressRouter, Reason, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// A TLS handshake that has not finished by now is abandoned, so a client
/// that connects and sends nothing does not hold a connection slot.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The node id a broker outside a cluster answers as.
pub const STANDALONE_NODE_ID: &str = "felix";

/// [`Cluster`] for this broker.
pub struct BrokerCluster {
    auth: Arc<BrokerAuth>,
    ingress: Option<Arc<IngressRouter>>,
    endpoints: Option<Arc<ClientEndpoints>>,
    local: Endpoint,
}

impl BrokerCluster {
    /// `local_node_id` is the membership node id, or [`STANDALONE_NODE_ID`]
    /// outside a cluster; `advertise` is this broker's Kafka address.
    pub fn new(
        auth: Arc<BrokerAuth>,
        ingress: Option<Arc<IngressRouter>>,
        endpoints: Option<Arc<ClientEndpoints>>,
        local_node_id: &str,
        advertise: &str,
    ) -> Result<Self> {
        let local = Endpoint::parse(local_node_id, advertise)
            .with_context(|| format!("Kafka advertise address {advertise:?}"))?;
        Ok(Self {
            auth,
            ingress,
            endpoints,
            local,
        })
    }
}

#[async_trait]
impl Cluster for BrokerCluster {
    async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<Principal, String> {
        match self.auth.authenticate(tenant_id, token).await {
            Ok(context) => Ok(Principal::with_permissions(tenant_id, context.matcher)),
            // The token's own fault is safe to say. Anything else is the
            // control plane being unreachable, whose error text can carry its
            // URL, so it is logged here and not handed to the client.
            Err(err) => match err.downcast_ref::<AuthzError>() {
                Some(authz) => Err(format!("token rejected: {authz}")),
                None => {
                    tracing::warn!(error = %err, "kafka authentication could not verify a token");
                    Err("the broker could not verify the token right now".to_string())
                }
            },
        }
    }

    fn brokers(&self) -> Vec<Endpoint> {
        let mut brokers = vec![self.local.clone()];
        let Some(endpoints) = &self.endpoints else {
            return brokers;
        };
        for endpoint in endpoints.kafka_snapshot().iter() {
            if endpoint.node_id == self.local.node_id {
                continue;
            }
            match Endpoint::parse(endpoint.node_id.clone(), &endpoint.addr) {
                Some(parsed) => brokers.push(parsed),
                None => tracing::warn!(
                    node_id = %endpoint.node_id,
                    addr = %endpoint.addr,
                    "skipping a broker whose Kafka address does not parse",
                ),
            }
        }
        brokers
    }

    fn local_node_id(&self) -> String {
        self.local.node_id.clone()
    }

    fn placement(&self, shard: &ShardRef<'_>) -> Placement {
        let key = ShardKey {
            tenant_id: shard.tenant_id.to_string(),
            namespace: shard.namespace.to_string(),
            stream: shard.stream.to_string(),
            shard: shard.shard,
            kind: ShardKind::Stream,
        };
        let route = self
            .ingress
            .as_deref()
            .and_then(|ingress| ingress.route(&key));
        let replicas = route
            .as_ref()
            .map(|route| {
                route
                    .replicas
                    .iter()
                    .map(|replica| replica.node_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        match dispatch(self.ingress.as_deref(), &key) {
            Dispatch::Local { .. } => Placement::Local { replicas },
            Dispatch::Forward { node_id, .. } => Placement::Remote {
                leader: node_id,
                replicas,
            },
            // This broker will not forward across regions, but a Kafka client
            // connects to the leader itself.
            Dispatch::Unavailable(Reason::RegionNotRoutable { .. }) => match route {
                Some(route) => Placement::Remote {
                    leader: route.leader.node_id,
                    replicas,
                },
                None => Placement::Unavailable,
            },
            Dispatch::Unavailable(_) => Placement::Unavailable,
        }
    }
}

/// A bound Kafka listener, not yet accepting.
pub struct KafkaListener {
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    service: KafkaService,
    max_connections: usize,
}

impl KafkaListener {
    /// Bind the listener. `tls` is required when the config asks for TLS.
    pub async fn bind(
        config: &KafkaListenerConfig,
        tls: Option<Arc<rustls::ServerConfig>>,
        broker: Arc<Broker>,
        cluster: Arc<dyn Cluster>,
        cluster_id: String,
    ) -> Result<Self> {
        let listener = TcpListener::bind(config.listen)
            .await
            .with_context(|| format!("bind the Kafka listener on {}", config.listen))?;
        let tls = match (config.tls, tls) {
            (true, Some(server)) => Some(TlsAcceptor::from(server)),
            (true, None) => anyhow::bail!("FELIX_KAFKA_TLS is on but no certificate was given"),
            (false, _) => None,
        };
        let service = KafkaService::new(
            broker,
            cluster,
            Settings {
                anonymous_tenant: config.anonymous_tenant.clone(),
                default_namespace: config.default_namespace.clone(),
                cluster_id,
            },
        );
        Ok(Self {
            listener,
            tls,
            service,
            max_connections: config.max_connections,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept until `shutdown`, serving each connection on `connections` so a
    /// drain waits for it. A connection sees the same token and closes
    /// between requests, or ends a long-polling fetch early.
    pub async fn serve(self, shutdown: CancellationToken, connections: TaskTracker) {
        let slots = Arc::new(Semaphore::new(self.max_connections));
        loop {
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                accepted = self.listener.accept() => accepted,
            };
            let (socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tracing::warn!(error = %err, "kafka accept failed");
                    continue;
                }
            };
            let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                tracing::warn!(%peer, max = self.max_connections, "kafka connection limit reached; closing");
                metrics::counter!("felix_kafka_refused_total", "reason" => "connection_limit")
                    .increment(1);
                continue;
            };
            let _ = socket.set_nodelay(true);
            let tls = self.tls.clone();
            let service = self.service.clone();
            let shutdown = shutdown.clone();
            connections.spawn(async move {
                let _slot = slot;
                match tls {
                    None => service.serve_connection(socket, shutdown).await,
                    Some(tls) => {
                        match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, tls.accept(socket)).await
                        {
                            Ok(Ok(stream)) => service.serve_connection(stream, shutdown).await,
                            Ok(Err(err)) => {
                                tracing::debug!(%peer, error = %err, "kafka TLS handshake failed");
                            }
                            Err(_) => tracing::debug!(%peer, "kafka TLS handshake timed out"),
                        }
                    }
                }
            });
        }
    }
}

/// The TLS config the Kafka listener serves, from the broker's certificate.
pub fn tls_config(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<Arc<rustls::ServerConfig>> {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .context("kafka TLS protocol versions")?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .context("kafka TLS certificate")?;
    Ok(Arc::new(config))
}
