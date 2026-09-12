//! A local multi-node Felix cluster, for integration and failure tests.
//!
//! Starts one control plane and N broker processes, waits for each of them to
//! actually be usable, and tears everything down when the [`Cluster`] is
//! dropped.
//!
//! # What is real
//!
//! **Brokers are real processes.** Each has its own client-facing QUIC port,
//! internal peer port, metrics port, node identity, credential, and data
//! directory, and they reach each other over the internal transport exactly as
//! they would in a deployment. Stopping one is a real process exit.
//!
//! **The control plane is in process.** See [`controlplane`] for why, and for
//! what that does and does not exercise.
//!
//! # Waiting
//!
//! Nothing here sleeps for a fixed duration and hopes. Every wait polls a real
//! signal — a readiness endpoint, a node's placement eligibility in the catalog,
//! an assignment naming a leader — and fails with what it was still waiting for
//! rather than proceeding into a confusing failure later.
pub mod client;
pub mod controlplane;
pub mod ports;
pub mod scenarios;
pub mod session;
pub mod wait;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};

pub use controlplane::ControlPlane;

/// How long any single start-up wait may take before the harness gives up.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How many times a broker may be started before the cluster gives up on it.
///
/// More than one because port selection is inherently racy: a port is probed,
/// released, and only then handed to the child, and anything on the machine can
/// take it in between. Three attempts make that vanishingly unlikely without
/// masking a broker that is genuinely misconfigured — which fails identically
/// every time and still surfaces, with its log.
const MAX_SPAWN_ATTEMPTS: u32 = 3;

/// Who leads a shard, and at which generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub leader: String,
    pub generation: u64,
}

/// One broker process.
pub struct BrokerNode {
    pub node_id: String,
    /// Where clients publish and subscribe.
    pub client_addr: SocketAddr,
    /// Where peer brokers forward to. This is what the catalog advertises.
    pub internal_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub data_dir: PathBuf,
    /// `None` once the node has been stopped.
    process: Option<Child>,
}

impl BrokerNode {
    pub fn is_running(&self) -> bool {
        self.process.is_some()
    }

    /// The exit status if this broker has already stopped.
    ///
    /// A broker that refuses its configuration exits within milliseconds. Left
    /// unchecked, that becomes a readiness timeout tens of seconds later that
    /// says nothing about why.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.process.as_mut()?.try_wait().ok().flatten()
    }

    /// The tail of this broker's log, for a start-up failure to quote.
    ///
    /// An exit status alone cannot distinguish a lost port from a refused
    /// credential, and those need opposite responses.
    fn failure_reason(&self) -> String {
        let Ok(log) = std::fs::read_to_string(self.data_dir.join("broker.log")) else {
            return String::new();
        };
        let tail: Vec<&str> = log
            .lines()
            .filter(|line| !line.trim().is_empty())
            .rev()
            .take(5)
            .collect();
        if tail.is_empty() {
            return String::new();
        }
        let mut lines = tail;
        lines.reverse();
        format!(":\n  {}", lines.join("\n  "))
    }
}

/// A running cluster.
pub struct Cluster {
    pub control_plane: Option<ControlPlane>,
    pub nodes: Vec<BrokerNode>,
    pub tenant_id: String,
    pub namespace: String,
    /// Presented to brokers over QUIC.
    pub client_token: String,
    /// Presented to the control plane's HTTP API for reads.
    pub admin_token: String,
    /// Presented for membership writes, which reads alone cannot do.
    pub operator_token: String,
    /// May subscribe, may not publish.
    pub subscribe_only_token: String,
    http: reqwest::Client,
    /// Kept so a broker that loses the port race can be started again.
    binary: PathBuf,
    config: ClusterConfig,
    /// Held so the data directories outlive the brokers and are removed with
    /// the cluster.
    _root: tempfile::TempDir,
}

/// A stream to create when the cluster starts.
///
/// Replication and consistency are separate from the shard count because a
/// failure test needs them apart: a shard with no replicas has nothing to fail
/// over to, and a `Leader` stream acknowledges before a follower has the record,
/// so neither proves anything about the other.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    pub name: String,
    pub shards: u32,
    /// Copies of each shard, leader included. `1` is leader-only.
    pub replication_factor: u32,
    /// `"Leader"` or `"Quorum"`, as the control plane spells them.
    pub consistency: String,
}

impl StreamSpec {
    /// An unreplicated, leader-acknowledged stream — the default a cluster gets
    /// when a test says nothing about replication.
    pub fn new(name: impl Into<String>, shards: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor: 1,
            consistency: "Leader".to_string(),
        }
    }

    /// Replicated across `replication_factor` brokers, acknowledged by a
    /// majority. What a failover test needs: records that are on more than one
    /// broker by the time the publish returns.
    pub fn quorum(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Quorum".to_string(),
        }
    }

    /// Replicated, but acknowledged by the leader alone.
    pub fn replicated(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Leader".to_string(),
        }
    }
}

/// How to build a cluster.
#[derive(Clone)]
pub struct ClusterConfig {
    pub nodes: usize,
    pub tenant_id: String,
    pub namespace: String,
    /// Streams to create.
    pub streams: Vec<StreamSpec>,
    /// Inherit the parent's stdout/stderr rather than discarding it. Useful when
    /// running the harness by hand; noisy inside a test.
    pub inherit_output: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            streams: vec![StreamSpec::new("orders", 1)],
            inherit_output: false,
        }
    }
}

impl Cluster {
    /// Start a cluster and wait until every part of it is usable.
    ///
    /// "Usable" means each broker answers `/ready`, the control plane considers
    /// each one placeable, every stream's shards have a leader, and the leader
    /// has opened them. A publish issued the moment this returns is expected to
    /// succeed.
    pub async fn start(config: ClusterConfig) -> Result<Self> {
        if config.nodes == 0 {
            bail!("a cluster needs at least one broker");
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .no_proxy()
            .build()
            .context("build harness HTTP client")?;

        let control_plane = ControlPlane::start(&config.tenant_id).await?;
        let client_token = control_plane.client_token(&config.tenant_id)?;
        let admin_token = control_plane.admin_token(&config.tenant_id)?;
        let operator_token = control_plane.operator_token(&config.tenant_id)?;
        let subscribe_only_token = control_plane.subscribe_only_token(&config.tenant_id)?;

        // Metadata first: a broker syncs streams at startup, and one that starts
        // before its streams exist has to wait for the next sync to become
        // useful. Creating them up front means readiness means what it says.
        seed_metadata(&http, &control_plane, &config, &admin_token).await?;

        let root = tempfile::tempdir().context("create cluster data root")?;
        let binary = broker_binary()?;
        let mut nodes = Vec::with_capacity(config.nodes);
        for index in 0..config.nodes {
            nodes.push(
                spawn_broker(&binary, &control_plane, &config, root.path(), index)
                    .with_context(|| format!("start broker {index}"))?,
            );
        }

        let mut cluster = Self {
            control_plane: Some(control_plane),
            nodes,
            tenant_id: config.tenant_id.clone(),
            namespace: config.namespace.clone(),
            client_token,
            admin_token,
            operator_token,
            subscribe_only_token,
            http,
            binary: binary.clone(),
            config: config.clone(),
            _root: root,
        };

        // Two phases: the first needs the child handles, to tell "not ready yet"
        // from "already exited"; the rest only observes the cluster.
        cluster.await_brokers_ready().await?;
        cluster.await_cluster_ready(&config).await?;
        Ok(cluster)
    }

    /// Step placement once.
    ///
    /// Exposed because the harness's control plane does not run the reconciler
    /// on a timer: a test that slept for one would be timing-dependent in
    /// exactly the way the acceptance criteria rule out. A failure test has to
    /// drive placement while it waits, or nothing re-plans after the fault.
    pub async fn place_shards(&self) -> ::controlplane::placement::ReconcileOutcome {
        self.control_plane().place_shards().await
    }

    fn control_plane(&self) -> &ControlPlane {
        self.control_plane
            .as_ref()
            .expect("control plane is only taken during shutdown")
    }

    /// The control plane's address. Panics once it has been stopped, which is
    /// deliberate: a caller reading this after `stop_control_plane` is asking
    /// for a service that is gone.
    pub fn control_plane_url(&self) -> &str {
        &self.control_plane().base_url
    }

    /// Wait for every broker process to report ready.
    async fn await_brokers_ready(&mut self) -> Result<()> {
        // Written as a loop rather than through `wait::until` so a broker that
        // has already exited can be reported as such, with its status, instead
        // of timing out.
        for index in 0..self.nodes.len() {
            let deadline = std::time::Instant::now() + READY_TIMEOUT;
            let mut attempts = 1;
            loop {
                // Re-read each pass: a respawn gives this broker new ports, and
                // a URL captured before the loop would keep polling the address
                // the dead process had.
                let url = format!("http://{}/ready", self.nodes[index].metrics_addr);
                if let Some(status) = self.nodes[index].exited() {
                    // Ports are handed to the child after being probed and
                    // released, so another process can take one in between.
                    // Losing that race is not a cluster failure, it is a retry
                    // with different ports.
                    if attempts < MAX_SPAWN_ATTEMPTS {
                        attempts += 1;
                        self.respawn(index)?;
                        continue;
                    }
                    let node_id = &self.nodes[index].node_id;
                    let reason = self.nodes[index].failure_reason();
                    bail!(
                        "{node_id} exited before becoming ready after \
                         {MAX_SPAWN_ATTEMPTS} attempts ({status}){reason}"
                    );
                }
                let ok = matches!(
                    self.http.get(&url).send().await,
                    Ok(response) if response.status().is_success()
                );
                if ok {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    let node_id = &self.nodes[index].node_id;
                    bail!("timed out after {READY_TIMEOUT:?} waiting for {node_id} to be ready");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    /// Wait until the cluster as a whole can serve: every broker registered and
    /// placeable, every shard led, and a publish accepted.
    async fn await_cluster_ready(&self, config: &ClusterConfig) -> Result<()> {
        let expected: Vec<String> = self.nodes.iter().map(|n| n.node_id.clone()).collect();
        wait::until(
            READY_TIMEOUT,
            "every broker registered and placeable",
            || {
                let this = self;
                let expected = expected.clone();
                async move {
                    match this.placeable_nodes().await {
                        Ok(live) => expected.iter().all(|id| live.contains(id)),
                        Err(_) => false,
                    }
                }
            },
        )
        .await?;

        // Placement is stepped rather than waited on, then the result is
        // verified: a leader for every shard of every stream.
        let total_shards: usize = config.streams.iter().map(|spec| spec.shards as usize).sum();
        wait::until(
            READY_TIMEOUT,
            "every shard assigned a leader",
            || async move {
                self.control_plane().place_shards().await;
                match self.shard_owners().await {
                    Ok(owners) => owners.len() >= total_shards,
                    Err(_) => false,
                }
            },
        )
        .await?;

        // An assignment is not the same as a broker that has opened the shard.
        // That gap is exactly where a publish is refused as `NotReady`, and no
        // control-plane state distinguishes the two — so the only honest check
        // is a publish that succeeds.
        for spec in &config.streams {
            let stream = &spec.name;
            let stream = stream.clone();
            wait::until(
                READY_TIMEOUT,
                &format!("a publish to {stream} to be accepted"),
                || {
                    let stream = stream.clone();
                    async move { self.probe_publish(&stream).await.is_ok() }
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Publish one record through an arbitrary broker.
    ///
    /// Deliberately not through the owner: routing a publish from a non-owner is
    /// the thing that has to work, so the readiness check exercises it rather
    /// than the easy path.
    async fn probe_publish(&self, stream: &str) -> Result<()> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.is_running())
            .ok_or_else(|| anyhow!("no running broker"))?;
        self.publish_via(&node.node_id, stream, b"harness-probe".to_vec())
            .await
    }

    /// Publish one record through a named broker, whether or not it owns the
    /// shard.
    pub async fn publish_via(&self, node_id: &str, stream: &str, payload: Vec<u8>) -> Result<()> {
        self.publish_via_token(node_id, stream, payload, &self.client_token)
            .await
    }

    /// Publish through a named broker with a chosen credential.
    pub async fn publish_via_token(
        &self,
        node_id: &str,
        stream: &str,
        payload: Vec<u8>,
        token: &str,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, token).await?;
        let publisher = client.publisher().await.context("open publisher")?;
        // Acked, because an unacked publish would make every failure look like
        // success and the wait above would pass instantly.
        publisher
            .publish(
                &self.tenant_id,
                &self.namespace,
                stream,
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} via {node_id}"))
    }

    /// Subscribe on a named broker and return the client and subscription.
    ///
    /// Both are returned because dropping the client closes the connection the
    /// subscription is delivered on.
    pub async fn subscribe_on(
        &self,
        node_id: &str,
        stream: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        self.subscribe_on_with_token(node_id, stream, &self.client_token)
            .await
    }

    /// Subscribe on a named broker with a chosen credential.
    pub async fn subscribe_on_with_token(
        &self,
        node_id: &str,
        stream: &str,
        token: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, token).await?;
        let subscription = client
            .subscribe(&self.tenant_id, &self.namespace, stream)
            .await
            .with_context(|| format!("subscribe to {stream} on {node_id}"))?;
        Ok((client, subscription))
    }

    /// Subscribe on a named broker, replaying from the start of the stream.
    ///
    /// A failure test needs this: the records it cares about were published
    /// before the fault, and a live subscription would not replay them. Asking
    /// for history is also the stronger check — it reads what the broker has on
    /// disk rather than what it happens to fan out next.
    pub async fn replay_on(
        &self,
        node_id: &str,
        stream: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        let subscription = client
            .subscribe_from(
                &self.tenant_id,
                &self.namespace,
                stream,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .with_context(|| format!("replay {stream} on {node_id}"))?;
        Ok((client, subscription))
    }

    /// Every broker's client address, for a seed list.
    pub fn broker_addrs(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|node| node.client_addr).collect()
    }

    /// Publish through whichever broker in the cluster answers, the way an
    /// application with a seed list would.
    pub async fn publish_via_any(&self, stream: &str, payload: Vec<u8>) -> Result<()> {
        let client =
            client::connect_any(&self.broker_addrs(), &self.tenant_id, &self.client_token).await?;
        let publisher = client.publisher().await.context("open publisher")?;
        publisher
            .publish(
                &self.tenant_id,
                &self.namespace,
                stream,
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} through a seed endpoint"))
    }

    /// Node ids the control plane currently considers placeable.
    pub async fn placeable_nodes(&self) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Response {
            items: Vec<Item>,
        }
        #[derive(serde::Deserialize)]
        struct Item {
            node: Node,
            placement: Placement,
        }
        #[derive(serde::Deserialize)]
        struct Node {
            node_id: String,
        }
        #[derive(serde::Deserialize)]
        struct Placement {
            eligible: bool,
        }

        let response: Response = self
            .get(&format!("{}/v1/nodes", self.control_plane_url()))
            .await?;
        Ok(response
            .items
            .into_iter()
            .filter(|item| item.placement.eligible)
            .map(|item| item.node.node_id)
            .collect())
    }

    /// Full assignment detail: leader and generation, keyed by
    /// `tenant/namespace/stream/shard`.
    ///
    /// The generation is what a cross-broker failure needs in its diagnosis: a
    /// publish refused as stale and one refused because the shard moved look the
    /// same without it.
    pub async fn shard_assignments(&self) -> Result<HashMap<String, Assignment>> {
        #[derive(serde::Deserialize)]
        struct Response {
            items: Vec<Row>,
        }
        // `ShardAssignment` flattens its key, so the JSON is flat too.
        #[derive(serde::Deserialize)]
        struct Row {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            leader: String,
            generation: u64,
        }

        let response: Response = self
            .get(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(response
            .items
            .into_iter()
            .map(|row| {
                (
                    format!(
                        "{}/{}/{}/{}",
                        row.tenant_id, row.namespace, row.stream, row.shard
                    ),
                    Assignment {
                        leader: row.leader,
                        generation: row.generation,
                    },
                )
            })
            .collect())
    }

    /// Move a stream's shard off its current owner.
    ///
    /// Drains the owner so placement will not choose it, re-runs placement, and
    /// waits for the assignment to name someone else at a higher generation.
    /// Draining rather than stopping the broker on purpose: the node stays up
    /// and reachable, so what changes is ownership alone.
    ///
    /// Returns the new owner.
    pub async fn move_shard(&self, stream: &str) -> Result<String> {
        let key = format!("{}/{}/{}/0", self.tenant_id, self.namespace, stream);
        let before = self
            .shard_assignments()
            .await?
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("no assignment for {key}"))?;

        let url = format!(
            "{}/v1/nodes/{}/drain",
            self.control_plane_url(),
            before.leader
        );
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.operator_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .with_context(|| format!("drain {}", before.leader))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("drain {}: {status}: {body}", before.leader);
        }

        let key_for_wait = key.clone();
        let before_for_wait = before.clone();
        wait::until(
            READY_TIMEOUT,
            &format!("{key} to move off {}", before.leader),
            || {
                let key = key_for_wait.clone();
                let before = before_for_wait.clone();
                async move {
                    self.control_plane().place_shards().await;
                    match self.shard_assignments().await {
                        Ok(current) => current.get(&key).is_some_and(|now| {
                            now.leader != before.leader && now.generation > before.generation
                        }),
                        Err(_) => false,
                    }
                }
            },
        )
        .await?;

        let after = self
            .shard_assignments()
            .await?
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("no assignment for {key} after the move"))?;
        Ok(after.leader)
    }

    /// Which broker leads each shard, keyed by `tenant/namespace/stream/shard`.
    pub async fn shard_owners(&self) -> Result<HashMap<String, String>> {
        #[derive(serde::Deserialize)]
        struct Response {
            items: Vec<Assignment>,
        }
        // `ShardAssignment` flattens its key, so the JSON is flat too.
        #[derive(serde::Deserialize)]
        struct Assignment {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            leader: String,
        }

        let response: Response = self
            .get(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(response
            .items
            .into_iter()
            .map(|a| {
                (
                    format!("{}/{}/{}/{}", a.tenant_id, a.namespace, a.stream, a.shard),
                    a.leader,
                )
            })
            .collect())
    }

    /// Everything another process needs to talk to this cluster.
    pub fn session(&self) -> session::Session {
        session::Session {
            control_plane: self.control_plane_url().to_string(),
            tenant_id: self.tenant_id.clone(),
            namespace: self.namespace.clone(),
            client_token: self.client_token.clone(),
            admin_token: self.admin_token.clone(),
            nodes: self
                .nodes
                .iter()
                .map(|node| session::SessionNode {
                    node_id: node.node_id.clone(),
                    client_addr: node.client_addr,
                    metrics_addr: node.metrics_addr,
                })
                .collect(),
        }
    }

    /// The node that owns `stream`'s shard 0.
    /// Who owns each shard of `stream`, by shard index.
    ///
    /// The point of a multi-shard test: a stream placed across brokers has
    /// several owners, and which shard a key lands on decides which of them a
    /// publish reaches.
    pub async fn shard_owners_for(
        &self,
        stream: &str,
    ) -> Result<std::collections::HashMap<u32, String>> {
        let owners = self.shard_owners().await?;
        let prefix = format!("{}/{}/{}/", self.tenant_id, self.namespace, stream);
        Ok(owners
            .into_iter()
            .filter_map(|(key, node)| {
                let shard = key.strip_prefix(&prefix)?.parse().ok()?;
                Some((shard, node))
            })
            .collect())
    }

    /// Publish with a routing key, through a named broker.
    pub async fn publish_keyed_via(
        &self,
        node_id: &str,
        stream: &str,
        key: &[u8],
        payload: Vec<u8>,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        let publisher = client.publisher().await.context("open publisher")?;
        publisher
            .publish_keyed(
                &self.tenant_id,
                &self.namespace,
                stream,
                bytes::Bytes::copy_from_slice(key),
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} key {key:?} via {node_id}"))
    }

    /// Replay one shard of a stream from the broker that owns it.
    pub async fn replay_shard(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        let subscription = client
            .subscribe_shard(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .with_context(|| format!("replay {stream} shard {shard} on {node_id}"))?;
        Ok((client, subscription))
    }

    pub async fn owner(&self, stream: &str) -> Result<String> {
        let owners = self.shard_owners().await?;
        let key = format!("{}/{}/{}/0", self.tenant_id, self.namespace, stream);
        owners
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("no owner for {key}"))
    }

    /// The node that owns `stream`'s shard 0, and one that does not.
    ///
    /// This pair is the whole point of a multi-node harness: it is what lets a
    /// test publish somewhere the data does not belong.
    pub async fn owner_and_non_owner(&self, stream: &str) -> Result<(String, String)> {
        let owner = self.owner(stream).await?;
        let other = self
            .nodes
            .iter()
            .find(|node| node.node_id != owner && node.is_running())
            .ok_or_else(|| anyhow!("no running broker other than the owner {owner}"))?
            .node_id
            .clone();
        Ok((owner, other))
    }

    /// Read one counter or gauge from a broker's `/metrics`.
    ///
    /// `None` when the metric has never been recorded, which for a counter is
    /// the difference between "zero so far" and "this code path never ran" —
    /// the distinction a cross-broker test is usually making.
    pub async fn metric(&self, node_id: &str, name: &str) -> Result<Option<f64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let body = self
            .http
            .get(format!("http://{}/metrics", node.metrics_addr))
            .send()
            .await
            .context("scrape metrics")?
            .text()
            .await
            .context("read metrics body")?;

        let mut total = None;
        for line in body.lines() {
            if line.starts_with('#') {
                continue;
            }
            // `name`, `name{labels}`, then the value. Summed across label sets,
            // because a caller asking "did this happen" does not care which
            // label it happened under.
            let Some(rest) = line.strip_prefix(name) else {
                continue;
            };
            if !(rest.starts_with(' ') || rest.starts_with('{')) {
                continue;
            }
            if let Some(value) = rest.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
                *total.get_or_insert(0.0) += value;
            }
        }
        Ok(total)
    }

    pub fn node(&self, node_id: &str) -> Option<&BrokerNode> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }

    /// Start one broker again, with fresh ports.
    ///
    /// Keeps the node id and data directory: this is the same broker having
    /// another go, not a different one, and a durable log it already wrote must
    /// still be there.
    fn respawn(&mut self, index: usize) -> Result<()> {
        let node_id = self.nodes[index].node_id.clone();
        tracing::warn!(%node_id, "broker exited during start-up; starting it again");
        let control_plane = self
            .control_plane
            .as_ref()
            .ok_or_else(|| anyhow!("control plane is gone"))?;
        let replacement = spawn_broker(
            &self.binary,
            control_plane,
            &self.config,
            self._root.path(),
            index,
        )?;
        self.nodes[index] = replacement;
        Ok(())
    }

    /// Stop the control plane, leaving the brokers running.
    ///
    /// A failure primitive rather than a teardown: brokers keep serving on the
    /// authority they already hold, and lose it when their leases lapse. That is
    /// the partition this cluster can produce without touching the network.
    pub async fn stop_control_plane(&mut self) {
        if let Some(control_plane) = self.control_plane.take() {
            control_plane.shutdown().await;
        }
    }

    /// Whether the control plane is still running. `false` once
    /// [`Self::stop_control_plane`] has been called, after which the assignment
    /// and node endpoints are unreachable.
    pub fn control_plane_running(&self) -> bool {
        self.control_plane.is_some()
    }

    /// Stop one broker, and wait until the control plane agrees it is gone.
    ///
    /// The wait is the useful half: a test that kills a broker and immediately
    /// asserts is racing the expiry sweep.
    pub async fn stop_node(&mut self, node_id: &str) -> Result<()> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let Some(mut process) = node.process.take() else {
            return Ok(());
        };
        let _ = process.kill();
        let _ = process.wait();

        let node_id = node_id.to_string();
        wait::until(
            READY_TIMEOUT,
            &format!("control plane to notice {node_id} is gone"),
            || {
                let this = &*self;
                let node_id = node_id.clone();
                async move {
                    match this.placeable_nodes().await {
                        Ok(live) => !live.contains(&node_id),
                        Err(_) => false,
                    }
                }
            },
        )
        .await
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("GET {url}: {status}: {body}");
        }
        response.json().await.context("decode response")
    }

    /// Cut `node_id` off from every other broker, both ways.
    ///
    /// Distinct from pausing: the broker keeps running and keeps heartbeating,
    /// so the control plane still believes it is healthy. That is the state no
    /// other fault produces, and the one a replication design is most likely to
    /// get wrong.
    ///
    /// Written on both sides, because a partition is symmetric and a broker
    /// still reachable inbound would not be isolated.
    pub fn partition_node(&self, node_id: &str) -> Result<()> {
        let others: Vec<String> = self
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .filter(|id| id != node_id)
            .collect();
        for node in &self.nodes {
            let listed = if node.node_id == node_id {
                others.clone()
            } else {
                vec![node_id.to_string()]
            };
            std::fs::write(partition_file(&node.data_dir), listed.join("\n"))
                .with_context(|| format!("write the partition file for {}", node.node_id))?;
        }
        await_partition_reread();
        Ok(())
    }

    /// Reconnect everything.
    pub fn heal_partitions(&self) -> Result<()> {
        for node in &self.nodes {
            let path = partition_file(&node.data_dir);
            if path.exists() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("heal the partition for {}", node.node_id))?;
            }
        }
        await_partition_reread();
        Ok(())
    }

    /// Suspend a broker without stopping it.
    ///
    /// The process stays alive and keeps every lease and connection it holds,
    /// and answers nothing. That is the fault a kill cannot produce, and it is
    /// the one the commit-boundary lease check exists for: a broker suspended
    /// past its lease expiry must refuse the write it was in the middle of when
    /// it wakes, rather than committing to a shard someone else now leads.
    ///
    /// Unix only. Elsewhere there is no equivalent that leaves the process
    /// holding its state, and a test that quietly did something weaker would be
    /// worse than one that does not run.
    #[cfg(unix)]
    pub fn pause_node(&self, node_id: &str) -> Result<()> {
        self.signal(node_id, libc::SIGSTOP, "pause")?;
        // `kill` returns when the signal is queued, not when the process has
        // stopped. A test that probes straight afterwards can still be answered
        // by a broker that has not been descheduled yet -- which reads as "the
        // fault did not happen" and is this harness's fault, not the broker's.
        // Waiting for the kernel to say it is stopped is what makes `pause`
        // mean paused by the time it returns.
        self.await_stopped(node_id)
    }

    /// Whether the kernel currently reports this broker as stopped.
    ///
    /// Exposed so a test can check the fault is in effect rather than infer it
    /// from the broker failing to answer, which is the thing under test.
    #[cfg(unix)]
    pub fn is_paused(&self, node_id: &str) -> bool {
        self.node(node_id)
            .and_then(|node| node.process.as_ref())
            .is_some_and(|process| process_is_stopped(process.id()))
    }

    /// Wait until the kernel reports the broker as stopped.
    ///
    /// Polled rather than waited on: `waitpid` with `WUNTRACED` would reap the
    /// stop notification that `Child` relies on, and this harness needs the
    /// process handle to stay usable for the resume.
    #[cfg(unix)]
    fn await_stopped(&self, node_id: &str) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let pid = node
            .process
            .as_ref()
            .ok_or_else(|| anyhow!("cannot pause {node_id}: it is not running"))?
            .id();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if process_is_stopped(pid) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(anyhow!(
            "{node_id} did not stop within 5s of being sent SIGSTOP"
        ))
    }

    /// Let a suspended broker run again.
    #[cfg(unix)]
    pub fn resume_node(&self, node_id: &str) -> Result<()> {
        self.signal(node_id, libc::SIGCONT, "resume")
    }

    #[cfg(unix)]
    fn signal(&self, node_id: &str, signal: libc::c_int, what: &str) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let process = node
            .process
            .as_ref()
            .ok_or_else(|| anyhow!("cannot {what} {node_id}: it is not running"))?;
        let pid = process.id() as libc::pid_t;
        // Safety: `pid` came from a child this harness spawned and has not
        // reaped, so it names that child or nothing. `kill` reports an error
        // rather than misbehaving if the process is already gone.
        let sent = unsafe { libc::kill(pid, signal) };
        if sent != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("{what} {node_id} (pid {pid})"));
        }
        Ok(())
    }

    /// Kill a broker and return immediately.
    ///
    /// Unlike [`Cluster::stop_node`], this does not wait for the control plane
    /// to notice. A test measuring how long failover takes has to start its
    /// clock at the kill, not after the cluster has already reacted to it.
    pub fn kill_node(&mut self, node_id: &str) -> Result<()> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let Some(mut process) = node.process.take() else {
            return Ok(());
        };
        let _ = process.kill();
        // Reaped so the process does not linger as a zombie for the rest of the
        // test; the kill itself has already happened, so this does not wait on
        // anything the caller is timing.
        let _ = process.wait();
        Ok(())
    }

    /// Stop everything. Called by `Drop` too, so an aborted test leaves nothing
    /// behind — this exists for the case where a caller wants to wait for it.
    pub async fn shutdown(mut self) {
        self.kill_brokers();
        if let Some(control_plane) = self.control_plane.take() {
            control_plane.shutdown().await;
        }
    }

    fn kill_brokers(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

/// Whether the kernel reports `pid` as stopped.
///
/// Read through `ps` rather than `/proc`, which does not exist on macOS, and
/// the harness runs on developer machines as well as on Linux CI. The state
/// letter is `T` for a job-control stop on both; anything after it (`T+`, and
/// the extra flag letters macOS appends) is not part of the state.
#[cfg(unix)]
fn process_is_stopped(pid: u32) -> bool {
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "state=", "-p", &pid.to_string()])
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .starts_with('T')
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // A panicking test must not leave broker processes running. The data
        // root is a `TempDir`, so it goes with this too — but only after the
        // processes holding it are gone.
        self.kill_brokers();
    }
}

/// Create the tenant, namespace, and streams the cluster serves.
async fn seed_metadata(
    http: &reqwest::Client,
    control_plane: &ControlPlane,
    config: &ClusterConfig,
    token: &str,
) -> Result<()> {
    let base = &control_plane.base_url;
    post(
        http,
        &format!("{base}/v1/tenants"),
        token,
        serde_json::json!({
            "tenant_id": config.tenant_id,
            "display_name": config.tenant_id,
        }),
    )
    .await
    .context("create tenant")?;
    // The tenant now exists; bind the signing keys to it so every token this
    // harness mints verifies.
    control_plane.seed_tenant_keys(&config.tenant_id).await?;

    post(
        http,
        &format!("{base}/v1/tenants/{}/namespaces", config.tenant_id),
        token,
        serde_json::json!({
            "tenant_id": config.tenant_id,
            "namespace": config.namespace,
            "display_name": config.namespace,
        }),
    )
    .await
    .context("create namespace")?;

    for spec in &config.streams {
        let stream = &spec.name;
        post(
            http,
            &format!(
                "{base}/v1/tenants/{}/namespaces/{}/streams",
                config.tenant_id, config.namespace
            ),
            token,
            serde_json::json!({
                "tenant_id": config.tenant_id,
                "namespace": config.namespace,
                "stream": stream,
                "kind": "Stream",
                "shards": spec.shards,
                "replication_factor": spec.replication_factor,
                "retention": { "max_age_seconds": null, "max_size_bytes": null },
                "consistency": spec.consistency,
                "delivery": "AtLeastOnce",
                // Durable, so the brokers gate readiness on having actually
                // recovered their logs. An ephemeral stream would let a broker
                // report ready before it could serve anything.
                "durable": true,
            }),
        )
        .await
        .with_context(|| format!("create stream {stream}"))?;
    }
    Ok(())
}

async fn post(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    body: serde_json::Value,
) -> Result<()> {
    let response = http
        .post(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    // Idempotent by design: a harness restarted against a warm control plane
    // should not fail because the tenant already exists.
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return Ok(());
    }
    let detail = response.text().await.unwrap_or_default();
    bail!("POST {url}: {status}: {detail}")
}

/// Start one broker process.
fn spawn_broker(
    binary: &PathBuf,
    control_plane: &ControlPlane,
    config: &ClusterConfig,
    root: &std::path::Path,
    index: usize,
) -> Result<BrokerNode> {
    let node_id = format!("broker-{index}");
    let client_addr = ports::free_udp()?;
    let internal_addr = ports::free_udp()?;
    let metrics_addr = ports::free_tcp()?;
    let data_dir = root.join(&node_id);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("create data dir {}", data_dir.display()))?;

    let token = control_plane.node_token(&config.tenant_id, &node_id)?;
    let mut command = Command::new(binary);
    command
        .env("FELIX_NODE_ID", &node_id)
        // The advertised address is the internal listener's: it is what peers
        // forward to, not what clients connect to.
        .env("FELIX_NODE_ADVERTISE_ADDR", internal_addr.to_string())
        .env("FELIX_NODE_TOKEN", &token)
        .env("FELIX_CONTROLPLANE_URL", &control_plane.base_url)
        .env("FELIX_REGION_ID", "local")
        .env("FELIX_QUIC_BIND", client_addr.to_string())
        // And where clients reach it, which is what discovery hands out. The
        // harness binds a concrete loopback port rather than 0.0.0.0, so the
        // bind address is also the reachable one.
        .env("FELIX_CLIENT_ADVERTISE_ADDR", client_addr.to_string())
        // Test-only peer severing, off until a test writes the file.
        .env("FELIX_PEER_PARTITION_FILE", partition_file(&data_dir))
        .env("FELIX_INTERNAL_BIND", internal_addr.to_string())
        .env("FELIX_BROKER_METRICS_BIND", metrics_addr.to_string())
        .env("FELIX_DURABLE_STORAGE_DIR", &data_dir)
        // Fast enough that ownership converges while a person is watching, and
        // still a real poll rather than a fixed wait.
        .env("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS", "200")
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").as_deref().unwrap_or("info"),
        );

    if config.inherit_output {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        // To a file rather than discarded: a broker that exits during start-up
        // takes its reason with it otherwise, and "exit status: 1" says nothing
        // about whether it lost a port or was refused by the control plane.
        let log = std::fs::File::create(data_dir.join("broker.log"))
            .with_context(|| format!("create log for {node_id}"))?;
        let errors = log.try_clone().context("clone log handle")?;
        command.stdout(Stdio::from(log)).stderr(Stdio::from(errors));
    }

    let process = command
        .spawn()
        .with_context(|| format!("spawn {}", binary.display()))?;

    Ok(BrokerNode {
        node_id,
        client_addr,
        internal_addr,
        metrics_addr,
        data_dir,
        process: Some(process),
    })
}

/// Locate the `felix-broker` binary built alongside this harness.
///
/// Taken from this executable's own directory rather than by running cargo: the
/// harness is often already inside a cargo invocation, and a nested one would
/// deadlock on the build lock.
fn broker_binary() -> Result<PathBuf> {
    let mut dir = std::env::current_exe().context("locate the running executable")?;
    dir.pop();
    // Integration test binaries live in `target/<profile>/deps`.
    if dir.ends_with("deps") {
        dir.pop();
    }
    let candidate = dir.join("felix-broker");
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "felix-broker not found at {}; build it first with `cargo build -p broker --bin felix-broker`",
        candidate.display()
    ))
}

/// Where a broker's test-only partition list lives.
///
/// Under the data directory, so it is cleaned up with the cluster and a broker
/// the harness restarts keeps the same one.
fn partition_file(data_dir: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    data_dir.as_ref().join("peer-partition")
}

/// Wait until every broker has re-read its partition file.
///
/// A broker caches its reading briefly rather than stat-ing a file on every
/// forwarded publish, so writing the file does not sever anything until that
/// cache expires. Returning before then would hand a caller a fault that is not
/// yet in effect, and the test would go on to prove nothing -- which is exactly
/// the mistake `pause_node` made before it learned to wait for the stop.
///
/// Generous against the broker's window rather than equal to it: this runs once
/// per fault, and a test that races the injector fails for a reason that has
/// nothing to do with what it is testing.
fn await_partition_reread() {
    std::thread::sleep(std::time::Duration::from_millis(400));
}
