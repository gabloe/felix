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
    /// Held so the data directories outlive the brokers and are removed with
    /// the cluster.
    _root: tempfile::TempDir,
}

/// How to build a cluster.
pub struct ClusterConfig {
    pub nodes: usize,
    pub tenant_id: String,
    pub namespace: String,
    /// Streams to create, and how many shards each has.
    pub streams: Vec<(String, u32)>,
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
            streams: vec![("orders".to_string(), 1)],
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
            _root: root,
        };

        // Two phases: the first needs the child handles, to tell "not ready yet"
        // from "already exited"; the rest only observes the cluster.
        cluster.await_brokers_ready().await?;
        cluster.await_cluster_ready(&config).await?;
        Ok(cluster)
    }

    fn control_plane(&self) -> &ControlPlane {
        self.control_plane
            .as_ref()
            .expect("control plane is only taken during shutdown")
    }

    pub fn control_plane_url(&self) -> &str {
        &self.control_plane().base_url
    }

    /// Wait for every broker process to report ready.
    async fn await_brokers_ready(&mut self) -> Result<()> {
        // Written as a loop rather than through `wait::until` so a broker that
        // has already exited can be reported as such, with its status, instead
        // of timing out.
        for index in 0..self.nodes.len() {
            let url = format!("http://{}/ready", self.nodes[index].metrics_addr);
            let deadline = std::time::Instant::now() + READY_TIMEOUT;
            loop {
                if let Some(status) = self.nodes[index].exited() {
                    let node_id = &self.nodes[index].node_id;
                    bail!("{node_id} exited before becoming ready ({status})");
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
        let total_shards: usize = config.streams.iter().map(|(_, s)| *s as usize).sum();
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
        for (stream, _) in &config.streams {
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

    for (stream, shards) in &config.streams {
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
                "shards": shards,
                "retention": { "max_age_seconds": null, "max_size_bytes": null },
                "consistency": "Leader",
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
        command.stdout(Stdio::null()).stderr(Stdio::null());
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
