//! Driving placement: stepping the reconciler, moving shards, and changing
//! membership.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use felix_controlplane_service::cluster::placement::{MovePolicy, ReconcileOutcome};

use super::{Cluster, READY_TIMEOUT};
use crate::node::spawn_broker;
use crate::wait;

impl Cluster {
    /// Step placement once.
    ///
    /// Exposed because the harness's control plane does not run the reconciler
    /// on a timer: a test that slept for one would be timing-dependent in
    /// exactly the way the acceptance criteria rule out. A failure test has to
    /// drive placement while it waits, or nothing re-plans after the fault.
    pub async fn place_shards(&self) -> ReconcileOutcome {
        self.control_plane().place_shards().await
    }

    /// Run the control plane's placement loop on `interval` and on the wakes a
    /// move's reports send, as a deployment does. For a test that measures a
    /// move rather than stepping it.
    pub fn run_placement(&self, interval: Duration) {
        self.control_plane().run_placement(interval);
    }

    /// Step placement once with up to `max_concurrent` shard moves in flight.
    pub async fn place_shards_moving(&self, max_concurrent: usize) -> ReconcileOutcome {
        self.control_plane()
            .place_shards_with(MovePolicy { max_concurrent })
            .await
    }

    /// Move a stream's shard off its current owner.
    ///
    /// Drains the owner and steps placement until the assignment names
    /// someone else at a higher generation. The move goes through the planned
    /// handoff -- staged, fenced, cut over -- so this takes several steps and
    /// needs the brokers to be shipping and reporting. Draining rather than
    /// stopping the broker on purpose: the node stays up and reachable, so
    /// what changes is ownership alone.
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

        self.drain_node(&before.leader).await?;

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

    /// Stop a stream's shard 0 halfway through a move, with its owner fenced.
    ///
    /// Drains the owner and steps placement until the assignment is
    /// `draining`: the owner has been told to stop serving and nobody else
    /// leads yet. It stays that way until placement is stepped again, which
    /// cuts over to the successor, so a caller can look at what a client sees
    /// during a move for as long as it likes. Every shard the owner leads is
    /// fenced with it. Returns the fenced owner.
    ///
    /// Needs a replica that can take the shard, so a stream replicated to more
    /// than one node.
    pub async fn fence_shard(&self, stream: &str) -> Result<String> {
        let owner = self.owner(stream).await?;
        self.drain_node(&owner).await?;
        wait::until(
            READY_TIMEOUT,
            &format!("{stream} to be fenced on {owner}"),
            || async move {
                // Every move at once, so this shard does not queue behind the
                // owner's others.
                self.place_shards_moving(usize::MAX).await;
                self.shard_fenced(stream, 0).await.unwrap_or(false)
            },
        )
        .await?;
        Ok(owner)
    }

    /// Start another broker and wait until the control plane can place on it.
    ///
    /// The node takes the next index, so its id follows the ones the cluster
    /// started with. Returns the new node id.
    pub async fn add_node(&mut self) -> Result<String> {
        let index = self.nodes.len();
        let control_plane = self
            .control_plane
            .as_ref()
            .ok_or_else(|| anyhow!("control plane is gone"))?;
        let node = spawn_broker(
            &self.binary,
            control_plane,
            &self.config,
            self._root.path(),
            index,
        )
        .with_context(|| format!("start broker {index}"))?;
        let node_id = node.node_id.clone();
        self.nodes.push(node);

        let deadline = Instant::now() + wait::budget(READY_TIMEOUT);
        loop {
            let url = format!("http://{}/ready", self.nodes[index].metrics_addr);
            if let Some(status) = self.nodes[index].exited() {
                let reason = self.nodes[index].failure_reason();
                bail!("{node_id} exited before becoming ready ({status}){reason}");
            }
            let ok = matches!(
                self.http.get(&url).send().await,
                Ok(response) if response.status().is_success()
            );
            if ok {
                break;
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for {node_id} to be ready");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let this = &*self;
        let expected = node_id.clone();
        wait::until(
            READY_TIMEOUT,
            &format!("{expected} to be placeable"),
            || {
                let expected = expected.clone();
                async move {
                    matches!(this.placeable_nodes().await, Ok(live) if live.contains(&expected))
                }
            },
        )
        .await?;
        Ok(node_id)
    }

    /// Mark a broker draining, as an operator would before removing it.
    ///
    /// The broker keeps running and keeps serving; placement moves its shards
    /// off it one step per `place_shards`. This only sets the lifecycle --
    /// drive placement and wait on `shard_owners` to see the shards go.
    pub async fn drain_node(&self, node_id: &str) -> Result<()> {
        self.post_lifecycle(node_id, "drain").await
    }

    /// Put a draining broker back into placement.
    pub async fn undrain_node(&self, node_id: &str) -> Result<()> {
        let url = format!("{}/v1/nodes/{node_id}", self.control_plane_url());
        let response = self
            .http
            .patch(&url)
            .bearer_auth(&self.operator_token)
            .json(&serde_json::json!({ "lifecycle": "live" }))
            .send()
            .await
            .with_context(|| format!("undrain {node_id}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("undrain {node_id}: {status}: {body}");
        }
        Ok(())
    }

    /// Step placement until `node_id` leads nothing, or the budget runs out.
    ///
    /// Each step advances every move in flight by one stage, so a drain of
    /// `n` shards at `max_concurrent` moves takes roughly `3n / max_concurrent`
    /// steps plus the catch-ups in between. On timeout the message says what
    /// the node still leads and why placement was waiting.
    pub async fn drain_until_empty(
        &self,
        node_id: &str,
        max_concurrent: usize,
        timeout: Duration,
    ) -> Result<()> {
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            let outcome = self.place_shards_moving(max_concurrent).await;
            let owners = self.shard_owners().await?;
            let still: Vec<&String> = owners
                .iter()
                .filter(|(_, leader)| leader.as_str() == node_id)
                .map(|(shard, _)| shard)
                .collect();
            if still.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{node_id} still leads {still:?} after {budget:?}; last pass: {outcome:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn post_lifecycle(&self, node_id: &str, action: &str) -> Result<()> {
        let url = format!("{}/v1/nodes/{node_id}/{action}", self.control_plane_url());
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.operator_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .with_context(|| format!("{action} {node_id}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("{action} {node_id}: {status}: {body}");
        }
        Ok(())
    }
}
