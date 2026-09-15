//! The metadata Raft core: group lifecycle, persistent log, and the seam.
//!
//! This module is the **entire** openraft surface of the control plane — no
//! openraft type escapes it. Everything outside talks to [`RaftHandle`] and
//! implements [`AppStateMachine`], which is what lets the consensus library
//! be upgraded (openraft is pre-1.0) or replaced without touching the store
//! traits or handlers. The design, including why openraft and why the log
//! lives outside `felix-storage`, is `docs/metadata-raft-design.md`.
//!
//! What lives where:
//! - [`store`] — the Raft log, vote, and current snapshot, in one crash-safe
//!   redb file per instance. Consensus state is the one thing here that must
//!   never lie about being on disk.
//! - [`network`] / [`http`] — Raft RPCs as JSON over HTTP between instances,
//!   riding the same listener the control plane already runs.
//! - [`types`] — the openraft type configuration. Commands and responses are
//!   opaque bytes at this layer; their meaning belongs to the application
//!   state machine (#338 gives them theirs).
mod http;
mod network;
mod store;
mod types;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

/// This instance's identity within the Raft group.
pub type NodeId = u64;

/// Where a refused proposal should go instead, when the refusal says.
fn forward_target(
    err: &openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
) -> Option<String> {
    if let openraft::error::RaftError::APIError(
        openraft::error::ClientWriteError::ForwardToLeader(forward),
    ) = err
    {
        forward.leader_node.as_ref().map(|node| node.addr.clone())
    } else {
        None
    }
}

/// The application half of the state machine seam.
///
/// The Raft core feeds every committed command to exactly one of these, in
/// log order, on every member. The contract that makes that meaningful:
///
/// - **`apply` must be deterministic.** Same command sequence, same state,
///   byte-identical `snapshot()` — on every instance, every time. No clocks,
///   no randomness, nothing read from outside the command and prior state.
///   Anything nondeterministic (timestamps, generated keys) is decided
///   *before* the command is proposed and carried inside it.
/// - The implementation owns its interior mutability; calls arrive
///   serialized from a single apply loop, but snapshot building may read
///   concurrently with nothing else, so a lock the implementation already
///   holds for serving reads is the right shape (the metadata store's
///   `RwLock`s, in #338).
/// - `restore` replaces the whole state with a previously produced snapshot.
///
/// Async because the metadata store's interior locks are async; the
/// determinism rule is about *what* apply computes, not how it schedules.
#[async_trait::async_trait]
pub trait AppStateMachine: Send + Sync + 'static {
    async fn apply(&self, command: &[u8]) -> Vec<u8>;
    async fn snapshot(&self) -> Vec<u8>;
    async fn restore(&self, snapshot: &[u8]);
}

/// Everything needed to start this instance's member of the group.
#[derive(Debug, Clone)]
pub struct RaftSettings {
    pub node_id: NodeId,
    /// Where the log, vote, and snapshots live. One directory per instance,
    /// surviving restarts — this is the state that makes a restart a rejoin
    /// rather than a fresh member.
    pub data_dir: PathBuf,
    /// How often the leader heartbeats followers.
    pub heartbeat_interval: Duration,
    /// Election timeout range; the minimum must comfortably exceed the
    /// heartbeat interval or healthy followers call elections.
    pub election_timeout: (Duration, Duration),
    /// Snapshot after this many log entries since the last one. Metadata
    /// state is small, so snapshots are cheap and the log stays short.
    pub snapshot_logs_since_last: u64,
    /// Log entries to keep behind the snapshot, so a briefly-lagging
    /// follower catches up from the log rather than a snapshot install.
    pub logs_kept_behind_snapshot: u64,
    /// Overall budget for one proposal, elections and forwarding included.
    ///
    /// openraft's own write path waits indefinitely for a commit — a leader
    /// that lost quorum queues proposals forever — so the bound has to live
    /// here, where "no quorum" becomes an error a caller can surface
    /// instead of a hang inside the seam.
    pub write_timeout: Duration,
}

impl RaftSettings {
    /// Defaults sized for a three-instance metadata group: elections settle
    /// in about a second, and the log is compacted often because state is
    /// kilobytes.
    pub fn new(node_id: NodeId, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            data_dir,
            heartbeat_interval: Duration::from_millis(150),
            election_timeout: (Duration::from_millis(600), Duration::from_millis(1200)),
            snapshot_logs_since_last: 500,
            logs_kept_behind_snapshot: 100,
            write_timeout: Duration::from_secs(10),
        }
    }
}

/// A snapshot of where this member stands, in the seam's own vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStatus {
    pub id: NodeId,
    /// The leader as this member currently believes; `None` during an
    /// election or before the group is initialized.
    pub leader: Option<NodeId>,
    pub term: u64,
    pub last_applied_index: Option<u64>,
    /// Voting members of the current membership config.
    pub voters: Vec<NodeId>,
}

/// One running member of the metadata Raft group.
///
/// Cheap to clone; all clones drive the same underlying node.
#[derive(Clone)]
pub struct RaftHandle {
    pub(super) raft: types::Raft,
    id: NodeId,
    write_timeout: Duration,
    /// For forwarding proposals to the leader; pooled per host underneath.
    forward: reqwest::Client,
}

/// Whether a background task that must run on exactly one instance should
/// run here, now.
///
/// `Always` is the non-Raft deployments' answer — M7's stores make duplicate
/// sweeps safe, so every instance runs them. Under Raft the gate is a
/// **linearizable leadership check** (openraft's read-index), which answers
/// two questions at once: this instance is the leader, *and* its applied
/// state is current enough to decide from — a deposed leader that has not
/// heard the news yet fails the check rather than sweeping from stale state.
#[derive(Clone)]
pub enum LeadershipGate {
    Always,
    Leader(RaftHandle),
}

impl LeadershipGate {
    pub async fn holds(&self) -> bool {
        match self {
            Self::Always => true,
            Self::Leader(handle) => handle.confirm_leadership().await,
        }
    }
}

impl RaftHandle {
    /// Open (or create) this instance's Raft state under
    /// `settings.data_dir` and start the node.
    ///
    /// Starting is not joining: a fresh node idles until either
    /// [`RaftHandle::initialize`] forms a new group or an existing leader
    /// adds it as a learner. A node with prior state on disk resumes its
    /// membership without ceremony — that is the point of the data dir.
    pub async fn start(settings: RaftSettings, app: Arc<dyn AppStateMachine>) -> Result<Self> {
        let config = openraft::Config {
            cluster_name: "felix-metadata".to_string(),
            heartbeat_interval: settings.heartbeat_interval.as_millis() as u64,
            election_timeout_min: settings.election_timeout.0.as_millis() as u64,
            election_timeout_max: settings.election_timeout.1.as_millis() as u64,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                settings.snapshot_logs_since_last,
            ),
            max_in_snapshot_log_to_keep: settings.logs_kept_behind_snapshot,
            ..Default::default()
        };
        let config = Arc::new(config.validate().context("raft config")?);

        std::fs::create_dir_all(&settings.data_dir).context("create raft data dir")?;
        let db = store::open(&settings.data_dir.join("raft.redb"))?;
        // What this node had committed before it stopped: the floor its
        // state machine must be replayed back to before anything serves
        // from it. Read before the node starts, because the node moves it.
        let committed_floor = store::persisted_committed_index(&db);
        let log_store = store::LogStore::new(Arc::clone(&db));
        let state_machine = store::StateMachineStore::open(db, app).await?;

        let raft = types::Raft::new(
            settings.node_id,
            config,
            network::HttpNetworkFactory::new(),
            log_store,
            state_machine,
        )
        .await
        .context("start raft node")?;

        // A restart is a rejoin, and a rejoin is not done until the state
        // machine holds everything this node had already acknowledged as
        // committed. Without this wait, a restarted member reports ready —
        // it knows a leader within a heartbeat — while its store is still
        // missing entries it committed in its previous life, and requests
        // routed to it read a world that never existed. The chaos suite
        // caught exactly that.
        if let Some(floor) = committed_floor {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let applied = raft
                    .metrics()
                    .borrow()
                    .last_applied
                    .map(|log_id| log_id.index)
                    .unwrap_or(0);
                if applied >= floor {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "state machine replay stalled: applied {applied} of committed {floor}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        Ok(Self {
            raft,
            id: settings.node_id,
            write_timeout: settings.write_timeout,
            forward: reqwest::Client::builder()
                .timeout(settings.write_timeout)
                .build()
                .context("build forwarding client")?,
        })
    }

    /// Form a brand-new group from `members` (`node_id -> reachable addr`,
    /// this node included). Exactly once per cluster, ever; a restart
    /// resumes from disk instead. Initializing two disjoint member sets is
    /// how split brain is manufactured, so this fails on a node that
    /// already has state.
    pub async fn initialize(&self, members: BTreeMap<NodeId, String>) -> Result<()> {
        let nodes: BTreeMap<NodeId, openraft::BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, openraft::BasicNode { addr }))
            .collect();
        self.raft
            .initialize(nodes)
            .await
            .context("initialize raft group")?;
        Ok(())
    }

    /// Propose one command and wait until it is committed and applied;
    /// returns the state machine's response.
    ///
    /// Works from any member: a follower forwards the bytes to the leader it
    /// knows, and an instance caught mid-election retries briefly before
    /// giving up. The bound matters — a caller must get "no leader" as an
    /// error it can surface, not an indefinite hang inside the seam.
    pub async fn write(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        const RETRY_DELAY: Duration = Duration::from_millis(250);
        // Each attempt is capped well below the whole budget: a single hung
        // hop — a forward to a leader that is frozen, not dead, so its
        // socket accepts and then stalls — must not consume every retry the
        // budget was meant to fund. The chaos suite's freeze fault found
        // exactly that.
        const ATTEMPT_CAP: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + self.write_timeout;

        let mut last_refusal = None;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // The counter to alert on: a proposal ran out its whole
                // budget, which means no leader or no quorum.
                metrics::counter!("felix_meta_raft_write_timeouts_total").increment(1);
                return Err(last_refusal
                    .unwrap_or_else(|| anyhow::anyhow!("no quorum committed the proposal"))
                    .context(format!(
                        "raft write: not committed within {:?} — no leader, or quorum lost",
                        self.write_timeout
                    )));
            }
            let attempt = remaining.min(ATTEMPT_CAP);
            // Bounded per attempt too: a leader that lost quorum queues
            // proposals forever, and that must become this instance's error,
            // not its hang.
            match tokio::time::timeout(attempt, self.raft.client_write(command.clone())).await {
                Err(_) => {
                    last_refusal = Some(anyhow::anyhow!("proposal not committed in time"));
                    continue;
                }
                Ok(Ok(response)) => return Ok(response.data),
                Ok(Err(err)) => match forward_target(&err) {
                    Some(addr) => match self.forward_to(&addr, &command, attempt).await {
                        Ok(bytes) => {
                            // The counter that says the load balancer keeps
                            // handing writes to followers — informational,
                            // since forwarding is correct, just one hop more.
                            metrics::counter!("felix_meta_raft_forwarded_proposals_total")
                                .increment(1);
                            return Ok(bytes);
                        }
                        // The leader we were told about may itself have just
                        // lost leadership; loop and re-ask.
                        Err(fwd_err) => last_refusal = Some(fwd_err),
                    },
                    None => last_refusal = Some(anyhow::anyhow!(err.to_string())),
                },
            }
            tokio::time::sleep(RETRY_DELAY.min(remaining)).await;
        }
    }

    async fn forward_to(&self, addr: &str, command: &[u8], budget: Duration) -> Result<Vec<u8>> {
        let response = self
            .forward
            .post(format!("http://{addr}/internal/raft/propose"))
            .timeout(budget)
            .body(command.to_vec())
            .send()
            .await
            .context("forward proposal")?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            anyhow::bail!("leader refused forwarded proposal: {status} {detail}");
        }
        Ok(response
            .bytes()
            .await
            .context("read forwarded response")?
            .to_vec())
    }

    /// Linearizable leadership check: true only when this instance is the
    /// leader *and* its applied state is current — openraft's read-index,
    /// so a deposed leader that has not heard the news fails it.
    pub async fn confirm_leadership(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    /// Entries this member may trail its own log by before readiness calls
    /// it unfit to serve. Small on purpose: metadata writes are rare, the
    /// only healthy lag is the handful of entries in flight between append
    /// and apply, and a member further behind is serving a past that
    /// callers cannot detect. (Startup replay is handled separately — a
    /// node does not finish starting until it has re-applied everything it
    /// had committed.)
    const READY_APPLY_LAG_MAX: u64 = 32;
    /// How stale a leader's last quorum acknowledgement may be before
    /// readiness stops trusting it. A leader that has not heard a quorum in
    /// this long is a leader in name only — likely partitioned with the
    /// minority — and must leave rotation before it serves stale reads or
    /// queues writes that cannot commit.
    const READY_QUORUM_ACK_MAX: Duration = Duration::from_secs(5);

    /// Whether this member is fit to serve, and if not, why — the store's
    /// readiness probe under the raft backend.
    ///
    /// Three questions, in order: does this member know a leader (an
    /// instance mid-election or partitioned off does not); is it applying
    /// what its log holds; and, when it *is* the leader, has a quorum
    /// acknowledged it recently. All answered from local metrics — a
    /// readiness probe must never cost a consensus round trip.
    pub fn readiness(&self) -> Result<(), String> {
        let metrics = self.raft.metrics().borrow().clone();
        let Some(leader) = metrics.current_leader else {
            return Err("no raft leader is known to this instance".to_string());
        };
        let last_log = metrics.last_log_index.unwrap_or(0);
        let applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);
        // A follower with a leader and an *empty* log has just joined an
        // established group — a wiped volume, a brand-new member — and
        // holds none of the group's state. Its apply-lag reads zero because
        // the lag is measured against its own log, which is exactly the
        // blind spot: until the first replication batch lands, it would
        // serve an empty world as ready. (A leader is exempt — it holds the
        // head by definition — and a genuinely new cluster is leaderless,
        // so this clause never blocks group formation.)
        if leader != self.id && last_log == 0 {
            return Err(
                "joined an established group; nothing replicated into this member yet".to_string(),
            );
        }
        let lag = last_log.saturating_sub(applied);
        if lag > Self::READY_APPLY_LAG_MAX {
            return Err(format!(
                "applied state trails the log by {lag} entries (bound {})",
                Self::READY_APPLY_LAG_MAX
            ));
        }
        if leader == self.id
            && let Some(millis) = metrics.millis_since_quorum_ack
            && millis > Self::READY_QUORUM_ACK_MAX.as_millis() as u64
        {
            return Err(format!(
                "leader without a quorum acknowledgement for {millis}ms (bound {}ms)",
                Self::READY_QUORUM_ACK_MAX.as_millis()
            ));
        }
        Ok(())
    }

    /// Publish this member's consensus position as gauges, once a second,
    /// until `shutdown`. The names an operator's dashboard needs to answer
    /// "who leads, and is everyone keeping up" — per-instance series, no
    /// unbounded labels.
    pub fn spawn_metrics(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let raft = self.raft.clone();
        let id = self.id;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = ticker.tick() => {
                        let metrics = raft.metrics().borrow().clone();
                        metrics::gauge!("felix_meta_raft_term").set(metrics.current_term as f64);
                        metrics::gauge!("felix_meta_raft_leader_known")
                            .set(if metrics.current_leader.is_some() { 1.0 } else { 0.0 });
                        metrics::gauge!("felix_meta_raft_is_leader")
                            .set(if metrics.current_leader == Some(id) { 1.0 } else { 0.0 });
                        metrics::gauge!("felix_meta_raft_last_log_index")
                            .set(metrics.last_log_index.unwrap_or(0) as f64);
                        metrics::gauge!("felix_meta_raft_last_applied_index")
                            .set(metrics.last_applied.map(|id| id.index).unwrap_or(0) as f64);
                        metrics::gauge!("felix_meta_raft_snapshot_index")
                            .set(metrics.snapshot.map(|id| id.index).unwrap_or(0) as f64);
                    }
                }
            }
        })
    }

    /// Add a node as a non-voting learner and wait until it has caught up.
    /// Learner-first is what keeps a join from costing quorum: the group's
    /// vote arithmetic only changes at [`RaftHandle::change_membership`],
    /// after the newcomer already holds the data.
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<()> {
        self.raft
            .add_learner(id, openraft::BasicNode { addr }, true)
            .await
            .context("add learner")?;
        Ok(())
    }

    /// Replace the set of voting members. Nodes being added must already be
    /// learners; removed nodes are retained as learners rather than
    /// abandoned, so a scale-down mistake is reversible.
    pub async fn change_membership(&self, voters: impl IntoIterator<Item = NodeId>) -> Result<()> {
        let ids: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        self.raft
            .change_membership(ids, true)
            .await
            .context("change membership")?;
        Ok(())
    }

    /// Ask for a snapshot now rather than at the log-size policy point.
    pub async fn trigger_snapshot(&self) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .context("trigger snapshot")?;
        Ok(())
    }

    pub fn status(&self) -> RaftStatus {
        let metrics = self.raft.metrics().borrow().clone();
        RaftStatus {
            id: self.id,
            leader: metrics.current_leader,
            term: metrics.current_term,
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            voters: metrics.membership_config.membership().voter_ids().collect(),
        }
    }

    /// Router serving this node's Raft RPCs, to be merged into the internal
    /// HTTP listener.
    pub fn rpc_router(&self) -> axum::Router {
        http::router(self.clone())
    }

    /// Stop participating. In-flight proposals fail; disk state remains, so
    /// the next [`RaftHandle::start`] with the same data dir resumes.
    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| anyhow::anyhow!("raft shutdown: {err}"))?;
        Ok(())
    }
}
