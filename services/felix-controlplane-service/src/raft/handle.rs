//! One running member of the group: starting it, changing the membership,
//! and stopping it.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use super::types::Raft;
use super::{AppStateMachine, NodeId, RaftSettings, RaftStatus, http, network, store};

/// One running member of the metadata Raft group.
///
/// Cheap to clone; all clones drive the same underlying node.
#[derive(Clone)]
pub struct RaftHandle {
    pub(super) raft: Raft,
    pub(super) id: NodeId,
    pub(super) write_timeout: Duration,
    /// For forwarding proposals to the leader; pooled per host underneath.
    pub(super) forward: reqwest::Client,
    /// Kept past construction only for [`AppStateMachine::restamp`], which
    /// runs on the proposal path rather than the apply loop.
    pub(super) app: Arc<dyn AppStateMachine>,
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
        let state_machine = store::StateMachineStore::open(db, Arc::clone(&app)).await?;

        let raft = Raft::new(
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
            app,
        })
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

    /// Router serving this node's Raft RPCs, to be merged into the internal
    /// HTTP listener.
    pub fn rpc_router(&self) -> axum::Router {
        http::router(self.clone())
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

    /// Linearizable leadership check: true only when this instance is the
    /// leader *and* its applied state is current — openraft's read-index,
    /// so a deposed leader that has not heard the news fails it.
    pub async fn confirm_leadership(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
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
