//! Entering the group at startup: forming it on first boot, or rejoining
//! after this member lost its state.
//!
//! Raft's safety assumes a voter never forgets its vote or its log. A member
//! that starts with no state cannot tell a first boot from a wiped volume by
//! looking at itself, so it neither votes nor stands for election until it
//! has asked its peers:
//!
//! - some peer's log is past the initial membership entry: the group exists.
//!   This member follows it without a vote until it has applied everything
//!   the confirmed leader held when asked, then votes again;
//! - a majority, this member included, answers and is empty: first boot.
//!   Every such member initializes with the same configured group, which
//!   openraft documents as safe;
//! - otherwise it asks again.
//!
//! The withheld vote is persisted, so a crash part-way through catching up
//! does not turn a half-filled log into a voter. The argument is in
//! `docs/metadata-raft-design.md` ("Rejoining after a lost volume").
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{NodeId, RaftHandle, store};

/// What a member reports to one deciding how to enter the group.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Standing {
    pub(super) last_log_index: Option<u64>,
}

/// The leader's answer to a rejoining member: apply up to here, then vote.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct CatchUpTarget {
    pub(super) index: u64,
}

const RETRY_INTERVAL: Duration = Duration::from_millis(200);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// A target can be a tail a deposed leader never commits; past this, ask
/// again rather than wait for it forever.
const CATCH_UP_WAIT: Duration = Duration::from_secs(30);

impl RaftHandle {
    /// Take this member's place in the group, in the background.
    ///
    /// A member that kept its state has nothing to do. One that starts empty,
    /// or was still catching up when it stopped, withholds its vote and works
    /// out whether to form the group or catch up with it, per the module
    /// docs. Call before the RPC routes serve, so no vote leaks out first.
    pub fn enter_group(
        &self,
        peers: BTreeMap<NodeId, String>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        if self.may_vote.load(Ordering::SeqCst) {
            let db = self.store()?;
            if store::holds_state(&db)? {
                return Ok(());
            }
            store::set_vote_withheld(&db, true)?;
            self.may_vote.store(false, Ordering::SeqCst);
            self.raft.runtime_config().elect(false);
        }
        let handle = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                () = handle.decide_and_join(peers) => {}
            }
        });
        Ok(())
    }

    async fn decide_and_join(&self, peers: BTreeMap<NodeId, String>) {
        let client = reqwest::Client::builder()
            .timeout(PROBE_TIMEOUT)
            .build()
            .expect("build raft join client");
        let others: Vec<String> = peers
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, addr)| addr.clone())
            .collect();

        // Holding state while withholding the vote means an earlier catch-up
        // was interrupted; the group exists, so there is nothing to decide.
        if !self
            .store()
            .and_then(|db| store::holds_state(&db))
            .unwrap_or(true)
        {
            loop {
                let mut empty = 1; // this member
                let mut exists = false;
                for addr in &others {
                    let url = format!("http://{addr}/internal/raft/standing");
                    let standing = match client.get(url).send().await {
                        Ok(response) => response.json::<Standing>().await.ok(),
                        Err(_) => None,
                    };
                    match standing {
                        Some(Standing {
                            last_log_index: Some(index),
                        }) if index >= 1 => exists = true,
                        Some(_) => empty += 1,
                        None => {}
                    }
                }
                if exists {
                    break;
                }
                if empty > peers.len() / 2 {
                    self.form_group(peers).await;
                    return;
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
        tracing::info!("raft group exists; catching up before this member votes");
        self.catch_up(&client, &others).await;
    }

    async fn form_group(&self, peers: BTreeMap<NodeId, String>) {
        // Cleared before initialize writes anything: a crash in between
        // leaves an empty member, which simply decides again.
        if let Err(err) = self
            .store()
            .and_then(|db| store::set_vote_withheld(&db, false))
        {
            tracing::error!(error = %err, "could not record that this member may vote");
            return;
        }
        self.allow_votes();
        // A peer may have initialized first with the same configuration;
        // openraft reports that as an error, and this member resumes.
        if let Err(err) = self.initialize(peers).await {
            tracing::info!(error = %err, "raft group not initialized here (a peer was first)");
        }
    }

    async fn catch_up(&self, client: &reqwest::Client, others: &[String]) {
        loop {
            let mut target = None;
            for addr in others {
                let url = format!("http://{addr}/internal/raft/catch-up-target");
                if let Ok(response) = client.get(url).send().await
                    && response.status().is_success()
                    && let Ok(answer) = response.json::<CatchUpTarget>().await
                {
                    target = Some(answer.index);
                    break;
                }
            }
            let Some(target) = target else {
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            };
            let deadline = tokio::time::Instant::now() + CATCH_UP_WAIT;
            while tokio::time::Instant::now() < deadline {
                let applied = self
                    .raft
                    .metrics()
                    .borrow()
                    .last_applied
                    .map(|log_id| log_id.index)
                    .unwrap_or(0);
                if applied >= target {
                    if let Err(err) = self
                        .store()
                        .and_then(|db| store::set_vote_withheld(&db, false))
                    {
                        tracing::error!(error = %err, "could not record that this member may vote");
                        return;
                    }
                    self.allow_votes();
                    tracing::info!(applied, "caught up with the raft group; voting again");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    fn store(&self) -> anyhow::Result<std::sync::Arc<redb::Database>> {
        self.db
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("raft node has stopped"))
    }

    fn allow_votes(&self) {
        self.may_vote.store(true, Ordering::SeqCst);
        self.raft.runtime_config().elect(true);
    }

    /// The leader's side of a rejoin: its last log index, but only once a
    /// quorum has confirmed it still leads. A deposed leader's answer could
    /// be short of what the group committed without it.
    pub(super) async fn catch_up_target(&self) -> Option<CatchUpTarget> {
        if !self.confirm_leadership().await {
            return None;
        }
        let index = self.raft.metrics().borrow().last_log_index?;
        Some(CatchUpTarget { index })
    }

    pub(super) fn standing(&self) -> Standing {
        Standing {
            last_log_index: self.raft.metrics().borrow().last_log_index,
        }
    }
}
