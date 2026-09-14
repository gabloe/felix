//! Persistent Raft state: log, vote, and the current snapshot.
//!
//! Everything lives in one redb file per instance. redb gives crash-safe,
//! fsynced-on-commit transactions, which is exactly the guarantee Raft
//! storage must not fake: a vote or a log entry acknowledged to the core and
//! then lost is how a cluster elects two leaders for one term. Choosing an
//! embedded ACID store over hand-rolled files is deliberate — consensus
//! durability plumbing is the last place Felix should be inventive
//! (`docs/metadata-raft-design.md#library`).
//!
//! This log is deliberately **not** `felix-storage`: Raft truncates
//! divergent uncommitted suffixes, and the segment store's never-rewritten
//! invariant exists precisely to forbid that. Separate log, separate rules.
//!
//! The state machine itself is volatile and rebuilt on startup from the
//! persisted snapshot plus log replay — the "persistent snapshot" option in
//! openraft's contract. That keeps [`super::AppStateMachine`] free of any
//! durability obligation, which is what lets #338 plug the in-memory
//! metadata store in unchanged.

// The error type and its size are openraft's, fixed by the storage traits.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use openraft::storage::{
    LogFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{AnyError, ErrorSubject, ErrorVerb, RaftLogReader, StorageIOError};
use redb::{Database, ReadableTable, TableDefinition};

use super::AppStateMachine;
use super::types::{
    Entry, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership, TypeConfig,
};

const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const KEY_VOTE: &str = "vote";
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_SNAPSHOT_META: &str = "snapshot_meta";
const KEY_SNAPSHOT_DATA: &str = "snapshot_data";

/// Open (or create) the store file and make sure both tables exist, so
/// every later read can assume them.
pub(super) fn open(path: &Path) -> Result<Arc<Database>> {
    let db = Database::create(path).context("open raft store")?;
    let txn = db.begin_write().context("init raft store")?;
    txn.open_table(LOGS).context("init log table")?;
    txn.open_table(META).context("init meta table")?;
    txn.commit().context("init raft store commit")?;
    Ok(Arc::new(db))
}

fn read_err(err: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&err)).into()
}

fn write_err(err: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&err)).into()
}

fn read_meta<T: serde::de::DeserializeOwned>(
    db: &Database,
    key: &str,
) -> Result<Option<T>, StorageError> {
    let txn = db.begin_read().map_err(read_err)?;
    let table = txn.open_table(META).map_err(read_err)?;
    let Some(guard) = table.get(key).map_err(read_err)? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(guard.value()).map_err(read_err)?;
    Ok(Some(value))
}

fn write_meta<T: serde::Serialize>(
    db: &Database,
    key: &str,
    value: &T,
) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec(value).map_err(write_err)?;
    let txn = db.begin_write().map_err(write_err)?;
    {
        let mut table = txn.open_table(META).map_err(write_err)?;
        table.insert(key, bytes.as_slice()).map_err(write_err)?;
    }
    txn.commit().map_err(write_err)?;
    Ok(())
}

/// The Raft log and vote. Clones share the database; openraft hands clones
/// to its replication tasks as log readers.
#[derive(Clone)]
pub(super) struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub(super) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn owned_bound(bound: std::ops::Bound<&u64>) -> std::ops::Bound<u64> {
        match bound {
            std::ops::Bound::Included(index) => std::ops::Bound::Included(*index),
            std::ops::Bound::Excluded(index) => std::ops::Bound::Excluded(*index),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOGS).map_err(read_err)?;
        let start = Self::owned_bound(range.start_bound());
        let end = Self::owned_bound(range.end_bound());
        let mut entries = Vec::new();
        for item in table.range((start, end)).map_err(read_err)? {
            let (_, value) = item.map_err(read_err)?;
            entries.push(serde_json::from_slice(value.value()).map_err(read_err)?);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged: Option<LogId> = read_meta(&self.db, KEY_LAST_PURGED)?;
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOGS).map_err(read_err)?;
        let last = match table.last().map_err(read_err)? {
            Some((_, value)) => {
                let entry: Entry = serde_json::from_slice(value.value()).map_err(read_err)?;
                Some(entry.log_id)
            }
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError> {
        // The commit below fsyncs; a vote that is acknowledged but not
        // durable lets this node vote twice for one term after a crash.
        write_meta(&self.db, KEY_VOTE, vote)
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError> {
        read_meta(&self.db, KEY_VOTE)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        write_meta(&self.db, KEY_COMMITTED, &committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(read_meta(&self.db, KEY_COMMITTED)?.flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            for entry in entries {
                let bytes = serde_json::to_vec(&entry).map_err(write_err)?;
                table
                    .insert(entry.log_id.index, bytes.as_slice())
                    .map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        // Only after the durable commit: this callback is openraft's "these
        // entries survive a crash" signal, and everything downstream
        // (commit, apply, acknowledgements) leans on it being true.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Conflict resolution: a deposed leader's uncommitted suffix goes
        // away. This is the operation the felix-storage segment log
        // deliberately cannot do, and why this log is not that log.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            let doomed: Vec<u64> = table
                .range(log_id.index..)
                .map_err(write_err)?
                .map(|item| item.map(|(key, _)| key.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for index in doomed {
                table.remove(index).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Compaction behind a snapshot. The purge marker commits in the same
        // transaction as the deletions so a crash between them cannot leave
        // the log claiming a hole it does not have.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut meta = txn.open_table(META).map_err(write_err)?;
            let bytes = serde_json::to_vec(&log_id).map_err(write_err)?;
            meta.insert(KEY_LAST_PURGED, bytes.as_slice())
                .map_err(write_err)?;
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            let doomed: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(write_err)?
                .map(|item| item.map(|(key, _)| key.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for index in doomed {
                table.remove(index).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }
}

/// Bookkeeping the snapshot must capture atomically with the app state.
struct Applied {
    last_applied: Option<LogId>,
    membership: StoredMembership,
}

struct SmInner {
    db: Arc<Database>,
    app: Arc<dyn AppStateMachine>,
    /// One lock over the bookkeeping *and* every app mutation: a snapshot
    /// built under it pairs `(last_applied, membership)` with exactly the
    /// app state those describe, never half a batch later.
    applied: Mutex<Applied>,
}

/// The state machine adapter: volatile app state, persistent snapshots.
#[derive(Clone)]
pub(super) struct StateMachineStore {
    inner: Arc<SmInner>,
}

impl StateMachineStore {
    /// Rebuild the app state from the last persisted snapshot, if any. Log
    /// entries after it are replayed by openraft on startup — that pairing
    /// is what makes a volatile state machine safe.
    pub(super) fn open(db: Arc<Database>, app: Arc<dyn AppStateMachine>) -> Result<Self> {
        let mut applied = Applied {
            last_applied: None,
            membership: StoredMembership::default(),
        };
        let meta: Option<SnapshotMeta> =
            read_meta(&db, KEY_SNAPSHOT_META).map_err(|err| anyhow::anyhow!("{err}"))?;
        if let Some(meta) = meta {
            let data: Option<Vec<u8>> =
                read_meta(&db, KEY_SNAPSHOT_DATA).map_err(|err| anyhow::anyhow!("{err}"))?;
            let data = data.context("snapshot meta without snapshot data")?;
            app.restore(&data);
            applied.last_applied = meta.last_log_id;
            applied.membership = meta.last_membership;
        }
        Ok(Self {
            inner: Arc::new(SmInner {
                db,
                app,
                applied: Mutex::new(applied),
            }),
        })
    }

    fn persist_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), StorageError> {
        // Meta and data commit together: a snapshot that exists without its
        // descriptor (or the reverse) reads as no snapshot at all.
        let meta_bytes = serde_json::to_vec(meta).map_err(write_err)?;
        let data_bytes = serde_json::to_vec(&data).map_err(write_err)?;
        let txn = self.inner.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(META).map_err(write_err)?;
            table
                .insert(KEY_SNAPSHOT_META, meta_bytes.as_slice())
                .map_err(write_err)?;
            table
                .insert(KEY_SNAPSHOT_DATA, data_bytes.as_slice())
                .map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }
}

pub(super) struct SnapshotBuilder {
    inner: Arc<SmInner>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let (data, meta) = {
            let applied = self.inner.applied.lock().expect("applied lock");
            let data = self.inner.app.snapshot();
            let snapshot_id = format!(
                "{}-{}",
                applied.last_applied.map(|id| id.index).unwrap_or(0),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            let meta = SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership.clone(),
                snapshot_id,
            };
            (data, meta)
        };

        let store = StateMachineStore {
            inner: Arc::clone(&self.inner),
        };
        store.persist_snapshot(&meta, &data)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let applied = self.inner.applied.lock().expect("applied lock");
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut applied = self.inner.applied.lock().expect("applied lock");
        for entry in entries {
            let response = match entry.payload {
                openraft::EntryPayload::Blank => Vec::new(),
                openraft::EntryPayload::Normal(ref command) => self.inner.app.apply(command),
                openraft::EntryPayload::Membership(ref membership) => {
                    applied.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    Vec::new()
                }
            };
            applied.last_applied = Some(entry.log_id);
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let data = snapshot.into_inner();
        // Persist before replacing live state: a crash between the two
        // leaves either the old state (snapshot re-sent) or the new
        // (rebuilt on open) — never neither.
        self.persist_snapshot(meta, &data)?;
        let mut applied = self.inner.applied.lock().expect("applied lock");
        self.inner.app.restore(&data);
        applied.last_applied = meta.last_log_id;
        applied.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let meta: Option<SnapshotMeta> = read_meta(&self.inner.db, KEY_SNAPSHOT_META)?;
        let Some(meta) = meta else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = read_meta(&self.inner.db, KEY_SNAPSHOT_DATA)?;
        let Some(data) = data else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::{StoreBuilder, Suite};

    /// A state machine that remembers nothing; the storage suite never
    /// applies app commands (openraft cannot construct a `C::D`), it
    /// exercises logs, votes, membership, and snapshots.
    struct NullApp;

    impl AppStateMachine for NullApp {
        fn apply(&self, _command: &[u8]) -> Vec<u8> {
            Vec::new()
        }
        fn snapshot(&self) -> Vec<u8> {
            Vec::new()
        }
        fn restore(&self, _snapshot: &[u8]) {}
    }

    struct Builder;

    /// Keeps the store's directory alive for the duration of one suite case.
    struct Guard(#[allow(dead_code)] tempfile::TempDir);

    impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, Guard> for Builder {
        async fn build(&self) -> Result<(Guard, LogStore, StateMachineStore), StorageError> {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = open(&dir.path().join("raft.redb")).expect("open store");
            let log_store = LogStore::new(Arc::clone(&db));
            let state_machine =
                StateMachineStore::open(db, Arc::new(NullApp)).expect("open state machine");
            Ok((Guard(dir), log_store, state_machine))
        }
    }

    /// openraft's own storage conformance suite, run against the redb
    /// store — the same idea as running the node/shard contract suites
    /// against every metadata backend: the trait's semantics are the
    /// author's tests, not ours to re-invent.
    #[test]
    fn satisfies_openrafts_storage_contract() {
        Suite::test_all(Builder).expect("storage suite");
    }

    /// The one property the suite cannot see: state survives closing and
    /// reopening the file. A vote that does not is a double vote waiting
    /// for a crash.
    #[test]
    fn vote_and_purge_marker_survive_reopen() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("raft.redb");

            let vote = openraft::Vote::new(7, 3);
            let purged = LogId::new(openraft::CommittedLeaderId::new(7, 3), 11);
            {
                let db = open(&path).expect("open");
                let mut store = LogStore::new(db);
                store.save_vote(&vote).await.expect("save vote");
                store.purge(purged).await.expect("purge");
            }

            let db = open(&path).expect("reopen");
            let mut store = LogStore::new(db);
            assert_eq!(store.read_vote().await.expect("read vote"), Some(vote));
            let state = store.get_log_state().await.expect("log state");
            assert_eq!(state.last_purged_log_id, Some(purged));
            assert_eq!(state.last_log_id, Some(purged));
        });
    }
}
