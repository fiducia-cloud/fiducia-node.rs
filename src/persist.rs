//! Crash-safe Raft persistence for a single shard.
//!
//! Raft requires its hard state and replicated data to survive a restart
//! **before** the node acts on them, or its safety guarantees break:
//! `current_term`, `voted_for`, the replicated `log`, and the latest
//! state-machine snapshot. Without them a restarted member could vote twice in
//! one term or forget an entry it had already acknowledged — and because this
//! engine keeps its state machine in memory, a pod restart would otherwise drop
//! every committed record, letting a "leader + one follower" group collapse to a
//! single durable copy. This module gives each shard a small on-disk home for
//! that state, fsync'd before the caller treats the matching action as durable.
//!
//! We also persist `commit_index`. The Raft paper treats it as volatile, but it
//! does so assuming a persisted state machine; here the state machine is restored
//! from a snapshot and replays the retained suffix up to `commit_index`, so the
//! pointer has to survive too.
//!
//! Layout, under `<data_dir>/shard-<id>/`:
//!   * `meta` — JSON `{current_term, voted_for, commit_index}`, written with the
//!     write-tmp → fsync → rename → fsync(dir) dance so a torn write can't
//!     corrupt the live copy.
//!   * `log`  — newline-delimited JSON, one [`LogEntry`] per line, appended and
//!     fsync'd. A trailing torn line (crash mid-append) is dropped on load and
//!     the file is canonicalized so the next append starts from clean bytes.
//!   * `snapshot` — atomic JSON state-machine image plus its last included
//!     index/term. Once durable, the matching log prefix is removed.
//!
//! NOTE: fsync here is synchronous inside the shard actor's task — correctness
//! over throughput for this build. A high-write deployment should move the log
//! to a batched group-commit writer; the call sites are already the only places
//! that touch the disk, so that change stays local.

#[cfg(test)]
use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::consensus::LogEntry;
use fiducia_routing::ShardId;

#[derive(Debug, Default, Serialize, Deserialize)]
struct Meta {
    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
}

/// Durable state-machine image at one committed Raft index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state: Vec<u8>,
}

/// Raft state recovered from disk at boot. Empty (all-zero, empty log) for a
/// fresh shard with no prior on-disk state.
#[derive(Debug, Default)]
pub struct Recovered {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub commit_index: u64,
    pub log: Vec<LogEntry>,
    pub snapshot: Option<PersistedSnapshot>,
}

/// A shard's durable store: the `meta` file plus an append handle to `log`.
pub struct ShardStore {
    dir: PathBuf,
    log_path: PathBuf,
    meta_path: PathBuf,
    snapshot_path: PathBuf,
    log_file: File,
    /// Number of log entries known to be on disk (so appends write only the tail).
    durable_len: usize,
    #[cfg(test)]
    fail_next: Cell<Option<PersistOp>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistOp {
    Meta,
    Append,
    Rewrite,
    Snapshot,
}

impl ShardStore {
    /// Open (creating if needed) the store for `shard_id` under `root`, returning
    /// it alongside the [`Recovered`] state to seed the actor. On open the log is
    /// canonicalized: any torn trailing record is dropped and the file rewritten
    /// so subsequent appends start from clean bytes.
    pub fn open(root: &Path, shard_id: ShardId) -> io::Result<(Self, Recovered)> {
        let dir = root.join(format!("shard-{shard_id}"));
        fs::create_dir_all(&dir)?;
        let meta_path = dir.join("meta");
        let log_path = dir.join("log");
        let snapshot_path = dir.join("snapshot");

        let persisted_meta: Option<Meta> = read_optional_json(&meta_path, "raft meta")?;
        let meta_was_missing = persisted_meta.is_none();
        let meta = persisted_meta.unwrap_or_default();
        if meta.current_term == 0 && meta.voted_for.is_some() {
            return Err(invalid_data("raft meta contains a vote in term zero"));
        }

        // Parse every complete JSON line. Only a malformed final fragment without
        // a newline can be a torn append; malformed complete records are durable
        // corruption and must abort recovery.
        let snapshot: Option<PersistedSnapshot> =
            read_optional_json(&snapshot_path, "raft snapshot")?;
        let snapshot_index = snapshot
            .as_ref()
            .map(|value| value.last_included_index)
            .unwrap_or(0);
        let snapshot_term = snapshot
            .as_ref()
            .map(|value| value.last_included_term)
            .unwrap_or(0);
        if (snapshot_index == 0) != (snapshot_term == 0) {
            return Err(invalid_data(format!(
                "snapshot index/term mismatch: index={snapshot_index}, term={snapshot_term}"
            )));
        }

        let mut log: Vec<LogEntry> = Vec::new();
        let log_bytes = match fs::read(&log_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        let terminated = log_bytes.ends_with(b"\n");
        let mut previous_index: Option<u64> = None;
        let mut previous_term: Option<u64> = None;
        let lines: Vec<&[u8]> = log_bytes.split(|byte| *byte == b'\n').collect();
        for (line_number, line) in lines.iter().enumerate() {
            let is_last = line_number + 1 == lines.len();
            if line.is_empty() {
                if is_last && (terminated || log_bytes.is_empty()) {
                    continue;
                }
                return Err(invalid_data(format!(
                    "raft log contains an empty record at line {}",
                    line_number + 1
                )));
            }
            let entry = match serde_json::from_slice::<LogEntry>(line) {
                Ok(entry) => entry,
                Err(_) if is_last && !terminated => {
                    // A crash can leave only the final, unterminated JSON fragment.
                    // It was never acknowledged durable, so discard precisely that
                    // fragment. A malformed newline-terminated record is corruption.
                    break;
                }
                Err(error) => {
                    return Err(invalid_data(format!(
                        "invalid raft log record at line {}: {error}",
                        line_number + 1
                    )));
                }
            };
            if entry.index == 0 || entry.term == 0 {
                return Err(invalid_data(format!(
                    "raft log record at line {} has zero index or term",
                    line_number + 1
                )));
            }
            if let Some(previous) = previous_term {
                if entry.term < previous {
                    return Err(invalid_data(format!(
                        "raft log term descends at line {}: previous {}, found {}",
                        line_number + 1,
                        previous,
                        entry.term
                    )));
                }
            }
            previous_term = Some(entry.term);
            if let Some(previous) = previous_index {
                let expected = previous
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("raft log index overflow"))?;
                if entry.index != expected {
                    return Err(invalid_data(format!(
                        "raft log index gap at line {}: expected {}, found {}",
                        line_number + 1,
                        expected,
                        entry.index
                    )));
                }
            }
            previous_index = Some(entry.index);
            if entry.index == snapshot_index && entry.term != snapshot_term {
                return Err(invalid_data(format!(
                    "raft log term {} at snapshot index {} disagrees with snapshot term {}",
                    entry.term, snapshot_index, snapshot_term
                )));
            }
            if entry.index > snapshot_index {
                let expected = snapshot_index
                    .checked_add(log.len() as u64)
                    .and_then(|index| index.checked_add(1))
                    .ok_or_else(|| invalid_data("raft log suffix index overflow"))?;
                if entry.index != expected {
                    return Err(invalid_data(format!(
                        "raft log suffix gap after snapshot: expected index {expected}, found {}",
                        entry.index
                    )));
                }
                log.push(entry);
            }
        }

        let durable_tail = log
            .last()
            .map(|entry| entry.index)
            .unwrap_or(snapshot_index);
        if meta_was_missing && (snapshot.is_some() || !log.is_empty()) {
            return Err(invalid_data(
                "raft meta is missing while durable snapshot/log state exists",
            ));
        }
        let durable_term = meta.current_term;
        if snapshot_term > durable_term || log.iter().any(|entry| entry.term > durable_term) {
            return Err(invalid_data(format!(
                "durable snapshot/log term exceeds persisted current term {durable_term}"
            )));
        }
        if meta.commit_index > durable_tail {
            return Err(invalid_data(format!(
                "persisted commit index {} exceeds durable snapshot/log tail {durable_tail}",
                meta.commit_index
            )));
        }
        // A durable snapshot is itself proof that its included index committed.
        // This can legitimately be newer than meta after a crash between the
        // snapshot rename and the following meta rename, so recover upward only.
        let recovered_commit = meta.commit_index.max(snapshot_index);

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)?;
        let mut store = ShardStore {
            dir,
            log_path,
            meta_path,
            snapshot_path,
            log_file,
            durable_len: 0,
            #[cfg(test)]
            fail_next: Cell::new(None),
        };
        // Canonicalize: rewrite the file to exactly the entries we trust.
        store.rewrite(&log)?;
        if meta_was_missing {
            // Establish a durable empty hard-state record before this store can
            // subsequently acquire a vote, append a log entry, or install a
            // snapshot. Future recovery can then distinguish a fresh shard from
            // externally deleted/corrupt metadata.
            store.save_meta(0, None, 0)?;
        }

        let recovered = Recovered {
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            commit_index: recovered_commit,
            log,
            snapshot,
        };
        Ok((store, recovered))
    }

    /// Durably record the hard state. Atomic via tmp-file + rename + dir fsync.
    pub fn save_meta(
        &self,
        current_term: u64,
        voted_for: Option<&str>,
        commit_index: u64,
    ) -> io::Result<()> {
        #[cfg(test)]
        self.maybe_fail(PersistOp::Meta)?;
        let meta = Meta {
            current_term,
            voted_for: voted_for.map(str::to_string),
            commit_index,
        };
        let bytes =
            serde_json::to_vec(&meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tmp = self.meta_path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.meta_path)?;
        sync_dir(&self.dir)
    }

    /// Append the entries beyond what's already durable, then fsync. The caller
    /// must use [`Self::rewrite`] instead whenever the log was truncated (a
    /// conflicting suffix replaced), since this only ever extends the file.
    pub fn append_tail(&mut self, log: &[LogEntry]) -> io::Result<()> {
        if log.len() <= self.durable_len {
            return Ok(());
        }
        #[cfg(test)]
        self.maybe_fail(PersistOp::Append)?;
        let mut buf = Vec::new();
        for entry in &log[self.durable_len..] {
            serde_json::to_writer(&mut buf, entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            buf.push(b'\n');
        }
        self.log_file.write_all(&buf)?;
        self.log_file.sync_all()?;
        self.durable_len = log.len();
        Ok(())
    }

    /// Replace the entire log file with `log` (used when Raft truncates a
    /// conflicting suffix). Atomic via tmp-file + rename + dir fsync; reopens the
    /// append handle on the fresh inode.
    pub fn rewrite(&mut self, log: &[LogEntry]) -> io::Result<()> {
        #[cfg(test)]
        self.maybe_fail(PersistOp::Rewrite)?;
        let tmp = self.log_path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            let mut buf = Vec::new();
            for entry in log {
                serde_json::to_writer(&mut buf, entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                buf.push(b'\n');
            }
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.log_path)?;
        sync_dir(&self.dir)?;
        self.log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.log_path)?;
        self.durable_len = log.len();
        Ok(())
    }

    /// Atomically persist a state-machine snapshot, then compact the log to the
    /// suffix after it. Writing the snapshot first makes a crash between the two
    /// steps safe: recovery filters the still-present prefix by snapshot index.
    pub fn save_snapshot(
        &mut self,
        snapshot: &PersistedSnapshot,
        remaining_log: &[LogEntry],
    ) -> io::Result<()> {
        #[cfg(test)]
        self.maybe_fail(PersistOp::Snapshot)?;
        let tmp = self.snapshot_path.with_extension("tmp");
        {
            let mut file = File::create(&tmp)?;
            serde_json::to_writer(&mut file, snapshot)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.snapshot_path)?;
        sync_dir(&self.dir)?;
        self.rewrite(remaining_log)
    }

    #[cfg(test)]
    pub(crate) fn fail_next(&self, operation: PersistOp) {
        self.fail_next.set(Some(operation));
    }

    #[cfg(test)]
    fn maybe_fail(&self, operation: PersistOp) -> io::Result<()> {
        if self.fail_next.get() == Some(operation) {
            self.fail_next.set(None);
            return Err(io::Error::other(format!(
                "injected {operation:?} persistence failure"
            )));
        }
        Ok(())
    }
}

fn read_optional_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    description: &str,
) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Err(invalid_data(format!(
            "{description} file is empty: {}",
            path.display()
        ))),
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| invalid_data(format!("invalid {description}: {error}"))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// fsync a directory so a rename of one of its entries is itself durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Command;

    fn entry(index: u64, term: u64, key: &str) -> LogEntry {
        LogEntry {
            term,
            index,
            proposed_at_ms: 1_000,
            command: Some(Command::KvPut {
                key: key.to_string(),
                value: "v".to_string(),
                ttl_ms: None,
                prev_revision: None,
            }),
        }
    }

    fn tmpdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "fiducia-persist-test-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn open_error(root: &Path, shard: ShardId) -> io::Error {
        match ShardStore::open(root, shard) {
            Ok(_) => panic!("corrupt shard store unexpectedly opened"),
            Err(error) => error,
        }
    }

    #[test]
    fn fresh_shard_recovers_empty() {
        let root = tmpdir();
        let (_store, rec) = ShardStore::open(&root, 3).unwrap();
        assert_eq!(rec.current_term, 0);
        assert_eq!(rec.voted_for, None);
        assert_eq!(rec.commit_index, 0);
        assert!(rec.log.is_empty());
    }

    #[test]
    fn meta_and_log_round_trip_across_reopen() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 1).unwrap();
            store.save_meta(7, Some("node-b"), 2).unwrap();
            store
                .append_tail(&[entry(1, 7, "a"), entry(2, 7, "b")])
                .unwrap();
        }
        let (_store, rec) = ShardStore::open(&root, 1).unwrap();
        assert_eq!(rec.current_term, 7);
        assert_eq!(rec.voted_for.as_deref(), Some("node-b"));
        assert_eq!(rec.commit_index, 2);
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.log[1].index, 2);
    }

    #[test]
    fn append_tail_only_writes_new_entries() {
        let root = tmpdir();
        let (mut store, _) = ShardStore::open(&root, 0).unwrap();
        store.save_meta(1, None, 0).unwrap();
        store.append_tail(&[entry(1, 1, "a")]).unwrap();
        let log = vec![entry(1, 1, "a"), entry(2, 1, "b")];
        store.append_tail(&log).unwrap(); // appends only index 2
        let (_s, rec) = ShardStore::open(&root, 0).unwrap();
        assert_eq!(rec.log.len(), 2);
    }

    #[test]
    fn rewrite_replaces_conflicting_suffix() {
        let root = tmpdir();
        let (mut store, _) = ShardStore::open(&root, 0).unwrap();
        store
            .append_tail(&[entry(1, 1, "a"), entry(2, 1, "b")])
            .unwrap();
        // Truncate index 2 and replace it with a higher-term entry.
        store.save_meta(5, None, 0).unwrap();
        store
            .rewrite(&[entry(1, 1, "a"), entry(2, 5, "c")])
            .unwrap();
        let (_s, rec) = ShardStore::open(&root, 0).unwrap();
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.log[1].term, 5);
    }

    #[test]
    fn torn_trailing_record_is_dropped_on_load() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 9).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
        }
        // Simulate a crash mid-append: a partial JSON line with no newline.
        let log_path = root.join("shard-9").join("log");
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(b"{\"term\":1,\"index\":2,\"comm").unwrap();
        f.sync_all().unwrap();
        drop(f);

        let (_store, rec) = ShardStore::open(&root, 9).unwrap();
        assert_eq!(rec.log.len(), 1, "torn record must be dropped");
        assert_eq!(rec.log[0].index, 1);
    }

    #[test]
    fn newline_terminated_corruption_is_rejected_instead_of_truncated() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 10).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
        }
        let log_path = root.join("shard-10").join("log");
        let mut file = OpenOptions::new().append(true).open(log_path).unwrap();
        file.write_all(b"not-json\n").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let error = open_error(&root, 10);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("line 2"));
    }

    #[test]
    fn gapped_log_is_rejected_instead_of_reindexed_by_vector_length() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 11).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store
                .rewrite(&[entry(1, 1, "a"), entry(3, 1, "missing-two")])
                .unwrap();
        }

        let error = open_error(&root, 11);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("index gap"));
    }

    #[test]
    fn commit_beyond_durable_tail_is_rejected_instead_of_clamped_down() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 12).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
            store.save_meta(1, Some("node-a"), 2).unwrap();
        }

        let error = open_error(&root, 12);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("commit index 2"));
    }

    #[test]
    fn missing_meta_with_existing_log_is_rejected() {
        let root = tmpdir();
        let shard_dir = root.join("shard-13");
        fs::create_dir_all(&shard_dir).unwrap();
        let mut log = File::create(shard_dir.join("log")).unwrap();
        serde_json::to_writer(&mut log, &entry(1, 1, "orphaned")).unwrap();
        log.write_all(b"\n").unwrap();
        log.sync_all().unwrap();

        let error = open_error(&root, 13);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("meta is missing"));
    }

    #[test]
    fn log_term_ahead_of_persisted_term_is_rejected() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 14).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store.append_tail(&[entry(1, 2, "future")]).unwrap();
        }

        let error = open_error(&root, 14);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("term exceeds"));
    }

    #[test]
    fn descending_log_terms_are_rejected_as_impossible_raft_history() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 15).unwrap();
            store.save_meta(3, None, 0).unwrap();
            store
                .rewrite(&[entry(1, 3, "newer"), entry(2, 2, "older")])
                .unwrap();
        }

        let error = open_error(&root, 15);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("term descends"));
    }

    #[test]
    fn term_one_log_cannot_hide_behind_term_zero_hard_state() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 16).unwrap();
            store.append_tail(&[entry(1, 1, "future")]).unwrap();
        }

        let error = open_error(&root, 16);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("persisted current term 0"));
    }

    #[test]
    fn snapshot_compacts_log_and_survives_reopen() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 4).unwrap();
            store
                .append_tail(&[
                    entry(1, 2, "old-a"),
                    entry(2, 2, "old-b"),
                    entry(3, 3, "tail"),
                ])
                .unwrap();
            store.save_meta(3, Some("node-a"), 3).unwrap();
            store
                .save_snapshot(
                    &PersistedSnapshot {
                        last_included_index: 2,
                        last_included_term: 2,
                        state: br#"{"revision":2}"#.to_vec(),
                    },
                    &[entry(3, 3, "tail")],
                )
                .unwrap();
        }
        let (_store, recovered) = ShardStore::open(&root, 4).unwrap();
        let snapshot = recovered.snapshot.unwrap();
        assert_eq!(snapshot.last_included_index, 2);
        assert_eq!(snapshot.last_included_term, 2);
        assert_eq!(recovered.log.len(), 1);
        assert_eq!(recovered.log[0].index, 3);
    }

    #[test]
    fn snapshot_is_a_commit_floor_after_interrupted_meta_update() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 5).unwrap();
            store.save_meta(2, None, 0).unwrap();
            store
                .append_tail(&[entry(1, 1, "a"), entry(2, 2, "b")])
                .unwrap();
            store
                .save_snapshot(
                    &PersistedSnapshot {
                        last_included_index: 2,
                        last_included_term: 2,
                        state: br#"{"revision":2}"#.to_vec(),
                    },
                    &[],
                )
                .unwrap();
            // Deliberately omit save_meta: this models a crash after the atomic
            // snapshot rename but before the commit pointer rename.
        }

        let (_store, recovered) = ShardStore::open(&root, 5).unwrap();
        assert_eq!(recovered.commit_index, 2);
        assert_eq!(recovered.snapshot.unwrap().last_included_index, 2);
    }
}
