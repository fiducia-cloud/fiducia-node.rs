//! Crash-safe Raft persistence for a single shard.
//!
//! Raft requires three pieces of state to survive a restart **before** the node
//! acts on them, or its safety guarantees break: `current_term`, `voted_for`,
//! and the replicated `log`. Without them a restarted member could vote twice in
//! one term or forget an entry it had already acknowledged — and because this
//! engine keeps its state machine in memory, a pod restart would otherwise drop
//! every committed record, letting a "leader + one follower" group collapse to a
//! single durable copy. This module gives each shard a small on-disk home for
//! that state, fsync'd before the caller treats the matching action as durable.
//!
//! We also persist `commit_index`. The Raft paper treats it as volatile, but it
//! does so assuming a persisted state machine; here the state machine is rebuilt
//! by **replaying the log up to `commit_index`** at boot, so the pointer has to
//! survive too.
//!
//! Layout, under `<data_dir>/shard-<id>/`:
//!   * `meta` — JSON `{current_term, voted_for, commit_index}`, written with the
//!     write-tmp → fsync → rename → fsync(dir) dance so a torn write can't
//!     corrupt the live copy.
//!   * `log`  — newline-delimited JSON, one [`LogEntry`] per line, appended and
//!     fsync'd. A trailing torn line (crash mid-append) is dropped on load and
//!     the file is canonicalized so the next append starts from clean bytes.
//!
//! NOTE: fsync here is synchronous inside the shard actor's task — correctness
//! over throughput for this build. A high-write deployment should move the log
//! to a batched group-commit writer; the call sites are already the only places
//! that touch the disk, so that change stays local.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
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

/// Raft state recovered from disk at boot. Empty (all-zero, empty log) for a
/// fresh shard with no prior on-disk state.
#[derive(Debug, Default)]
pub struct Recovered {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub commit_index: u64,
    pub log: Vec<LogEntry>,
}

/// A shard's durable store: the `meta` file plus an append handle to `log`.
pub struct ShardStore {
    dir: PathBuf,
    log_path: PathBuf,
    meta_path: PathBuf,
    log_file: File,
    /// Number of log entries known to be on disk (so appends write only the tail).
    durable_len: usize,
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

        let meta: Meta = match fs::read(&meta_path) {
            Ok(bytes) if !bytes.is_empty() => serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            _ => Meta::default(),
        };

        // Parse every complete JSON line; stop at the first that fails to parse —
        // that's a record torn by a crash mid-append, and everything after it.
        let mut log: Vec<LogEntry> = Vec::new();
        if let Ok(file) = File::open(&log_path) {
            for line in BufReader::new(file).split(b'\n') {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<LogEntry>(&line) {
                    Ok(entry) => log.push(entry),
                    Err(_) => break,
                }
            }
        }

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)?;
        let mut store = ShardStore {
            dir,
            log_path,
            meta_path,
            log_file,
            durable_len: 0,
        };
        // Canonicalize: rewrite the file to exactly the entries we trust.
        store.rewrite(&log)?;

        let recovered = Recovered {
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            commit_index: meta.commit_index,
            log,
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
        store.rewrite(&[entry(1, 1, "a"), entry(2, 5, "c")]).unwrap();
        let (_s, rec) = ShardStore::open(&root, 0).unwrap();
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.log[1].term, 5);
    }

    #[test]
    fn torn_trailing_record_is_dropped_on_load() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 9).unwrap();
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
}
