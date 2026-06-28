//! Peer transport — how a shard's Raft messages reach the *same shard's* actor
//! on another node.
//!
//! One [`Transport`] serves every shard on a node: a leader sends `AppendEntries`
//! / `RequestVote` to a peer for a specific shard, and the peer demuxes the call
//! to that shard's [`ShardActor`](crate::consensus) inbox. There are two backings,
//! chosen at bootstrap:
//!
//!   * [`Transport::Http`] — production: a `reqwest` client posting JSON to the
//!     peer's `/raft/{shard}/…` endpoints (served by [`crate::raft_api`]).
//!   * [`Transport::Loopback`] — tests: an in-process registry of every node's
//!     shard inboxes, so a whole multi-node cluster runs deterministically in one
//!     process with no sockets. This is what makes the Raft logic unit-testable.
//!
//! The two are interchangeable because both ultimately deliver a [`ShardMsg`] to
//! the target shard actor and await its reply over a `oneshot`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::consensus::{LogEntry, ShardMsg};
use fiducia_routing::ShardId;

// ---------------------------------------------------------------------------
// Raft RPC wire types. These cross the network (HTTP) and the in-process
// loopback identically, so they are plain serializable structs.
// ---------------------------------------------------------------------------

/// `AppendEntries` — the leader's log-replication + heartbeat RPC for one shard.
/// With an empty `entries` it is a pure heartbeat (also carries `leader_commit`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    /// Leader's term.
    pub term: u64,
    /// Leader id (its addressable node id) so followers can redirect clients.
    pub leader_id: String,
    /// Index of the log entry immediately preceding `entries` (0 = none).
    pub prev_log_index: u64,
    /// Term of the `prev_log_index` entry (the consistency check).
    pub prev_log_term: u64,
    /// New entries to store (empty = heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's `commit_index`, so the follower can advance its own.
    pub leader_commit: u64,
}

/// Reply to [`AppendEntriesReq`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    /// Follower's `current_term` (lets a stale leader discover it must step down).
    pub term: u64,
    /// Whether the consistency check passed and the entries were stored.
    pub success: bool,
    /// Follower's last log index afterward — lets the leader set `match_index`
    /// on success and fast-rewind `next_index` on failure.
    pub match_index: u64,
}

/// `RequestVote` — a candidate solicits a vote for one shard's Raft group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReq {
    /// Candidate's term.
    pub term: u64,
    /// Candidate's addressable node id.
    pub candidate_id: String,
    /// Candidate's last log index (the up-to-date check).
    pub last_log_index: u64,
    /// Term of the candidate's last log entry.
    pub last_log_term: u64,
}

/// Reply to [`RequestVoteReq`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResp {
    /// Voter's `current_term` (lets a stale candidate discover it must step down).
    pub term: u64,
    /// Whether the vote was granted.
    pub granted: bool,
}

// ---------------------------------------------------------------------------
// Loopback registry: an in-process directory of node_id → shard → inbox.
// ---------------------------------------------------------------------------

/// Shared map of every loopback node's shard inboxes. Cloned (cheaply, it is an
/// `Arc`) into each node's [`Transport::Loopback`] and into the test harness, so
/// a node can reach any peer's shard actor by `node_id`.
type LoopbackShardInboxes = HashMap<ShardId, mpsc::Sender<ShardMsg>>;
type LoopbackNodes = HashMap<String, LoopbackShardInboxes>;

#[derive(Clone, Default)]
pub struct LoopbackRegistry {
    nodes: Arc<Mutex<LoopbackNodes>>,
}

#[allow(dead_code)] // the loopback registry is the in-process test harness
impl LoopbackRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one shard inbox for a node (called per shard at bootstrap).
    pub fn register(&self, node_id: &str, shard: ShardId, inbox: mpsc::Sender<ShardMsg>) {
        self.nodes
            .lock()
            .unwrap()
            .entry(node_id.to_string())
            .or_default()
            .insert(shard, inbox);
    }

    /// Remove a whole node from the registry — used to simulate a node going
    /// away (its peers can then no longer reach it, like a crash/partition).
    pub fn deregister(&self, node_id: &str) {
        self.nodes.lock().unwrap().remove(node_id);
    }

    fn sender(&self, node_id: &str, shard: ShardId) -> Option<mpsc::Sender<ShardMsg>> {
        self.nodes
            .lock()
            .unwrap()
            .get(node_id)?
            .get(&shard)
            .cloned()
    }
}

// ---------------------------------------------------------------------------
// Transport: the outbound side a leader/candidate uses to reach peers.
// ---------------------------------------------------------------------------

/// Outbound peer RPC. `None` from a send means "couldn't reach the peer this
/// time" — Raft tolerates dropped messages, so callers simply retry on the next
/// tick.
pub enum Transport {
    /// In-process delivery to another node's shard inbox (the test harness).
    #[allow(dead_code)]
    Loopback(LoopbackRegistry),
    /// JSON-over-HTTP to a peer's `/raft/{shard}/…` endpoints (production).
    Http(reqwest::Client),
}

impl Transport {
    #[allow(dead_code)]
    pub fn loopback(registry: LoopbackRegistry) -> Self {
        Transport::Loopback(registry)
    }

    /// Production HTTP transport. Peers are addressed as `host:port`; intra-cluster
    /// traffic is plain HTTP (no TLS backend pulled in).
    pub fn http() -> Self {
        Transport::Http(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap_or_default(),
        )
    }

    /// If loopback, the registry to register local shard inboxes into.
    pub fn loopback_registry(&self) -> Option<&LoopbackRegistry> {
        match self {
            Transport::Loopback(r) => Some(r),
            Transport::Http(_) => None,
        }
    }

    /// Send `AppendEntries` to `peer` for `shard`; `None` if unreachable.
    pub async fn append_entries(
        &self,
        peer: &str,
        shard: ShardId,
        req: AppendEntriesReq,
    ) -> Option<AppendEntriesResp> {
        match self {
            Transport::Loopback(reg) => {
                let inbox = reg.sender(peer, shard)?;
                let (resp, rx) = oneshot::channel();
                inbox
                    .send(ShardMsg::AppendEntries { req, resp })
                    .await
                    .ok()?;
                rx.await.ok()
            }
            Transport::Http(client) => {
                let url = format!("http://{peer}/raft/{shard}/append");
                client
                    .post(url)
                    .json(&req)
                    .send()
                    .await
                    .ok()?
                    .json()
                    .await
                    .ok()
            }
        }
    }

    /// Send `RequestVote` to `peer` for `shard`; `None` if unreachable.
    pub async fn request_vote(
        &self,
        peer: &str,
        shard: ShardId,
        req: RequestVoteReq,
    ) -> Option<RequestVoteResp> {
        match self {
            Transport::Loopback(reg) => {
                let inbox = reg.sender(peer, shard)?;
                let (resp, rx) = oneshot::channel();
                inbox.send(ShardMsg::RequestVote { req, resp }).await.ok()?;
                rx.await.ok()
            }
            Transport::Http(client) => {
                let url = format!("http://{peer}/raft/{shard}/vote");
                client
                    .post(url)
                    .json(&req)
                    .send()
                    .await
                    .ok()?
                    .json()
                    .await
                    .ok()
            }
        }
    }
}
