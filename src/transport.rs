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

/// InstallSnapshot transfers compacted state to a follower whose next required
/// log entry is no longer retained.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReq {
    pub term: u64,
    pub leader_id: String,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResp {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// `RequestVote` — a candidate solicits a vote for one shard's Raft group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReq {
    /// Candidate's term. For a **pre-vote** this is the candidate's *would-be*
    /// term (`current_term + 1`) which it has **not** actually adopted.
    pub term: u64,
    /// Candidate's addressable node id.
    pub candidate_id: String,
    /// Candidate's last log index (the up-to-date check).
    pub last_log_index: u64,
    /// Term of the candidate's last log entry.
    pub last_log_term: u64,
    /// PreVote round (Raft thesis §9.6): a non-binding straw poll the candidate
    /// runs *before* incrementing its term, so a partitioned node that keeps
    /// timing out can't inflate its term and force a healthy leader to step down
    /// when it rejoins. A voter granting a pre-vote changes **no** state.
    /// `#[serde(default)]` keeps it wire-compatible with peers that never send it.
    #[serde(default)]
    pub pre_vote: bool,
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
                // Total request timeouts are request-specific below: a vote or
                // append should fail quickly, while a bounded snapshot needs a
                // materially longer transfer window.
                .connect_timeout(crate::peer_config::rpc_timeout())
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
                let body = encode_peer_body("append", peer, shard, &req)?;
                let response = with_internal_auth(client.post(url))
                    .header("content-type", "application/json")
                    .timeout(crate::peer_config::rpc_timeout())
                    .body(body)
                    .send()
                    .await;
                decode_peer_response("append", peer, shard, response).await
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
                let response = with_internal_auth(client.post(url))
                    .json(&req)
                    .timeout(crate::peer_config::rpc_timeout())
                    .send()
                    .await;
                decode_peer_response("vote", peer, shard, response).await
            }
        }
    }

    /// Send a compacted state-machine snapshot to a lagging follower.
    pub async fn install_snapshot(
        &self,
        peer: &str,
        shard: ShardId,
        req: InstallSnapshotReq,
    ) -> Option<InstallSnapshotResp> {
        match self {
            Transport::Loopback(reg) => {
                let inbox = reg.sender(peer, shard)?;
                let (resp, rx) = oneshot::channel();
                inbox
                    .send(ShardMsg::InstallSnapshot { req, resp })
                    .await
                    .ok()?;
                rx.await.ok()
            }
            Transport::Http(client) => {
                let url = format!("http://{peer}/raft/{shard}/snapshot");
                let body = encode_peer_body("snapshot", peer, shard, &req)?;
                let response = with_internal_auth(client.post(url))
                    .header("content-type", "application/json")
                    .timeout(crate::peer_config::snapshot_timeout())
                    .body(body)
                    .send()
                    .await;
                decode_peer_response("snapshot", peer, shard, response).await
            }
        }
    }
}

fn encode_peer_body<T: Serialize>(
    rpc: &str,
    peer: &str,
    shard: ShardId,
    request: &T,
) -> Option<Vec<u8>> {
    let body = match serde_json::to_vec(request) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(rpc, peer, shard, %error, "failed to serialize Raft peer request");
            return None;
        }
    };
    let limit = crate::peer_config::max_body_bytes();
    if body.len() > limit {
        tracing::error!(
            rpc,
            peer,
            shard,
            body_bytes = body.len(),
            max_body_bytes = limit,
            "Raft peer request exceeds the configured body limit"
        );
        return None;
    }
    Some(body)
}

async fn decode_peer_response<T: for<'de> Deserialize<'de>>(
    rpc: &str,
    peer: &str,
    shard: ShardId,
    response: Result<reqwest::Response, reqwest::Error>,
) -> Option<T> {
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(rpc, peer, shard, %error, "Raft peer request failed");
            return None;
        }
    };
    if !response.status().is_success() {
        tracing::warn!(
            rpc,
            peer,
            shard,
            status = %response.status(),
            "Raft peer rejected request"
        );
        return None;
    }
    match response.json().await {
        Ok(decoded) => Some(decoded),
        Err(error) => {
            tracing::warn!(rpc, peer, shard, %error, "Raft peer returned an invalid response");
            None
        }
    }
}

/// Attach the trusted-hop header to an outbound `/raft` RPC when the cluster
/// secret is configured, so peers running with the guard enabled accept it.
fn with_internal_auth(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match crate::internal_auth::secret() {
        Some(secret) => builder.header(crate::internal_auth::INTERNAL_AUTH_HEADER, secret),
        None => builder,
    }
}
