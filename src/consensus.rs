//! Raft consensus core — **sharded / multi-Raft**, actor-per-shard (skeleton).
//!
//! Fiducia does not run one Raft group for the whole keyspace. It runs *many*:
//! the keyspace is partitioned into shards, and **each shard is its own
//! independent Raft group** with its own log, term, and elected leader. A
//! physical [`Node`] hosts a replica of many shards and, at any moment, is the
//! **leader for some shards and a follower for others** (the "multi-Raft" design
//! used by CockroachDB ranges / TiKV regions).
//!
//! ## Concurrency model: one actor task per shard
//!
//! A node is a **single process** running on a multi-threaded Tokio runtime.
//! Each shard is driven by its **own async task** ([`ShardActor`]) that *owns*
//! that shard's Raft state and state machine — there are no locks on the hot
//! path. Everyone else (HTTP handlers, peer transport) talks to a shard by
//! sending it a [`ShardMsg`] over an `mpsc` channel and awaiting a `oneshot`
//! reply. This:
//!
//!   * spreads shards across all CPU cores (the runtime schedules the tasks);
//!   * isolates shards — a busy shard yields at `.await` and can't starve others;
//!   * avoids holding a lock across the network I/O a real Raft step performs.
//!
//! Shared, *not* per-shard: a single [`Transport`] multiplexes peer RPC for all
//! shards over one connection per peer, and (eventually) a single storage engine
//! batches fsync across shards. That shared I/O is the reason a node is one
//! process rather than one-process-per-shard.
//!
//! The current build is a **single-node skeleton**: each actor is the sole member
//! (and thus leader) of its shard, and "commit" means "append + apply locally".
//! The shape is the real multi-Raft shape; the cluster path slots in at the
//! `TODO`s (peer RPC, elections, replication, quorum commit, snapshotting,
//! placement). Shard placement / scaling / failure handling live in the control
//! plane, `fiducia-brain`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    http::{header::LOCATION, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::state::{
    Command, KvEntry, Leadership, LockState, RateLimitSnapshot, Schedule, ScheduleRun,
    ServiceInstance, StateMachine,
};

/// Identifier of a shard (one independent Raft group). Re-exported from the
/// shared routing crate so the type and the `key → shard` mapping can't drift
/// between the node, the load balancer, and the brain.
pub use fiducia_routing::ShardId;

/// Depth of each shard actor's inbox before senders must wait.
const SHARD_INBOX_CAPACITY: usize = 1024;

/// A node's role *within a single shard's* Raft group. A node holds a `Role` per
/// shard it replicates — `Leader` for some, `Follower` for others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One entry in a shard's replicated log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Raft term in which the entry was created (per shard).
    pub term: u64,
    /// 1-based position in the shard's log.
    pub index: u64,
    /// The state-machine command this entry carries.
    pub command: Command,
}

/// Static identity + cluster membership for this physical node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Stable identifier for this node (e.g. `node-a`).
    pub node_id: String,
    /// Addresses of peer nodes. Empty in single-node mode.
    pub peers: Vec<String>,
    /// Number of shards the keyspace is partitioned into.
    pub shard_count: u32,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: std::env::var("FIDUCIA_NODE_ID").unwrap_or_else(|_| "node-a".to_string()),
            peers: std::env::var("FIDUCIA_PEERS")
                .ok()
                .map(|s| {
                    s.split(',')
                        .filter(|p| !p.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
            shard_count: std::env::var("FIDUCIA_SHARD_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16),
        }
    }
}

// ---------------------------------------------------------------------------
// Messages: how the outside world talks to a shard actor.
// ---------------------------------------------------------------------------

/// A message in a shard actor's inbox. Replies come back over the embedded
/// `oneshot` channels, so callers never touch the shard's state directly.
pub enum ShardMsg {
    /// A client mutation to order through this shard's Raft group.
    Propose {
        command: Command,
        resp: oneshot::Sender<Result<ProposeOutcome, ProposeError>>,
    },
    /// A read served off this shard's applied state.
    Query {
        request: ReadRequest,
        resp: oneshot::Sender<Result<ReadResponse, ProposeError>>,
    },
    /// An inbound peer Raft RPC, demuxed to this shard by the transport.
    Raft(RaftRpc),
    /// A request for this shard's consensus status.
    Status { resp: oneshot::Sender<ShardStatus> },
}

/// Peer-to-peer Raft messages (stub).
///
/// TODO(cluster): give these real bodies and a response path:
///   * `AppendEntries` — term, leader_id, prev_log_index/term, entries, leader_commit
///   * `RequestVote`   — term, candidate_id, last_log_index/term
#[derive(Debug, Clone)]
pub enum RaftRpc {
    AppendEntries,
    RequestVote,
}

/// A single-key read, routed to its owning shard.
///
/// Multi-shard scans (prefix/list) are not modeled here — a handler fans those
/// out across shards itself.
pub enum ReadRequest {
    Kv { key: String },
    Lock { key: String },
    RateLimit { tenant: String, key: String },
    Schedule { name: String },
    ScheduleHistory { name: String },
    Election { name: String },
    Service { service: String },
}

impl ReadRequest {
    /// Key used to route this read to its owning shard.
    pub fn routing_key(&self) -> &str {
        match self {
            ReadRequest::Kv { key } => key,
            ReadRequest::Lock { key } => key,
            ReadRequest::RateLimit { key, .. } => key,
            ReadRequest::Schedule { name } | ReadRequest::ScheduleHistory { name } => name,
            ReadRequest::Election { name } => name,
            ReadRequest::Service { service } => service,
        }
    }
}

/// The answer to a [`ReadRequest`], typed by domain.
#[derive(Debug)]
pub enum ReadResponse {
    Kv(Option<KvEntry>),
    Lock(LockState),
    RateLimit(Option<RateLimitSnapshot>),
    Schedule(Option<Schedule>),
    ScheduleHistory(Vec<ScheduleRun>),
    Election(Option<Leadership>),
    Service(Vec<ServiceInstance>),
}

// ---------------------------------------------------------------------------
// Transport: shared peer RPC (stub).
// ---------------------------------------------------------------------------

/// Shared peer transport (stub).
///
/// One transport serves *all* shards: a single connection per peer multiplexes
/// every shard's Raft traffic, and inbound frames are demuxed by `shard_id` to
/// the right [`ShardActor`]'s inbox. That sharing is why a node is one process.
///
/// TODO(cluster): implement real RPC (e.g. gRPC/QUIC), one connection per peer,
/// plus the inbound side that routes received `RaftRpc`s to shard inboxes.
pub struct Transport {
    #[allow(dead_code)]
    node_id: String,
    #[allow(dead_code)]
    peers: Vec<String>,
}

impl Transport {
    pub fn new(node_id: String, peers: Vec<String>) -> Self {
        Transport { node_id, peers }
    }

    /// Send one Raft RPC to a peer for a given shard. No-op in the skeleton.
    pub async fn send(&self, _peer: &str, _shard: ShardId, _rpc: RaftRpc) {
        // TODO(cluster): real outbound RPC over the shared per-peer connection.
    }
}

// ---------------------------------------------------------------------------
// Shard actor: owns one shard's Raft group + state-machine partition.
// ---------------------------------------------------------------------------

/// The owned state and event loop for one shard. Created at bootstrap and run as
/// its own task; reached only via its [`ShardMsg`] inbox.
struct ShardActor {
    shard_id: ShardId,
    node_id: String,
    transport: Arc<Transport>,

    // --- Raft state (owned by this task; no locks) ---
    role: Role,
    current_term: u64,
    leader_id: Option<String>,
    #[allow(dead_code)]
    voted_for: Option<String>,
    log: Vec<LogEntry>,
    commit_index: u64,

    // --- the state-machine partition holding this shard's keys ---
    state: StateMachine,
}

impl ShardActor {
    fn new(shard_id: ShardId, node_id: String, transport: Arc<Transport>) -> Self {
        ShardActor {
            shard_id,
            node_id: node_id.clone(),
            transport,
            // TODO(cluster): start as Follower and run a per-shard election.
            // Single-node leads every shard from t=0.
            role: Role::Leader,
            current_term: 1,
            leader_id: Some(node_id),
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            state: StateMachine::new(),
        }
    }

    /// The shard's event loop: drain the inbox and fire the election/heartbeat
    /// tick until every sender is dropped (node shutdown).
    async fn run(mut self, mut inbox: mpsc::Receiver<ShardMsg>) {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                maybe = inbox.recv() => {
                    let Some(msg) = maybe else { break }; // all senders gone
                    match msg {
                        ShardMsg::Propose { command, resp } => {
                            let _ = resp.send(self.handle_propose(command));
                        }
                        ShardMsg::Query { request, resp } => {
                            let _ = resp.send(self.handle_query(request));
                        }
                        ShardMsg::Raft(rpc) => self.handle_raft(rpc).await,
                        ShardMsg::Status { resp } => {
                            let _ = resp.send(self.status());
                        }
                    }
                }
                _ = tick.tick() => self.on_tick().await,
            }
        }
    }

    /// Order a client mutation through this shard's Raft group.
    ///
    /// TODO(cluster): replicate to the shard's followers via `AppendEntries` and
    /// only commit (and apply) once a quorum of the shard's members has acked.
    fn handle_propose(&mut self, command: Command) -> Result<ProposeOutcome, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            });
        }

        let index = self.log.len() as u64 + 1;
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            command: command.clone(),
        });

        // Single-node fast path: a one-member quorum is satisfied immediately.
        self.commit_index = index;
        let applied = self.state.apply(command);

        Ok(ProposeOutcome {
            shard: self.shard_id,
            log_index: index,
            revision: applied.revision,
            output: applied.output,
        })
    }

    /// Serve a read off applied state.
    ///
    /// TODO(cluster): gate behind the leader lease for linearizability once
    /// multi-node Raft is wired.
    fn handle_query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            });
        }

        match request {
            ReadRequest::Kv { key } => Ok(ReadResponse::Kv(self.state.kv_get(&key))),
            ReadRequest::Lock { key } => Ok(ReadResponse::Lock(self.state.lock_get(&key))),
            ReadRequest::RateLimit { tenant, key } => Ok(ReadResponse::RateLimit(
                self.state.rate_limit_get(&tenant, &key),
            )),
            ReadRequest::Schedule { name } => {
                Ok(ReadResponse::Schedule(self.state.schedule_get(&name)))
            }
            ReadRequest::ScheduleHistory { name } => Ok(ReadResponse::ScheduleHistory(
                self.state.schedule_history(&name),
            )),
            ReadRequest::Election { name } => {
                Ok(ReadResponse::Election(self.state.election_get(&name)))
            }
            ReadRequest::Service { service } => {
                Ok(ReadResponse::Service(self.state.service_list(&service)))
            }
        }
    }

    /// Handle an inbound peer Raft RPC.
    ///
    /// TODO(cluster): implement `AppendEntries` / `RequestVote`, possibly
    /// replying via `self.transport`.
    async fn handle_raft(&mut self, _rpc: RaftRpc) {
        // TODO(cluster)
    }

    /// Periodic tick: election timeout (followers) / heartbeats (leaders).
    ///
    /// TODO(cluster): on a leader, broadcast heartbeats via `self.transport`; on
    /// a follower, start an election if the leader has gone quiet.
    async fn on_tick(&mut self) {
        // TODO(cluster). Single-node leader has nothing to do here yet.
        let _ = &self.transport;
        let _ = &self.node_id;
    }

    fn status(&self) -> ShardStatus {
        ShardStatus {
            shard_id: self.shard_id,
            role: self.role,
            term: self.current_term,
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index,
            last_log_index: self.log.len() as u64,
        }
    }
}

// ---------------------------------------------------------------------------
// Node: the router/front for this process's shard actors.
// ---------------------------------------------------------------------------

/// A Fiducia node: a host for many shard actors, plus the router that maps keys
/// to shards and the shared peer transport.
pub struct Node {
    config: NodeConfig,
    shards: HashMap<ShardId, mpsc::Sender<ShardMsg>>,
    #[allow(dead_code)]
    transport: Arc<Transport>,
}

impl Node {
    /// Boot a single-node cluster owning every shard: spawn one actor task per
    /// shard, each the sole member — and therefore leader — of its Raft group.
    ///
    /// Must be called from within a Tokio runtime (it spawns the actor tasks).
    pub fn bootstrap(config: NodeConfig) -> Self {
        let transport = Arc::new(Transport::new(config.node_id.clone(), config.peers.clone()));
        let mut shards = HashMap::new();
        for shard_id in 0..config.shard_count {
            let (tx, rx) = mpsc::channel(SHARD_INBOX_CAPACITY);
            let actor = ShardActor::new(shard_id, config.node_id.clone(), transport.clone());
            tokio::spawn(actor.run(rx));
            shards.insert(shard_id, tx);
        }
        Node {
            config,
            shards,
            transport,
        }
    }

    /// Map a routing key to its owning shard.
    ///
    /// TODO(cluster): route through a membership-updated shard map once shards can
    /// split/merge or move between nodes.
    pub fn shard_for(&self, key: &str) -> ShardId {
        fiducia_routing::shard_for(key, self.config.shard_count)
    }

    fn sender(&self, shard: ShardId) -> Option<&mpsc::Sender<ShardMsg>> {
        // Single-node hosts every shard; once placement is dynamic this can miss
        // (this node may not replicate `shard`).
        self.shards.get(&shard)
    }

    /// Propose a command to the Raft group of the shard that owns its key.
    pub async fn propose(&self, command: Command) -> Result<ProposeOutcome, ProposeError> {
        let shard = self.shard_for(command.routing_key());
        let Some(tx) = self.sender(shard) else {
            return Err(ProposeError::Unavailable { shard });
        };
        let (resp, rx) = oneshot::channel();
        if tx.send(ShardMsg::Propose { command, resp }).await.is_err() {
            return Err(ProposeError::Unavailable { shard });
        }
        rx.await.unwrap_or(Err(ProposeError::Unavailable { shard }))
    }

    /// Serve a single-key read from the owning shard. `None` means the shard is
    /// unreachable on this node.
    pub async fn query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        let shard = self.shard_for(request.routing_key());
        let Some(tx) = self.sender(shard) else {
            return Err(ProposeError::Unavailable { shard });
        };
        let (resp, rx) = oneshot::channel();
        if tx.send(ShardMsg::Query { request, resp }).await.is_err() {
            return Err(ProposeError::Unavailable { shard });
        }
        rx.await.unwrap_or(Err(ProposeError::Unavailable { shard }))
    }

    /// Per-shard consensus status across all shards this node hosts.
    pub async fn status(&self) -> NodeStatus {
        let mut shards: Vec<ShardStatus> = Vec::with_capacity(self.shards.len());
        for tx in self.shards.values() {
            let (resp, rx) = oneshot::channel();
            if tx.send(ShardMsg::Status { resp }).await.is_ok() {
                if let Ok(status) = rx.await {
                    shards.push(status);
                }
            }
        }
        shards.sort_by_key(|s| s.shard_id);
        let leading_shards: Vec<ShardId> = shards
            .iter()
            .filter(|s| s.role == Role::Leader)
            .map(|s| s.shard_id)
            .collect();
        let following_shards: Vec<ShardId> = shards
            .iter()
            .filter(|s| s.role == Role::Follower)
            .map(|s| s.shard_id)
            .collect();
        NodeStatus {
            node_id: self.config.node_id.clone(),
            peers: self.config.peers.clone(),
            shard_count: self.config.shard_count,
            leader_count: leading_shards.len(),
            follower_count: following_shards.len(),
            leading_shards,
            following_shards,
            shards,
        }
    }
}

// ---------------------------------------------------------------------------
// Status + result types.
// ---------------------------------------------------------------------------

/// Per-shard consensus status, surfaced by `/v1/status`.
#[derive(Debug, Clone, Serialize)]
pub struct ShardStatus {
    pub shard_id: ShardId,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_log_index: u64,
}

/// Whole-node status: identity, membership, and a row per hosted shard.
#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub peers: Vec<String>,
    pub shard_count: u32,
    /// Count of hosted shards for which this node is currently leader.
    pub leader_count: usize,
    /// Count of hosted shards for which this node is currently follower.
    pub follower_count: usize,
    /// Shards for which this node is currently the leader.
    pub leading_shards: Vec<ShardId>,
    /// Shards for which this node is currently a follower.
    pub following_shards: Vec<ShardId>,
    pub shards: Vec<ShardStatus>,
}

/// Result of a successfully committed proposal.
#[derive(Debug, Clone, Serialize)]
pub struct ProposeOutcome {
    /// Shard whose Raft group committed the command.
    pub shard: ShardId,
    /// Index assigned in that shard's log.
    pub log_index: u64,
    /// Revision produced by applying the command to that shard's state machine.
    pub revision: u64,
    /// Domain-specific output from the committed state-machine command.
    pub output: Value,
}

/// Why a proposal could not be committed.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ProposeError {
    /// This node is not the leader of the target shard.
    NotLeader {
        shard: ShardId,
        /// Reroutable leader base URL for the client/LB HTTP plane, when known.
        leader: Option<String>,
    },
    /// The target shard is not reachable on this node (no replica / actor gone).
    Unavailable { shard: ShardId },
}

/// Render a proposal result as an HTTP response.
///
/// Followers return a redirect plus leader headers so the LB can repair a stale
/// shard->leader cache without already knowing the current leader.
pub fn propose_response(result: Result<ProposeOutcome, ProposeError>, uri: &Uri) -> Response {
    match result {
        Ok(outcome) => {
            Json(serde_json::json!({ "committed": true, "result": outcome })).into_response()
        }
        Err(err) => error_response(err, uri),
    }
}

pub fn read_error_response(err: ProposeError, uri: &Uri) -> Response {
    error_response(err, uri)
}

fn error_response(err: ProposeError, uri: &Uri) -> Response {
    match err {
        ProposeError::NotLeader { shard, leader } => {
            let body = Json(serde_json::json!({
                "committed": false,
                "error": {
                    "reason": "not_leader",
                    "shard": shard,
                    "leader": leader,
                }
            }));
            let mut response = (StatusCode::TEMPORARY_REDIRECT, body).into_response();
            response
                .headers_mut()
                .insert("x-fiducia-not-leader", HeaderValue::from_static("true"));
            response.headers_mut().insert(
                "x-fiducia-shard",
                HeaderValue::from_str(&shard.to_string())
                    .unwrap_or_else(|_| HeaderValue::from_static("")),
            );
            if let Some(leader) = leader {
                if let Ok(value) = HeaderValue::from_str(&leader) {
                    response.headers_mut().insert("x-fiducia-leader", value);
                }
                if let Some(location) = leader_location(&leader, uri) {
                    if let Ok(value) = HeaderValue::from_str(&location) {
                        response.headers_mut().insert(LOCATION, value);
                    }
                }
            }
            response
        }
        other => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "committed": false, "error": other })),
        )
            .into_response(),
    }
}

fn leader_location(leader: &str, uri: &Uri) -> Option<String> {
    if !(leader.starts_with("http://") || leader.starts_with("https://")) {
        return None;
    }
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    Some(format!("{}{}", leader.trim_end_matches('/'), path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};

    fn command() -> Command {
        Command::KvDelete {
            key: "orders/checkout".to_string(),
        }
    }

    fn follower() -> ShardActor {
        let transport = Arc::new(Transport::new("follower-a".to_string(), vec![]));
        let mut actor = ShardActor::new(7, "follower-a".to_string(), transport);
        actor.role = Role::Follower;
        actor.leader_id = Some("http://leader-a:8090".to_string());
        actor
    }

    #[test]
    fn follower_propose_returns_not_leader_with_known_leader() {
        let mut actor = follower();
        let err = actor
            .handle_propose(command())
            .expect_err("follower must reject writes");

        match err {
            ProposeError::NotLeader { shard, leader } => {
                assert_eq!(shard, 7);
                assert_eq!(leader.as_deref(), Some("http://leader-a:8090"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn follower_query_returns_not_leader_with_known_leader() {
        let actor = follower();
        let err = actor
            .handle_query(ReadRequest::Kv {
                key: "orders/checkout".to_string(),
            })
            .expect_err("follower must reject linearizable reads");

        match err {
            ProposeError::NotLeader { shard, leader } => {
                assert_eq!(shard, 7);
                assert_eq!(leader.as_deref(), Some("http://leader-a:8090"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn not_leader_http_response_redirects_to_leader_and_names_shard() {
        let uri: Uri = "/v1/kv/orders/checkout?wait=true".parse().unwrap();
        let response = propose_response(
            Err(ProposeError::NotLeader {
                shard: 7,
                leader: Some("http://leader-a:8090".to_string()),
            }),
            &uri,
        );

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response.headers().get("x-fiducia-not-leader").unwrap(),
            "true"
        );
        assert_eq!(response.headers().get("x-fiducia-shard").unwrap(), "7");
        assert_eq!(
            response.headers().get("x-fiducia-leader").unwrap(),
            "http://leader-a:8090"
        );
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "http://leader-a:8090/v1/kv/orders/checkout?wait=true"
        );

        let body = to_bytes(Body::from(response.into_body()), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["reason"], "not_leader");
        assert_eq!(json["error"]["leader"], "http://leader-a:8090");
        assert_eq!(json["error"]["shard"], 7);
    }
}
