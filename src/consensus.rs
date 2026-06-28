//! Raft consensus core — **sharded / multi-Raft**, actor-per-shard.
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
//! Each shard is driven by its **own async task** ([`ShardActor`]) that *owns*
//! that shard's Raft state and state machine — there are no locks on the hot
//! path. Everyone else (HTTP handlers, the peer transport) talks to a shard by
//! sending it a [`ShardMsg`] over an `mpsc` channel and awaiting a `oneshot`
//! reply. Outbound RPCs are **never awaited inside the actor**: the actor spawns
//! the send and the reply comes back as another [`ShardMsg`] (`VoteReply` /
//! `AppendReply`) into its own inbox, so a slow peer can't stall the shard.
//!
//! ## What is implemented
//!
//! A faithful single-shard Raft: randomized leader election, log replication with
//! the `AppendEntries` consistency check, quorum commit (a leader commits an
//! index once a majority of the group has it *and* it is from the leader's term —
//! enforced via an empty no-op appended on election), step-down on a higher term,
//! and linearizable reads gated to the leader. Client writes block until their
//! entry commits (the `pending` waiters).
//!
//! ## Fixed-membership simplification
//!
//! Every node hosts every shard, so a shard's Raft group is `self + peers`
//! (constant). Dynamic membership — splitting/moving shards between nodes,
//! learners, and the placement that drives it — is the control plane
//! `fiducia-brain`'s job and is not done here.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    http::{header::LOCATION, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::state::{
    Command, KvEntry, Leadership, LockState, RateLimitSnapshot, Schedule, ScheduleRun,
    SemaphoreState, ServiceInstance, StateMachine,
};
use crate::transport::{
    AppendEntriesReq, AppendEntriesResp, LoopbackRegistry, RequestVoteReq, RequestVoteResp,
    Transport,
};

/// Identifier of a shard (one independent Raft group). Re-exported from the
/// shared routing crate so the type and the `key → shard` mapping can't drift
/// between the node, the load balancer, and the brain.
pub use fiducia_routing::ShardId;

/// Depth of each shard actor's inbox before senders must wait.
const SHARD_INBOX_CAPACITY: usize = 1024;
/// How often a shard actor wakes to check election/heartbeat deadlines.
const TICK: Duration = Duration::from_millis(20);
/// Leaders send heartbeats this often (must be << the election timeout).
const HEARTBEAT: Duration = Duration::from_millis(50);
/// Election timeout base; the actual timeout is `MIN + rand(0..JITTER)` so peers
/// don't all campaign at once (the standard split-vote avoidance).
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_JITTER_MS: u64 = 150;
/// How long a client write waits for its entry to commit before giving up.
const COMMIT_WAIT: Duration = Duration::from_secs(5);
/// Capacity of each shard's change-event broadcast (feeds KV watches).
const CHANGE_BUFFER: usize = 256;

/// A node's role *within a single shard's* Raft group. A node holds a `Role` per
/// shard it replicates — `Leader` for some, `Follower` for others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One entry in a shard's replicated log. `command` is `None` for the no-op a new
/// leader appends on election (so it can commit entries inherited from prior
/// terms — Raft's leader-completeness rule — without a client write).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Raft term in which the entry was created (per shard).
    pub term: u64,
    /// 1-based position in the shard's log.
    pub index: u64,
    /// The state-machine command, or `None` for a leader-election no-op.
    pub command: Option<Command>,
}

/// A change applied to a shard's state machine, broadcast to watch clients.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeEvent {
    /// Domain-specific event name, such as `"put"`, `"election_campaign"`, or
    /// `"service_register"`.
    pub kind: &'static str,
    pub key: String,
    pub revision: u64,
}

/// Static identity + cluster membership for this physical node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Stable, addressable identifier for this node (e.g. `node-a:8090`). Used as
    /// the Raft member id and as the redirect target sent to clients.
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
    /// A client mutation to order through this shard's Raft group. The reply is
    /// sent only once the entry **commits** (or fails fast if not the leader).
    Propose {
        command: Command,
        resp: oneshot::Sender<Result<ProposeOutcome, ProposeError>>,
    },
    /// A read served off this shard's applied state (leader only).
    Query {
        request: ReadRequest,
        resp: oneshot::Sender<Result<ReadResponse, ProposeError>>,
    },
    /// Inbound `AppendEntries` from a peer leader.
    AppendEntries {
        req: AppendEntriesReq,
        resp: oneshot::Sender<AppendEntriesResp>,
    },
    /// Inbound `RequestVote` from a peer candidate.
    RequestVote {
        req: RequestVoteReq,
        resp: oneshot::Sender<RequestVoteResp>,
    },
    /// A peer's reply to a `RequestVote` this shard sent (routed back to self).
    VoteReply { from: String, resp: RequestVoteResp },
    /// A peer's reply to an `AppendEntries` this shard sent (routed back to self).
    AppendReply {
        from: String,
        /// Last index the leader tried to replicate in that RPC.
        up_to: u64,
        /// `None` if the peer was unreachable.
        resp: Option<AppendEntriesResp>,
    },
    /// Subscribe to this shard's change stream (for a KV watch).
    Subscribe {
        resp: oneshot::Sender<broadcast::Receiver<ChangeEvent>>,
    },
    /// A request for this shard's consensus status.
    Status { resp: oneshot::Sender<ShardStatus> },
}

/// A read routed to its owning shard, except prefix reads which are fanned out
/// across every hosted shard by [`Node::query_kv_prefix`].
pub enum ReadRequest {
    Kv { key: String },
    KvPrefix { prefix: String },
    Lock { key: String },
    Semaphore { key: String },
    RateLimit { tenant: String, key: String },
    Schedule { name: String },
    ScheduleHistory { name: String },
    Election { name: String },
    Services,
    Service { service: String },
}

impl ReadRequest {
    /// Key used to route this read to its owning shard. Lock/semaphore reads route
    /// to the same lock-coordinator shard as their writes (see [`Command::routing_key`]).
    pub fn routing_key(&self) -> &str {
        match self {
            ReadRequest::Kv { key } | ReadRequest::KvPrefix { prefix: key } => key,
            ReadRequest::Lock { .. } | ReadRequest::Semaphore { .. } => crate::state::LOCK_DOMAIN,
            ReadRequest::RateLimit { key, .. } => key,
            ReadRequest::Schedule { name } | ReadRequest::ScheduleHistory { name } => name,
            ReadRequest::Election { name } => name,
            ReadRequest::Services | ReadRequest::Service { .. } => crate::state::SERVICE_DOMAIN,
        }
    }
}

/// The answer to a [`ReadRequest`], typed by domain.
#[derive(Debug)]
pub enum ReadResponse {
    Kv(Option<KvEntry>),
    KvPrefix(Vec<(String, KvEntry)>),
    Lock(LockState),
    Semaphore(SemaphoreState),
    RateLimit(Option<RateLimitSnapshot>),
    Schedule(Option<Schedule>),
    ScheduleHistory(Vec<ScheduleRun>),
    Election(Option<Leadership>),
    Services(Vec<String>),
    Service(Vec<ServiceInstance>),
}

// ---------------------------------------------------------------------------
// Leader-only volatile state.
// ---------------------------------------------------------------------------

/// Per-peer replication bookkeeping a node keeps **only while it leads** a shard.
#[derive(Default)]
struct LeaderState {
    /// Next log index to send to each peer.
    next_index: HashMap<String, u64>,
    /// Highest index known replicated on each peer.
    match_index: HashMap<String, u64>,
    /// Whether an `AppendEntries` is already outstanding to a peer (so we don't
    /// pile on duplicates, which would over-rewind `next_index`).
    in_flight: HashMap<String, bool>,
}

// ---------------------------------------------------------------------------
// Shard actor: owns one shard's Raft group + state-machine partition.
// ---------------------------------------------------------------------------

/// The owned state and event loop for one shard. Created at bootstrap and run as
/// its own task; reached only via its [`ShardMsg`] inbox.
struct ShardActor {
    shard_id: ShardId,
    node_id: String,
    /// All members of this shard's Raft group (`self + peers`), fixed.
    peers: Vec<String>,
    members: usize,
    transport: Arc<Transport>,
    /// A clone of this actor's own inbox, so spawned RPC tasks can route replies
    /// back in as `VoteReply` / `AppendReply`.
    self_tx: mpsc::Sender<ShardMsg>,

    // --- persistent-ish Raft state (in-memory in this build) ---
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    leader_id: Option<String>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,

    // --- candidate state ---
    votes: HashSet<String>,
    // --- leader state ---
    leader: Option<LeaderState>,

    // --- timers ---
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    rng: Rng,

    // --- client write waiters: log index → who is blocked on its commit ---
    pending: HashMap<u64, oneshot::Sender<Result<ProposeOutcome, ProposeError>>>,
    // --- change stream feeding KV watches ---
    changes: broadcast::Sender<ChangeEvent>,

    // --- the state-machine partition holding this shard's keys ---
    state: StateMachine,
}

impl ShardActor {
    fn new(
        shard_id: ShardId,
        node_id: String,
        peers: Vec<String>,
        transport: Arc<Transport>,
        self_tx: mpsc::Sender<ShardMsg>,
    ) -> Self {
        let members = peers.len() + 1;
        let single = members == 1;
        let (changes, _) = broadcast::channel(CHANGE_BUFFER);
        let mut actor = ShardActor {
            shard_id,
            node_id: node_id.clone(),
            peers,
            members,
            transport,
            self_tx,
            // A single-node shard leads itself from t=0 (no one to elect against);
            // a real group starts as a follower and runs an election.
            role: if single { Role::Leader } else { Role::Follower },
            current_term: 1,
            voted_for: None,
            leader_id: if single { Some(node_id.clone()) } else { None },
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            votes: HashSet::new(),
            leader: if single {
                Some(LeaderState::default())
            } else {
                None
            },
            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
            rng: Rng::seeded(&node_id, shard_id),
            pending: HashMap::new(),
            changes,
            state: StateMachine::new(),
        };
        actor.reset_election_deadline();
        actor
    }

    /// The shard's event loop: drain the inbox and fire the election/heartbeat
    /// tick until every sender is dropped (node shutdown).
    async fn run(mut self, mut inbox: mpsc::Receiver<ShardMsg>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                maybe = inbox.recv() => {
                    let Some(msg) = maybe else { break }; // all senders gone
                    self.handle(msg);
                }
                _ = tick.tick() => self.on_tick(),
            }
        }
    }

    fn handle(&mut self, msg: ShardMsg) {
        match msg {
            ShardMsg::Propose { command, resp } => self.on_propose(command, resp),
            ShardMsg::Query { request, resp } => {
                let _ = resp.send(self.handle_query(request));
            }
            ShardMsg::AppendEntries { req, resp } => {
                let out = self.handle_append_entries(req);
                let _ = resp.send(out);
            }
            ShardMsg::RequestVote { req, resp } => {
                let out = self.handle_request_vote(req);
                let _ = resp.send(out);
            }
            ShardMsg::VoteReply { from, resp } => self.handle_vote_reply(from, resp),
            ShardMsg::AppendReply { from, up_to, resp } => {
                self.handle_append_reply(from, up_to, resp)
            }
            ShardMsg::Subscribe { resp } => {
                let _ = resp.send(self.changes.subscribe());
            }
            ShardMsg::Status { resp } => {
                let _ = resp.send(self.status());
            }
        }
    }

    // --- timing -----------------------------------------------------------

    fn reset_election_deadline(&mut self) {
        let jitter = self.rng.below(ELECTION_JITTER_MS);
        self.election_deadline = Instant::now() + Duration::from_millis(ELECTION_MIN_MS + jitter);
    }

    /// Periodic tick: leaders heartbeat; everyone else campaigns once their
    /// election timeout elapses without hearing from a leader.
    fn on_tick(&mut self) {
        let now = Instant::now();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_deadline {
                    self.heartbeat_deadline = now + HEARTBEAT;
                    self.broadcast_append_entries();
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
        }
    }

    // --- elections --------------------------------------------------------

    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else {
            self.log
                .get((index - 1) as usize)
                .map(|e| e.term)
                .unwrap_or(0)
        }
    }

    fn majority(&self) -> usize {
        self.members / 2 + 1
    }

    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.node_id.clone());
        self.reset_election_deadline();

        if self.votes.len() >= self.majority() {
            // Single-member group: we already have a majority.
            self.become_leader();
            return;
        }

        let req = RequestVoteReq {
            term: self.current_term,
            candidate_id: self.node_id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        for peer in self.peers.clone() {
            let transport = self.transport.clone();
            let self_tx = self.self_tx.clone();
            let shard = self.shard_id;
            let req = req.clone();
            tokio::spawn(async move {
                if let Some(resp) = transport.request_vote(&peer, shard, req).await {
                    let _ = self_tx.send(ShardMsg::VoteReply { from: peer, resp }).await;
                }
            });
        }
    }

    fn handle_vote_reply(&mut self, from: String, resp: RequestVoteResp) {
        if resp.term > self.current_term {
            self.step_down(resp.term, None);
            return;
        }
        if self.role != Role::Candidate || resp.term != self.current_term {
            return;
        }
        if resp.granted {
            self.votes.insert(from);
            if self.votes.len() >= self.majority() {
                self.become_leader();
            }
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id.clone());
        self.votes.clear();
        let mut ls = LeaderState::default();
        let next = self.last_log_index() + 1;
        for peer in &self.peers {
            ls.next_index.insert(peer.clone(), next);
            ls.match_index.insert(peer.clone(), 0);
            ls.in_flight.insert(peer.clone(), false);
        }
        self.leader = Some(ls);

        // No-op for the new term so prior-term entries can commit (and so a single
        // write isn't needed to make progress). Committing this proves leadership.
        let index = self.last_log_index() + 1;
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            command: None,
        });

        self.heartbeat_deadline = Instant::now() + HEARTBEAT;
        self.maybe_advance_commit(); // single-node commits the no-op immediately
        self.broadcast_append_entries();
        tracing::info!(
            shard = self.shard_id,
            term = self.current_term,
            "became leader"
        );
    }

    /// Convert to follower at `term`, optionally learning the new leader. Fails
    /// any outstanding client writes so they retry against the real leader.
    fn step_down(&mut self, term: u64, leader: Option<String>) {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        if leader.is_some() {
            self.leader_id = leader;
        }
        self.reset_election_deadline();
        self.fail_pending();
    }

    fn fail_pending(&mut self) {
        let leader = self.leader_id.clone();
        for (_, resp) in self.pending.drain() {
            let _ = resp.send(Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: leader.clone(),
            }));
        }
    }

    // --- replication (leader → followers) ---------------------------------

    fn broadcast_append_entries(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        for peer in self.peers.clone() {
            self.send_append_to(&peer);
        }
    }

    fn send_append_to(&mut self, peer: &str) {
        let Some(ls) = self.leader.as_mut() else {
            return;
        };
        if *ls.in_flight.get(peer).unwrap_or(&false) {
            return;
        }
        let next = *ls.next_index.get(peer).unwrap_or(&1);
        ls.in_flight.insert(peer.to_string(), true);

        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .skip(prev_log_index as usize)
            .cloned()
            .collect();
        let up_to = self.last_log_index();
        let req = AppendEntriesReq {
            term: self.current_term,
            leader_id: self.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        let transport = self.transport.clone();
        let self_tx = self.self_tx.clone();
        let shard = self.shard_id;
        let peer_owned = peer.to_string();
        tokio::spawn(async move {
            let resp = transport.append_entries(&peer_owned, shard, req).await;
            let _ = self_tx
                .send(ShardMsg::AppendReply {
                    from: peer_owned,
                    up_to,
                    resp,
                })
                .await;
        });
    }

    fn handle_append_reply(&mut self, from: String, up_to: u64, resp: Option<AppendEntriesResp>) {
        if let Some(ls) = self.leader.as_mut() {
            ls.in_flight.insert(from.clone(), false);
        }
        let Some(resp) = resp else {
            return; // peer unreachable; retry next tick
        };
        if resp.term > self.current_term {
            self.step_down(resp.term, None);
            return;
        }
        if self.role != Role::Leader || resp.term != self.current_term {
            return;
        }
        let mut more = false;
        if let Some(ls) = self.leader.as_mut() {
            if resp.success {
                ls.match_index.insert(from.clone(), up_to);
                ls.next_index.insert(from.clone(), up_to + 1);
            } else {
                // Log mismatch: rewind and retry from an earlier index.
                let cur = ls.next_index.get(&from).copied().unwrap_or(1);
                let backoff = resp
                    .match_index
                    .saturating_add(1)
                    .min(cur.saturating_sub(1));
                ls.next_index.insert(from.clone(), backoff.max(1));
                more = true;
            }
        }
        if resp.success {
            self.maybe_advance_commit();
        }
        if more {
            self.send_append_to(&from);
        }
    }

    /// Advance `commit_index` to the highest index replicated on a majority that
    /// is **from the current term** (Raft's commit rule), then apply.
    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matches: Vec<u64> = Vec::with_capacity(self.members);
        matches.push(self.last_log_index()); // self has everything
        if let Some(ls) = &self.leader {
            for peer in &self.peers {
                matches.push(ls.match_index.get(peer).copied().unwrap_or(0));
            }
        }
        matches.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let n = matches[self.majority() - 1]; // highest index on ≥ majority
        if n > self.commit_index && self.term_at(n) == self.current_term {
            self.commit_index = n;
            self.apply_committed();
        }
    }

    // --- replication (follower side) --------------------------------------

    fn handle_append_entries(&mut self, req: AppendEntriesReq) -> AppendEntriesResp {
        // Reject a stale leader.
        if req.term < self.current_term {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        // Recognize this leader for our term (or a newer one).
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }
        self.become_follower_of(req.leader_id.clone());

        // Log-consistency check at prev_log_index.
        if req.prev_log_index > 0 && self.term_at(req.prev_log_index) != req.prev_log_term {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                // Hint: how far we *do* match, so the leader can rewind quickly.
                match_index: self
                    .last_log_index()
                    .min(req.prev_log_index.saturating_sub(1)),
            };
        }

        // Append, truncating on the first conflicting term.
        let mut idx = req.prev_log_index;
        for entry in req.entries {
            idx += 1;
            match self.log.get((idx - 1) as usize) {
                Some(existing) if existing.term == entry.term => {} // already have it
                Some(_) => {
                    self.log.truncate((idx - 1) as usize);
                    self.log.push(entry);
                }
                None => self.log.push(entry),
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
            self.apply_committed();
        }

        AppendEntriesResp {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }

    fn become_follower_of(&mut self, leader: String) {
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.leader = None;
        self.votes.clear();
        self.reset_election_deadline();
        // Anything we were leading is no longer ours to commit.
        self.fail_pending();
    }

    fn handle_request_vote(&mut self, req: RequestVoteReq) -> RequestVoteResp {
        if req.term < self.current_term {
            return RequestVoteResp {
                term: self.current_term,
                granted: false,
            };
        }
        if req.term > self.current_term {
            self.step_down(req.term, None);
        }

        let log_ok = (req.last_log_term > self.last_log_term())
            || (req.last_log_term == self.last_log_term()
                && req.last_log_index >= self.last_log_index());
        let can_vote = self
            .voted_for
            .as_deref()
            .map(|v| v == req.candidate_id)
            .unwrap_or(true);

        if can_vote && log_ok {
            self.voted_for = Some(req.candidate_id.clone());
            self.reset_election_deadline();
            RequestVoteResp {
                term: self.current_term,
                granted: true,
            }
        } else {
            RequestVoteResp {
                term: self.current_term,
                granted: false,
            }
        }
    }

    // --- client proposals + applying --------------------------------------

    fn on_propose(
        &mut self,
        command: Command,
        resp: oneshot::Sender<Result<ProposeOutcome, ProposeError>>,
    ) {
        if self.role != Role::Leader {
            let _ = resp.send(Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            }));
            return;
        }
        let index = self.last_log_index() + 1;
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            command: Some(command),
        });
        // Block the client on this index committing.
        self.pending.insert(index, resp);

        if self.members == 1 {
            // One-member quorum: commit (and apply, which resolves the waiter) now.
            self.commit_index = index;
            self.apply_committed();
        } else {
            self.broadcast_append_entries();
        }
    }

    /// Apply every newly-committed entry in order, resolving client waiters and
    /// publishing change events.
    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let i = self.last_applied;
            let Some(entry) = self.log.get((i - 1) as usize) else {
                break;
            };
            let Some(command) = entry.command.clone() else {
                continue; // no-op
            };
            let applied = self.state.apply(command.clone());
            self.publish_change(&command, applied.revision);
            if let Some(resp) = self.pending.remove(&i) {
                let _ = resp.send(Ok(ProposeOutcome {
                    shard: self.shard_id,
                    log_index: i,
                    revision: applied.revision,
                    output: applied.output,
                }));
            }
        }
    }

    fn publish_change(&self, command: &Command, revision: u64) {
        let event = match command {
            Command::KvPut { key, .. } => Some(ChangeEvent {
                kind: "put",
                key: key.clone(),
                revision,
            }),
            Command::KvDelete { key } => Some(ChangeEvent {
                kind: "delete",
                key: key.clone(),
                revision,
            }),
            Command::ElectionCampaign { name, .. } => Some(ChangeEvent {
                kind: "election_campaign",
                key: name.clone(),
                revision,
            }),
            Command::ElectionRenew { name, .. } => Some(ChangeEvent {
                kind: "election_renew",
                key: name.clone(),
                revision,
            }),
            Command::ElectionResign { name, .. } => Some(ChangeEvent {
                kind: "election_resign",
                key: name.clone(),
                revision,
            }),
            Command::ServiceRegister { service, .. } => Some(ChangeEvent {
                kind: "service_register",
                key: service.clone(),
                revision,
            }),
            Command::ServiceHeartbeat { service, .. } => Some(ChangeEvent {
                kind: "service_heartbeat",
                key: service.clone(),
                revision,
            }),
            Command::ServiceDeregister { service, .. } => Some(ChangeEvent {
                kind: "service_deregister",
                key: service.clone(),
                revision,
            }),
            _ => None,
        };
        if let Some(event) = event {
            let _ = self.changes.send(event); // ignore "no subscribers"
        }
    }

    /// Serve a read off applied state.
    ///
    /// Single-shard reads stay leader-only for linearizability. A prefix read
    /// spans shards, so it is served from this node's locally committed shard
    /// snapshots and merged by [`Node::query_kv_prefix`].
    fn handle_query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        if !matches!(&request, ReadRequest::KvPrefix { .. }) && self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            });
        }
        match request {
            ReadRequest::Kv { key } => Ok(ReadResponse::Kv(self.state.kv_get(&key))),
            ReadRequest::KvPrefix { prefix } => {
                Ok(ReadResponse::KvPrefix(self.state.kv_prefix(&prefix)))
            }
            ReadRequest::Lock { key } => Ok(ReadResponse::Lock(self.state.lock_get(&key))),
            ReadRequest::Semaphore { key } => {
                Ok(ReadResponse::Semaphore(self.state.semaphore_get(&key)))
            }
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
            ReadRequest::Services => Ok(ReadResponse::Services(self.state.service_names())),
            ReadRequest::Service { service } => {
                Ok(ReadResponse::Service(self.state.service_list(&service)))
            }
        }
    }

    fn status(&self) -> ShardStatus {
        ShardStatus {
            shard_id: self.shard_id,
            role: self.role,
            term: self.current_term,
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index,
            last_log_index: self.last_log_index(),
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
    /// Kept alive so the shared transport outlives the actors that clone it.
    #[allow(dead_code)]
    transport: Arc<Transport>,
    /// Shard actor handles — used by `shutdown` (failover tests / graceful stop).
    #[allow(dead_code)]
    tasks: Vec<JoinHandle<()>>,
}

impl Node {
    /// Boot this node's shard actors over the given [`Transport`]. With no peers
    /// each actor is the sole member — and therefore leader — of its group; with
    /// peers they run real elections.
    ///
    /// Must be called from within a Tokio runtime (it spawns the actor tasks).
    pub fn bootstrap(config: NodeConfig, transport: Transport) -> Self {
        let transport = Arc::new(transport);
        let mut shards = HashMap::new();
        let mut tasks = Vec::new();
        for shard_id in 0..config.shard_count {
            let (tx, rx) = mpsc::channel(SHARD_INBOX_CAPACITY);
            if let Some(reg) = transport.loopback_registry() {
                reg.register(&config.node_id, shard_id, tx.clone());
            }
            let actor = ShardActor::new(
                shard_id,
                config.node_id.clone(),
                config.peers.clone(),
                transport.clone(),
                tx.clone(),
            );
            tasks.push(tokio::spawn(actor.run(rx)));
            shards.insert(shard_id, tx);
        }
        Node {
            config,
            shards,
            transport,
            tasks,
        }
    }

    /// Convenience for `main`: boot with the production HTTP transport.
    pub fn bootstrap_http(config: NodeConfig) -> Self {
        Self::bootstrap(config, Transport::http())
    }

    /// Map a routing key to its owning shard.
    pub fn shard_for(&self, key: &str) -> ShardId {
        fiducia_routing::shard_for(key, self.config.shard_count)
    }

    fn sender(&self, shard: ShardId) -> Option<&mpsc::Sender<ShardMsg>> {
        self.shards.get(&shard)
    }

    /// Propose a command to the Raft group of the shard that owns its key. Returns
    /// once the entry **commits** on a quorum (or fast on not-leader/timeout).
    pub async fn propose(&self, command: Command) -> Result<ProposeOutcome, ProposeError> {
        let shard = self.shard_for(command.routing_key());
        let Some(tx) = self.sender(shard) else {
            return Err(ProposeError::Unavailable { shard });
        };
        let (resp, rx) = oneshot::channel();
        if tx.send(ShardMsg::Propose { command, resp }).await.is_err() {
            return Err(ProposeError::Unavailable { shard });
        }
        match tokio::time::timeout(COMMIT_WAIT, rx).await {
            Ok(Ok(result)) => result,
            // Sender dropped (actor gone) or commit timed out.
            _ => Err(ProposeError::Unavailable { shard }),
        }
    }

    /// Serve a single-key read from the owning shard.
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

    /// Query every hosted shard for entries under a prefix and merge the partial
    /// results in key order.
    pub async fn query_kv_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(String, KvEntry)>, ProposeError> {
        let mut entries = Vec::new();
        for (shard, tx) in &self.shards {
            let (resp, rx) = oneshot::channel();
            let request = ReadRequest::KvPrefix {
                prefix: prefix.clone(),
            };
            if tx.send(ShardMsg::Query { request, resp }).await.is_err() {
                return Err(ProposeError::Unavailable { shard: *shard });
            }
            match rx
                .await
                .unwrap_or(Err(ProposeError::Unavailable { shard: *shard }))?
            {
                ReadResponse::KvPrefix(mut shard_entries) => entries.append(&mut shard_entries),
                _ => return Err(ProposeError::Unavailable { shard: *shard }),
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }

    /// Deliver an inbound `AppendEntries` to the owning shard actor.
    pub async fn append_entries(
        &self,
        shard: ShardId,
        req: AppendEntriesReq,
    ) -> Option<AppendEntriesResp> {
        let tx = self.sender(shard)?;
        let (resp, rx) = oneshot::channel();
        tx.send(ShardMsg::AppendEntries { req, resp }).await.ok()?;
        rx.await.ok()
    }

    /// Deliver an inbound `RequestVote` to the owning shard actor.
    pub async fn request_vote(
        &self,
        shard: ShardId,
        req: RequestVoteReq,
    ) -> Option<RequestVoteResp> {
        let tx = self.sender(shard)?;
        let (resp, rx) = oneshot::channel();
        tx.send(ShardMsg::RequestVote { req, resp }).await.ok()?;
        rx.await.ok()
    }

    /// Subscribe to the change stream of the shard owning `key` (for a KV watch).
    pub async fn watch(&self, key: &str) -> Option<broadcast::Receiver<ChangeEvent>> {
        let shard = self.shard_for(key);
        let tx = self.sender(shard)?;
        let (resp, rx) = oneshot::channel();
        tx.send(ShardMsg::Subscribe { resp }).await.ok()?;
        rx.await.ok()
    }

    /// Subscribe to every shard hosted by this node. Used by prefix watches
    /// because keys under one prefix can hash to many shards.
    pub async fn watch_all(&self) -> Vec<broadcast::Receiver<ChangeEvent>> {
        let mut receivers = Vec::with_capacity(self.shards.len());
        for tx in self.shards.values() {
            let (resp, rx) = oneshot::channel();
            if tx.send(ShardMsg::Subscribe { resp }).await.is_ok() {
                if let Ok(receiver) = rx.await {
                    receivers.push(receiver);
                }
            }
        }
        receivers
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

    /// Stop all shard actors and, for loopback, remove this node from the
    /// registry — i.e. simulate the node going away (used by failover tests).
    #[allow(dead_code)]
    pub fn shutdown(&self, registry: Option<&LoopbackRegistry>) {
        for task in &self.tasks {
            task.abort();
        }
        if let Some(reg) = registry {
            reg.deregister(&self.config.node_id);
        }
    }
}

// ---------------------------------------------------------------------------
// A tiny deterministic PRNG for randomized election timeouts (no rand dep).
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn seeded(node_id: &str, shard: ShardId) -> Self {
        // Mix the node id and shard so peers desynchronize their timeouts.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in node_id.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        h ^= shard as u64;
        // Also fold in real time so restarts don't replay the same schedule.
        h ^= now_nanos();
        Rng(h | 1)
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
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
    /// The target shard is not reachable on this node, or the write did not commit
    /// in time (e.g. quorum lost).
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

    // --- response-shaping unit test (no cluster) --------------------------

    // --- shared-interface contract (node wire types ⇄ fiducia-interfaces) -

    #[test]
    fn propose_error_redirect_is_wire_compatible_with_shared_interface() {
        // The load balancer parses the node's NotLeader redirect via
        // `fiducia_interfaces::ProposeError` to learn the leader to retry against.
        // This pins that the node emits exactly the shape the LB consumes.
        let node_err = ProposeError::NotLeader {
            shard: 7,
            leader: Some("http://leader-a:8090".to_string()),
        };
        let json = serde_json::to_value(&node_err).unwrap();
        assert_eq!(json["reason"], "not_leader");
        assert_eq!(json["shard"], 7);
        assert_eq!(json["leader"], "http://leader-a:8090");

        let shared: fiducia_interfaces::ProposeError = serde_json::from_value(json).unwrap();
        assert!(matches!(
            shared.reason,
            fiducia_interfaces::ProposeErrorReason::NotLeader
        ));
        assert_eq!(shared.shard, 7);
        assert_eq!(shared.leader.as_deref(), Some("http://leader-a:8090"));
    }

    #[test]
    fn propose_outcome_is_wire_compatible_with_shared_interface() {
        let outcome = ProposeOutcome {
            shard: 3,
            log_index: 42,
            revision: 9,
            output: serde_json::json!({ "ok": true }),
        };
        let shared: fiducia_interfaces::ProposeOutcome =
            serde_json::from_value(serde_json::to_value(&outcome).unwrap()).unwrap();
        assert_eq!(shared.shard, 3);
        assert_eq!(shared.log_index, 42);
        assert_eq!(shared.revision, 9);
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

    // --- multi-node cluster tests over the in-process loopback transport ---

    fn node(id: &str, peers: &[&str], shard_count: u32, reg: &LoopbackRegistry) -> Node {
        Node::bootstrap(
            NodeConfig {
                node_id: id.to_string(),
                peers: peers.iter().map(|s| s.to_string()).collect(),
                shard_count,
            },
            Transport::loopback(reg.clone()),
        )
    }

    fn put(key: &str, value: &str) -> Command {
        Command::KvPut {
            key: key.to_string(),
            value: value.to_string(),
            ttl_ms: None,
            prev_revision: None,
        }
    }

    async fn leader_of(nodes: &[&Node], shard: ShardId) -> Option<usize> {
        for (i, n) in nodes.iter().enumerate() {
            let st = n.status().await;
            if st
                .shards
                .iter()
                .any(|s| s.shard_id == shard && s.role == Role::Leader)
            {
                return Some(i);
            }
        }
        None
    }

    /// Poll for a leader of `shard`, up to `tries` × 20ms.
    async fn await_leader(nodes: &[&Node], shard: ShardId, tries: u32) -> usize {
        for _ in 0..tries {
            if let Some(i) = leader_of(nodes, shard).await {
                return i;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no leader elected for shard {shard}");
    }

    #[tokio::test]
    async fn single_node_leads_and_commits_immediately() {
        let reg = LoopbackRegistry::new();
        let n = node("solo", &[], 4, &reg);

        let out = n.propose(put("flags/x", "on")).await.expect("commit");
        assert!(out.output["ok"].as_bool().unwrap());

        match n
            .query(ReadRequest::Kv {
                key: "flags/x".to_string(),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "on"),
            other => panic!("unexpected read: {other:?}"),
        }
    }

    #[tokio::test]
    async fn kv_prefix_query_fans_out_across_shards() {
        let reg = LoopbackRegistry::new();
        let n = node("solo-prefix", &[], 8, &reg);
        let mut selected = Vec::new();
        for i in 0..1_000 {
            let key = format!("flags/key-{i}");
            let shard = n.shard_for(&key);
            if selected
                .first()
                .map(|(first_shard, _): &(ShardId, String)| *first_shard != shard)
                .unwrap_or(true)
            {
                selected.push((shard, key));
            }
            if selected.len() == 2 {
                break;
            }
        }
        assert_eq!(
            selected.len(),
            2,
            "expected two prefix keys on different shards"
        );

        for (_, key) in &selected {
            n.propose(put(key, "kept")).await.expect("commit");
        }
        n.propose(put("other/key", "ignored"))
            .await
            .expect("commit");

        let entries = n
            .query_kv_prefix("flags/".to_string())
            .await
            .expect("prefix read");
        let keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
        let shards: std::collections::HashSet<_> =
            entries.iter().map(|(key, _)| n.shard_for(key)).collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.starts_with("flags/")));
        assert_eq!(shards.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kv_prefix_query_reads_committed_snapshots_on_followers() {
        let reg = LoopbackRegistry::new();
        let a = node("a", &["b", "c"], 4, &reg);
        let b = node("b", &["a", "c"], 4, &reg);
        let c = node("c", &["a", "b"], 4, &reg);
        let nodes = [&a, &b, &c];
        let mut selected = Vec::new();
        for i in 0..1_000 {
            let key = format!("flags/multi-{i}");
            let shard = a.shard_for(&key);
            if selected
                .first()
                .map(|(first_shard, _): &(ShardId, String)| *first_shard != shard)
                .unwrap_or(true)
            {
                selected.push((shard, key));
            }
            if selected.len() == 2 {
                break;
            }
        }
        assert_eq!(selected.len(), 2);

        for (shard, key) in &selected {
            let leader_idx = await_leader(&nodes, *shard, 150).await;
            nodes[leader_idx]
                .propose(put(key, "kept"))
                .await
                .expect("commit prefix key");
        }

        for n in nodes {
            let entries = await_prefix_entries(n, "flags/", 2).await;
            let shards: std::collections::HashSet<_> =
                entries.iter().map(|(key, _)| n.shard_for(key)).collect();
            assert_eq!(entries.len(), 2);
            assert_eq!(shards.len(), 2);
        }
    }

    async fn await_prefix_entries(
        node: &Node,
        prefix: &str,
        expected_len: usize,
    ) -> Vec<(String, KvEntry)> {
        for _ in 0..100 {
            let entries = node
                .query_kv_prefix(prefix.to_string())
                .await
                .expect("prefix query should not require every shard to lead locally");
            if entries.len() == expected_len {
                return entries;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("prefix query did not observe {expected_len} entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_group_elects_one_leader_and_replicates() {
        let reg = LoopbackRegistry::new();
        let a = node("a", &["b", "c"], 2, &reg);
        let b = node("b", &["a", "c"], 2, &reg);
        let c = node("c", &["a", "b"], 2, &reg);
        let nodes = [&a, &b, &c];

        // Pick a key and find the leader of the shard that owns it.
        let key = "orders/checkout";
        let shard = a.shard_for(key);
        let leader_idx = await_leader(&nodes, shard, 100).await;

        // Exactly one leader across the group for that shard.
        let mut leaders = 0;
        for n in &nodes {
            let st = n.status().await;
            if st
                .shards
                .iter()
                .any(|s| s.shard_id == shard && s.role == Role::Leader)
            {
                leaders += 1;
            }
        }
        assert_eq!(leaders, 1, "exactly one leader per shard");

        // A write on the leader commits (needs a 2/3 quorum).
        let out = nodes[leader_idx]
            .propose(put(key, "v1"))
            .await
            .expect("quorum commit");
        assert!(out.output["ok"].as_bool().unwrap());

        // A non-leader rejects the write with a redirect to the leader.
        let follower_idx = (0..3).find(|i| *i != leader_idx).unwrap();
        let err = nodes[follower_idx]
            .propose(put(key, "v2"))
            .await
            .expect_err("follower must redirect");
        assert!(matches!(err, ProposeError::NotLeader { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leadership_fails_over_when_the_leader_dies() {
        let reg = LoopbackRegistry::new();
        let a = node("a", &["b", "c"], 1, &reg);
        let b = node("b", &["a", "c"], 1, &reg);
        let c = node("c", &["a", "b"], 1, &reg);
        let nodes = [&a, &b, &c];

        // Initial leader of shard 0, write a value through it.
        let leader_idx = await_leader(&nodes, 0, 150).await;
        nodes[leader_idx]
            .propose(put("k", "before"))
            .await
            .expect("write before failover");

        // Kill the leader.
        nodes[leader_idx].shutdown(Some(&reg));

        // A new leader emerges among the survivors, and accepts a write on the
        // surviving 2/3 quorum.
        let survivors: Vec<&Node> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .map(|(_, n)| *n)
            .collect();
        let new_leader = await_leader(&survivors, 0, 200).await;
        let out = survivors[new_leader]
            .propose(put("k", "after"))
            .await
            .expect("new leader commits on the surviving quorum");
        assert!(out.output["ok"].as_bool().unwrap());
    }
}
