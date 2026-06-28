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
use std::path::PathBuf;
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

use crate::persist::{Recovered, ShardStore};
use crate::state::{
    Command, ElectionEntry, KvEntry, KvListItem, Leadership, LockInventory, LockState,
    RateLimitSnapshot, Schedule, ScheduleRun, SemaphoreState, ServiceInstance, ServiceSummary,
    StateMachine,
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
/// How long a client write waits for its entry to commit before giving up.
const COMMIT_WAIT: Duration = Duration::from_secs(5);
/// Capacity of each shard's change-event broadcast (feeds KV watches).
const CHANGE_BUFFER: usize = 256;

/// Raft timing knobs. The timer *durations* default to the original **LAN**
/// values, so an unconfigured node keeps the same heartbeat/election cadence as
/// before; the one behaviour change with no env set is that PreVote is **on** by
/// default (strictly safer — see [`RaftTiming::pre_vote`]). For a cross-cloud
/// (WAN) deployment the durations must be sized **above** the inter-cloud
/// round-trip + jitter, or transatlantic latency triggers spurious elections and
/// leadership flapping — set e.g. `FIDUCIA_RAFT_HEARTBEAT_MS=150`,
/// `FIDUCIA_RAFT_ELECTION_MIN_MS=1000`, `FIDUCIA_RAFT_ELECTION_JITTER_MS=1000`.
/// PreVote can be disabled with `FIDUCIA_RAFT_PREVOTE=off`.
#[derive(Debug, Clone, Copy)]
pub struct RaftTiming {
    /// How often a shard actor wakes to check election/heartbeat deadlines.
    pub tick: Duration,
    /// How often a leader sends heartbeats (must be `<<` the election timeout).
    pub heartbeat: Duration,
    /// Election-timeout base; the actual timeout is `min + rand(0..jitter)` so
    /// peers don't all campaign at once (split-vote avoidance).
    pub election_min_ms: u64,
    pub election_jitter_ms: u64,
    /// PreVote (Raft thesis §9.6): run a non-binding straw poll before
    /// incrementing the term, so a partitioned/laggy node can't disrupt a healthy
    /// leader on rejoin. Strictly safer on a WAN; on by default.
    pub pre_vote: bool,
    /// CheckQuorum + leader lease (Raft thesis §6.2 / §6.4). A leader that has not
    /// heard back from a majority of the group within one `election_min_ms` window
    /// must assume it may have been partitioned away and a new leader elected
    /// elsewhere, so it (a) steps down on the next tick and (b) refuses to serve a
    /// linearizable read in the meantime. Without this, a partitioned-but-unaware
    /// leader keeps `role == Leader` (it only steps down on seeing a *higher* term)
    /// and can answer a stale read — e.g. "lock L is free" after a new leader on the
    /// majority side already granted it. The lease is correct only under bounded
    /// clock drift: it is sized at `election_min_ms`, i.e. no longer than a
    /// follower's own election timeout, so a fresh leader cannot have committed
    /// before the old lease expires. On by default (strictly safer); the one
    /// liveness cost is that an isolated leader gives up leadership a lease sooner.
    /// Disable with `FIDUCIA_RAFT_CHECK_QUORUM=off`.
    pub check_quorum: bool,
}

impl Default for RaftTiming {
    fn default() -> Self {
        RaftTiming {
            tick: Duration::from_millis(20),
            heartbeat: Duration::from_millis(50),
            election_min_ms: 150,
            election_jitter_ms: 150,
            pre_vote: true,
            check_quorum: true,
        }
    }
}

impl RaftTiming {
    /// Read timing from the environment, falling back to the LAN defaults, then
    /// run it through [`sanitized`](Self::sanitized) so an operator typo can never
    /// produce a panicking or self-flapping configuration.
    pub fn from_env() -> Self {
        fn ms(var: &str, default: u64) -> u64 {
            std::env::var(var)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default)
        }
        let d = RaftTiming::default();
        RaftTiming {
            tick: Duration::from_millis(ms("FIDUCIA_RAFT_TICK_MS", d.tick.as_millis() as u64)),
            heartbeat: Duration::from_millis(ms(
                "FIDUCIA_RAFT_HEARTBEAT_MS",
                d.heartbeat.as_millis() as u64,
            )),
            election_min_ms: ms("FIDUCIA_RAFT_ELECTION_MIN_MS", d.election_min_ms),
            election_jitter_ms: ms("FIDUCIA_RAFT_ELECTION_JITTER_MS", d.election_jitter_ms),
            pre_vote: std::env::var("FIDUCIA_RAFT_PREVOTE")
                .ok()
                .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off"))
                .unwrap_or(d.pre_vote),
            check_quorum: std::env::var("FIDUCIA_RAFT_CHECK_QUORUM")
                .ok()
                .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off"))
                .unwrap_or(d.check_quorum),
        }
        .sanitized()
    }

    /// Clamp degenerate / unsafe values into a working range. Guards against
    /// operator typos that would otherwise be fatal or self-defeating:
    ///   * a **zero** `tick` or `heartbeat` panics `tokio::time::interval`;
    ///   * a `tick` coarser than the `heartbeat` makes the actor notice deadlines
    ///     late, so heartbeats and elections fire behind schedule;
    ///   * an election timeout **below** the heartbeat guarantees a leader can
    ///     never out-heartbeat its own election timer → perpetual flapping.
    ///
    /// Pure (only side effect is a warning log), so it is unit-tested directly.
    pub fn sanitized(mut self) -> RaftTiming {
        if self.tick.is_zero() {
            self.tick = Duration::from_millis(1);
        }
        if self.heartbeat.is_zero() {
            self.heartbeat = Duration::from_millis(1);
        }
        // Deadlines are only re-checked once per tick, so the tick must be at least
        // as fine as the heartbeat.
        if self.tick > self.heartbeat {
            self.tick = self.heartbeat;
        }
        let heartbeat_ms = self.heartbeat.as_millis() as u64;
        // Hard floor: election timeout must be at least 2x the heartbeat or the
        // cluster cannot hold a stable leader. Clamp up if misconfigured.
        let floor = heartbeat_ms.saturating_mul(2).max(1);
        if self.election_min_ms < floor {
            tracing::warn!(
                heartbeat_ms,
                requested_election_min_ms = self.election_min_ms,
                clamped_to_ms = floor,
                "raft timing: election timeout below 2x the heartbeat — clamped up to \
                 avoid guaranteed leadership flapping"
            );
            self.election_min_ms = floor;
        } else if self.election_min_ms < heartbeat_ms.saturating_mul(3) {
            // Soft guidance: 3x is the comfortable margin on a lossy / WAN link.
            tracing::warn!(
                heartbeat_ms,
                election_min_ms = self.election_min_ms,
                "raft timing: election timeout is under 3x the heartbeat — spurious \
                 elections are likely on a WAN; consider raising FIDUCIA_RAFT_ELECTION_MIN_MS"
            );
        }
        self
    }
}

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

/// A change applied to a shard's state machine, broadcast to watchers (KV,
/// elections, discovery). `scope` lets a watcher ignore changes from a different
/// primitive that happens to share a name with what it's watching.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeEvent {
    /// Which primitive changed: `"kv"`, `"election"`, or `"service"`.
    pub scope: &'static str,
    /// Domain verb: kv `put`/`delete`; election `elected`/`renewed`/`resigned`;
    /// service `register`/`heartbeat`/`deregister`.
    pub kind: &'static str,
    /// The watched name: kv key, election name, or service name.
    pub key: String,
    pub revision: u64,
    /// Optional payload (the new `Leadership` or `ServiceInstance`) so watchers
    /// can act on a single event without a follow-up read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
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
    /// Directory for durable per-shard Raft state (term/vote/log). `None` runs
    /// fully in-memory — the mode used by the in-process loopback tests; a real
    /// deployment points this at a persistent volume so a pod restart can't drop
    /// a member's log.
    pub data_dir: Option<PathBuf>,
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
            // Default to the conventional PVC mount; a deployment can override
            // with FIDUCIA_DATA_DIR. The directory must be writable (the pod
            // mounts a PersistentVolume there).
            data_dir: Some(
                std::env::var("FIDUCIA_DATA_DIR")
                    .unwrap_or_else(|_| "/var/lib/fiducia".to_string())
                    .into(),
            ),
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
    /// A serializable (non-leader) read off this shard's local applied state, for
    /// list/range fan-outs where slightly-stale results are acceptable and no
    /// single shard is authoritative.
    QueryLocal {
        request: ReadRequest,
        resp: oneshot::Sender<ReadResponse>,
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
    /// `pre_vote` echoes whether the request that produced it was a pre-vote, so
    /// the candidate counts it toward the right round.
    VoteReply {
        from: String,
        pre_vote: bool,
        resp: RequestVoteResp,
    },
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

/// A single-key read, routed to its owning shard.
pub enum ReadRequest {
    Kv { key: String },
    Lock { key: String },
    Semaphore { key: String },
    RateLimit { tenant: String, key: String },
    Schedule { name: String },
    ScheduleHistory { name: String },
    Election { name: String },
    Service { service: String },
    /// Range read: every KV key under `prefix` on one shard. Fanned out across
    /// shards by [`Node::list_kv`] and served serializably (no leader gate).
    KvList { prefix: String },
    /// Every service with live instances on one shard. Fanned out by
    /// [`Node::list_services`] and served serializably.
    ServiceList,
    /// Every schedule definition on one shard. Fanned out by
    /// [`Node::list_schedules`] for the firing loop to find due fires.
    ScheduleList,
    /// Whole-coordinator lock inventory: every grant + the FIFO wait queue. All
    /// lock state lives on the [`LOCK_DOMAIN`](crate::state::LOCK_DOMAIN) shard,
    /// so this routes to that single shard.
    LockInventory,
    /// Snapshot of every counting semaphore on the lock-coordinator shard.
    SemaphoreInventory,
    /// Every named election with live leadership on one shard. Elections route by
    /// name, so [`Node::list_elections`] fans this out and merges.
    ElectionList,
}

impl ReadRequest {
    /// Key used to route this read to its owning shard. Lock/semaphore reads route
    /// to the same lock-coordinator shard as their writes (see [`Command::routing_key`]).
    pub fn routing_key(&self) -> &str {
        match self {
            ReadRequest::Kv { key } => key,
            ReadRequest::Lock { .. } | ReadRequest::Semaphore { .. } => crate::state::LOCK_DOMAIN,
            ReadRequest::RateLimit { key, .. } => key,
            ReadRequest::Schedule { name } | ReadRequest::ScheduleHistory { name } => name,
            ReadRequest::Election { name } => name,
            ReadRequest::Service { service } => service,
            // Lock/semaphore inventory shares the single lock-coordinator shard.
            ReadRequest::LockInventory | ReadRequest::SemaphoreInventory => crate::state::LOCK_DOMAIN,
            // List reads fan out across all shards rather than routing to one.
            ReadRequest::KvList { prefix } => prefix,
            ReadRequest::ServiceList | ReadRequest::ScheduleList | ReadRequest::ElectionList => "",
        }
    }
}

/// The answer to a [`ReadRequest`], typed by domain.
#[derive(Debug)]
pub enum ReadResponse {
    Kv(Option<KvEntry>),
    Lock(LockState),
    Semaphore(SemaphoreState),
    RateLimit(Option<RateLimitSnapshot>),
    Schedule(Option<Schedule>),
    ScheduleHistory(Vec<ScheduleRun>),
    Election(Option<Leadership>),
    Service(Vec<ServiceInstance>),
    KvList(Vec<KvListItem>),
    ServiceList(Vec<ServiceSummary>),
    ScheduleList(Vec<Schedule>),
    LockInventory(LockInventory),
    SemaphoreInventory(Vec<SemaphoreState>),
    ElectionList(Vec<ElectionEntry>),
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
    /// When we last received a reply from each peer *at our current term* — proof
    /// the peer still acknowledges us as leader. Drives CheckQuorum / the leader
    /// lease: if a majority's most-recent contact has aged past one election
    /// timeout, we may have been partitioned and must step down (see
    /// [`RaftTiming::check_quorum`]).
    last_contact: HashMap<String, Instant>,
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

    // --- Raft state. `current_term`, `voted_for`, and `log` are the bits Raft
    //     must persist before acting on them; `store`, when present, is their
    //     durable home (see `crate::persist`). `commit_index`/`last_applied` are
    //     volatile but recoverable by replaying the log up to the persisted
    //     commit point. `None` store = in-memory only (loopback tests). ---
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    leader_id: Option<String>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Durable backing for term/vote/log, or `None` for an in-memory shard.
    store: Option<ShardStore>,

    // --- candidate state ---
    votes: HashSet<String>,
    // --- pre-vote (straw-poll) state, for the would-be term `pre_vote_term` ---
    pre_votes: HashSet<String>,
    pre_vote_term: u64,
    // --- leader state ---
    leader: Option<LeaderState>,

    // --- timers ---
    timing: RaftTiming,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    /// When we last heard from a valid leader (an `AppendEntries`). Tracked
    /// **separately** from `election_deadline` (which we reset for our own
    /// campaigning) so pre-vote's leader-stickiness reflects the *leader's*
    /// liveness, not our candidacy.
    last_leader_contact: Instant,
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
        timing: RaftTiming,
        store: Option<ShardStore>,
        recovered: Recovered,
    ) -> Self {
        let members = peers.len() + 1;
        let single = members == 1;
        let (changes, _) = broadcast::channel(CHANGE_BUFFER);
        // Seed from disk when we have it. A fresh shard recovers `term == 0`; this
        // engine numbers terms from 1, so keep the floor at 1 for a clean start.
        let current_term = recovered.current_term.max(1);
        let recovered_commit = recovered.commit_index.min(recovered.log.len() as u64);
        let mut actor = ShardActor {
            shard_id,
            node_id: node_id.clone(),
            peers,
            members,
            transport,
            self_tx,
            // We always restart as a follower (even if we last led) so a stale term
            // can't serve writes before re-validation; a single-node shard is the
            // exception — it has no one to elect against, so it leads from t=0.
            role: if single { Role::Leader } else { Role::Follower },
            current_term,
            voted_for: recovered.voted_for,
            leader_id: if single { Some(node_id.clone()) } else { None },
            log: recovered.log,
            commit_index: recovered_commit,
            last_applied: 0,
            store,
            votes: HashSet::new(),
            pre_votes: HashSet::new(),
            pre_vote_term: 0,
            leader: if single {
                Some(LeaderState::default())
            } else {
                None
            },
            timing,
            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
            last_leader_contact: Instant::now(),
            rng: Rng::seeded(&node_id, shard_id),
            pending: HashMap::new(),
            changes,
            state: StateMachine::new(),
        };
        actor.reset_election_deadline();
        // Rebuild the in-memory state machine from the recovered log up to the
        // committed point (the state machine itself is not persisted).
        if actor.commit_index > 0 {
            actor.apply_committed();
        }
        actor
    }

    /// The shard's event loop: drain the inbox and fire the election/heartbeat
    /// tick until every sender is dropped (node shutdown).
    async fn run(mut self, mut inbox: mpsc::Receiver<ShardMsg>) {
        let mut tick = tokio::time::interval(self.timing.tick);
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
            ShardMsg::QueryLocal { request, resp } => {
                let _ = resp.send(self.handle_query_local(request));
            }
            ShardMsg::AppendEntries { req, resp } => {
                let out = self.handle_append_entries(req);
                let _ = resp.send(out);
            }
            ShardMsg::RequestVote { req, resp } => {
                let out = self.handle_request_vote(req);
                let _ = resp.send(out);
            }
            ShardMsg::VoteReply {
                from,
                pre_vote,
                resp,
            } => self.handle_vote_reply(from, pre_vote, resp),
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
        let jitter = self.rng.below(self.timing.election_jitter_ms);
        self.election_deadline =
            Instant::now() + Duration::from_millis(self.timing.election_min_ms + jitter);
    }

    /// Periodic tick: leaders heartbeat; everyone else campaigns once their
    /// election timeout elapses without hearing from a leader.
    fn on_tick(&mut self) {
        let now = Instant::now();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_deadline {
                    self.heartbeat_deadline = now + self.timing.heartbeat;
                    self.broadcast_append_entries();
                }
                // CheckQuorum: a leader that can no longer reach a majority steps
                // down so it can't keep accepting doomed writes or answering stale
                // reads while a new leader forms on the majority side.
                if self.timing.check_quorum && self.members > 1 && !self.leader_lease_held() {
                    self.relinquish_no_quorum();
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    // With PreVote, time-out starts a non-binding straw poll first;
                    // only a pre-vote majority escalates to a real (term-bumping)
                    // election. Single-member groups never reach here (they lead
                    // from t=0), so there is always a peer to poll.
                    if self.timing.pre_vote && self.members > 1 {
                        self.start_pre_election();
                    } else {
                        self.start_election();
                    }
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

    /// PreVote straw poll: ask peers whether they *would* vote for us at
    /// `current_term + 1`, **without adopting that term or changing any state**.
    /// Only a majority of grants escalates to a real [`start_election`]. This is
    /// what stops a partitioned node — whose term has run ahead while it was
    /// isolated — from forcing a healthy leader to step down when it reconnects.
    fn start_pre_election(&mut self) {
        // Run the straw poll strictly as a follower: abandon any failed candidacy
        // so a late vote-reply from the prior term can't complete a stale election
        // while this pre-poll is in flight. The term is *not* bumped here.
        self.role = Role::Follower;
        self.votes.clear();
        self.reset_election_deadline();
        let would_be_term = self.current_term + 1;
        self.pre_vote_term = would_be_term;
        self.pre_votes.clear();
        self.pre_votes.insert(self.node_id.clone());
        tracing::debug!(
            shard = ?self.shard_id,
            node = %self.node_id,
            would_be_term,
            members = self.members,
            "raft: election timeout — starting pre-vote straw poll"
        );
        // (Unreachable for members > 1, but keep the single-member invariant.)
        if self.pre_votes.len() >= self.majority() {
            self.start_election();
            return;
        }
        self.solicit_votes(would_be_term, true);
    }

    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        tracing::info!(
            shard = ?self.shard_id,
            node = %self.node_id,
            term = self.current_term,
            members = self.members,
            "raft: election timeout — starting campaign as candidate"
        );
        self.voted_for = Some(self.node_id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.node_id.clone());
        self.reset_election_deadline();
        // Durable before we ask anyone for a vote in this term.
        self.persist_hard_state();

        if self.votes.len() >= self.majority() {
            // Single-member group: we already have a majority.
            self.become_leader();
            return;
        }
        self.solicit_votes(self.current_term, false);
    }

    /// Send `RequestVote` (real or pre-vote) to every peer for `term`, routing
    /// each reply back into our own inbox as a `VoteReply` tagged with `pre_vote`
    /// so it is counted toward the right round.
    fn solicit_votes(&self, term: u64, pre_vote: bool) {
        let req = RequestVoteReq {
            term,
            candidate_id: self.node_id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
            pre_vote,
        };
        for peer in self.peers.clone() {
            let transport = self.transport.clone();
            let self_tx = self.self_tx.clone();
            let shard = self.shard_id;
            let req = req.clone();
            tokio::spawn(async move {
                if let Some(resp) = transport.request_vote(&peer, shard, req).await {
                    let _ = self_tx
                        .send(ShardMsg::VoteReply {
                            from: peer,
                            pre_vote,
                            resp,
                        })
                        .await;
                }
            });
        }
    }

    fn handle_vote_reply(&mut self, from: String, pre_vote: bool, resp: RequestVoteResp) {
        // A higher term anywhere means we're behind: adopt it and stand down.
        if resp.term > self.current_term {
            self.step_down(resp.term, None);
            return;
        }
        if pre_vote {
            // Pre-vote round: we are still a Follower at `current_term`; a majority
            // of grants for the would-be term promotes us to a real election.
            // Ignore replies once our term has advanced past this round.
            if self.pre_vote_term != self.current_term + 1 {
                return;
            }
            if resp.granted {
                self.pre_votes.insert(from);
                if self.pre_votes.len() >= self.majority() {
                    self.start_election();
                }
            }
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
        tracing::info!(
            shard = ?self.shard_id,
            node = %self.node_id,
            term = self.current_term,
            votes = self.votes.len(),
            members = self.members,
            "raft: won election — now leader for shard"
        );
        // The peers that just voted for us *are* fresh majority contact, so seed the
        // leader lease from them — otherwise the very first lease window would look
        // expired and we'd step down before the first heartbeat round returns.
        let voters = std::mem::take(&mut self.votes);
        let now = Instant::now();
        let mut ls = LeaderState::default();
        let next = self.last_log_index() + 1;
        for peer in &self.peers {
            ls.next_index.insert(peer.clone(), next);
            ls.match_index.insert(peer.clone(), 0);
            ls.in_flight.insert(peer.clone(), false);
            if voters.contains(peer) {
                ls.last_contact.insert(peer.clone(), now);
            }
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
        // Durable before this entry can count toward a commit.
        self.persist_log_append();

        self.heartbeat_deadline = Instant::now() + self.timing.heartbeat;
        self.maybe_advance_commit(); // single-node commits the no-op immediately
        self.broadcast_append_entries();
    }

    /// Convert to follower at `term`, optionally learning the new leader. Fails
    /// any outstanding client writes so they retry against the real leader.
    fn step_down(&mut self, term: u64, leader: Option<String>) {
        if self.role != Role::Follower {
            tracing::info!(
                shard = ?self.shard_id,
                node = %self.node_id,
                term,
                "raft: stepped down to follower"
            );
        }
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        if leader.is_some() {
            self.leader_id = leader;
        }
        self.persist_hard_state();
        self.reset_election_deadline();
        self.fail_pending();
    }

    // --- durability: persist before acting (no-ops for an in-memory shard) ----

    /// Persist `current_term`, `voted_for`, and `commit_index`. Call after any
    /// change to them and **before** the action that relies on them (granting a
    /// vote, campaigning, committing). A persist failure is logged, not hidden —
    /// it means we may be running without the durability the caller assumes.
    fn persist_hard_state(&mut self) {
        if let Some(store) = self.store.as_ref() {
            if let Err(e) =
                store.save_meta(self.current_term, self.voted_for.as_deref(), self.commit_index)
            {
                tracing::error!(shard = ?self.shard_id, error = %e, "raft: failed to persist hard state");
            }
        }
    }

    /// Persist newly-appended tail entries (pure-append path).
    fn persist_log_append(&mut self) {
        if let Some(store) = self.store.as_mut() {
            if let Err(e) = store.append_tail(&self.log) {
                tracing::error!(shard = ?self.shard_id, error = %e, "raft: failed to persist log append");
            }
        }
    }

    /// Persist the full log after a conflicting suffix was truncated/replaced.
    fn persist_log_rewrite(&mut self) {
        if let Some(store) = self.store.as_mut() {
            if let Err(e) = store.rewrite(&self.log) {
                tracing::error!(shard = ?self.shard_id, error = %e, "raft: failed to persist log rewrite");
            }
        }
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
            // Any reply at our term is proof this peer still sees us as leader —
            // refresh the lease clock regardless of log success/mismatch.
            ls.last_contact.insert(from.clone(), Instant::now());
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
            self.persist_hard_state(); // record the advanced commit pointer
        }
    }

    // --- CheckQuorum / leader lease ---------------------------------------

    /// Whether this leader has confirmed contact with a majority of the group
    /// within the last election timeout — i.e. holds a valid leader lease and may
    /// safely act as leader (serve a linearizable read, stay leader).
    ///
    /// Returns `true` when CheckQuorum is disabled or this is a single-member group
    /// (the node alone *is* the majority), so the feature is byte-identical to the
    /// old behaviour when off.
    fn leader_lease_held(&self) -> bool {
        if !self.timing.check_quorum || self.members == 1 {
            return true;
        }
        let Some(ls) = self.leader.as_ref() else {
            return false; // not actually leading
        };
        // Most-recent-contact instant per member: `now` for self, last reply for
        // each peer (absent ⇒ never). The majority-th most recent of these is the
        // latest moment at which a majority was in contact; the lease holds for one
        // election timeout past it.
        let now = Instant::now();
        let never = now.checked_sub(Duration::from_secs(86_400)).unwrap_or(now);
        let mut contacts: Vec<Instant> = Vec::with_capacity(self.members);
        contacts.push(now); // self
        for peer in &self.peers {
            contacts.push(ls.last_contact.get(peer).copied().unwrap_or(never));
        }
        contacts.sort_unstable_by(|a, b| b.cmp(a)); // most-recent first
        let majority_contact = contacts[self.majority() - 1];
        majority_contact.elapsed() < Duration::from_millis(self.timing.election_min_ms)
    }

    /// Step down because the leader lease lapsed (CheckQuorum). We keep the same
    /// term — we have *not* observed a newer one, we have simply lost contact — and
    /// become a follower so we stop serving authoritative reads/writes. The normal
    /// election timeout then governs whether we (or someone with quorum) campaign.
    fn relinquish_no_quorum(&mut self) {
        tracing::warn!(
            shard = ?self.shard_id,
            node = %self.node_id,
            term = self.current_term,
            "raft: leader lease lapsed (no majority contact within an election timeout) \
             — stepping down to avoid split-brain / stale reads (check-quorum)"
        );
        let term = self.current_term;
        self.step_down(term, None);
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
            // Durable before we answer this RPC (even the reject path below).
            self.persist_hard_state();
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
        let mut truncated = false;
        let mut grew = false;
        for entry in req.entries {
            idx += 1;
            match self.log.get((idx - 1) as usize) {
                Some(existing) if existing.term == entry.term => {} // already have it
                Some(_) => {
                    self.log.truncate((idx - 1) as usize);
                    self.log.push(entry);
                    truncated = true;
                }
                None => {
                    self.log.push(entry);
                    grew = true;
                }
            }
        }
        // Persist the log change before acking success: a full rewrite if we
        // truncated a conflicting suffix, otherwise just the appended tail.
        if truncated {
            self.persist_log_rewrite();
        } else if grew {
            self.persist_log_append();
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
            self.apply_committed();
            self.persist_hard_state(); // record the advanced commit pointer
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
        self.last_leader_contact = Instant::now(); // heard from the leader
        self.reset_election_deadline();
        // Anything we were leading is no longer ours to commit.
        self.fail_pending();
    }

    fn handle_request_vote(&mut self, req: RequestVoteReq) -> RequestVoteResp {
        // PreVote is answered without mutating any Raft state (no term bump, no
        // `voted_for`, no deadline reset) — that read-only-ness is the whole point.
        if req.pre_vote {
            return self.handle_pre_vote(&req);
        }
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
            // Durable before we tell the candidate it has our vote.
            self.persist_hard_state();
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

    /// Answer a PreVote straw poll. Pure read: changes nothing. Grant only if
    ///   * the candidate's would-be term isn't stale (`req.term >= current_term`),
    ///   * its log is at least as up-to-date as ours, **and**
    ///   * we are not currently being served by a live leader — i.e. we know of no
    ///     leader, or our own election timeout has already lapsed.
    ///
    /// That last clause is the leader-stickiness that makes pre-vote *refuse* to
    /// disrupt a healthy leader: while heartbeats keep arriving, `election_deadline`
    /// stays in the future, so a rejoining/partitioned node's pre-vote is denied
    /// and it can never bump the cluster's term. At cold start `leader_id` is
    /// `None`, so the first election is still granted immediately.
    fn handle_pre_vote(&self, req: &RequestVoteReq) -> RequestVoteResp {
        let log_ok = (req.last_log_term > self.last_log_term())
            || (req.last_log_term == self.last_log_term()
                && req.last_log_index >= self.last_log_index());
        // A leader is presumed alive if we know one AND we've heard from it within
        // an election timeout. At cold start `leader_id` is `None`, so the first
        // election is granted; once a known leader stops heartbeating, contact goes
        // stale and pre-votes flow again so failover can proceed.
        let leader_alive = self.leader_id.is_some()
            && self.last_leader_contact.elapsed() < Duration::from_millis(self.timing.election_min_ms);
        RequestVoteResp {
            term: self.current_term,
            granted: req.term >= self.current_term && log_ok && !leader_alive,
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
        // Durable before this entry can count toward a commit / be acked.
        self.persist_log_append();
        // Block the client on this index committing.
        self.pending.insert(index, resp);

        if self.members == 1 {
            // One-member quorum: commit (and apply, which resolves the waiter) now.
            self.commit_index = index;
            self.apply_committed();
            self.persist_hard_state(); // record the advanced commit pointer
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
            self.publish_change(&command, &applied.output, applied.revision);
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

    fn publish_change(&self, command: &Command, output: &serde_json::Value, revision: u64) {
        // Only publish changes that actually mutated state: a campaign that lost
        // or a renew by a stale token must not look like a leadership change.
        let flagged = |field: &str| output.get(field).and_then(|v| v.as_bool()).unwrap_or(false);
        let detail = |field: &str| output.get(field).cloned();
        let event = match command {
            Command::KvPut { key, .. } => Some(ChangeEvent {
                scope: "kv",
                kind: "put",
                key: key.clone(),
                revision,
                detail: None,
            }),
            Command::KvDelete { key } => Some(ChangeEvent {
                scope: "kv",
                kind: "delete",
                key: key.clone(),
                revision,
                detail: None,
            }),
            Command::ElectionCampaign { name, .. } if flagged("won") => Some(ChangeEvent {
                scope: "election",
                kind: "elected",
                key: name.clone(),
                revision,
                detail: detail("leadership"),
            }),
            Command::ElectionRenew { name, .. } if flagged("renewed") => Some(ChangeEvent {
                scope: "election",
                kind: "renewed",
                key: name.clone(),
                revision,
                detail: detail("leadership"),
            }),
            Command::ElectionResign { name, .. } if flagged("resigned") => Some(ChangeEvent {
                scope: "election",
                kind: "resigned",
                key: name.clone(),
                revision,
                detail: None,
            }),
            Command::ServiceRegister { service, .. } if flagged("registered") => Some(ChangeEvent {
                scope: "service",
                kind: "register",
                key: service.clone(),
                revision,
                detail: detail("instance"),
            }),
            Command::ServiceHeartbeat { service, .. } if flagged("heartbeat") => Some(ChangeEvent {
                scope: "service",
                kind: "heartbeat",
                key: service.clone(),
                revision,
                detail: detail("instance"),
            }),
            Command::ServiceDeregister { service, .. } if flagged("deregistered") => {
                Some(ChangeEvent {
                    scope: "service",
                    kind: "deregister",
                    key: service.clone(),
                    revision,
                    detail: None,
                })
            }
            _ => None,
        };
        if let Some(event) = event {
            let _ = self.changes.send(event); // ignore "no subscribers"
        }
    }

    /// Serve a read off applied state — leader only, for linearizability.
    fn handle_query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            });
        }
        // Linearizable read gate (leader lease): a leader that hasn't confirmed a
        // majority within the last election timeout might already be deposed, so it
        // must not answer authoritatively. Closes the sub-tick window before
        // CheckQuorum's `on_tick` step-down fires. The client retries (503) and is
        // rerouted to whoever actually holds quorum. Serializable list/fan-out reads
        // (handle_query_local) deliberately skip this — stale results are allowed
        // there by contract.
        if !matches!(
            request,
            ReadRequest::KvList { .. }
                | ReadRequest::ServiceList
                | ReadRequest::ScheduleList
                | ReadRequest::ElectionList
        ) && !self.leader_lease_held()
        {
            return Err(ProposeError::Unavailable {
                shard: self.shard_id,
            });
        }
        match request {
            ReadRequest::Kv { key } => Ok(ReadResponse::Kv(self.state.kv_get(&key))),
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
            ReadRequest::Service { service } => {
                Ok(ReadResponse::Service(self.state.service_list(&service)))
            }
            // Lock/semaphore inventory is a linearizable read of the single
            // coordinator shard, so it stays leader-gated like the per-key reads.
            ReadRequest::LockInventory => {
                Ok(ReadResponse::LockInventory(self.state.lock_inventory()))
            }
            ReadRequest::SemaphoreInventory => Ok(ReadResponse::SemaphoreInventory(
                self.state.semaphore_inventory(),
            )),
            // List reads are served serializably; route them through the local
            // path even if they reach here.
            list @ (ReadRequest::KvList { .. }
            | ReadRequest::ServiceList
            | ReadRequest::ScheduleList
            | ReadRequest::ElectionList) => Ok(self.handle_query_local(list)),
        }
    }

    /// Serializable read off local applied state — used for list/range fan-outs.
    /// No leader gate: every shard replica can answer for its own slice, and the
    /// fan-out merges them. Only list variants are expected here.
    fn handle_query_local(&self, request: ReadRequest) -> ReadResponse {
        match request {
            ReadRequest::KvList { prefix } => ReadResponse::KvList(self.state.kv_list(&prefix)),
            ReadRequest::ServiceList => ReadResponse::ServiceList(self.state.service_names()),
            ReadRequest::ScheduleList => ReadResponse::ScheduleList(self.state.schedule_list()),
            ReadRequest::ElectionList => {
                ReadResponse::ElectionList(self.state.election_inventory())
            }
            ReadRequest::LockInventory => {
                ReadResponse::LockInventory(self.state.lock_inventory())
            }
            ReadRequest::SemaphoreInventory => {
                ReadResponse::SemaphoreInventory(self.state.semaphore_inventory())
            }
            // A single-key read arriving on the local path: serve it off applied
            // state too rather than erroring.
            ReadRequest::Kv { key } => ReadResponse::Kv(self.state.kv_get(&key)),
            ReadRequest::Lock { key } => ReadResponse::Lock(self.state.lock_get(&key)),
            ReadRequest::Semaphore { key } => {
                ReadResponse::Semaphore(self.state.semaphore_get(&key))
            }
            ReadRequest::RateLimit { tenant, key } => {
                ReadResponse::RateLimit(self.state.rate_limit_get(&tenant, &key))
            }
            ReadRequest::Schedule { name } => ReadResponse::Schedule(self.state.schedule_get(&name)),
            ReadRequest::ScheduleHistory { name } => {
                ReadResponse::ScheduleHistory(self.state.schedule_history(&name))
            }
            ReadRequest::Election { name } => {
                ReadResponse::Election(self.state.election_get(&name))
            }
            ReadRequest::Service { service } => {
                ReadResponse::Service(self.state.service_list(&service))
            }
        }
    }

    fn status(&self) -> ShardStatus {
        // Quorum + replication are leader-side knowledge: only the leader tracks
        // each peer's match_index. A follower reports an empty replication view and
        // `has_quorum = false` (it cannot vouch for the group's health).
        let (replication, healthy_replicas, has_quorum) = if self.role == Role::Leader {
            let last = self.last_log_index();
            let mut reps = Vec::with_capacity(self.peers.len());
            let mut caught_up = 1usize; // self always has the committed prefix
            if let Some(ls) = &self.leader {
                for peer in &self.peers {
                    let match_index = ls.match_index.get(peer).copied().unwrap_or(0);
                    if match_index >= self.commit_index {
                        caught_up += 1;
                    }
                    reps.push(PeerReplication {
                        peer: peer.clone(),
                        match_index,
                        lag: last.saturating_sub(match_index),
                        in_flight: ls.in_flight.get(peer).copied().unwrap_or(false),
                    });
                }
            }
            reps.sort_by(|a, b| a.peer.cmp(&b.peer));
            (reps, caught_up, caught_up >= self.majority())
        } else {
            (Vec::new(), 0, false)
        };
        ShardStatus {
            shard_id: self.shard_id,
            role: self.role,
            term: self.current_term,
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            last_log_index: self.last_log_index(),
            healthy_replicas,
            has_quorum,
            replication,
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
    /// In-process per-operation latency + outcome metrics (see `/v1/observe/metrics`).
    metrics: Arc<crate::metrics::Metrics>,
}

impl Node {
    /// Boot this node's shard actors over the given [`Transport`]. With no peers
    /// each actor is the sole member — and therefore leader — of its group; with
    /// peers they run real elections.
    ///
    /// Must be called from within a Tokio runtime (it spawns the actor tasks).
    pub fn bootstrap(config: NodeConfig, transport: Transport) -> Self {
        let transport = Arc::new(transport);
        let timing = RaftTiming::from_env();
        let mut shards = HashMap::new();
        let mut tasks = Vec::new();
        for shard_id in 0..config.shard_count {
            let (tx, rx) = mpsc::channel(SHARD_INBOX_CAPACITY);
            if let Some(reg) = transport.loopback_registry() {
                reg.register(&config.node_id, shard_id, tx.clone());
            }
            // Open durable storage when a data dir is configured. Failing closed
            // here (panic) is deliberate: a coordination engine that silently
            // ran without durability would be worse than a visible crashloop.
            let (store, recovered) = match &config.data_dir {
                Some(dir) => {
                    let (s, r) = ShardStore::open(dir, shard_id).unwrap_or_else(|e| {
                        panic!("fiducia-node: cannot open durable store for shard {shard_id} under {dir:?}: {e}")
                    });
                    (Some(s), r)
                }
                None => (None, Recovered::default()),
            };
            let actor = ShardActor::new(
                shard_id,
                config.node_id.clone(),
                config.peers.clone(),
                transport.clone(),
                tx.clone(),
                timing,
                store,
                recovered,
            );
            tasks.push(tokio::spawn(actor.run(rx)));
            shards.insert(shard_id, tx);
        }
        Node {
            config,
            shards,
            transport,
            tasks,
            metrics: Arc::new(crate::metrics::Metrics::new()),
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
        // Telemetry: capture the op label + routing key BEFORE the command is moved
        // into the shard actor, so every lock/semaphore/kv write emits one outcome
        // event with op/key/shard/latency. This single chokepoint covers all writes.
        let op = command.kind();
        let routing_key = command.routing_key().to_string();
        let shard = self.shard_for(&routing_key);
        let started = std::time::Instant::now();
        let result = async {
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
        .await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1e3;
        self.metrics.record(op, elapsed_ms, result.is_ok());
        match &result {
            Ok(_) => tracing::info!(
                op,
                shard = ?shard,
                key = %routing_key,
                elapsed_ms,
                committed = true,
                "propose committed"
            ),
            Err(ProposeError::NotLeader { leader, .. }) => tracing::debug!(
                op,
                shard = ?shard,
                key = %routing_key,
                elapsed_ms,
                leader = leader.as_deref().unwrap_or("unknown"),
                "propose redirected: this node is not the shard leader"
            ),
            Err(ProposeError::Unavailable { .. }) => tracing::warn!(
                op,
                shard = ?shard,
                key = %routing_key,
                elapsed_ms,
                "propose unavailable: shard not hosted here or commit lost quorum"
            ),
        }
        result
    }

    /// Serve a single-key read from the owning shard.
    pub async fn query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        let routing_key = request.routing_key().to_string();
        let shard = self.shard_for(&routing_key);
        let started = std::time::Instant::now();
        let Some(tx) = self.sender(shard) else {
            tracing::debug!(shard = ?shard, key = %routing_key, "query unavailable: shard not hosted here");
            self.metrics.record("read", started.elapsed().as_secs_f64() * 1e3, false);
            return Err(ProposeError::Unavailable { shard });
        };
        let (resp, rx) = oneshot::channel();
        if tx.send(ShardMsg::Query { request, resp }).await.is_err() {
            self.metrics.record("read", started.elapsed().as_secs_f64() * 1e3, false);
            return Err(ProposeError::Unavailable { shard });
        }
        let result = rx.await.unwrap_or(Err(ProposeError::Unavailable { shard }));
        self.metrics
            .record("read", started.elapsed().as_secs_f64() * 1e3, result.is_ok());
        tracing::debug!(
            shard = ?shard,
            key = %routing_key,
            ok = result.is_ok(),
            "query served"
        );
        result
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

    /// Subscribe to the change stream of the shard owning `key` (for a watch).
    /// Works for any primitive routed by name: a KV key, an election name, or a
    /// service name all hash to one shard, and the caller filters by scope+key.
    pub async fn watch(&self, key: &str) -> Option<broadcast::Receiver<ChangeEvent>> {
        let shard = self.shard_for(key);
        let tx = self.sender(shard)?;
        let (resp, rx) = oneshot::channel();
        tx.send(ShardMsg::Subscribe { resp }).await.ok()?;
        rx.await.ok()
    }

    /// Fan a serializable read out across **every shard this node hosts**, then
    /// merge. `make` builds a fresh request per shard ([`ReadRequest`] isn't
    /// `Clone`). Used for list/range operations that no single shard owns.
    async fn query_all_shards(
        &self,
        make: impl Fn() -> ReadRequest,
    ) -> Vec<ReadResponse> {
        let mut out = Vec::with_capacity(self.shards.len());
        for tx in self.shards.values() {
            let (resp, rx) = oneshot::channel();
            if tx
                .send(ShardMsg::QueryLocal {
                    request: make(),
                    resp,
                })
                .await
                .is_ok()
            {
                if let Ok(response) = rx.await {
                    out.push(response);
                }
            }
        }
        out
    }

    /// Every live KV entry under `prefix`, merged across shards and sorted by key.
    pub async fn list_kv(&self, prefix: &str) -> Vec<KvListItem> {
        let mut items: Vec<KvListItem> = self
            .query_all_shards(|| ReadRequest::KvList {
                prefix: prefix.to_string(),
            })
            .await
            .into_iter()
            .filter_map(|r| match r {
                ReadResponse::KvList(v) => Some(v),
                _ => None,
            })
            .flatten()
            .collect();
        items.sort_by(|a, b| a.key.cmp(&b.key));
        items
    }

    /// Every service with live instances, merged across shards. A service name
    /// routes to a single shard, so counts don't need de-duping across shards,
    /// but we still merge defensively in case a name appears more than once.
    pub async fn list_services(&self) -> Vec<ServiceSummary> {
        let mut merged: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
        for response in self.query_all_shards(|| ReadRequest::ServiceList).await {
            if let ReadResponse::ServiceList(summaries) = response {
                for summary in summaries {
                    *merged.entry(summary.service).or_default() += summary.instances;
                }
            }
        }
        merged
            .into_iter()
            .map(|(service, instances)| ServiceSummary { service, instances })
            .collect()
    }

    /// Every schedule definition across all shards this node hosts. The firing
    /// loop reads this, keeps only schedules whose shard it currently leads, and
    /// fires the due ones.
    pub async fn list_schedules(&self) -> Vec<Schedule> {
        self.query_all_shards(|| ReadRequest::ScheduleList)
            .await
            .into_iter()
            .filter_map(|r| match r {
                ReadResponse::ScheduleList(v) => Some(v),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// The whole-coordinator lock inventory (every grant + the wait queue). All
    /// lock state lives on one shard, so this is a single leader-gated read; a
    /// non-leader of the lock shard returns `NotLeader` for the caller to redirect.
    pub async fn lock_inventory(&self) -> Result<LockInventory, ProposeError> {
        match self.query(ReadRequest::LockInventory).await? {
            ReadResponse::LockInventory(inv) => Ok(inv),
            _ => Err(ProposeError::Unavailable {
                shard: self.shard_for(crate::state::LOCK_DOMAIN),
            }),
        }
    }

    /// A snapshot of every counting semaphore on the lock-coordinator shard.
    pub async fn semaphore_inventory(&self) -> Result<Vec<SemaphoreState>, ProposeError> {
        match self.query(ReadRequest::SemaphoreInventory).await? {
            ReadResponse::SemaphoreInventory(list) => Ok(list),
            _ => Err(ProposeError::Unavailable {
                shard: self.shard_for(crate::state::LOCK_DOMAIN),
            }),
        }
    }

    /// Every named election's current leader, merged across all shards this node
    /// hosts (elections route by name) and sorted by name.
    pub async fn list_elections(&self) -> Vec<ElectionEntry> {
        let mut out: Vec<ElectionEntry> = self
            .query_all_shards(|| ReadRequest::ElectionList)
            .await
            .into_iter()
            .filter_map(|r| match r {
                ReadResponse::ElectionList(v) => Some(v),
                _ => None,
            })
            .flatten()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Aggregated per-operation call metrics (counts, error rate, latency).
    pub fn metrics(&self) -> &crate::metrics::Metrics {
        &self.metrics
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

/// Per-shard consensus status, surfaced by `/v1/status` and `/v1/observe/shards`.
#[derive(Debug, Clone, Serialize)]
pub struct ShardStatus {
    pub shard_id: ShardId,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    /// Highest log index applied to the state machine (≤ `commit_index`); the gap
    /// is apply lag.
    pub last_applied: u64,
    pub last_log_index: u64,
    /// Replicas (incl. self) caught up to `commit_index`. Leader-only; 0 elsewhere.
    pub healthy_replicas: usize,
    /// Whether a majority of the group is caught up — i.e. the shard can survive
    /// the loss of one more member without losing quorum. Leader-only.
    pub has_quorum: bool,
    /// Per-peer replication progress. Populated only while this node leads.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub replication: Vec<PeerReplication>,
}

/// One follower's replication progress, as seen by the shard leader.
#[derive(Debug, Clone, Serialize)]
pub struct PeerReplication {
    pub peer: String,
    /// Highest log index the leader knows this peer has stored.
    pub match_index: u64,
    /// How far behind the leader's log tail this peer is (`last_log_index - match`).
    pub lag: u64,
    /// Whether an `AppendEntries` to this peer is currently outstanding.
    pub in_flight: bool,
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
                // In-memory: the loopback cluster tests don't touch disk.
                data_dir: None,
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
    async fn observability_reads_surface_locks_elections_and_quorum() {
        let reg = LoopbackRegistry::new();
        let n = node("solo", &[], 4, &reg);

        // Take a lock and win an election, then read them back through the
        // observability fan-outs (not the per-key getters).
        n.propose(Command::LockAcquire {
            keys: vec!["orders/42".to_string()],
            holder: "worker-a".to_string(),
            ttl_ms: 30_000,
            wait: false,
        })
        .await
        .expect("lock commit");
        n.propose(Command::ElectionCampaign {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            ttl_ms: 30_000,
            metadata: std::collections::HashMap::new(),
        })
        .await
        .expect("campaign commit");

        let inv = n.lock_inventory().await.expect("lock inventory");
        assert_eq!(inv.held.len(), 1);
        assert_eq!(inv.held[0].holder, "worker-a");

        let elections = n.list_elections().await;
        assert_eq!(elections.len(), 1);
        assert_eq!(elections[0].name, "scheduler");
        assert_eq!(elections[0].leadership.leader, "node-a");

        // A single-node group is its own majority, so every led shard reports
        // quorum, and the metrics registry recorded the proposals above.
        let status = n.status().await;
        assert!(status.shards.iter().all(|s| s.has_quorum));
        assert!(status
            .shards
            .iter()
            .all(|s| s.last_applied == s.commit_index));
        let ops = n.metrics().snapshot();
        assert!(
            ops.iter().any(|o| o.op == "lock.acquire" && o.count >= 1),
            "propose path should have recorded lock.acquire latency"
        );
    }

    #[tokio::test]
    async fn committed_state_survives_a_restart_via_the_durable_store() {
        // A single-node group with a real on-disk store. Commit a write, drop the
        // node (simulating a pod restart), boot a fresh node on the SAME data dir,
        // and prove the committed value is recovered by log replay — the whole
        // point of persisting term/vote/log instead of running in memory.
        let dir = std::env::temp_dir().join(format!(
            "fiducia-node-restart-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = || NodeConfig {
            node_id: "solo".to_string(),
            peers: vec![],
            shard_count: 1,
            data_dir: Some(dir.clone()),
        };

        {
            let reg = LoopbackRegistry::new();
            let n = Node::bootstrap(cfg(), Transport::loopback(reg));
            let out = n.propose(put("orders/42", "paid")).await.expect("commit");
            assert!(out.output["ok"].as_bool().unwrap());
            n.shutdown(None); // simulate the process going away
        }

        {
            let reg = LoopbackRegistry::new();
            let n = Node::bootstrap(cfg(), Transport::loopback(reg));
            match n
                .query(ReadRequest::Kv {
                    key: "orders/42".to_string(),
                })
                .await
            {
                Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "paid"),
                other => panic!("committed write was not recovered after restart: {other:?}"),
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
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

    /// Cluster-wide: poll until **every** shard in `0..shard_count` has settled on
    /// exactly one leader across `nodes`, or panic. Returns each shard's leader idx.
    async fn await_all_shards_converged(
        nodes: &[&Node],
        shard_count: u32,
        tries: u32,
    ) -> Vec<usize> {
        for _ in 0..tries {
            // Snapshot every node once per round (status is a per-shard scan).
            let statuses: Vec<NodeStatus> = {
                let mut v = Vec::with_capacity(nodes.len());
                for n in nodes {
                    v.push(n.status().await);
                }
                v
            };
            let mut leaders = Vec::with_capacity(shard_count as usize);
            let mut all_single = true;
            for shard in 0..shard_count {
                let holders: Vec<usize> = statuses
                    .iter()
                    .enumerate()
                    .filter(|(_, st)| {
                        st.shards
                            .iter()
                            .any(|s| s.shard_id == shard && s.role == Role::Leader)
                    })
                    .map(|(i, _)| i)
                    .collect();
                if holders.len() == 1 {
                    leaders.push(holders[0]);
                } else {
                    all_single = false;
                    break;
                }
            }
            if all_single {
                return leaders;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("cluster did not converge to one leader per shard");
    }

    /// The headline multi-Raft property the node exists to provide: a **single
    /// process** (one [`Node`]) is simultaneously the **leader of 2+ shards** and a
    /// **follower of 2+ other shards**, each shard an independent Raft group with
    /// its own term/log/leader. This is what "1+ leaders and 1+ followers in one
    /// process" means; the test pins it so a refactor can't quietly collapse the
    /// per-shard isolation back into a single global Raft group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_process_simultaneously_leads_and_follows_multiple_shards() {
        let reg = LoopbackRegistry::new();
        // 12 shards over 3 nodes: concentration of all leadership on one node
        // (the only split with zero mixed-role nodes) has probability
        // 3·(1/3)^12 ≈ 6e-6, so a mixed-role node is observed deterministically
        // in practice while the elections stay genuinely independent per shard.
        let shard_count = 12;
        let a = node("a", &["b", "c"], shard_count, &reg);
        let b = node("b", &["a", "c"], shard_count, &reg);
        let c = node("c", &["a", "b"], shard_count, &reg);
        let nodes = [&a, &b, &c];

        let leader_of_shard = await_all_shards_converged(&nodes, shard_count, 400).await;

        // (1) Exactly one leader per shard, cluster-wide (guaranteed by Raft; the
        //     convergence helper already enforced it — assert the count again).
        assert_eq!(leader_of_shard.len(), shard_count as usize);

        // (2) Each node hosts every shard, in exactly one of {leader, follower}
        //     once converged (no shard left perpetually mid-election).
        let mut mixed_role_nodes = 0;
        for n in &nodes {
            let st = n.status().await;
            assert_eq!(
                st.leader_count + st.follower_count,
                shard_count as usize,
                "node {} must host all {shard_count} shards as leader or follower",
                st.node_id,
            );
            assert_eq!(
                st.leading_shards.len(),
                st.leader_count,
                "leading_shards must match leader_count"
            );
            assert_eq!(
                st.following_shards.len(),
                st.follower_count,
                "following_shards must match follower_count"
            );
            if st.leader_count >= 2 && st.follower_count >= 2 {
                mixed_role_nodes += 1;
            }
        }
        // (3) The headline assertion: at least one single process holds multiple
        //     leader roles AND multiple follower roles at the same time.
        assert!(
            mixed_role_nodes >= 1,
            "expected a node leading >=2 shards while following >=2 others"
        );

        // (4) Writes routed to keys owned by different shards each commit through
        //     that shard's own leader — proving the mixed roles are functional,
        //     not just a status artifact. Drive enough distinct keys to touch at
        //     least two different shards led by (potentially) different nodes.
        let mut shards_written: HashSet<ShardId> = HashSet::new();
        for i in 0..(shard_count * 2) {
            let key = format!("orders/{i}");
            let shard = a.shard_for(&key);
            let leader_idx = leader_of_shard[shard as usize];
            let out = nodes[leader_idx]
                .propose(put(&key, "v"))
                .await
                .expect("write commits via the owning shard's leader");
            assert_eq!(out.shard, shard);
            assert!(out.output["ok"].as_bool().unwrap());
            shards_written.insert(shard);
        }
        assert!(
            shards_written.len() >= 2,
            "writes must commit across multiple independent shards"
        );

        // (5) A write sent to a NON-leader of its shard is redirected, not served
        //     by the wrong replica — per-shard leadership is enforced per shard.
        let shard0_leader = leader_of_shard[0];
        let key0 = (0..)
            .map(|i| format!("k/{i}"))
            .find(|k| a.shard_for(k) == 0)
            .unwrap();
        let non_leader = (0..3).find(|i| *i != shard0_leader).unwrap();
        let err = nodes[non_leader]
            .propose(put(&key0, "v"))
            .await
            .expect_err("a non-leader of shard 0 must redirect");
        assert!(matches!(err, ProposeError::NotLeader { shard: 0, .. }));
    }

    // --- WAN timing + PreVote ---------------------------------------------

    /// Unset env must reproduce the original LAN constants exactly, so a node that
    /// configures nothing behaves byte-for-byte as before this change.
    #[test]
    fn raft_timing_defaults_match_the_original_lan_constants() {
        let t = RaftTiming::default();
        assert_eq!(t.tick, Duration::from_millis(20));
        assert_eq!(t.heartbeat, Duration::from_millis(50));
        assert_eq!(t.election_min_ms, 150);
        assert_eq!(t.election_jitter_ms, 150);
        assert!(t.pre_vote, "pre-vote on by default");
        // Defaults are already sane: sanitize is a no-op on them.
        let s = t.sanitized();
        assert_eq!(s.tick, t.tick);
        assert_eq!(s.heartbeat, t.heartbeat);
        assert_eq!(s.election_min_ms, t.election_min_ms);
    }

    /// Operator-typo guards: `sanitized` must never return a config that panics
    /// `tokio::time::interval` or that can't hold a stable leader.
    #[test]
    fn raft_timing_sanitized_clamps_degenerate_values() {
        let timing = |tick, hb, emin| RaftTiming {
            tick: Duration::from_millis(tick),
            heartbeat: Duration::from_millis(hb),
            election_min_ms: emin,
            election_jitter_ms: 0,
            pre_vote: true,
            check_quorum: true,
        };

        // Zero tick/heartbeat would panic tokio's interval — floored to 1ms.
        let t = timing(0, 0, 0).sanitized();
        assert_eq!(t.tick, Duration::from_millis(1));
        assert_eq!(t.heartbeat, Duration::from_millis(1));
        assert!(t.election_min_ms >= 2, "election clamped to >= 2x heartbeat");

        // Tick coarser than the heartbeat is clamped down to the heartbeat.
        let t = timing(500, 150, 1000).sanitized();
        assert_eq!(t.tick, Duration::from_millis(150));
        assert_eq!(t.election_min_ms, 1000, "a sane election timeout is preserved");

        // Election timeout below 2x the heartbeat is clamped up.
        let t = timing(20, 150, 100).sanitized();
        assert_eq!(t.election_min_ms, 300, "clamped to 2x heartbeat");

        // A realistic WAN config passes through untouched.
        let t = timing(20, 150, 1000).sanitized();
        assert_eq!(t.tick, Duration::from_millis(20));
        assert_eq!(t.election_min_ms, 1000);
    }

    /// Build a bare follower shard actor (3-member group) for white-box tests of
    /// the pre-vote decision. Not wired into any cluster.
    fn follower_actor() -> ShardActor {
        let reg = LoopbackRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        ShardActor::new(
            0,
            "a".to_string(),
            vec!["b".to_string(), "c".to_string()],
            Arc::new(Transport::loopback(reg)),
            tx,
            RaftTiming::default(),
            None,
            Recovered::default(),
        )
    }

    /// A 3-member actor forced into the leader role with empty replication state,
    /// for exercising the leader lease / CheckQuorum logic without a live cluster.
    fn leader_actor() -> ShardActor {
        let mut a = follower_actor();
        a.role = Role::Leader;
        a.leader_id = Some("a".to_string());
        let mut ls = LeaderState::default();
        for p in &a.peers {
            ls.next_index.insert(p.clone(), 1);
            ls.match_index.insert(p.clone(), 0);
            ls.in_flight.insert(p.clone(), false);
        }
        a.leader = Some(ls);
        a
    }

    /// CheckQuorum/leader-lease: a leader holds the lease only while a *majority*
    /// has contacted it within an election timeout. Once the lease lapses it must
    /// refuse linearizable reads and (on the next tick) step down — closing the
    /// stale-leader read hole where a partitioned old leader answers authoritatively
    /// after a new leader has formed on the majority side.
    #[test]
    fn leader_lease_gates_reads_and_steps_down_on_lost_quorum() {
        let mut a = leader_actor();
        let read = || ReadRequest::Kv {
            key: "k".to_string(),
        };

        // No peer has acked yet: self alone is a minority of 3 → lease not held, and
        // a linearizable read is refused (retryable Unavailable, not a stale answer).
        assert!(!a.leader_lease_held());
        assert!(matches!(
            a.handle_query(read()),
            Err(ProposeError::Unavailable { .. })
        ));

        // One peer acks → self + b = majority → lease held → read served.
        a.leader
            .as_mut()
            .unwrap()
            .last_contact
            .insert("b".to_string(), Instant::now());
        assert!(a.leader_lease_held());
        assert!(a.handle_query(read()).is_ok());

        // That contact ages past the election timeout → lease lapses → read refused.
        a.leader.as_mut().unwrap().last_contact.insert(
            "b".to_string(),
            Instant::now() - Duration::from_millis(a.timing.election_min_ms + 50),
        );
        assert!(!a.leader_lease_held());
        assert!(matches!(
            a.handle_query(read()),
            Err(ProposeError::Unavailable { .. })
        ));

        // A tick with the lease lapsed steps the leader down (no higher term seen —
        // it simply lost contact). Keep the heartbeat deadline in the future so the
        // tick exercises only the lease check, not network I/O.
        a.heartbeat_deadline = Instant::now() + Duration::from_secs(60);
        a.on_tick();
        assert_eq!(a.role, Role::Follower);
        assert!(a.leader.is_none());
    }

    /// With CheckQuorum disabled the lease logic is byte-identical to the old
    /// behaviour: a leader with zero majority contact still serves reads and never
    /// steps down for want of acks (it only steps down on a higher term).
    #[test]
    fn check_quorum_off_preserves_old_unconfirmed_leader_behaviour() {
        let mut a = leader_actor();
        a.timing.check_quorum = false;

        assert!(a.leader_lease_held(), "disabled ⇒ always held");
        assert!(a
            .handle_query(ReadRequest::Kv {
                key: "k".to_string()
            })
            .is_ok());

        a.heartbeat_deadline = Instant::now() + Duration::from_secs(60);
        a.on_tick();
        assert_eq!(a.role, Role::Leader, "no step-down when check-quorum is off");
    }

    fn pre_vote_req(term: u64, last_log_index: u64, last_log_term: u64) -> RequestVoteReq {
        RequestVoteReq {
            term,
            candidate_id: "z".to_string(),
            last_log_index,
            last_log_term,
            pre_vote: true,
        }
    }

    /// The anti-disruption property: while a leader is alive (election deadline in
    /// the future), a pre-vote is **denied** — so a rejoining node can never bump
    /// the cluster's term. With no leader (or a lapsed deadline) it is granted, so
    /// genuine elections still proceed.
    #[test]
    fn pre_vote_is_denied_under_a_live_leader_and_granted_otherwise() {
        let mut a = follower_actor();

        // Cold start: no leader known → granted (first election must be able to run).
        assert!(a.leader_id.is_none());
        assert!(a.handle_pre_vote(&pre_vote_req(2, 0, 0)).granted);

        // Healthy leader, contact still fresh → denied (no disruption).
        a.leader_id = Some("b".to_string());
        a.last_leader_contact = Instant::now();
        assert!(!a.handle_pre_vote(&pre_vote_req(2, 0, 0)).granted);
        // ...and the round must not have mutated our state (structurally enforced
        // by `&self`, but assert the observable bits too).
        assert_eq!(a.current_term, 1);
        assert_eq!(a.voted_for, None);
        assert_eq!(a.role, Role::Follower);

        // Leader known but contact has gone stale (missed heartbeats) → granted.
        a.last_leader_contact = Instant::now() - Duration::from_secs(1);
        assert!(a.handle_pre_vote(&pre_vote_req(2, 0, 0)).granted);
    }

    /// Pre-vote still enforces the two safety checks: a stale would-be term and a
    /// behind log are both refused even when no leader is alive.
    #[test]
    fn pre_vote_refuses_stale_term_and_behind_log() {
        let mut a = follower_actor();
        a.leader_id = None; // remove the leader-stickiness clause from the picture

        // Stale would-be term (< our current term) → denied.
        assert!(!a.handle_pre_vote(&pre_vote_req(0, 0, 0)).granted);

        // We now hold one entry at term 1: a candidate behind on the log is denied,
        // a caught-up one is granted.
        a.log.push(LogEntry {
            term: 1,
            index: 1,
            command: None,
        });
        assert!(
            !a.handle_pre_vote(&pre_vote_req(5, 0, 0)).granted,
            "behind log must be denied"
        );
        assert!(
            a.handle_pre_vote(&pre_vote_req(5, 1, 1)).granted,
            "caught-up log granted"
        );
    }
}
