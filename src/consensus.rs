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
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, Instant};

use crate::persist::{PersistedSnapshot, Recovered, ShardStore};
use crate::state::{
    BarrierState, BudgetState, ClaimState, Command, CounterEntry, DecisionState, EffectState,
    ElectionEntry, HandoffState, IdempotencyRecord, KvEntry, KvListItem, Leadership, LockInventory,
    LockState, RateLimitSnapshot, Schedule, ScheduleRun, SemaphoreState, ServiceInstance,
    ServiceSummary, StateMachine, TaskState, CURRENT_COMMAND_PROTOCOL, LEGACY_COMMAND_PROTOCOL,
};
use crate::transport::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, LoopbackRegistry,
    RequestVoteReq, RequestVoteResp, Transport,
};

/// Identifier of a shard (one independent Raft group). Re-exported from the
/// shared routing crate so the type and the `key → shard` mapping can't drift
/// between the node, the load balancer, and the brain.
pub use fiducia_routing::ShardId;

/// Depth of each shard actor's inbox before senders must wait.
const SHARD_INBOX_CAPACITY: usize = 1024;
/// How long a client write waits for its entry to commit before giving up.
const COMMIT_WAIT: Duration = Duration::from_secs(5);
/// Bound local actor-status collection so `/readyz` fails unavailable instead
/// of hanging behind a wedged/full shard inbox.
const STATUS_WAIT: Duration = Duration::from_secs(1);
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
#[derive(Debug, Clone, Copy, Serialize)]
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
    /// Committed entries applied past the last snapshot before the shard folds
    /// them into a new snapshot and compacts the log (`0` disables compaction).
    /// Resolved once at boot from `FIDUCIA_RAFT_SNAPSHOT_THRESHOLD` — it was
    /// previously re-read from the process environment after every applied
    /// command, on the apply hot path.
    pub snapshot_threshold: u64,
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
            snapshot_threshold: 1024,
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
                .map(|s| {
                    !matches!(
                        s.trim().to_ascii_lowercase().as_str(),
                        "0" | "false" | "off"
                    )
                })
                .unwrap_or(d.pre_vote),
            check_quorum: std::env::var("FIDUCIA_RAFT_CHECK_QUORUM")
                .ok()
                .map(|s| {
                    !matches!(
                        s.trim().to_ascii_lowercase().as_str(),
                        "0" | "false" | "off"
                    )
                })
                .unwrap_or(d.check_quorum),
            snapshot_threshold: ms("FIDUCIA_RAFT_SNAPSHOT_THRESHOLD", d.snapshot_threshold),
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
    /// Leader-stamped wall clock used by every replica while applying this
    /// entry. Zero identifies legacy entries written before deterministic apply
    /// time; recovery treats those as epoch-anchored so old leases expire rather
    /// than being resurrected at restart.
    #[serde(default)]
    pub proposed_at_ms: u64,
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

/// De-duplicate the configured peer list and drop any entry equal to this node's
/// own id, preserving first-occurrence order. `members = peers.len()+1` and the
/// quorum derived from it (`members/2+1`) must count exactly the distinct OTHER
/// members. A peer listed twice — or this node accidentally listing itself in
/// `FIDUCIA_PEERS` — inflates `members`, so a single follower's ack is over-counted
/// toward commit (a leader could advance `commit_index` without a real majority),
/// and because votes are tracked in a set the quorum threshold can exceed the
/// number of distinct voters so no leader is ever elected.
fn resolve_peers(raw: impl IntoIterator<Item = String>, self_id: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    raw.into_iter()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty() && p != self_id)
        .filter(|p| seen.insert(p.clone()))
        .collect()
}

impl Default for NodeConfig {
    fn default() -> Self {
        let node_id = std::env::var("FIDUCIA_NODE_ID").unwrap_or_else(|_| "node-a".to_string());
        let peers = resolve_peers(
            std::env::var("FIDUCIA_PEERS")
                .ok()
                .map(|s| s.split(',').map(String::from).collect::<Vec<_>>())
                .unwrap_or_default(),
            &node_id,
        );
        Self {
            node_id,
            peers,
            shard_count: std::env::var("FIDUCIA_SHARD_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16)
                .max(1),
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
    /// Inbound state-machine snapshot from a leader that compacted the required
    /// log prefix.
    InstallSnapshot {
        req: InstallSnapshotReq,
        resp: oneshot::Sender<InstallSnapshotResp>,
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
        /// RPC round-trip latency measured by the spawned transport task.
        rtt_ms: Option<u64>,
        /// `None` if the peer was unreachable.
        resp: Option<AppendEntriesResp>,
    },
    SnapshotReply {
        from: String,
        up_to: u64,
        resp: Option<InstallSnapshotResp>,
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
    Kv {
        key: String,
    },
    #[allow(dead_code)]
    KvPrefix {
        prefix: String,
    },
    Counter {
        key: String,
    },
    Barrier {
        name: String,
    },
    Task {
        name: String,
    },
    Effect {
        name: String,
    },
    Handoff {
        name: String,
    },
    Decision {
        name: String,
    },
    Budget {
        name: String,
    },
    Claim {
        name: String,
    },
    Lock {
        key: String,
    },
    Semaphore {
        key: String,
    },
    RateLimit {
        tenant: String,
        key: String,
    },
    Idempotency {
        key: String,
    },
    Schedule {
        name: String,
    },
    ScheduleHistory {
        name: String,
    },
    Election {
        name: String,
    },
    Service {
        service: String,
    },
    /// Range read: every KV key under `prefix` on one shard. Fanned out across
    /// shards by [`Node::list_kv`] and served serializably (no leader gate).
    KvList {
        prefix: String,
    },
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
            ReadRequest::Kv { key } | ReadRequest::KvPrefix { prefix: key } => key,
            ReadRequest::Counter { key } => key,
            ReadRequest::Barrier { name }
            | ReadRequest::Task { name }
            | ReadRequest::Effect { name }
            | ReadRequest::Handoff { name }
            | ReadRequest::Decision { name }
            | ReadRequest::Budget { name }
            | ReadRequest::Claim { name } => name,
            ReadRequest::Lock { .. } | ReadRequest::Semaphore { .. } => crate::state::LOCK_DOMAIN,
            ReadRequest::RateLimit { key, .. } | ReadRequest::Idempotency { key } => key,
            ReadRequest::Schedule { name } | ReadRequest::ScheduleHistory { name } => name,
            ReadRequest::Election { name } => name,
            // Service discovery lives on the single SERVICE_DOMAIN shard — writes
            // route every register/heartbeat/deregister there (see Command::routing_key).
            // A per-service read must hit that same shard, NOT shard_for(service_name),
            // or it reads a different (empty) shard than the one holding the instances.
            ReadRequest::Service { .. } => crate::state::SERVICE_DOMAIN,
            // Lock/semaphore inventory shares the single lock-coordinator shard.
            ReadRequest::LockInventory | ReadRequest::SemaphoreInventory => {
                crate::state::LOCK_DOMAIN
            }
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
    #[allow(dead_code)]
    KvPrefix(Vec<(String, KvEntry)>),
    Counter(Option<CounterEntry>),
    Barrier(Option<BarrierState>),
    Task(Option<TaskState>),
    Effect(Option<EffectState>),
    Handoff(Option<HandoffState>),
    Decision(Option<DecisionState>),
    Budget(Option<BudgetState>),
    Claim(Option<ClaimState>),
    Lock(LockState),
    Semaphore(SemaphoreState),
    RateLimit(Option<RateLimitSnapshot>),
    Idempotency(Option<IdempotencyRecord>),
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
    /// Parser capability positively advertised by each configured peer in this
    /// leadership term. Missing peers remain V1 and block the V2 activation
    /// barrier; capability is deliberately not inferred from reachability.
    peer_command_protocol: HashMap<String, u16>,
}

struct PendingProposal {
    started_at: Instant,
    resp: oneshot::Sender<Result<ProposeOutcome, ProposeError>>,
}

/// Per-shard metric snapshot surfaced through `/v1/status`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ShardMetrics {
    /// Last successful AppendEntries round-trip observed by the leader.
    pub append_rtt_ms_last: Option<u64>,
    /// Last client proposal latency from leader append to quorum commit/apply.
    pub quorum_rtt_ms_last: Option<u64>,
    /// Current max `leader_last_log_index - follower_match_index` across peers.
    pub follower_lag_max: u64,
    /// Observed leadership changes into or out of leader role on this shard.
    pub leader_transfer_count: u64,
}

/// Build the next contiguous AppendEntries batch without exceeding the
/// configured entry count or target serialized size. A single oversized entry
/// is still returned because the Raft log cannot split one command; the shared
/// peer-body preflight remains the final hard ceiling for that case.
fn bounded_append_request(
    mut request: AppendEntriesReq,
    suffix: &[LogEntry],
    max_entries: usize,
    max_bytes: usize,
) -> AppendEntriesReq {
    request.entries.clear();
    for entry in suffix.iter().take(max_entries.max(1)) {
        request.entries.push(entry.clone());
        let body_bytes = serde_json::to_vec(&request)
            .map(|body| body.len())
            .unwrap_or(usize::MAX);
        if body_bytes > max_bytes.max(1) {
            if request.entries.len() > 1 {
                request.entries.pop();
            }
            break;
        }
    }
    request
}

/// Send only the contiguous prefix that a peer may safely receive. A command is
/// sendable when the peer advertises a parser new enough, or when the command's
/// protocol has already been durably activated (in which case a downgraded peer
/// must fail closed rather than keep participating with divergent state).
fn appendable_protocol_prefix(
    suffix: &[LogEntry],
    peer_protocol: u16,
    active_protocol: u16,
) -> &[LogEntry] {
    let compatible = suffix
        .iter()
        .take_while(|entry| {
            entry.command.as_ref().is_none_or(|command| {
                let required = command.required_protocol();
                required <= peer_protocol || required <= active_protocol
            })
        })
        .count();
    &suffix[..compatible]
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
    /// Highest log index represented by the installed state-machine snapshot.
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_state: Vec<u8>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Durable backing for term/vote/log, or `None` for an in-memory shard.
    store: Option<ShardStore>,
    /// First durable-storage failure observed by this actor. Once set, the shard
    /// remains fail-closed until process restart and successful recovery.
    storage_fault: Option<String>,

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
    pending: HashMap<u64, PendingProposal>,
    // --- change stream feeding KV watches ---
    changes: broadcast::Sender<ChangeEvent>,

    // --- the state-machine partition holding this shard's keys ---
    state: StateMachine,
    // --- low-cardinality metrics for Raft operations ---
    metrics: ShardMetrics,
}

impl ShardActor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        shard_id: ShardId,
        node_id: String,
        peers: Vec<String>,
        transport: Arc<Transport>,
        self_tx: mpsc::Sender<ShardMsg>,
        timing: RaftTiming,
        store: Option<ShardStore>,
        recovered: Recovered,
    ) -> Result<Self, String> {
        let members = peers.len() + 1;
        let single = members == 1;
        let (changes, _) = broadcast::channel(CHANGE_BUFFER);
        // Seed from disk when we have it. A fresh shard recovers `term == 0`; this
        // engine numbers terms from 1, so keep the floor at 1 for a clean start.
        let current_term = recovered.current_term.max(1);
        let snapshot_index = recovered
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let snapshot_term = recovered
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_term)
            .unwrap_or(0);
        let snapshot_state = recovered
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.state.clone())
            .unwrap_or_default();
        // Recovery-invariant violations are returned as errors so the caller can
        // quarantine THIS shard (fail-closed, visible in /readyz and /v1/status)
        // instead of aborting the process and taking every healthy shard with it.
        for (offset, entry) in recovered.log.iter().enumerate() {
            let expected = snapshot_index
                .checked_add(offset as u64)
                .and_then(|index| index.checked_add(1))
                .ok_or_else(|| format!("shard {shard_id}: recovered log index overflow"))?;
            if entry.index != expected {
                return Err(format!(
                    "invalid recovered state for shard {shard_id}: expected log index {expected}, found {}",
                    entry.index
                ));
            }
        }
        let recovered_tail = recovered
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or(snapshot_index);
        if recovered.commit_index > recovered_tail {
            return Err(format!(
                "invalid recovered state for shard {shard_id}: commit index {} exceeds durable tail {recovered_tail}",
                recovered.commit_index
            ));
        }
        let recovered_commit = recovered.commit_index.max(snapshot_index);
        let state = StateMachine::new();
        if let Some(snapshot) = recovered.snapshot.as_ref() {
            if let Err(error) = state.restore(&snapshot.state) {
                return Err(format!(
                    "invalid state-machine snapshot for shard {shard_id}: {error}"
                ));
            }
        }
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
            snapshot_index,
            snapshot_term,
            snapshot_state,
            log: recovered.log,
            commit_index: recovered_commit,
            last_applied: snapshot_index,
            store,
            storage_fault: None,
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
            state,
            metrics: ShardMetrics::default(),
        };
        actor.reset_election_deadline();
        // Rebuild the in-memory state machine from the recovered log up to the
        // committed point (the state machine itself is not persisted).
        if actor.commit_index > 0 {
            actor.apply_committed();
        }
        Ok(actor)
    }

    /// Build a shard actor that is fail-closed from its first tick: it hosts the
    /// shard id (so `/v1/status` and `/readyz` report it) but participates in
    /// nothing — `storage_fault` gates every vote, append, proposal, and read,
    /// exactly as if durable storage had failed mid-run. Used when a shard's
    /// on-disk state cannot be opened or validated at boot: the alternative
    /// (aborting the process) would needlessly take every healthy shard on this
    /// node out of its quorum.
    #[allow(clippy::too_many_arguments)]
    fn quarantined(
        shard_id: ShardId,
        node_id: String,
        peers: Vec<String>,
        transport: Arc<Transport>,
        self_tx: mpsc::Sender<ShardMsg>,
        timing: RaftTiming,
        reason: String,
    ) -> Self {
        let members = peers.len() + 1;
        let (changes, _) = broadcast::channel(CHANGE_BUFFER);
        let mut actor = ShardActor {
            shard_id,
            node_id: node_id.clone(),
            peers,
            members,
            transport,
            self_tx,
            // Never a leader — not even in single-member mode. A quarantined
            // shard must not serve anything until its durable state is repaired
            // and the node restarts.
            role: Role::Follower,
            current_term: 1,
            voted_for: None,
            leader_id: None,
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_state: Vec::new(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            store: None,
            storage_fault: Some(reason),
            votes: HashSet::new(),
            pre_votes: HashSet::new(),
            pre_vote_term: 0,
            leader: None,
            timing,
            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
            last_leader_contact: Instant::now(),
            rng: Rng::seeded(&node_id, shard_id),
            pending: HashMap::new(),
            changes,
            state: StateMachine::new(),
            metrics: ShardMetrics::default(),
        };
        actor.reset_election_deadline();
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
            ShardMsg::InstallSnapshot { req, resp } => {
                let out = self.handle_install_snapshot(req);
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
            ShardMsg::AppendReply {
                from,
                up_to,
                rtt_ms,
                resp,
            } => self.handle_append_reply(from, up_to, rtt_ms, resp),
            ShardMsg::SnapshotReply { from, up_to, resp } => {
                self.handle_snapshot_reply(from, up_to, resp)
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
        if self.storage_fault.is_some() {
            return;
        }
        let now = Instant::now();
        match self.role {
            Role::Leader => {
                // A single-member shard can activate immediately; a replicated
                // shard waits until AppendEntries replies have populated every
                // peer's parser capability below.
                self.maybe_activate_command_protocol();
                if self.storage_fault.is_some() {
                    return;
                }
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
        self.snapshot_index.saturating_add(self.log.len() as u64)
    }

    fn last_log_term(&self) -> u64 {
        self.log
            .last()
            .map(|entry| entry.term)
            .unwrap_or(self.snapshot_term)
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else if index == self.snapshot_index {
            self.snapshot_term
        } else if index < self.snapshot_index {
            0
        } else {
            self.log
                .get((index - self.snapshot_index - 1) as usize)
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
        if self.storage_fault.is_some() {
            return;
        }
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
        if self.storage_fault.is_some() {
            return;
        }
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
        if let Err(error) = self.persist_hard_state() {
            self.fail_storage("persisting election term and self-vote", error);
            return;
        }

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
        if self.storage_fault.is_some() {
            return;
        }
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
        if self.storage_fault.is_some() {
            return;
        }
        self.record_leader_transfer(self.role, Role::Leader, "became_leader");
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
            ls.peer_command_protocol
                .insert(peer.clone(), LEGACY_COMMAND_PROTOCOL);
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
            proposed_at_ms: now_ms(),
            command: None,
        });
        // Durable before this entry can count toward a commit.
        if let Err(error) = self.persist_log_append() {
            self.fail_storage("persisting leader no-op", error);
            return;
        }

        self.heartbeat_deadline = Instant::now() + self.timing.heartbeat;
        self.maybe_advance_commit(); // single-node commits the no-op immediately
        self.broadcast_append_entries();
    }

    /// Convert to follower at `term`, optionally learning the new leader. Fails
    /// any outstanding client writes so they retry against the real leader.
    fn step_down(&mut self, term: u64, leader: Option<String>) {
        if term < self.current_term {
            tracing::warn!(
                shard = ?self.shard_id,
                node = %self.node_id,
                requested_term = term,
                current_term = self.current_term,
                "raft: ignored an attempt to step down into an older term"
            );
            return;
        }
        if self.role != Role::Follower {
            tracing::info!(
                shard = ?self.shard_id,
                node = %self.node_id,
                term,
                "raft: stepped down to follower"
            );
        }
        self.record_leader_transfer(self.role, Role::Follower, "step_down");
        // A member may cast at most one durable vote per term. CheckQuorum
        // relinquishes leadership without observing a newer term, so clearing
        // `voted_for` on that same-term step-down would let this member vote for
        // a second candidate in the term it originally won. That creates two
        // legitimate leaders and a same-term AppendEntries storm. Only adopting
        // a genuinely newer term resets the vote.
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        self.leader_id = leader;
        if let Err(error) = self.persist_hard_state() {
            self.fail_storage("persisting higher term while stepping down", error);
            return;
        }
        self.reset_election_deadline();
        self.fail_pending();
    }

    // --- durability: persist before acting (no-ops for an in-memory shard) ----

    /// Persist `current_term`, `voted_for`, and `commit_index`. Call after any
    /// change to them and **before** the action that relies on them (granting a
    /// vote, campaigning, committing). Failures must be propagated to the caller;
    /// the shard cannot safely keep participating after one.
    fn persist_hard_state(&self) -> std::io::Result<()> {
        self.persist_hard_state_at(self.commit_index)
    }

    fn persist_hard_state_at(&self, commit_index: u64) -> std::io::Result<()> {
        if let Some(store) = self.store.as_ref() {
            store.save_meta(self.current_term, self.voted_for.as_deref(), commit_index)?;
        }
        Ok(())
    }

    /// Persist newly-appended tail entries (pure-append path).
    fn persist_log_append(&mut self) -> std::io::Result<()> {
        if let Some(store) = self.store.as_mut() {
            store.append_tail(&self.log)?;
        }
        Ok(())
    }

    /// Persist the full log after a conflicting suffix was truncated/replaced.
    fn persist_log_rewrite(&mut self) -> std::io::Result<()> {
        if let Some(store) = self.store.as_mut() {
            store.rewrite(&self.log)?;
        }
        Ok(())
    }

    fn fail_storage(&mut self, operation: &'static str, error: std::io::Error) {
        let detail = format!("{operation}: {error}");
        if self.storage_fault.is_none() {
            tracing::error!(
                shard = ?self.shard_id,
                node = %self.node_id,
                %operation,
                error = %error,
                "raft: durable storage failed; shard is now fail-closed until restart"
            );
            self.storage_fault = Some(detail);
        }
        self.record_leader_transfer(self.role, Role::Follower, "storage_fault");
        self.role = Role::Follower;
        self.leader_id = None;
        self.leader = None;
        self.votes.clear();
        self.pre_votes.clear();
        self.fail_pending_unavailable();
    }

    fn fail_pending(&mut self) {
        let leader = self.leader_id.clone();
        for (_, pending) in self.pending.drain() {
            let _ = pending.resp.send(Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: leader.clone(),
            }));
        }
    }

    fn fail_pending_unavailable(&mut self) {
        for (_, pending) in self.pending.drain() {
            let _ = pending.resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
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

    fn all_peers_support_current_command_protocol(&self) -> bool {
        self.members == 1
            || self.leader.as_ref().is_some_and(|leader| {
                self.peers.iter().all(|peer| {
                    leader
                        .peer_command_protocol
                        .get(peer)
                        .copied()
                        .unwrap_or(LEGACY_COMMAND_PROTOCOL)
                        >= CURRENT_COMMAND_PROTOCOL
                })
            })
    }

    /// Phase two of the rolling command upgrade. Phase one is passive: every
    /// current follower advertises the current parser in ordinary AppendEntries replies,
    /// while leaders continue emitting legacy-compatible commands. Only after
    /// *all* configured peers have positively advertised the current protocol do we append this
    /// replicated activation record. Its commit, not the volatile advertisements,
    /// is the durable emission gate.
    fn maybe_activate_command_protocol(&mut self) {
        if self.storage_fault.is_some()
            || self.role != Role::Leader
            || self.state.command_protocol() >= CURRENT_COMMAND_PROTOCOL
        {
            return;
        }
        if !self.all_peers_support_current_command_protocol() {
            return;
        }
        // A prior leader may already have appended the barrier without getting
        // it committed. Replicate that exact record; never grow duplicates on
        // each heartbeat or leadership term.
        let activation_already_logged = self.log.iter().any(|entry| {
            matches!(
                entry.command.as_ref(),
                Some(Command::ActivateCommandProtocol { version })
                    if *version == CURRENT_COMMAND_PROTOCOL
            )
        });
        if activation_already_logged {
            // A single-member restart can recover the activation after its log
            // fsync succeeded but its commit-index fsync failed. Finish that
            // existing record rather than waiting forever or appending another.
            if self.members == 1 {
                self.maybe_advance_commit();
            }
            return;
        }
        let Some(index) = self.last_log_index().checked_add(1) else {
            self.fail_storage(
                "allocating command-protocol activation index",
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Raft log index overflow"),
            );
            return;
        };
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            proposed_at_ms: now_ms(),
            command: Some(Command::ActivateCommandProtocol {
                version: CURRENT_COMMAND_PROTOCOL,
            }),
        });
        if let Err(error) = self.persist_log_append() {
            self.fail_storage("persisting command-protocol activation", error);
            return;
        }
        if self.members == 1 {
            if let Err(error) = self.persist_hard_state_at(index) {
                self.fail_storage("persisting command-protocol activation commit", error);
                return;
            }
            self.commit_index = index;
            self.apply_committed();
        } else {
            self.broadcast_append_entries();
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
        let peer_command_protocol = ls
            .peer_command_protocol
            .get(peer)
            .copied()
            .unwrap_or(LEGACY_COMMAND_PROTOCOL);
        ls.in_flight.insert(peer.to_string(), true);
        if next <= self.snapshot_index {
            let req = InstallSnapshotReq {
                term: self.current_term,
                leader_id: self.node_id.clone(),
                last_included_index: self.snapshot_index,
                last_included_term: self.snapshot_term,
                state: self.snapshot_state.clone(),
            };
            let transport = self.transport.clone();
            let self_tx = self.self_tx.clone();
            let shard = self.shard_id;
            let peer_owned = peer.to_string();
            let up_to = self.snapshot_index;
            tokio::spawn(async move {
                let resp = transport.install_snapshot(&peer_owned, shard, req).await;
                let _ = self_tx
                    .send(ShardMsg::SnapshotReply {
                        from: peer_owned,
                        up_to,
                        resp,
                    })
                    .await;
            });
            return;
        }

        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let max_entries = crate::peer_config::append_max_entries();
        let max_bytes = crate::peer_config::append_max_bytes();
        let suffix_start = prev_log_index.saturating_sub(self.snapshot_index) as usize;
        let suffix = appendable_protocol_prefix(
            self.log.get(suffix_start..).unwrap_or(&[]),
            peer_command_protocol,
            self.state.command_protocol(),
        );
        let req = bounded_append_request(
            AppendEntriesReq {
                term: self.current_term,
                leader_id: self.node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: self.commit_index,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            },
            suffix,
            max_entries,
            max_bytes,
        );
        let up_to = prev_log_index.saturating_add(req.entries.len() as u64);
        if req.entries.len() == 1
            && serde_json::to_vec(&req)
                .map(|body| body.len() > max_bytes)
                .unwrap_or(true)
        {
            tracing::warn!(
                shard = self.shard_id,
                peer,
                max_bytes,
                "single Raft log entry exceeds append batch target"
            );
        }

        let transport = self.transport.clone();
        let self_tx = self.self_tx.clone();
        let shard = self.shard_id;
        let peer_owned = peer.to_string();
        tokio::spawn(async move {
            let started_at = Instant::now();
            let resp = transport.append_entries(&peer_owned, shard, req).await;
            let rtt_ms = Some(duration_millis(started_at.elapsed()));
            let _ = self_tx
                .send(ShardMsg::AppendReply {
                    from: peer_owned,
                    up_to,
                    rtt_ms,
                    resp,
                })
                .await;
        });
    }

    fn handle_append_reply(
        &mut self,
        from: String,
        up_to: u64,
        rtt_ms: Option<u64>,
        resp: Option<AppendEntriesResp>,
    ) {
        if self.storage_fault.is_some() {
            return;
        }
        if let Some(ls) = self.leader.as_mut() {
            ls.in_flight.insert(from.clone(), false);
        }
        if let Some(rtt_ms) = rtt_ms {
            self.metrics.append_rtt_ms_last = Some(rtt_ms);
            tracing::debug!(
                metric.name = "fiducia.raft.append_entries_rtt_ms",
                shard = self.shard_id,
                peer = %from,
                rtt_ms,
                "append entries round-trip"
            );
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
        if let Some(ls) = self.leader.as_mut() {
            ls.peer_command_protocol
                .insert(from.clone(), resp.command_protocol);
        }
        let leader_last_log_index = self.last_log_index();
        let successful = resp.success && resp.match_index >= up_to;
        let mut more = false;
        if let Some(ls) = self.leader.as_mut() {
            // Any reply at our term is proof this peer still sees us as leader —
            // refresh the lease clock regardless of log success/mismatch.
            ls.last_contact.insert(from.clone(), Instant::now());
            if successful {
                let matched = ls.match_index.get(&from).copied().unwrap_or(0).max(up_to);
                ls.match_index.insert(from.clone(), matched);
                ls.next_index
                    .insert(from.clone(), matched.saturating_add(1));
                more = matched < leader_last_log_index;
            } else {
                // Log mismatch: rewind and retry from an earlier index.
                let cur = ls.next_index.get(&from).copied().unwrap_or(1);
                let backoff = resp
                    .match_index
                    .saturating_add(1)
                    .min(cur.saturating_sub(1));
                ls.next_index.insert(from.clone(), backoff.max(1));
                // Let the next heartbeat drive the retry. Immediate retries are
                // useful while a follower is accepting contiguous batches, but
                // a mismatch (especially an irreconcilable committed-prefix
                // conflict) would otherwise spin a network/error-log hot loop.
                more = false;
            }
        }
        if successful {
            self.maybe_advance_commit();
        }
        self.maybe_activate_command_protocol();
        self.refresh_follower_lag_metric();
        if more {
            self.send_append_to(&from);
        }
    }

    fn handle_snapshot_reply(
        &mut self,
        from: String,
        up_to: u64,
        resp: Option<InstallSnapshotResp>,
    ) {
        if self.storage_fault.is_some() {
            return;
        }
        if let Some(leader) = self.leader.as_mut() {
            leader.in_flight.insert(from.clone(), false);
        }
        let Some(resp) = resp else {
            return;
        };
        if resp.term > self.current_term {
            self.step_down(resp.term, None);
            return;
        }
        if self.role != Role::Leader || resp.term != self.current_term {
            return;
        }
        if resp.success {
            if let Some(leader) = self.leader.as_mut() {
                leader.match_index.insert(from.clone(), up_to);
                leader.next_index.insert(from.clone(), up_to + 1);
                leader.last_contact.insert(from.clone(), Instant::now());
            }
            self.send_append_to(&from);
        }
    }

    /// Advance `commit_index` to the highest index replicated on a majority that
    /// is **from the current term** (Raft's commit rule), then apply.
    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader || self.storage_fault.is_some() {
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
        let mut n = matches[self.majority() - 1]; // highest index on ≥ majority
        if self.state.command_protocol() < CURRENT_COMMAND_PROTOCOL
            && !self.all_peers_support_current_command_protocol()
        {
            let active = self.state.command_protocol();
            if let Some(first_newer) = self.log.iter().find(|entry| {
                entry.index > self.commit_index
                    && entry
                        .command
                        .as_ref()
                        .is_some_and(|command| command.required_protocol() > active)
            }) {
                // A pre-gate build may have persisted an uncommitted command from
                // a newer protocol. Do not commit across it until every configured
                // member has completed parser phase one for this binary.
                n = n.min(first_newer.index.saturating_sub(1));
            }
        }
        if n > self.commit_index && self.term_at(n) == self.current_term {
            // Persist the new commit pointer before applying or resolving any
            // client waiter. A successful apply must always be restartable.
            if let Err(error) = self.persist_hard_state_at(n) {
                self.fail_storage("persisting leader commit index", error);
                return;
            }
            self.commit_index = n;
            self.apply_committed();
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

    fn refresh_follower_lag_metric(&mut self) {
        let Some(ls) = &self.leader else {
            self.metrics.follower_lag_max = 0;
            return;
        };
        let leader_last_log_index = self.last_log_index();
        self.metrics.follower_lag_max = self
            .peers
            .iter()
            .map(|peer| {
                leader_last_log_index.saturating_sub(ls.match_index.get(peer).copied().unwrap_or(0))
            })
            .max()
            .unwrap_or(0);
        tracing::debug!(
            metric.name = "fiducia.raft.follower_lag_entries",
            shard = self.shard_id,
            follower_lag_max = self.metrics.follower_lag_max,
            "updated follower lag"
        );
    }

    // --- replication (follower side) --------------------------------------

    fn handle_install_snapshot(&mut self, req: InstallSnapshotReq) -> InstallSnapshotResp {
        if self.storage_fault.is_some() {
            return InstallSnapshotResp {
                term: self.current_term,
                success: false,
                match_index: self.commit_index,
            };
        }
        if req.term < self.current_term {
            return InstallSnapshotResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        if req.term > self.current_term {
            // Step down, don't just adopt the term: the metadata check below can
            // return early, and a leader that kept `Role::Leader` at the newer
            // term would go on broadcasting AppendEntries in a term it never won
            // — two leaders in one term, both committing.
            self.step_down(req.term, None);
            if self.storage_fault.is_some() {
                return InstallSnapshotResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.commit_index,
                };
            }
        }
        if (req.last_included_index == 0) != (req.last_included_term == 0)
            || req.last_included_term > req.term
        {
            tracing::warn!(
                shard = ?self.shard_id,
                snapshot_index = req.last_included_index,
                snapshot_term = req.last_included_term,
                leader_term = req.term,
                "raft: rejected snapshot with invalid index/term metadata"
            );
            return InstallSnapshotResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        self.become_follower_of(req.leader_id);
        if req.last_included_index <= self.snapshot_index {
            return InstallSnapshotResp {
                term: self.current_term,
                success: true,
                match_index: self.snapshot_index,
            };
        }
        if req.last_included_index <= self.commit_index {
            // This follower already has committed state at least as new as the
            // offered snapshot. Installing it could roll the materialized state
            // backward, so acknowledge our newer durable position instead.
            return InstallSnapshotResp {
                term: self.current_term,
                success: true,
                match_index: self.last_log_index(),
            };
        }

        let restored = StateMachine::new();
        if let Err(error) = restored.restore(&req.state) {
            tracing::error!(shard = ?self.shard_id, %error, "raft: rejected invalid snapshot");
            return InstallSnapshotResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        let keep_suffix = self.term_at(req.last_included_index) == req.last_included_term;
        let remaining = if keep_suffix {
            self.log
                .iter()
                .filter(|entry| entry.index > req.last_included_index)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let snapshot = PersistedSnapshot {
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            state: req.state.clone(),
        };
        if let Some(store) = self.store.as_mut() {
            if let Err(error) = store.save_snapshot(&snapshot, &remaining) {
                self.fail_storage("persisting installed snapshot", error);
                return InstallSnapshotResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.commit_index,
                };
            }
        }
        let new_commit_index = self.commit_index.max(req.last_included_index);
        if let Err(error) = self.persist_hard_state_at(new_commit_index) {
            self.fail_storage("persisting installed snapshot commit index", error);
            return InstallSnapshotResp {
                term: self.current_term,
                success: false,
                match_index: self.commit_index,
            };
        }
        self.state = restored;
        self.snapshot_index = req.last_included_index;
        self.snapshot_term = req.last_included_term;
        self.snapshot_state = req.state;
        self.log = remaining;
        self.commit_index = new_commit_index;
        self.last_applied = self.snapshot_index;
        InstallSnapshotResp {
            term: self.current_term,
            success: true,
            match_index: self.snapshot_index,
        }
    }

    fn handle_append_entries(&mut self, req: AppendEntriesReq) -> AppendEntriesResp {
        if self.storage_fault.is_some() {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.commit_index,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            };
        }
        // Reject a stale leader.
        if req.term < self.current_term {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            };
        }
        // Recognize this leader for our term (or a newer one). Stepping down is
        // part of adopting the term, not a later step: every validation check
        // below can return early, and a leader that kept `Role::Leader` while
        // holding the newer term would keep broadcasting AppendEntries in a term
        // it never won — two leaders in one term, both committing. `step_down`
        // persists the hard state, durable before we answer this RPC (even on
        // the reject paths below).
        if req.term > self.current_term {
            self.step_down(req.term, None);
            if self.storage_fault.is_some() {
                return AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.commit_index,
                    command_protocol: CURRENT_COMMAND_PROTOCOL,
                };
            }
        }

        // The vector position is not the log index: reject a malformed/gapped
        // leader payload rather than storing entries under the wrong offsets.
        if req.prev_log_term > req.term {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            };
        }
        let mut expected_index = req.prev_log_index;
        for entry in &req.entries {
            let Some(next) = expected_index.checked_add(1) else {
                return AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    command_protocol: CURRENT_COMMAND_PROTOCOL,
                };
            };
            if entry.index != next || entry.term == 0 || entry.term > req.term {
                tracing::warn!(
                    shard = ?self.shard_id,
                    expected_index = next,
                    entry_index = entry.index,
                    entry_term = entry.term,
                    "raft: rejected malformed/gapped AppendEntries payload"
                );
                return AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    command_protocol: CURRENT_COMMAND_PROTOCOL,
                };
            }
            expected_index = next;
        }
        if self.role == Role::Leader
            && req.term == self.current_term
            && req.leader_id != self.node_id
        {
            tracing::error!(
                shard = ?self.shard_id,
                node = %self.node_id,
                term = self.current_term,
                competing_leader = %req.leader_id,
                "raft: observed a competing leader in the same term"
            );
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
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            };
        }

        // Append, truncating on the first conflicting term.
        let mut idx = req.prev_log_index;
        let mut truncated = false;
        let mut grew = false;
        for entry in req.entries {
            idx += 1;
            if idx <= self.snapshot_index {
                continue;
            }
            let offset = (idx - self.snapshot_index - 1) as usize;
            match self.log.get(offset) {
                Some(existing) if existing.term == entry.term => {} // already have it
                Some(_) => {
                    if idx <= self.commit_index {
                        tracing::error!(
                            shard = ?self.shard_id,
                            conflict_index = idx,
                            commit_index = self.commit_index,
                            "raft: rejected AppendEntries that would overwrite committed state"
                        );
                        return AppendEntriesResp {
                            term: self.current_term,
                            success: false,
                            match_index: self.commit_index,
                            command_protocol: CURRENT_COMMAND_PROTOCOL,
                        };
                    }
                    self.log.truncate(offset);
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
        let persisted = if truncated {
            self.persist_log_rewrite()
        } else if grew {
            self.persist_log_append()
        } else {
            Ok(())
        };
        if let Err(error) = persisted {
            self.fail_storage("persisting follower log update", error);
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.commit_index,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            };
        }

        if req.leader_commit > self.commit_index {
            let new_commit_index = req.leader_commit.min(self.last_log_index());
            if new_commit_index > self.commit_index {
                if let Err(error) = self.persist_hard_state_at(new_commit_index) {
                    self.fail_storage("persisting follower commit index", error);
                    return AppendEntriesResp {
                        term: self.current_term,
                        success: false,
                        match_index: self.commit_index,
                        command_protocol: CURRENT_COMMAND_PROTOCOL,
                    };
                }
                self.commit_index = new_commit_index;
                self.apply_committed();
                if self.storage_fault.is_some() {
                    return AppendEntriesResp {
                        term: self.current_term,
                        success: false,
                        match_index: self.commit_index,
                        command_protocol: CURRENT_COMMAND_PROTOCOL,
                    };
                }
            }
        }

        AppendEntriesResp {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        }
    }

    fn become_follower_of(&mut self, leader: String) {
        self.record_leader_transfer(self.role, Role::Follower, "append_entries");
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.leader = None;
        self.votes.clear();
        self.last_leader_contact = Instant::now(); // heard from the leader
        self.reset_election_deadline();
        // Anything we were leading is no longer ours to commit.
        self.fail_pending();
    }

    fn record_leader_transfer(&mut self, from: Role, to: Role, reason: &'static str) {
        if from == to || (from != Role::Leader && to != Role::Leader) {
            return;
        }
        self.metrics.leader_transfer_count += 1;
        tracing::info!(
            metric.name = "fiducia.raft.leader_transfer",
            shard = self.shard_id,
            from = ?from,
            to = ?to,
            reason,
            count = self.metrics.leader_transfer_count,
            "observed raft leadership transition"
        );
    }

    fn handle_request_vote(&mut self, req: RequestVoteReq) -> RequestVoteResp {
        if self.storage_fault.is_some() {
            return RequestVoteResp {
                term: self.current_term,
                granted: false,
            };
        }
        if req.last_log_term > req.term {
            return RequestVoteResp {
                term: self.current_term,
                granted: false,
            };
        }
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
            if self.storage_fault.is_some() {
                return RequestVoteResp {
                    term: self.current_term,
                    granted: false,
                };
            }
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
            if let Err(error) = self.persist_hard_state() {
                self.fail_storage("persisting granted vote", error);
                return RequestVoteResp {
                    term: self.current_term,
                    granted: false,
                };
            }
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
            && self.last_leader_contact.elapsed()
                < Duration::from_millis(self.timing.election_min_ms);
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
        if self.storage_fault.is_some() {
            let _ = resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
            return;
        }
        if self.role != Role::Leader {
            let _ = resp.send(Err(ProposeError::NotLeader {
                shard: self.shard_id,
                leader: self.leader_id.clone(),
            }));
            return;
        }
        // The activation record is consensus-internal. Never let a client route
        // one arbitrary shard across the compatibility boundary.
        if matches!(command, Command::ActivateCommandProtocol { .. }) {
            let _ = resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
            return;
        }
        self.maybe_activate_command_protocol();
        if self.storage_fault.is_some() {
            let _ = resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
            return;
        }
        let command = match command.for_active_protocol(self.state.command_protocol()) {
            Ok(command) => command,
            Err(required) => {
                tracing::warn!(
                    shard = self.shard_id,
                    active_command_protocol = self.state.command_protocol(),
                    required_command_protocol = required,
                    "proposal held behind rolling command-protocol activation"
                );
                let _ = resp.send(Err(ProposeError::Unavailable {
                    shard: self.shard_id,
                }));
                return;
            }
        };
        let Some(index) = self.last_log_index().checked_add(1) else {
            let _ = resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
            return;
        };
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            proposed_at_ms: now_ms(),
            command: Some(command),
        });
        // Durable before this entry can count toward a commit / be acked.
        if let Err(error) = self.persist_log_append() {
            self.fail_storage("persisting proposed log entry", error);
            let _ = resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
            return;
        }
        // Block the client on this index committing.
        self.pending.insert(
            index,
            PendingProposal {
                started_at: Instant::now(),
                resp,
            },
        );

        if self.members == 1 {
            // One-member quorum: commit (and apply, which resolves the waiter) now.
            if let Err(error) = self.persist_hard_state_at(index) {
                self.fail_storage("persisting single-member commit index", error);
                return;
            }
            self.commit_index = index;
            self.apply_committed();
        } else {
            self.broadcast_append_entries();
        }
    }

    /// Apply every newly-committed entry in order, resolving client waiters and
    /// publishing change events.
    fn apply_committed(&mut self) {
        let mut completed: Vec<(u64, PendingProposal, u64, Value)> = Vec::new();
        while self.last_applied < self.commit_index {
            let Some(i) = self.last_applied.checked_add(1) else {
                self.fail_storage(
                    "advancing applied index",
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "applied index overflow"),
                );
                self.fail_completed(completed);
                return;
            };
            let offset = i
                .checked_sub(self.snapshot_index)
                .and_then(|relative| relative.checked_sub(1))
                .and_then(|relative| usize::try_from(relative).ok());
            let Some(entry) = offset.and_then(|offset| self.log.get(offset)) else {
                self.fail_storage(
                    "applying committed log",
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("missing durable entry at committed index {i}"),
                    ),
                );
                self.fail_completed(completed);
                return;
            };
            self.last_applied = i;
            let Some(command) = entry.command.clone() else {
                continue; // no-op
            };
            let applied = self.state.apply_at(command.clone(), entry.proposed_at_ms);
            self.publish_change(&command, &applied.output, applied.revision);
            if let Some(pending) = self.pending.remove(&i) {
                completed.push((i, pending, applied.revision, applied.output));
            }
        }
        self.maybe_compact();
        if self.storage_fault.is_some() {
            self.fail_completed(completed);
            return;
        }
        for (index, pending, revision, output) in completed {
            let quorum_rtt_ms = duration_millis(pending.started_at.elapsed());
            self.metrics.quorum_rtt_ms_last = Some(quorum_rtt_ms);
            tracing::info!(
                metric.name = "fiducia.raft.quorum_rtt_ms",
                shard = self.shard_id,
                log_index = index,
                quorum_rtt_ms,
                "proposal committed on quorum"
            );
            let _ = pending.resp.send(Ok(ProposeOutcome {
                shard: self.shard_id,
                log_index: index,
                revision,
                output,
            }));
        }
    }

    fn fail_completed(&self, completed: Vec<(u64, PendingProposal, u64, Value)>) {
        for (_, pending, _, _) in completed {
            let _ = pending.resp.send(Err(ProposeError::Unavailable {
                shard: self.shard_id,
            }));
        }
    }

    fn maybe_compact(&mut self) {
        let threshold = self.timing.snapshot_threshold;
        if threshold == 0 || self.last_applied.saturating_sub(self.snapshot_index) < threshold {
            return;
        }
        let last_included_index = self.last_applied;
        let last_included_term = self.term_at(last_included_index);
        let Ok(state) = self.state.snapshot() else {
            tracing::error!(shard = ?self.shard_id, "raft: failed to serialize state snapshot");
            return;
        };
        let remove = last_included_index.saturating_sub(self.snapshot_index) as usize;
        let remaining = self.log.get(remove..).unwrap_or(&[]).to_vec();
        let snapshot = PersistedSnapshot {
            last_included_index,
            last_included_term,
            state: state.clone(),
        };
        if let Some(store) = self.store.as_mut() {
            if let Err(error) = store.save_snapshot(&snapshot, &remaining) {
                self.fail_storage("persisting compacted snapshot", error);
                return;
            }
        }
        self.log = remaining;
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
        self.snapshot_state = state;
        tracing::info!(
            shard = ?self.shard_id,
            last_included_index,
            remaining_entries = self.log.len(),
            "raft: compacted committed log into state snapshot"
        );
    }

    fn publish_change(&self, command: &Command, output: &serde_json::Value, revision: u64) {
        // Only publish changes that actually mutated state: a campaign that lost
        // or a renew by a stale token must not look like a leadership change.
        let flagged = |field: &str| output.get(field).and_then(|v| v.as_bool()).unwrap_or(false);
        let detail = |field: &str| output.get(field).cloned();
        let event = match command {
            // A compare-and-set put that lost (`ok: false`, `cas_mismatch`)
            // mutated nothing — watchers must not see a phantom change.
            Command::KvPut { key, .. } if flagged("ok") => Some(ChangeEvent {
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
            Command::ServiceRegister { service, .. } if flagged("registered") => {
                Some(ChangeEvent {
                    scope: "service",
                    kind: "register",
                    key: service.clone(),
                    revision,
                    detail: detail("instance"),
                })
            }
            Command::ServiceHeartbeat { service, .. } if flagged("heartbeat") => {
                Some(ChangeEvent {
                    scope: "service",
                    kind: "heartbeat",
                    key: service.clone(),
                    revision,
                    detail: detail("instance"),
                })
            }
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

    /// Serve a read off applied state.
    ///
    /// Single-shard reads stay leader-only for linearizability. A prefix read
    /// spans shards, so it is served from this node's locally committed shard
    /// snapshots and merged by [`Node::query_kv_prefix`].
    fn handle_query(&self, request: ReadRequest) -> Result<ReadResponse, ProposeError> {
        if self.storage_fault.is_some() {
            return Err(ProposeError::Unavailable {
                shard: self.shard_id,
            });
        }
        if !matches!(&request, ReadRequest::KvPrefix { .. }) && self.role != Role::Leader {
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
            ReadRequest::KvPrefix { .. }
                | ReadRequest::KvList { .. }
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
            ReadRequest::KvPrefix { prefix } => {
                Ok(ReadResponse::KvPrefix(self.state.kv_prefix(&prefix)))
            }
            ReadRequest::Counter { key } => Ok(ReadResponse::Counter(self.state.counter_get(&key))),
            ReadRequest::Barrier { name } => {
                Ok(ReadResponse::Barrier(self.state.barrier_get(&name)))
            }
            ReadRequest::Task { name } => Ok(ReadResponse::Task(self.state.task_get(&name))),
            ReadRequest::Effect { name } => Ok(ReadResponse::Effect(self.state.effect_get(&name))),
            ReadRequest::Handoff { name } => {
                Ok(ReadResponse::Handoff(self.state.handoff_get(&name)))
            }
            ReadRequest::Decision { name } => {
                Ok(ReadResponse::Decision(self.state.decision_get(&name)))
            }
            ReadRequest::Budget { name } => Ok(ReadResponse::Budget(self.state.budget_get(&name))),
            ReadRequest::Claim { name } => Ok(ReadResponse::Claim(self.state.claim_get(&name))),
            ReadRequest::Lock { key } => Ok(ReadResponse::Lock(self.state.lock_get(&key))),
            ReadRequest::Semaphore { key } => {
                Ok(ReadResponse::Semaphore(self.state.semaphore_get(&key)))
            }
            ReadRequest::RateLimit { tenant, key } => Ok(ReadResponse::RateLimit(
                self.state.rate_limit_get(&tenant, &key),
            )),
            ReadRequest::Idempotency { key } => {
                Ok(ReadResponse::Idempotency(self.state.idempotency_get(&key)))
            }
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
            ReadRequest::LockInventory => ReadResponse::LockInventory(self.state.lock_inventory()),
            ReadRequest::SemaphoreInventory => {
                ReadResponse::SemaphoreInventory(self.state.semaphore_inventory())
            }
            // A single-key read arriving on the local path: serve it off applied
            // state too rather than erroring.
            ReadRequest::Kv { key } => ReadResponse::Kv(self.state.kv_get(&key)),
            ReadRequest::Counter { key } => ReadResponse::Counter(self.state.counter_get(&key)),
            ReadRequest::Barrier { name } => ReadResponse::Barrier(self.state.barrier_get(&name)),
            ReadRequest::Task { name } => ReadResponse::Task(self.state.task_get(&name)),
            ReadRequest::Effect { name } => ReadResponse::Effect(self.state.effect_get(&name)),
            ReadRequest::Handoff { name } => ReadResponse::Handoff(self.state.handoff_get(&name)),
            ReadRequest::Decision { name } => {
                ReadResponse::Decision(self.state.decision_get(&name))
            }
            ReadRequest::Budget { name } => ReadResponse::Budget(self.state.budget_get(&name)),
            ReadRequest::Claim { name } => ReadResponse::Claim(self.state.claim_get(&name)),
            ReadRequest::Lock { key } => ReadResponse::Lock(self.state.lock_get(&key)),
            ReadRequest::Semaphore { key } => {
                ReadResponse::Semaphore(self.state.semaphore_get(&key))
            }
            ReadRequest::RateLimit { tenant, key } => {
                ReadResponse::RateLimit(self.state.rate_limit_get(&tenant, &key))
            }
            ReadRequest::Schedule { name } => {
                ReadResponse::Schedule(self.state.schedule_get(&name))
            }
            ReadRequest::ScheduleHistory { name } => {
                ReadResponse::ScheduleHistory(self.state.schedule_history(&name))
            }
            ReadRequest::Election { name } => {
                ReadResponse::Election(self.state.election_get(&name))
            }
            ReadRequest::Service { service } => {
                ReadResponse::Service(self.state.service_list(&service))
            }
            ReadRequest::KvPrefix { prefix } => {
                ReadResponse::KvPrefix(self.state.kv_prefix(&prefix))
            }
            ReadRequest::Idempotency { key } => {
                ReadResponse::Idempotency(self.state.idempotency_get(&key))
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
                        command_protocol: ls
                            .peer_command_protocol
                            .get(peer)
                            .copied()
                            .unwrap_or(LEGACY_COMMAND_PROTOCOL),
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
            snapshot_index: self.snapshot_index,
            retained_log_entries: self.log.len(),
            storage_healthy: self.storage_fault.is_none(),
            storage_error: self.storage_fault.clone(),
            parser_command_protocol: CURRENT_COMMAND_PROTOCOL,
            active_command_protocol: self.state.command_protocol(),
            healthy_replicas,
            has_quorum,
            replication,
            metrics: self.metrics.clone(),
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
    /// KV value encryption at rest. `Some` when `FIDUCIA_KV_ENCRYPTION_KEY` is
    /// configured (the default posture); `None` disables sealing. See
    /// [`crate::kv::KvCipher`].
    kv_cipher: Option<Arc<crate::kv::KvCipher>>,
}

impl Node {
    /// Boot this node's shard actors over the given [`Transport`]. With no peers
    /// each actor is the sole member — and therefore leader — of its group; with
    /// peers they run real elections.
    ///
    /// Must be called from within a Tokio runtime (it spawns the actor tasks).
    pub fn bootstrap(config: NodeConfig, transport: Transport) -> Self {
        assert!(config.shard_count > 0, "shard_count must be > 0");
        let transport = Arc::new(transport);
        let timing = RaftTiming::from_env();
        let mut shards = HashMap::new();
        let mut tasks = Vec::new();
        for shard_id in 0..config.shard_count {
            let (tx, rx) = mpsc::channel(SHARD_INBOX_CAPACITY);
            if let Some(reg) = transport.loopback_registry() {
                reg.register(&config.node_id, shard_id, tx.clone());
            }
            // Unusable durable state fails closed at SHARD scope, not process
            // scope: the affected shard is quarantined (hosted but refusing all
            // participation, loud in logs, /readyz, and /v1/status) while every
            // other shard keeps serving its quorum. Aborting the whole node here
            // would turn one corrupt shard directory into a full-node crashloop
            // that costs every shard group on this node a member. Running the
            // shard WITHOUT durability is never an option — quarantine keeps it
            // fail-closed exactly like a mid-run storage fault.
            let quarantine = |reason: String| {
                tracing::error!(
                    shard = shard_id,
                    node = %config.node_id,
                    %reason,
                    "raft: shard quarantined at boot — durable state unusable; \
                     the shard is fail-closed on this node until its store is \
                     repaired and the node restarts"
                );
                ShardActor::quarantined(
                    shard_id,
                    config.node_id.clone(),
                    config.peers.clone(),
                    transport.clone(),
                    tx.clone(),
                    timing,
                    reason,
                )
            };
            let actor = match &config.data_dir {
                Some(dir) => match ShardStore::open(dir, shard_id) {
                    Ok((store, recovered)) => ShardActor::new(
                        shard_id,
                        config.node_id.clone(),
                        config.peers.clone(),
                        transport.clone(),
                        tx.clone(),
                        timing,
                        Some(store),
                        recovered,
                    )
                    .unwrap_or_else(&quarantine),
                    Err(error) => {
                        quarantine(format!("cannot open durable store under {dir:?}: {error}"))
                    }
                },
                None => ShardActor::new(
                    shard_id,
                    config.node_id.clone(),
                    config.peers.clone(),
                    transport.clone(),
                    tx.clone(),
                    timing,
                    None,
                    Recovered::default(),
                )
                .unwrap_or_else(&quarantine),
            };
            tasks.push(tokio::spawn(actor.run(rx)));
            shards.insert(shard_id, tx);
        }
        Node {
            config,
            shards,
            transport,
            tasks,
            metrics: Arc::new(crate::metrics::Metrics::new()),
            kv_cipher: crate::kv::KvCipher::from_env()
                .unwrap_or_else(|error| panic!("invalid KV protection configuration: {error}"))
                .map(Arc::new),
        }
    }

    /// The KV-at-rest cipher, when configured. `None` means values are stored
    /// verbatim (encryption disabled).
    pub fn kv_cipher(&self) -> Option<&crate::kv::KvCipher> {
        self.kv_cipher.as_deref()
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
            self.metrics
                .record("read", started.elapsed().as_secs_f64() * 1e3, false);
            return Err(ProposeError::Unavailable { shard });
        };
        let (resp, rx) = oneshot::channel();
        if tx.send(ShardMsg::Query { request, resp }).await.is_err() {
            self.metrics
                .record("read", started.elapsed().as_secs_f64() * 1e3, false);
            return Err(ProposeError::Unavailable { shard });
        }
        let result = rx.await.unwrap_or(Err(ProposeError::Unavailable { shard }));
        self.metrics.record(
            "read",
            started.elapsed().as_secs_f64() * 1e3,
            result.is_ok(),
        );
        tracing::debug!(
            shard = ?shard,
            key = %routing_key,
            ok = result.is_ok(),
            "query served"
        );
        result
    }

    /// Query every hosted shard for entries under a prefix and merge the partial
    /// results in deterministic key order.
    #[allow(dead_code)]
    pub async fn query_kv_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(String, KvEntry)>, ProposeError> {
        let mut entries = Vec::new();
        let mut shards: Vec<_> = self.shards.iter().map(|(shard, tx)| (*shard, tx)).collect();
        shards.sort_by_key(|(shard, _)| *shard);

        for (shard, tx) in shards {
            let (resp, rx) = oneshot::channel();
            let request = ReadRequest::KvPrefix {
                prefix: prefix.clone(),
            };
            if tx.send(ShardMsg::Query { request, resp }).await.is_err() {
                return Err(ProposeError::Unavailable { shard });
            }
            match rx
                .await
                .unwrap_or(Err(ProposeError::Unavailable { shard }))?
            {
                ReadResponse::KvPrefix(mut shard_entries) => entries.append(&mut shard_entries),
                _ => return Err(ProposeError::Unavailable { shard }),
            }
        }

        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
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
    pub async fn install_snapshot(
        &self,
        shard: ShardId,
        req: InstallSnapshotReq,
    ) -> Option<InstallSnapshotResp> {
        let tx = self.sender(shard)?;
        let (resp, rx) = oneshot::channel();
        tx.send(ShardMsg::InstallSnapshot { req, resp })
            .await
            .ok()?;
        rx.await.ok()
    }

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
    async fn query_all_shards(&self, make: impl Fn() -> ReadRequest) -> Vec<ReadResponse> {
        let mut out = Vec::with_capacity(self.shards.len());
        for (shard_id, tx) in self.shards.iter() {
            let (resp, rx) = oneshot::channel();
            let sent = tx
                .send(ShardMsg::QueryLocal {
                    request: make(),
                    resp,
                })
                .await
                .is_ok();
            let response = if sent { rx.await.ok() } else { None };
            match response {
                Some(response) => out.push(response),
                // A shard that can't answer makes every merged list (kv, services,
                // schedules, elections) silently partial; `status()` reports such
                // shards as unresponsive, so at minimum leave a log trail here.
                None => tracing::warn!(
                    shard = shard_id,
                    "shard did not answer a local query; merged results are partial"
                ),
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
        let mut merged: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
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

    /// Subscribe to every shard hosted by this node. Used by prefix watches
    /// because keys under one prefix can hash to many shards.
    pub async fn watch_all(&self) -> Vec<broadcast::Receiver<ChangeEvent>> {
        let mut receivers = Vec::with_capacity(self.shards.len());
        let mut shards: Vec<_> = self.shards.iter().map(|(shard, tx)| (*shard, tx)).collect();
        shards.sort_by_key(|(shard, _)| *shard);
        for (_, tx) in shards {
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
        let mut hosted_shards: Vec<ShardId> = self.shards.keys().copied().collect();
        hosted_shards.sort_unstable();
        let mut unresponsive_shards = Vec::new();
        let mut requests = JoinSet::new();
        for (&shard_id, tx) in &self.shards {
            let tx = tx.clone();
            requests.spawn(async move {
                let status = tokio::time::timeout(STATUS_WAIT, async move {
                    let (resp, rx) = oneshot::channel();
                    tx.send(ShardMsg::Status { resp }).await.ok()?;
                    rx.await.ok()
                })
                .await
                .ok()
                .flatten();
                (shard_id, status)
            });
        }
        while let Some(result) = requests.join_next().await {
            if let Ok((shard_id, status)) = result {
                match status {
                    Some(status) => shards.push(status),
                    None => unresponsive_shards.push(shard_id),
                }
            }
        }
        shards.sort_by_key(|s| s.shard_id);
        unresponsive_shards.sort_unstable();
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
            timing: RaftTiming::from_env(),
            hosted_shards,
            unresponsive_shards,
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
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
    /// Highest index included in the durable state-machine snapshot.
    pub snapshot_index: u64,
    /// Log suffix still retained after compaction.
    pub retained_log_entries: usize,
    /// False after a durable write failure. The shard then refuses votes,
    /// replication acknowledgements, linearizable reads, and proposals until a
    /// restart successfully reopens and validates its store.
    pub storage_healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_error: Option<String>,
    /// Highest command protocol this binary can parse (phase-one capability).
    pub parser_command_protocol: u16,
    /// Replicated command protocol currently allowed for emission on this shard.
    pub active_command_protocol: u16,
    /// Replicas (incl. self) caught up to `commit_index`. Leader-only; 0 elsewhere.
    pub healthy_replicas: usize,
    /// Whether a majority of the group is caught up — i.e. the shard can survive
    /// the loss of one more member without losing quorum. Leader-only.
    pub has_quorum: bool,
    /// Per-peer replication progress. Populated only while this node leads.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub replication: Vec<PeerReplication>,
    pub metrics: ShardMetrics,
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
    /// Parser capability last advertised by this peer in the current term.
    pub command_protocol: u16,
}

/// Whole-node status: identity, membership, and a row per hosted shard.
#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub peers: Vec<String>,
    pub shard_count: u32,
    pub timing: RaftTiming,
    /// Shards for which this node hosts a local actor.
    pub hosted_shards: Vec<ShardId>,
    /// Hosted shard actors that did not answer the bounded status probe. These
    /// remain hosted; reporting them separately prevents a wedged actor from
    /// disappearing from inventory or producing an all-healthy rollup.
    pub unresponsive_shards: Vec<ShardId>,
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
    use crate::persist::PersistOp;
    use axum::body::to_bytes;

    // Peer resolution feeds `members = peers.len()+1` and the quorum
    // `members/2+1`, so it must yield exactly the distinct OTHER members: self and
    // duplicates dropped, first-occurrence order preserved. A stray entry would
    // over-count commit acks and can make the quorum threshold exceed the distinct
    // voter count (no leader electable).
    #[test]
    fn resolve_peers_drops_self_and_duplicates() {
        let peers = resolve_peers(
            vec![
                "node.vultr.fiducia.cloud:9090".to_string(),
                " node-a:8090 ".to_string(), // self (padded) — dropped
                "node.vultr.fiducia.cloud:9090".to_string(), // duplicate — collapsed
                "".to_string(),              // empty — dropped
                "node.civo.fiducia.cloud:9090".to_string(),
            ],
            "node-a:8090",
        );
        assert_eq!(
            peers,
            vec![
                "node.vultr.fiducia.cloud:9090".to_string(),
                "node.civo.fiducia.cloud:9090".to_string(),
            ]
        );
        assert_eq!(
            peers.len() + 1,
            3,
            "3-member group, quorum 2, tolerates 1 loss"
        );
    }

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

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
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

    #[test]
    fn append_batches_respect_count_and_wire_size_without_dropping_an_entry() {
        let entries: Vec<_> = (1..=4)
            .map(|index| LogEntry {
                term: 3,
                index,
                proposed_at_ms: 1,
                command: Some(put(&format!("k-{index}"), &"v".repeat(512))),
            })
            .collect();
        let request = || AppendEntriesReq {
            term: 3,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        };

        let by_count = bounded_append_request(request(), &entries, 2, usize::MAX);
        assert_eq!(by_count.entries.len(), 2);
        assert_eq!(by_count.entries[0].index, 1);
        assert_eq!(by_count.entries[1].index, 2);

        let one = bounded_append_request(request(), &entries, 1, usize::MAX);
        let one_bytes = serde_json::to_vec(&one).unwrap().len();
        let by_size = bounded_append_request(request(), &entries, 4, one_bytes);
        assert_eq!(by_size.entries.len(), 1);
        assert!(serde_json::to_vec(&by_size).unwrap().len() <= one_bytes);

        // A single command is indivisible: return it and let the transport's
        // absolute peer-body preflight decide whether it can be sent.
        let indivisible = bounded_append_request(request(), &entries, 4, 1);
        assert_eq!(indivisible.entries.len(), 1);
    }

    #[test]
    fn append_capability_exchange_is_previous_release_wire_compatible() {
        #[allow(dead_code)]
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(tag = "op", rename_all = "snake_case")]
        enum PreviousCommand {
            LockAcquire {
                keys: Vec<String>,
                holder: String,
                ttl_ms: u64,
                wait: bool,
            },
        }
        #[allow(dead_code)]
        #[derive(Debug, Serialize, Deserialize)]
        struct PreviousLogEntry {
            term: u64,
            index: u64,
            proposed_at_ms: u64,
            command: Option<PreviousCommand>,
        }
        #[allow(dead_code)]
        #[derive(Debug, Serialize, Deserialize)]
        struct PreviousAppendEntriesReq {
            term: u64,
            leader_id: String,
            prev_log_index: u64,
            prev_log_term: u64,
            entries: Vec<PreviousLogEntry>,
            leader_commit: u64,
        }
        #[allow(dead_code)]
        #[derive(Debug, Serialize, Deserialize)]
        struct PreviousAppendEntriesResp {
            term: u64,
            success: bool,
            match_index: u64,
        }

        let current_request = AppendEntriesReq {
            term: 7,
            leader_id: "current".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 7,
                index: 1,
                proposed_at_ms: 100,
                command: Some(Command::LockAcquire {
                    keys: vec!["legacy-shape".to_string()],
                    holder: "worker".to_string(),
                    ttl_ms: 1_000,
                    wait: false,
                    wait_timeout_ms: None,
                }),
            }],
            leader_commit: 0,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        };
        let previous: PreviousAppendEntriesReq =
            serde_json::from_slice(&serde_json::to_vec(&current_request).unwrap()).unwrap();
        assert_eq!(previous.entries.len(), 1);
        assert!(matches!(
            previous.entries[0].command,
            Some(PreviousCommand::LockAcquire { .. })
        ));

        let previous_request = PreviousAppendEntriesReq {
            term: 7,
            leader_id: "previous".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        let current: AppendEntriesReq =
            serde_json::from_slice(&serde_json::to_vec(&previous_request).unwrap()).unwrap();
        assert_eq!(current.command_protocol, LEGACY_COMMAND_PROTOCOL);

        let current_response = AppendEntriesResp {
            term: 7,
            success: true,
            match_index: 1,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        };
        let previous_response: PreviousAppendEntriesResp =
            serde_json::from_slice(&serde_json::to_vec(&current_response).unwrap()).unwrap();
        assert!(previous_response.success);
        let previous_response = PreviousAppendEntriesResp {
            term: 7,
            success: true,
            match_index: 1,
        };
        let current_response: AppendEntriesResp =
            serde_json::from_slice(&serde_json::to_vec(&previous_response).unwrap()).unwrap();
        assert_eq!(current_response.command_protocol, LEGACY_COMMAND_PROTOCOL);

        let activation = serde_json::to_value(Command::ActivateCommandProtocol {
            version: CURRENT_COMMAND_PROTOCOL,
        })
        .unwrap();
        assert!(
            serde_json::from_value::<PreviousCommand>(activation).is_err(),
            "the activation record itself is the persisted downgrade refusal"
        );
    }

    #[test]
    fn append_prefix_withholds_recovered_v2_entries_until_the_peer_advertises() {
        let entries = vec![
            LogEntry {
                term: 3,
                index: 1,
                proposed_at_ms: 1,
                command: Some(put("legacy", "safe")),
            },
            LogEntry {
                term: 3,
                index: 2,
                proposed_at_ms: 2,
                command: Some(Command::LockAcquireAttempt {
                    keys: vec!["unsafe-before-advertisement".to_string()],
                    holder: "worker".to_string(),
                    request_id: "recovered-attempt".to_string(),
                    ttl_ms: 1_000,
                    wait: false,
                    wait_timeout_ms: None,
                }),
            },
            LogEntry {
                term: 3,
                index: 3,
                proposed_at_ms: 3,
                command: Some(put("after", "not-contiguous-yet")),
            },
        ];
        assert_eq!(
            appendable_protocol_prefix(&entries, LEGACY_COMMAND_PROTOCOL, LEGACY_COMMAND_PROTOCOL)
                .len(),
            1
        );
        assert_eq!(
            appendable_protocol_prefix(&entries, CURRENT_COMMAND_PROTOCOL, LEGACY_COMMAND_PROTOCOL)
                .len(),
            3
        );
        assert_eq!(
            appendable_protocol_prefix(&entries, LEGACY_COMMAND_PROTOCOL, CURRENT_COMMAND_PROTOCOL)
                .len(),
            3,
            "after durable activation, a downgraded peer must fail closed"
        );
    }

    #[test]
    fn protocol_prefix_distinguishes_lock_v2_from_cron_v3() {
        let entries = vec![
            LogEntry {
                term: 3,
                index: 1,
                proposed_at_ms: 1,
                command: Some(Command::LockAcquireAttempt {
                    keys: vec!["lock".to_string()],
                    holder: "worker".to_string(),
                    request_id: "attempt".to_string(),
                    ttl_ms: 1_000,
                    wait: false,
                    wait_timeout_ms: None,
                }),
            },
            LogEntry {
                term: 3,
                index: 2,
                proposed_at_ms: 2,
                command: Some(Command::ScheduleDelete {
                    name: "cron".to_string(),
                }),
            },
        ];
        assert_eq!(
            appendable_protocol_prefix(
                &entries,
                crate::state::LOCK_COMMAND_PROTOCOL,
                crate::state::LOCK_COMMAND_PROTOCOL,
            )
            .len(),
            1,
            "a V2 peer may receive the lock command but not the V3 cron command",
        );
        assert_eq!(
            appendable_protocol_prefix(
                &entries,
                CURRENT_COMMAND_PROTOCOL,
                crate::state::LOCK_COMMAND_PROTOCOL,
            )
            .len(),
            2,
        );
        assert_eq!(
            appendable_protocol_prefix(
                &entries,
                LEGACY_COMMAND_PROTOCOL,
                CURRENT_COMMAND_PROTOCOL,
            )
            .len(),
            2,
            "durable V3 activation makes downgraded peers fail closed",
        );
    }

    #[tokio::test]
    async fn status_keeps_unresponsive_shards_in_hosted_inventory() {
        let reg = LoopbackRegistry::new();
        let n = node("status-node", &[], 2, &reg);
        n.tasks[0].abort();
        tokio::task::yield_now().await;

        let status = n.status().await;
        assert_eq!(status.hosted_shards, vec![0, 1]);
        assert_eq!(status.unresponsive_shards, vec![0]);
        assert_eq!(status.shards.len(), 1);
        assert_eq!(status.shards[0].shard_id, 1);
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

        let attempt = n
            .propose(Command::LockAcquireAttempt {
                keys: vec!["single-node-v2".to_string()],
                holder: "worker".to_string(),
                request_id: "first-v2-attempt".to_string(),
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            })
            .await
            .expect("single member activates before its first V2-only proposal");
        assert_eq!(attempt.output["acquired"], true);
        let lock_shard = n.shard_for(crate::state::LOCK_DOMAIN);
        assert_eq!(
            n.status()
                .await
                .shards
                .iter()
                .find(|status| status.shard_id == lock_shard)
                .unwrap()
                .active_command_protocol,
            CURRENT_COMMAND_PROTOCOL
        );

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
            wait_timeout_ms: None,
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

    async fn await_all_shards_stable(
        nodes: &[&Node],
        shard_count: u32,
        tries: u32,
    ) -> Vec<NodeStatus> {
        let expected_hosted: Vec<_> = (0..shard_count).collect();
        for _ in 0..tries {
            let mut statuses = Vec::with_capacity(nodes.len());
            for node in nodes {
                statuses.push(node.status().await);
            }

            let every_node_hosts_every_shard = statuses.iter().all(|status| {
                status.hosted_shards == expected_hosted
                    && status.leader_count + status.follower_count == expected_hosted.len()
            });
            let every_shard_has_one_leader = (0..shard_count).all(|shard| {
                statuses
                    .iter()
                    .filter(|status| status.leading_shards.contains(&shard))
                    .count()
                    == 1
            });

            if every_node_hosts_every_shard && every_shard_has_one_leader {
                return statuses;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("shards did not converge to one leader per shard");
    }

    fn key_for_shard(node: &Node, shard: ShardId, label: &str) -> String {
        for i in 0..10_000 {
            let key = format!("{label}/shard-{shard}-{i}");
            if node.shard_for(&key) == shard {
                return key;
            }
        }
        panic!("could not find key for shard {shard}");
    }

    #[tokio::test]
    #[should_panic(expected = "shard_count must be > 0")]
    async fn bootstrap_rejects_zero_shard_count() {
        let reg = LoopbackRegistry::new();
        let _ = Node::bootstrap(
            NodeConfig {
                node_id: "zero-shards".to_string(),
                peers: vec![],
                shard_count: 0,
                data_dir: None,
            },
            Transport::loopback(reg),
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
    async fn single_process_hosts_multiple_leaders_and_followers_across_shards() {
        let reg = LoopbackRegistry::new();
        let shard_count = 16;
        let a = node("a", &["b", "c"], shard_count, &reg);
        let b = node("b", &["a", "c"], shard_count, &reg);
        let c = node("c", &["a", "b"], shard_count, &reg);
        let nodes = [&a, &b, &c];

        let statuses = await_all_shards_stable(&nodes, shard_count, 250).await;
        let (mixed_idx, mixed_status) = statuses
            .iter()
            .enumerate()
            .find(|(_, status)| status.leader_count >= 2 && status.follower_count >= 2)
            .expect("expected one process to lead 2+ shards and follow 2+ shards");

        let leading: std::collections::HashSet<_> =
            mixed_status.leading_shards.iter().copied().collect();
        let following: std::collections::HashSet<_> =
            mixed_status.following_shards.iter().copied().collect();
        assert!(leading.is_disjoint(&following));
        assert_eq!(leading.len(), mixed_status.leader_count);
        assert_eq!(following.len(), mixed_status.follower_count);
        assert_eq!(mixed_status.hosted_shards.len(), shard_count as usize);

        for shard in mixed_status.leading_shards.iter().take(2) {
            let key = key_for_shard(nodes[mixed_idx], *shard, "multi-leader");
            let out = nodes[mixed_idx]
                .propose(put(&key, "leader-write"))
                .await
                .expect("local leader shard should commit");
            assert_eq!(out.shard, *shard);
        }

        for shard in mixed_status.following_shards.iter().take(2) {
            let key = key_for_shard(nodes[mixed_idx], *shard, "multi-follower");
            let err = nodes[mixed_idx]
                .propose(put(&key, "follower-write"))
                .await
                .expect_err("local follower shard should redirect");
            match err {
                ProposeError::NotLeader {
                    shard: actual_shard,
                    leader,
                } => {
                    assert_eq!(actual_shard, *shard);
                    assert!(leader.is_some());
                }
                other => panic!("expected not-leader for follower shard, got {other:?}"),
            }
        }
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
        let leader_status = nodes[leader_idx].status().await;
        let shard_status = leader_status
            .shards
            .iter()
            .find(|s| s.shard_id == shard)
            .expect("leader shard status");
        assert!(shard_status.metrics.append_rtt_ms_last.is_some());
        assert!(shard_status.metrics.quorum_rtt_ms_last.is_some());
        assert!(shard_status.metrics.leader_transfer_count >= 1);

        // A non-leader rejects the write with a redirect to the leader.
        let follower_idx = (0..3).find(|i| *i != leader_idx).unwrap();
        let err = nodes[follower_idx]
            .propose(put(key, "v2"))
            .await
            .expect_err("follower must redirect");
        assert!(matches!(err, ProposeError::NotLeader { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn member_with_committed_log_rejects_stale_candidate_vote() {
        let reg = LoopbackRegistry::new();
        let a = node("a", &["b", "c"], 1, &reg);
        let b = node("b", &["a", "c"], 1, &reg);
        let c = node("c", &["a", "b"], 1, &reg);
        let nodes = [&a, &b, &c];

        let leader_idx = await_leader(&nodes, 0, 150).await;
        nodes[leader_idx]
            .propose(put("k", "committed"))
            .await
            .expect("quorum commit");

        let status = nodes[leader_idx].status().await;
        let shard_status = status
            .shards
            .iter()
            .find(|s| s.shard_id == 0)
            .expect("shard status");
        assert!(shard_status.last_log_index > 0);

        let vote = nodes[leader_idx]
            .request_vote(
                0,
                RequestVoteReq {
                    term: shard_status.term + 1,
                    candidate_id: "stale-candidate".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                    pre_vote: false,
                },
            )
            .await
            .expect("vote response");

        assert!(
            !vote.granted,
            "a member with committed entries must reject a stale candidate"
        );
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
            ..RaftTiming::default()
        };

        // Zero tick/heartbeat would panic tokio's interval — floored to 1ms.
        let t = timing(0, 0, 0).sanitized();
        assert_eq!(t.tick, Duration::from_millis(1));
        assert_eq!(t.heartbeat, Duration::from_millis(1));
        assert!(
            t.election_min_ms >= 2,
            "election clamped to >= 2x heartbeat"
        );

        // Tick coarser than the heartbeat is clamped down to the heartbeat.
        let t = timing(500, 150, 1000).sanitized();
        assert_eq!(t.tick, Duration::from_millis(150));
        assert_eq!(
            t.election_min_ms, 1000,
            "a sane election timeout is preserved"
        );

        // Election timeout below 2x the heartbeat is clamped up.
        let t = timing(20, 150, 100).sanitized();
        assert_eq!(t.election_min_ms, 300, "clamped to 2x heartbeat");

        // A realistic WAN config passes through untouched.
        let t = timing(20, 150, 1000).sanitized();
        assert_eq!(t.tick, Duration::from_millis(20));
        assert_eq!(t.election_min_ms, 1000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leadership_fails_over_when_the_leader_becomes_unresponsive() {
        let reg = LoopbackRegistry::new();
        let a = node("a", &["b", "c"], 1, &reg);
        let b = node("b", &["a", "c"], 1, &reg);
        let c = node("c", &["a", "b"], 1, &reg);
        let nodes = [&a, &b, &c];

        let leader_idx = await_leader(&nodes, 0, 150).await;
        nodes[leader_idx]
            .propose(put("k", "before"))
            .await
            .expect("write before failover");

        // Leave the leader registered, but stop its shard actors. Peers still
        // have a stale address for it, but Raft RPCs get no response.
        nodes[leader_idx].shutdown(None);

        let survivors: Vec<&Node> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .map(|(_, n)| *n)
            .collect();
        let new_leader = await_leader(&survivors, 0, 200).await;
        let out = survivors[new_leader]
            .propose(put("k", "after-unresponsive"))
            .await
            .expect("new leader commits while stale node is unresponsive");
        assert!(out.output["ok"].as_bool().unwrap());

        match survivors[new_leader]
            .query(ReadRequest::Kv {
                key: "k".to_string(),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "after-unresponsive"),
            other => panic!("unexpected read after unresponsive failover: {other:?}"),
        }
    }

    /// Linearizable-read fencing across a symmetric partition: once the
    /// isolated old leader's lease lapses and the majority side elects a new
    /// leader, the deposed leader must refuse a linearizable read with the
    /// typed propose error (never answer with the stale pre-partition value),
    /// while the new majority leader serves the post-partition write. This is
    /// the live multi-node counterpart of the actor-level
    /// `leader_lease_gates_reads_and_steps_down_on_lost_quorum`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deposed_leader_refuses_linearizable_reads_while_new_majority_leader_serves() {
        // Every node routes its *outbound* RPCs through its own registry, and
        // is *inbound-reachable* through entries in the other registries. That
        // gives per-node partition control that the single shared registry of
        // the other cluster tests cannot express: the leader can be cut off in
        // both directions while its shard actors stay alive to answer queries.
        let ids = ["a", "b", "c"];
        let regs = [
            LoopbackRegistry::new(),
            LoopbackRegistry::new(),
            LoopbackRegistry::new(),
        ];
        let a = node("a", &["b", "c"], 1, &regs[0]);
        let b = node("b", &["a", "c"], 1, &regs[1]);
        let c = node("c", &["a", "b"], 1, &regs[2]);
        let nodes = [&a, &b, &c];
        for (owner, reg) in regs.iter().enumerate() {
            for (other, node) in nodes.iter().enumerate() {
                if other != owner {
                    reg.register(ids[other], 0, node.shards[&0].clone());
                }
            }
        }

        let leader_idx = await_leader(&nodes, 0, 150).await;
        nodes[leader_idx]
            .propose(put("fence/k", "before-partition"))
            .await
            .expect("write before partition");

        // Symmetric partition around the leader: it can no longer reach any
        // peer (its lease must lapse), and no peer can reach it (the majority
        // is free to elect). Its actors keep running.
        let old_leader = nodes[leader_idx];
        for peer in 0..3 {
            if peer != leader_idx {
                regs[leader_idx].deregister(ids[peer]);
                regs[peer].deregister(ids[leader_idx]);
            }
        }

        let survivors: Vec<&Node> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .map(|(_, n)| *n)
            .collect();
        let new_leader = await_leader(&survivors, 0, 200).await;
        survivors[new_leader]
            .propose(put("fence/k", "after-partition"))
            .await
            .expect("majority side commits during the partition");

        // Bounded poll until the old leader learns it is deposed (the majority's
        // higher term reaches it over its still-working outbound links).
        let mut deposed = false;
        for _ in 0..200 {
            let status = old_leader.status().await;
            if status
                .shards
                .iter()
                .any(|s| s.shard_id == 0 && s.role != Role::Leader)
            {
                deposed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(deposed, "old leader never learned it was deposed");

        // The deposed leader must fence the read with the typed error — it may
        // never answer authoritatively with the stale value.
        match old_leader
            .query(ReadRequest::Kv {
                key: "fence/k".to_string(),
            })
            .await
        {
            Err(ProposeError::NotLeader { .. }) | Err(ProposeError::Unavailable { .. }) => {}
            other => panic!("deposed leader must refuse the linearizable read: {other:?}"),
        }

        // The majority leader serves the post-partition value linearizably.
        match survivors[new_leader]
            .query(ReadRequest::Kv {
                key: "fence/k".to_string(),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "after-partition"),
            other => panic!("majority leader should serve the fresh value: {other:?}"),
        }
    }

    /// Watch/change-feed contract: a committed KV put publishes exactly one
    /// change event carrying the written key and its commit revision, while a
    /// committed-but-lost compare-and-set publishes nothing — watchers only see
    /// mutations that actually happened, never phantom changes.
    #[tokio::test]
    async fn kv_put_publishes_one_change_event_and_a_failed_cas_publishes_none() {
        let reg = LoopbackRegistry::new();
        let n = node("watch-solo", &[], 2, &reg);
        let mut rx = n.watch("flags/watched").await.expect("subscribe to shard");

        let first = n
            .propose(put("flags/watched", "v1"))
            .await
            .expect("first put commits");
        let first_revision = first.output["revision"].as_u64().unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("change event within deadline")
            .expect("change stream open");
        assert_eq!(event.scope, "kv");
        assert_eq!(event.kind, "put");
        assert_eq!(event.key, "flags/watched");
        assert_eq!(event.revision, first_revision);

        // A CAS put against a wrong revision commits through the log but
        // mutates nothing — it must not publish a change event.
        let stale = n
            .propose(Command::KvPut {
                key: "flags/watched".to_string(),
                value: "v2".to_string(),
                ttl_ms: None,
                prev_revision: Some(first_revision + 999),
            })
            .await
            .expect("failed CAS still commits");
        assert_eq!(stale.output["ok"], false);
        assert_eq!(stale.output["reason"], "cas_mismatch");

        // The very next event on the stream is the follow-up successful put:
        // nothing was broadcast for the failed CAS in between.
        let second = n
            .propose(put("flags/watched", "v3"))
            .await
            .expect("second put commits");
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("change event within deadline")
            .expect("change stream open");
        assert_eq!(event.kind, "put");
        assert_eq!(event.key, "flags/watched");
        assert_eq!(event.revision, second.output["revision"].as_u64().unwrap());
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "no further change events should be pending"
        );
        match n
            .query(ReadRequest::Kv {
                key: "flags/watched".to_string(),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "v3"),
            other => panic!("unexpected read after CAS sequence: {other:?}"),
        }
    }

    // --- PreVote (anti-disruption straw poll) -----------------------------

    /// Bare follower shard actor (3-member group) for white-box tests of the
    /// pre-vote decision. Not wired into any cluster.
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
        .expect("fresh actor")
    }

    fn durable_actor(peers: Vec<String>) -> ShardActor {
        static NEXT_DIR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "fiducia-consensus-fault-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let (store, recovered) = ShardStore::open(&root, 0).unwrap();
        let reg = LoopbackRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        ShardActor::new(
            0,
            "a".to_string(),
            peers,
            Arc::new(Transport::loopback(reg)),
            tx,
            RaftTiming::default(),
            Some(store),
            recovered,
        )
        .expect("durable actor")
    }

    #[test]
    fn partitioned_follower_coordination_reads_preserve_applied_state_and_fencing() {
        // A local fan-out can reach a follower that is stale or partitioned.
        // Seed that follower with expired coordination holders plus live queued
        // successors. The read may hide expired holders, but it must not sweep,
        // promote, mint fencing authority, or advance the Raft apply index.
        let mut actor = follower_actor();
        let expired_at = now_ms().saturating_sub(10_000);

        let held_lock = actor.state.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["partitioned-lock".to_string()],
                holder: "expired-lock-holder".to_string(),
                ttl_ms: 1,
                wait: false,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(held_lock.output["fencing_token"], 1);
        let queued_lock = actor.state.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["partitioned-lock".to_string()],
                holder: "lock-waiter".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(queued_lock.output["queued"], true);

        let held_permit = actor.state.apply_at(
            Command::SemaphoreAcquire {
                key: "partitioned-pool".to_string(),
                holder: "expired-permit-holder".to_string(),
                limit: 1,
                ttl_ms: 1,
                wait: false,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(held_permit.output["fencing_token"], 2);
        let queued_permit = actor.state.apply_at(
            Command::SemaphoreAcquire {
                key: "partitioned-pool".to_string(),
                holder: "permit-waiter".to_string(),
                limit: 1,
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(queued_permit.output["queued"], true);

        let leadership = actor.state.apply_at(
            Command::ElectionCampaign {
                name: "partitioned-election".to_string(),
                candidate: "expired-candidate".to_string(),
                ttl_ms: 1,
                metadata: HashMap::new(),
            },
            expired_at,
        );
        assert_eq!(leadership.output["leadership"]["fencing_token"], 3);

        // Model these five commands as the follower's applied prefix. The local
        // reads below must not turn observation into another applied transition.
        actor.commit_index = 5;
        actor.last_applied = 5;
        let before_snapshot = actor.state.snapshot().unwrap();
        let before_fencing = leadership.output["leadership"]["fencing_token"]
            .as_u64()
            .unwrap();
        let before_last_applied = actor.last_applied;

        let lock_inventory = match actor.handle_query_local(ReadRequest::LockInventory) {
            ReadResponse::LockInventory(inventory) => inventory,
            other => panic!("unexpected lock inventory response: {other:?}"),
        };
        assert!(lock_inventory.held.is_empty());
        assert_eq!(lock_inventory.wait_queue.len(), 1);
        assert_eq!(lock_inventory.wait_queue[0].holder, "lock-waiter");

        let semaphore_inventory = match actor.handle_query_local(ReadRequest::SemaphoreInventory) {
            ReadResponse::SemaphoreInventory(inventory) => inventory,
            other => panic!("unexpected semaphore inventory response: {other:?}"),
        };
        assert!(semaphore_inventory[0].holders.is_empty());
        assert_eq!(semaphore_inventory[0].wait_queue.len(), 1);
        assert_eq!(semaphore_inventory[0].wait_queue[0].holder, "permit-waiter");

        let elections = match actor.handle_query_local(ReadRequest::ElectionList) {
            ReadResponse::ElectionList(elections) => elections,
            other => panic!("unexpected election inventory response: {other:?}"),
        };
        assert!(elections.is_empty());

        let after_snapshot = actor.state.snapshot().unwrap();
        assert_eq!(actor.last_applied, before_last_applied);
        assert_eq!(
            after_snapshot, before_snapshot,
            "follower-local reads must preserve holders, queues, deadlines, and fencing counters"
        );

        // A restored replica must continue above every token that existed before
        // the partitioned reads; expiry/promotion and the new grant may allocate
        // several fresh tokens, but none may reuse 1..=3.
        let restored = StateMachine::new();
        restored.restore(&after_snapshot).unwrap();
        let next = restored.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["after-restore".to_string()],
                holder: "new-holder".to_string(),
                ttl_ms: 30_000,
                wait: false,
                wait_timeout_ms: None,
            },
            now_ms(),
        );
        assert!(
            next.output["fencing_token"].as_u64().unwrap() > before_fencing,
            "snapshot restore after partitioned reads must never reuse fencing authority"
        );
    }

    #[test]
    fn vote_is_refused_and_shard_faults_when_vote_cannot_be_persisted() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.store.as_ref().unwrap().fail_next(PersistOp::Meta);

        let response = actor.handle_request_vote(RequestVoteReq {
            term: actor.current_term,
            candidate_id: "b".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        });

        assert!(
            !response.granted,
            "an unpersisted vote must never be granted"
        );
        assert!(actor.storage_fault.is_some());
        assert_eq!(actor.role, Role::Follower);
        assert!(!actor.status().storage_healthy);
    }

    #[test]
    fn append_is_not_acknowledged_when_log_fsync_fails() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.store.as_ref().unwrap().fail_next(PersistOp::Append);

        let response = actor.handle_append_entries(AppendEntriesReq {
            term: actor.current_term,
            leader_id: "b".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: actor.current_term,
                index: 1,
                proposed_at_ms: 1_000,
                command: Some(put("unsafe", "value")),
            }],
            leader_commit: 1,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });

        assert!(!response.success);
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.last_applied, 0);
        assert!(actor.state.kv_get("unsafe").is_none());
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn conflicting_append_is_not_acknowledged_when_log_rewrite_fails() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.log.push(LogEntry {
            term: actor.current_term,
            index: 1,
            proposed_at_ms: 1_000,
            command: Some(put("old", "value")),
        });
        let log = actor.log.clone();
        actor.store.as_mut().unwrap().append_tail(&log).unwrap();
        actor.store.as_ref().unwrap().fail_next(PersistOp::Rewrite);

        let response = actor.handle_append_entries(AppendEntriesReq {
            term: actor.current_term + 1,
            leader_id: "b".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: actor.current_term + 1,
                index: 1,
                proposed_at_ms: 2_000,
                command: Some(put("new", "value")),
            }],
            leader_commit: 0,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });

        assert!(!response.success);
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.last_applied, 0);
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn append_cannot_overwrite_an_already_committed_entry() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.log.push(LogEntry {
            term: actor.current_term,
            index: 1,
            proposed_at_ms: 1_000,
            command: Some(put("protected", "old")),
        });
        let log = actor.log.clone();
        actor.store.as_mut().unwrap().append_tail(&log).unwrap();
        actor
            .store
            .as_ref()
            .unwrap()
            .save_meta(actor.current_term, None, 1)
            .unwrap();
        actor.commit_index = 1;
        actor.apply_committed();

        let response = actor.handle_append_entries(AppendEntriesReq {
            term: actor.current_term + 1,
            leader_id: "b".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: actor.current_term + 1,
                index: 1,
                proposed_at_ms: 2_000,
                command: Some(put("protected", "new")),
            }],
            leader_commit: 1,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });

        assert!(!response.success);
        assert_eq!(actor.log[0].term, 1);
        assert_eq!(actor.state.kv_get("protected").unwrap().value, "old");
        assert!(actor.storage_fault.is_none());
    }

    #[test]
    fn malformed_gapped_append_is_rejected_without_mutating_the_log() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        let response = actor.handle_append_entries(AppendEntriesReq {
            term: actor.current_term,
            leader_id: "b".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: actor.current_term,
                index: 2,
                proposed_at_ms: 1_000,
                command: Some(put("gap", "value")),
            }],
            leader_commit: 0,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });

        assert!(!response.success);
        assert!(actor.log.is_empty());
        assert!(actor.storage_fault.is_none());
    }

    #[test]
    fn follower_commit_is_not_applied_when_commit_meta_fsync_fails() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.log.push(LogEntry {
            term: actor.current_term,
            index: 1,
            proposed_at_ms: 1_000,
            command: Some(put("committed", "must-not-apply")),
        });
        let log = actor.log.clone();
        actor.store.as_mut().unwrap().append_tail(&log).unwrap();
        actor.store.as_ref().unwrap().fail_next(PersistOp::Meta);

        let response = actor.handle_append_entries(AppendEntriesReq {
            term: actor.current_term,
            leader_id: "b".to_string(),
            prev_log_index: 1,
            prev_log_term: actor.current_term,
            entries: vec![],
            leader_commit: 1,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });

        assert!(!response.success);
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.last_applied, 0);
        assert!(actor.state.kv_get("committed").is_none());
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn proposal_is_unavailable_when_its_log_append_cannot_be_persisted() {
        let mut actor = durable_actor(Vec::new());
        actor.store.as_ref().unwrap().fail_next(PersistOp::Append);
        let (response, mut received) = oneshot::channel();

        actor.on_propose(put("proposal", "unsafe"), response);

        assert!(matches!(
            received.try_recv().unwrap(),
            Err(ProposeError::Unavailable { .. })
        ));
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.last_applied, 0);
        assert!(actor.state.kv_get("proposal").is_none());
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn proposal_is_not_applied_or_acknowledged_before_commit_meta_is_durable() {
        let mut actor = durable_actor(Vec::new());
        actor.store.as_ref().unwrap().fail_next(PersistOp::Meta);
        let (response, mut received) = oneshot::channel();

        actor.on_propose(put("proposal", "must-not-apply"), response);

        assert!(matches!(
            received.try_recv().unwrap(),
            Err(ProposeError::Unavailable { .. })
        ));
        assert_eq!(
            actor.log.len(),
            1,
            "the uncommitted durable entry is retained"
        );
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.last_applied, 0);
        assert!(actor.state.kv_get("proposal").is_none());
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn installed_snapshot_is_not_acknowledged_when_snapshot_fsync_fails() {
        let mut actor = durable_actor(vec!["b".to_string(), "c".to_string()]);
        actor.store.as_ref().unwrap().fail_next(PersistOp::Snapshot);
        let state = StateMachine::new();

        let response = actor.handle_install_snapshot(InstallSnapshotReq {
            term: actor.current_term,
            leader_id: "b".to_string(),
            last_included_index: 5,
            last_included_term: actor.current_term,
            state: state.snapshot().unwrap(),
        });

        assert!(!response.success);
        assert_eq!(actor.snapshot_index, 0);
        assert_eq!(actor.commit_index, 0);
        assert!(actor.storage_fault.is_some());
    }

    #[test]
    fn proposal_success_waits_for_required_compaction_snapshot_persistence() {
        let mut actor = durable_actor(Vec::new());
        actor.log = (1..=1024)
            .map(|index| LogEntry {
                term: actor.current_term,
                index,
                proposed_at_ms: 1_000 + index,
                command: (index == 1024).then(|| put("snapshot-boundary", "value")),
            })
            .collect();
        actor.commit_index = 1024;
        actor.last_applied = 1023;
        let (response, mut received) = oneshot::channel();
        actor.pending.insert(
            1024,
            PendingProposal {
                started_at: Instant::now(),
                resp: response,
            },
        );
        actor.store.as_ref().unwrap().fail_next(PersistOp::Snapshot);

        actor.apply_committed();

        assert!(matches!(
            received.try_recv().unwrap(),
            Err(ProposeError::Unavailable { .. })
        ));
        assert_eq!(actor.last_applied, 1024);
        assert!(actor.storage_fault.is_some());
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

    #[tokio::test]
    async fn command_protocol_activation_requires_every_peers_parser_advertisement() {
        let mut actor = leader_actor();
        assert_eq!(actor.state.command_protocol(), LEGACY_COMMAND_PROTOCOL);

        let (legacy_tx, mut legacy_rx) = oneshot::channel();
        actor.on_propose(
            Command::LockAcquireV2 {
                keys: vec!["rolling-lock".to_string()],
                holder: "worker".to_string(),
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            },
            legacy_tx,
        );
        assert!(matches!(
            actor.log[0].command,
            Some(Command::LockAcquire { .. })
        ));
        assert!(
            legacy_rx.try_recv().is_err(),
            "proposal still awaits quorum"
        );

        let before_rejected = actor.log.len();
        let (blocked_tx, mut blocked_rx) = oneshot::channel();
        actor.on_propose(
            Command::LockAcquireAttempt {
                keys: vec!["rolling-lock".to_string()],
                holder: "worker-2".to_string(),
                request_id: "attempt-before-activation".to_string(),
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            blocked_tx,
        );
        assert!(matches!(
            blocked_rx.try_recv().unwrap(),
            Err(ProposeError::Unavailable { .. })
        ));
        assert_eq!(actor.log.len(), before_rejected);

        actor.handle_append_reply(
            "b".to_string(),
            0,
            None,
            Some(AppendEntriesResp {
                term: actor.current_term,
                success: true,
                match_index: 0,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            }),
        );
        assert!(actor
            .log
            .iter()
            .all(|entry| !matches!(entry.command, Some(Command::ActivateCommandProtocol { .. }))));
        actor.handle_append_reply(
            "c".to_string(),
            0,
            None,
            Some(AppendEntriesResp {
                term: actor.current_term,
                success: true,
                match_index: 0,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            }),
        );
        let activation_index = actor.last_log_index();
        assert!(matches!(
            actor.log.last().unwrap().command,
            Some(Command::ActivateCommandProtocol {
                version: CURRENT_COMMAND_PROTOCOL
            })
        ));
        assert_eq!(
            actor.state.command_protocol(),
            LEGACY_COMMAND_PROTOCOL,
            "advertisement alone is not the emission gate"
        );

        actor.handle_append_reply(
            "b".to_string(),
            activation_index,
            None,
            Some(AppendEntriesResp {
                term: actor.current_term,
                success: true,
                match_index: activation_index,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            }),
        );
        assert_eq!(actor.state.command_protocol(), CURRENT_COMMAND_PROTOCOL);
        assert!(legacy_rx.try_recv().unwrap().is_ok());
        let status = actor.status();
        assert_eq!(status.parser_command_protocol, CURRENT_COMMAND_PROTOCOL);
        assert_eq!(status.active_command_protocol, CURRENT_COMMAND_PROTOCOL);

        let (attempt_tx, _attempt_rx) = oneshot::channel();
        actor.on_propose(
            Command::LockAcquireAttempt {
                keys: vec!["rolling-lock".to_string()],
                holder: "worker-2".to_string(),
                request_id: "attempt-after-activation".to_string(),
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            attempt_tx,
        );
        assert!(matches!(
            actor.log.last().unwrap().command,
            Some(Command::LockAcquireAttempt { .. })
        ));
    }

    #[test]
    fn recovered_newer_entry_cannot_commit_before_parser_quorum() {
        let mut actor = leader_actor();
        actor.log.push(LogEntry {
            term: actor.current_term,
            index: 1,
            proposed_at_ms: 1_000,
            command: Some(Command::LockAcquireAttempt {
                keys: vec!["recovered".to_string()],
                holder: "worker".to_string(),
                request_id: "pre-gate-entry".to_string(),
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            }),
        });
        {
            let leader = actor.leader.as_mut().unwrap();
            leader.match_index.insert("b".to_string(), 1);
            leader.match_index.insert("c".to_string(), 1);
            leader
                .peer_command_protocol
                .insert("b".to_string(), CURRENT_COMMAND_PROTOCOL);
            leader
                .peer_command_protocol
                .insert("c".to_string(), LEGACY_COMMAND_PROTOCOL);
        }
        actor.maybe_advance_commit();
        assert_eq!(actor.commit_index, 0);
        assert_eq!(actor.state.command_protocol(), LEGACY_COMMAND_PROTOCOL);

        actor
            .leader
            .as_mut()
            .unwrap()
            .peer_command_protocol
            .insert("c".to_string(), CURRENT_COMMAND_PROTOCOL);
        actor.maybe_advance_commit();
        assert_eq!(actor.commit_index, 1);
        assert_eq!(
            actor.state.command_protocol(),
            crate::state::LOCK_COMMAND_PROTOCOL
        );
        assert!(
            actor
                .state
                .snapshot()
                .unwrap()
                .starts_with(crate::state::PREVIOUS_PROTOCOL_SNAPSHOT_MAGIC),
            "implicit recovery activation still persists downgrade refusal"
        );
    }

    #[tokio::test]
    async fn successful_partial_append_immediately_schedules_the_next_batch() {
        let mut actor = leader_actor();
        actor.log = (1..=3)
            .map(|index| LogEntry {
                term: actor.current_term,
                index,
                proposed_at_ms: index,
                command: None,
            })
            .collect();

        actor.handle_append_reply(
            "b".to_string(),
            1,
            Some(1),
            Some(AppendEntriesResp {
                term: actor.current_term,
                success: true,
                match_index: 1,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            }),
        );

        let leader = actor.leader.as_ref().unwrap();
        assert_eq!(leader.match_index.get("b"), Some(&1));
        assert_eq!(leader.next_index.get("b"), Some(&2));
        assert_eq!(leader.in_flight.get("b"), Some(&true));
    }

    #[test]
    fn rejected_append_rewinds_without_starting_a_tight_retry_loop() {
        let mut actor = leader_actor();
        let leader = actor.leader.as_mut().unwrap();
        leader.next_index.insert("b".to_string(), 9);
        leader.in_flight.insert("b".to_string(), true);

        actor.handle_append_reply(
            "b".to_string(),
            8,
            Some(1),
            Some(AppendEntriesResp {
                term: actor.current_term,
                success: false,
                match_index: 4,
                command_protocol: CURRENT_COMMAND_PROTOCOL,
            }),
        );

        let leader = actor.leader.as_ref().unwrap();
        assert_eq!(leader.next_index.get("b"), Some(&5));
        assert_eq!(leader.in_flight.get("b"), Some(&false));
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

    /// CheckQuorum changes only the volatile role; it must not erase the durable
    /// vote that elected this member. Otherwise the member can grant a second
    /// vote in the same term and make two different candidates legitimate
    /// leaders, which then demote each other with same-term AppendEntries.
    #[test]
    fn same_term_quorum_step_down_preserves_vote_and_refuses_a_second_candidate() {
        let mut a = leader_actor();
        a.current_term = 7;
        a.voted_for = Some(a.node_id.clone());

        a.relinquish_no_quorum();

        assert_eq!(a.role, Role::Follower);
        assert_eq!(a.current_term, 7);
        assert_eq!(a.voted_for.as_deref(), Some("a"));
        assert_eq!(a.leader_id, None);

        let response = a.handle_request_vote(RequestVoteReq {
            term: 7,
            candidate_id: "b".to_string(),
            last_log_index: a.last_log_index(),
            last_log_term: a.last_log_term(),
            pre_vote: false,
        });
        assert!(!response.granted, "one member must not vote twice per term");
        assert_eq!(a.voted_for.as_deref(), Some("a"));
    }

    #[test]
    fn older_term_step_down_request_cannot_demote_a_current_leader() {
        let mut a = leader_actor();
        a.current_term = 9;
        a.voted_for = Some(a.node_id.clone());

        a.step_down(8, Some("b".to_string()));

        assert_eq!(a.role, Role::Leader);
        assert_eq!(a.current_term, 9);
        assert_eq!(a.voted_for.as_deref(), Some("a"));
        assert_eq!(a.leader_id.as_deref(), Some("a"));
        assert!(
            a.leader.is_some(),
            "volatile leader state must remain intact"
        );
    }

    #[test]
    fn higher_term_step_down_resets_the_vote_and_tracks_the_new_leader() {
        let mut a = leader_actor();
        a.current_term = 9;
        a.voted_for = Some(a.node_id.clone());

        a.step_down(10, Some("b".to_string()));

        assert_eq!(a.role, Role::Follower);
        assert_eq!(a.current_term, 10);
        assert_eq!(a.voted_for, None);
        assert_eq!(a.leader_id.as_deref(), Some("b"));
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
        assert_eq!(
            a.role,
            Role::Leader,
            "no step-down when check-quorum is off"
        );
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
            proposed_at_ms: 1_000,
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

    #[test]
    fn install_snapshot_restores_state_and_accepts_suffix_entries() {
        let image = StateMachine::new();
        image.apply_at(
            Command::KvPut {
                key: "from-snapshot".to_string(),
                value: "v1".to_string(),
                ttl_ms: None,
                prev_revision: None,
            },
            10_000,
        );
        let mut follower = follower_actor();
        let installed = follower.handle_install_snapshot(InstallSnapshotReq {
            term: 2,
            leader_id: "b".to_string(),
            last_included_index: 5,
            last_included_term: 2,
            state: image.snapshot().unwrap(),
        });
        assert!(installed.success);
        assert_eq!(installed.match_index, 5);
        assert_eq!(follower.snapshot_index, 5);
        assert_eq!(follower.last_applied, 5);

        let appended = follower.handle_append_entries(AppendEntriesReq {
            term: 2,
            leader_id: "b".to_string(),
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![LogEntry {
                term: 2,
                index: 6,
                proposed_at_ms: 11_000,
                command: Some(Command::KvPut {
                    key: "after-snapshot".to_string(),
                    value: "v2".to_string(),
                    ttl_ms: None,
                    prev_revision: None,
                }),
            }],
            leader_commit: 6,
            command_protocol: CURRENT_COMMAND_PROTOCOL,
        });
        assert!(appended.success);
        assert_eq!(follower.last_applied, 6);
        assert_eq!(follower.state.kv_get("from-snapshot").unwrap().value, "v1");
        assert_eq!(follower.state.kv_get("after-snapshot").unwrap().value, "v2");
    }

    #[test]
    fn committed_log_is_compacted_at_the_snapshot_threshold() {
        let mut actor = follower_actor();
        actor.log = (1..=1024)
            .map(|index| LogEntry {
                term: 1,
                index,
                proposed_at_ms: 10_000 + index,
                command: Some(Command::CounterAdd {
                    key: "compaction-count".to_string(),
                    delta: 1,
                    prev_revision: None,
                }),
            })
            .collect();
        actor.commit_index = 1024;
        actor.apply_committed();
        assert_eq!(actor.snapshot_index, 1024);
        assert_eq!(actor.snapshot_term, 1);
        assert_eq!(actor.last_applied, 1024);
        assert!(actor.log.is_empty());
        assert!(!actor.snapshot_state.is_empty());
        assert_eq!(
            actor.state.counter_get("compaction-count").unwrap().value,
            1024
        );
    }

    #[test]
    fn restart_restores_snapshot_then_replays_only_the_retained_suffix() {
        let snapshotted = StateMachine::new();
        snapshotted.apply_at(
            Command::KvPut {
                key: "before".to_string(),
                value: "snapshot".to_string(),
                ttl_ms: None,
                prev_revision: None,
            },
            20_000,
        );
        let reg = LoopbackRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let actor = ShardActor::new(
            0,
            "restart".to_string(),
            Vec::new(),
            Arc::new(Transport::loopback(reg)),
            tx,
            RaftTiming::default(),
            None,
            Recovered {
                current_term: 3,
                voted_for: None,
                commit_index: 6,
                snapshot: Some(PersistedSnapshot {
                    last_included_index: 5,
                    last_included_term: 2,
                    state: snapshotted.snapshot().unwrap(),
                }),
                log: vec![LogEntry {
                    term: 3,
                    index: 6,
                    proposed_at_ms: 21_000,
                    command: Some(Command::KvPut {
                        key: "after".to_string(),
                        value: "suffix".to_string(),
                        ttl_ms: None,
                        prev_revision: None,
                    }),
                }],
            },
        )
        .expect("recovered actor");
        assert_eq!(actor.last_applied, 6);
        assert_eq!(actor.snapshot_index, 5);
        assert_eq!(actor.state.kv_get("before").unwrap().value, "snapshot");
        assert_eq!(actor.state.kv_get("after").unwrap().value, "suffix");
    }

    /// Regression (boot quarantine): a shard whose on-disk state cannot be used
    /// must come up fail-closed WITHOUT taking the process — or its sibling
    /// shards — down with it. Here shard 0's directory holds a log entry whose
    /// index contradicts the recovery invariants; the node must still bootstrap,
    /// quarantine shard 0 (visible via status), and keep shard 1 serving writes.
    #[tokio::test]
    async fn corrupt_shard_is_quarantined_while_sibling_shards_keep_serving() {
        let root = std::env::temp_dir().join(format!(
            "fiducia-quarantine-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        // Seed shard 0 with durable state that violates recovery invariants:
        // a log whose first entry index (7) does not follow the snapshot base (0).
        // Strict recovery rejects it — previously as a process-killing panic.
        {
            let (mut store, _recovered) = ShardStore::open(&root, 0).unwrap();
            store
                .append_tail(&[LogEntry {
                    term: 1,
                    index: 7,
                    proposed_at_ms: 1_000,
                    command: Some(Command::KvPut {
                        key: "poison".to_string(),
                        value: "x".to_string(),
                        ttl_ms: None,
                        prev_revision: None,
                    }),
                }])
                .unwrap();
        }

        let node = Node::bootstrap(
            NodeConfig {
                node_id: "quarantine-node".to_string(),
                peers: Vec::new(), // single member: healthy shards lead from t=0
                shard_count: 2,
                data_dir: Some(root.clone()),
            },
            Transport::loopback(LoopbackRegistry::new()),
        );

        // Shard 0 is hosted but fail-closed; its status names the fault. Shard 1
        // stays healthy and (single-member) leads.
        let status = node.status().await;
        let shard0 = status
            .shards
            .iter()
            .find(|s| s.shard_id == 0)
            .expect("shard 0 reported");
        assert!(!shard0.storage_healthy, "corrupt shard must be quarantined");
        assert!(shard0.storage_error.is_some());
        assert_eq!(
            shard0.role,
            Role::Follower,
            "a quarantined shard never leads"
        );
        let shard1 = status
            .shards
            .iter()
            .find(|s| s.shard_id == 1)
            .expect("shard 1 reported");
        assert!(shard1.storage_healthy, "sibling shard must stay healthy");
        assert_eq!(
            shard1.role,
            Role::Leader,
            "single-member healthy shard leads"
        );

        // The node still serves writes: keys hash across both shards, so within a
        // few probes one must land on healthy shard 1 and commit; probes landing
        // on quarantined shard 0 must fail closed rather than hang or panic.
        let mut committed = false;
        for i in 0..16 {
            if node
                .propose(Command::KvPut {
                    key: format!("quarantine-probe-{i}"),
                    value: "v".to_string(),
                    ttl_ms: None,
                    prev_revision: None,
                })
                .await
                .is_ok()
            {
                committed = true;
                break;
            }
        }
        assert!(committed, "healthy shards must keep committing writes");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Failover over the REAL production transport: three nodes form a Raft
    /// group over HTTP (`Transport::http` → each peer's `/raft/{shard}/…`
    /// plane, exactly as deployed pods do), commit a write, then the leader is
    /// killed — server aborted, shard actors aborted, exactly like losing the
    /// node/cluster that hosted it. The surviving 2/3 must elect a new leader
    /// and keep committing writes. This is the in-process twin of the e2e
    /// chaos test (fiducia-e2e tests/chaos/cluster-failure.test.mjs), which
    /// proves the same invariant by scaling a real cluster's StatefulSet to 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_cluster_elects_new_leader_and_commits_after_leader_death() {
        // Bind three ephemeral listeners first so every member knows all peers.
        let mut listeners = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            ids.push(listener.local_addr().unwrap().to_string());
            listeners.push(listener);
        }

        let mut nodes: Vec<Arc<Node>> = Vec::new();
        let mut servers = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let peers: Vec<String> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, id)| id.clone())
                .collect();
            let node = Arc::new(Node::bootstrap(
                NodeConfig {
                    node_id: ids[i].clone(),
                    peers,
                    shard_count: 1, // one Raft group ⇒ one unambiguous leader
                    data_dir: None,
                },
                Transport::http(),
            ));
            let app = axum::Router::new()
                .nest("/raft", crate::raft_api::router())
                .with_state(node.clone());
            servers.push(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
            nodes.push(node);
        }

        // A leader must emerge from a real election over HTTP.
        let deadline = Instant::now() + Duration::from_secs(10);
        let leader = loop {
            let mut found = None;
            for (i, node) in nodes.iter().enumerate() {
                if node.status().await.leading_shards.contains(&0) {
                    found = Some(i);
                }
            }
            if let Some(i) = found {
                break i;
            }
            assert!(
                Instant::now() < deadline,
                "no leader elected over HTTP within 10s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // A write proposed at the leader commits on the full quorum.
        nodes[leader]
            .propose(Command::KvPut {
                key: "failover/before".to_string(),
                value: "pre-failover".to_string(),
                ttl_ms: None,
                prev_revision: None,
            })
            .await
            .expect("write commits with all three members alive");

        // Kill the leader: its HTTP plane and its shard actors go away together,
        // as they would when the hosting node/cluster dies.
        servers[leader].abort();
        nodes[leader].shutdown(None);

        // WRONG BEHAVIOR => FAIL: the surviving majority must elect a NEW
        // leader and accept writes. Retry proposals across the survivors until
        // one commits (routing follows leadership as it settles).
        let deadline = Instant::now() + Duration::from_secs(15);
        let new_leader = 'outer: loop {
            for (i, node) in nodes.iter().enumerate() {
                if i == leader {
                    continue;
                }
                if node
                    .propose(Command::KvPut {
                        key: "failover/after".to_string(),
                        value: "post-failover".to_string(),
                        ttl_ms: None,
                        prev_revision: None,
                    })
                    .await
                    .is_ok()
                {
                    break 'outer i;
                }
            }
            assert!(
                Instant::now() < deadline,
                "survivors did not elect a leader and commit within 15s of leader death"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_ne!(
            new_leader, leader,
            "the dead leader cannot have committed it"
        );

        // The pre-failover write survives, linearizably, on the new leader.
        match nodes[new_leader]
            .query(ReadRequest::Kv {
                key: "failover/before".to_string(),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(entry))) => assert_eq!(entry.value, "pre-failover"),
            other => panic!("pre-failover write lost after failover: {other:?}"),
        }

        for (i, node) in nodes.iter().enumerate() {
            if i != leader {
                node.shutdown(None);
            }
        }
        for server in servers {
            server.abort();
        }
    }
}
