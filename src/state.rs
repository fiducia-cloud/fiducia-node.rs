//! The replicated state machine.
//!
//! Every mutation exposed by Fiducia is represented as a [`Command`] and is applied
//! in committed-log order. The log may be local in single-node development, but
//! the state-machine semantics are the same ones the replicated path uses.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::indexed_queue::IndexedQueue;

/// Command protocol understood by the release immediately before the hardened
/// lock/semaphore command variants were introduced.
pub const LEGACY_COMMAND_PROTOCOL: u16 = 1;
/// Highest command protocol this binary can parse and apply.
pub const CURRENT_COMMAND_PROTOCOL: u16 = 2;
const COMMAND_PROTOCOL_ROUTING_KEY: &str = "\0fiducia-command-protocol";
/// A non-JSON prefix is deliberate: an older binary must reject, rather than
/// deserialize as an empty/default Store, a snapshot created after activation.
pub(crate) const CURRENT_PROTOCOL_SNAPSHOT_MAGIC: &[u8] = b"FIDUCIA-STATE-MACHINE\0V2\n";

/// Every mutation in the system, as it travels through the replicated log.
///
/// Read operations never become commands. They are served directly off applied
/// state after the request has reached the shard leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Command {
    // --- Replicated compatibility barrier ---------------------------------
    /// Internal, per-shard activation record. Leaders append this only after
    /// every configured peer has advertised parser support over AppendEntries.
    /// Its committed state stamp and snapshot envelope make downgrade refusal
    /// durable after the activating log prefix is compacted.
    ActivateCommandProtocol {
        version: u16,
    },

    // --- Config KV ---------------------------------------------------------
    KvPut {
        key: String,
        value: String,
        ttl_ms: Option<u64>,
        prev_revision: Option<u64>,
    },
    KvDelete {
        key: String,
    },

    // --- Distributed counters -------------------------------------------
    /// Atomically add `delta` (which may be negative) to counter `key`, creating
    /// it at 0 first. When `prev_revision` is set, the add is a compare-and-set:
    /// it applies only if the counter's current `mod_revision` matches.
    CounterAdd {
        key: String,
        delta: i64,
        prev_revision: Option<u64>,
    },
    /// Set counter `key` to an absolute `value` (e.g. reset to 0). `prev_revision`
    /// makes it a compare-and-set.
    CounterSet {
        key: String,
        value: i64,
        prev_revision: Option<u64>,
    },

    // --- Barriers (fan-in) ----------------------------------------------
    /// Create (or reconfigure, if unmet) a named barrier with a resolution
    /// policy. `expected` is the participant count for `all`/`any_veto`.
    BarrierCreate {
        name: String,
        policy: BarrierPolicy,
        expected: u32,
        deadline_ms: Option<u64>,
    },
    /// Record a participant's arrival (or veto). Repeat arrivals by the same
    /// participant are idempotent. Auto-creates an `all`-policy barrier if none
    /// exists yet, so simple fan-ins need no explicit create.
    BarrierArrive {
        name: String,
        participant: String,
        weight: u64,
        veto: bool,
    },

    // --- Durable tasks --------------------------------------------------
    /// Create a durable task if it does not already exist. Idempotent: a repeat
    /// create returns the existing task unchanged.
    TaskCreate {
        name: String,
        task_type: String,
        payload: Value,
        deadline_ms: Option<u64>,
    },
    /// Claim a pending (or lease-expired) task, minting a fresh fencing token and
    /// an ownership lease. An actively owned task is not re-granted.
    TaskClaim {
        name: String,
        worker: String,
        ttl_ms: u64,
    },
    /// Report progress and a checkpoint, renewing the lease. Rejected unless the
    /// caller presents the current owner + fencing token.
    TaskProgress {
        name: String,
        worker: String,
        fencing_token: u64,
        percent: u32,
        checkpoint: Value,
    },
    /// Complete a task with a durable result. Requires the current fencing token.
    TaskComplete {
        name: String,
        worker: String,
        fencing_token: u64,
        result: Value,
    },
    /// Fail a task. `retryable` returns it to Pending for reassignment; otherwise
    /// it ends Failed. Requires the current fencing token.
    TaskFail {
        name: String,
        worker: String,
        fencing_token: u64,
        retryable: bool,
    },
    /// Cancel a task (terminal), regardless of owner.
    TaskCancel {
        name: String,
    },

    // --- Approval-escrow effects ----------------------------------------
    /// Prepare a side effect for later authorization. Idempotent: a repeat
    /// prepare returns the existing effect. `required_approvals` of 0 is
    /// pre-approved and may be committed immediately.
    EffectPrepare {
        name: String,
        effect_type: String,
        payload: Value,
        risk: String,
        idempotency_key: String,
        required_approvals: u32,
    },
    /// Record one principal's approval. Duplicate approvals by the same principal
    /// count once; reaching `required_approvals` moves the effect to Approved.
    EffectApprove {
        name: String,
        principal: String,
    },
    /// Commit an approved effect exactly once, recording the durable result.
    /// A repeat commit replays without re-executing.
    EffectCommit {
        name: String,
        result: Value,
    },
    /// Abort a prepared/approved effect (terminal); it can never be committed.
    EffectAbort {
        name: String,
    },

    // --- Atomic ownership handoffs --------------------------------------
    /// Offer to transfer ownership of `resource` from `from` (presenting its
    /// current `from_token`) to `to`, with a context manifest and an accept
    /// deadline. The original owner keeps authority until the offer is accepted.
    HandoffOffer {
        name: String,
        resource: String,
        from: String,
        to: String,
        from_token: u64,
        context: Value,
        ttl_ms: u64,
    },
    /// Accept an offered handoff, minting a strictly higher fencing token for the
    /// new owner. Only the offered recipient may accept.
    HandoffAccept {
        name: String,
        to: String,
    },
    /// Reject an offered handoff; ownership stays with the original owner.
    HandoffReject {
        name: String,
        to: String,
    },

    // --- Decisions (typed weighted voting) ------------------------------
    /// Propose a decision with typed options and a resolution policy.
    DecisionPropose {
        name: String,
        question: String,
        options: Vec<String>,
        policy: DecisionPolicy,
        deadline_ms: Option<u64>,
    },
    /// Cast (or replace) one voter's vote. `option` of `None` abstains; `veto`
    /// aborts the decision; `weight` drives resolution and `evidence` records why.
    DecisionVote {
        name: String,
        voter: String,
        option: Option<String>,
        confidence: f32,
        weight: u64,
        veto: bool,
        evidence: Vec<String>,
    },

    // --- Hierarchical budgets -------------------------------------------
    /// Create or re-cap a budget with a per-axis ceiling (usd_micros, tokens,
    /// tool_calls); a `None` axis is unlimited.
    BudgetSet {
        name: String,
        limit: BudgetAmount,
    },
    /// Reserve `amount` against a budget under `reservation_id`. Rejected if it
    /// would exceed any limited axis, so concurrent holders cannot each spend the
    /// same remaining dollar.
    BudgetReserve {
        name: String,
        reservation_id: String,
        holder: String,
        amount: BudgetAmount,
    },
    /// Commit a reservation with the `actual` spend (≤ reserved). Frees the
    /// difference between reserved and actual.
    BudgetCommit {
        name: String,
        reservation_id: String,
        actual: BudgetAmount,
    },
    /// Release a still-held reservation, returning its full headroom.
    BudgetRelease {
        name: String,
        reservation_id: String,
    },

    // --- Claims (contestable ledger) ------------------------------------
    /// Assert (or re-assert) a claim: a versioned subject/predicate/value with
    /// confidence and evidence. Re-asserting bumps the version and resets it to
    /// Asserted.
    ClaimAssert {
        name: String,
        subject: String,
        predicate: String,
        value: Value,
        confidence: f32,
        author: String,
        evidence: Vec<String>,
        valid_until_ms: Option<u64>,
    },
    /// Record an agent's support for a claim.
    ClaimSupport {
        name: String,
        agent: String,
    },
    /// Record an agent's contest of a claim, moving it to Contested.
    ClaimContest {
        name: String,
        agent: String,
        reason: String,
    },
    /// An authorized process accepts or rejects a claim (terminal).
    ClaimResolve {
        name: String,
        accepted: bool,
    },
    /// Supersede a claim with a newer one (terminal), pointing to its successor.
    ClaimSupersede {
        name: String,
        superseded_by: String,
    },

    // --- Mutual-exclusion locks (multi-key UNION) -------------------------
    /// Acquire a lock over the **union** of `keys` atomically (all-or-nothing):
    /// the grant conflicts with anyone holding *any* of those keys, and is queued
    /// (FIFO, deadlock-free) until *every* member key is free. A single-key lock
    /// is just `keys = [k]`. This is the live-mutex "lock on a combination of
    /// keys" primitive. See [`LOCK_DOMAIN`] for why these route together.
    LockAcquire {
        keys: Vec<String>,
        holder: String,
        ttl_ms: u64,
        wait: bool,
        /// Historical entries omit this and retain their original indefinite
        /// waits; newly accepted V2/attempt commands use a bounded default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait_timeout_ms: Option<u64>,
    },
    /// Hardened acquire for callers that do not yet supply an attempt id. This
    /// separate log variant keeps historical `LockAcquire` replay byte-for-byte
    /// compatible while applying bounded queue leases and non-renewing retries
    /// to all newly accepted HTTP requests.
    LockAcquireV2 {
        keys: Vec<String>,
        holder: String,
        ttl_ms: u64,
        wait: bool,
        wait_timeout_ms: Option<u64>,
    },
    /// Attempt-scoped acquire used by hardened clients. `request_id` is a
    /// cryptographically unique acquisition identity reused only for retries.
    LockAcquireAttempt {
        keys: Vec<String>,
        holder: String,
        request_id: String,
        ttl_ms: u64,
        wait: bool,
        wait_timeout_ms: Option<u64>,
    },
    /// Extend an active union-lock lease only when holder, exact key set, and
    /// fencing token all match. The token is preserved.
    LockRenew {
        keys: Vec<String>,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    },
    /// Release a held lock by its fencing token, freeing every member key at once
    /// and promoting the next grantable waiter(s).
    LockRelease {
        holder: String,
        fencing_token: u64,
    },
    /// Idempotently remove one queued `(holder, canonical key-set)` request.
    LockCancel {
        keys: Vec<String>,
        holder: String,
    },
    /// Attempt-scoped cancellation. Its tombstone prevents a late acquire with
    /// the same request_id from committing after cancellation was acknowledged.
    LockCancelAttempt {
        keys: Vec<String>,
        holder: String,
        request_id: String,
    },

    // --- Counting semaphores ----------------------------------------------
    /// Acquire one permit of a counting semaphore `key` that admits up to `limit`
    /// concurrent holders. Beyond the cap, callers queue (FIFO) when `wait`.
    SemaphoreAcquire {
        key: String,
        holder: String,
        limit: u32,
        ttl_ms: u64,
        wait: bool,
        /// Historical entries omit this and retain their original indefinite
        /// waits; newly accepted V2/attempt commands use a bounded default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait_timeout_ms: Option<u64>,
    },
    /// Hardened, immutable-limit acquire without an attempt id. Historical
    /// `SemaphoreAcquire` entries retain their old reconfiguration semantics so
    /// uncompacted logs replay identically during rolling upgrades.
    SemaphoreAcquireV2 {
        key: String,
        holder: String,
        limit: u32,
        ttl_ms: u64,
        wait: bool,
        wait_timeout_ms: Option<u64>,
    },
    SemaphoreAcquireAttempt {
        key: String,
        holder: String,
        request_id: String,
        limit: u32,
        ttl_ms: u64,
        wait: bool,
        wait_timeout_ms: Option<u64>,
    },
    /// Extend one active permit lease without changing its fencing token.
    SemaphoreRenew {
        key: String,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    },
    /// Release one held permit by its fencing token, admitting the next waiter.
    SemaphoreRelease {
        key: String,
        holder: String,
        fencing_token: u64,
    },
    /// Idempotently remove one queued holder from a semaphore.
    SemaphoreCancel {
        key: String,
        holder: String,
    },
    SemaphoreCancelAttempt {
        key: String,
        holder: String,
        request_id: String,
    },

    // --- Rate limiting -----------------------------------------------------
    RateLimitCheck {
        key: String,
        tenant: String,
        algorithm: RateLimitAlgorithm,
        limit: u32,
        window_ms: u64,
        refill_per_second: Option<f64>,
        cost: u32,
    },

    // --- Idempotency / dedupe ----------------------------------------------
    /// Claim an idempotency key. First claim wins; later claims return the
    /// existing active record as a duplicate.
    ///
    /// The lease is split in two: `ttl_ms` is the **in-flight lease** — how long a
    /// *claimed-but-not-completed* record is held before an abandoned claim
    /// expires and the key becomes re-claimable. It is short (sized to the max
    /// request duration), so a holder that crashes between claim and complete
    /// frees the key in seconds instead of poisoning it for the whole retention
    /// window. `retention_ms` is how long the record lives *after* completion so
    /// duplicates can replay the stored result; `complete` extends the lease to
    /// it. `None` retention falls back to `ttl_ms`, preserving the old
    /// single-window behaviour for log entries written before the split.
    IdempotencyClaim {
        key: String,
        owner: String,
        ttl_ms: u64,
        #[serde(default)]
        retention_ms: Option<u64>,
        metadata: HashMap<String, String>,
    },
    /// Mark a claimed key complete and optionally store the domain result for
    /// duplicate callers to replay. Extends the lease from the short in-flight
    /// window to the record's full `retention_ms`.
    IdempotencyComplete {
        key: String,
        owner: String,
        fencing_token: u64,
        result: Option<Value>,
    },
    /// Extend the in-flight lease on a still-claimed key without completing it.
    /// Lets a holder running a long operation keep its claim alive past the
    /// initial `ttl_ms`. Rejected once the record is completed (the retention
    /// lease governs then) or by a non-holder.
    IdempotencyRenew {
        key: String,
        owner: String,
        fencing_token: u64,
        ttl_ms: u64,
    },
    /// Release a still-claimed key so a retry can re-execute immediately, instead
    /// of waiting out the in-flight lease. Used when the holder's operation failed
    /// transiently (5xx / timeout) and left nothing durable to replay. Refused
    /// once completed — a stored result must survive — or for a non-holder.
    IdempotencyAbandon {
        key: String,
        owner: String,
        fencing_token: u64,
    },

    // --- Cron / scheduling -------------------------------------------------
    ScheduleUpsert {
        name: String,
        cron: Option<String>,
        one_shot_at_ms: Option<u64>,
        target: ScheduleTarget,
        delivery: DeliverySemantics,
        max_retries: u32,
        /// Proposer-stamped wall clock, so every replica computes the same initial
        /// `next_fire` deterministically (the state machine must not read the clock).
        now_ms: u64,
    },
    ScheduleRecordRun {
        name: String,
        fire_id: String,
        fired_at_ms: u64,
    },
    /// Claim one due fire for delivery. Committed through Raft, so exactly one
    /// claim per `fire_id_ms` wins even across a leader change — this is what makes
    /// firing leader-elected with no duplicates. The state machine validates the
    /// fire is the schedule's legitimate next fire and advances the cursor.
    ScheduleClaimFire {
        name: String,
        fire_id_ms: u64,
    },
    /// Record the outcome of delivering a claimed fire (after retries).
    ScheduleRecordResult {
        name: String,
        fire_id_ms: u64,
        delivered: bool,
        attempts: u32,
        error: Option<String>,
    },

    // --- Leader election ---------------------------------------------------
    ElectionCampaign {
        name: String,
        candidate: String,
        ttl_ms: u64,
        /// Opaque candidate facts published to observers/watchers (e.g. address,
        /// region, version) so the leader is *discoverable*, not just named.
        #[serde(default)]
        metadata: HashMap<String, String>,
    },
    ElectionRenew {
        name: String,
        candidate: String,
        fencing_token: u64,
        /// Lease extension; when omitted the leadership's original campaign TTL
        /// is reused instead of a hard-coded default.
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    ElectionResign {
        name: String,
        candidate: String,
        fencing_token: u64,
    },

    // --- Service discovery -------------------------------------------------
    ServiceRegister {
        service: String,
        instance_id: String,
        address: String,
        ttl_ms: u64,
        metadata: HashMap<String, String>,
    },
    ServiceHeartbeat {
        service: String,
        instance_id: String,
        ttl_ms: Option<u64>,
    },
    ServiceDeregister {
        service: String,
        instance_id: String,
    },
}

/// Routing key under which **all** lock + semaphore state lives, so the entire
/// lock service is one linearizable Raft group (one shard leader).
///
/// This is the price of correct multi-key **union** locking: to grant `{A,B,C}`
/// atomically and detect that it conflicts with a holder of `{B}`, one state
/// machine must see every member key together. Routing every lock/semaphore
/// command to a single coordinator (the live-mutex single-broker model) gives
/// exactly that. KV/rate-limit/etc. stay sharded by their own key; service
/// discovery has its own coordinator because listing service names is global.
/// Sharding the lock space across coordinators (cross-shard 2PC for sets that
/// span them) is the documented scaling path.
///
/// Defined in the shared [`fiducia_routing`] crate so the node, the load
/// balancer, and the brain route locks to the **same** coordinator shard.
pub const LOCK_DOMAIN: &str = fiducia_routing::LOCK_COORDINATION_KEY;

/// Routing key under which all service-discovery state lives.
///
/// This keeps `GET /v1/services` linearizable without asking every shard leader
/// for a partial service-name list. Individual service lookups still return one
/// service's instances, but all discovery mutations and reads meet in the same
/// replicated state machine.
pub const SERVICE_DOMAIN: &str = fiducia_routing::SERVICE_DISCOVERY_KEY;

impl Command {
    /// Key used to route this command to its owning shard.
    pub fn routing_key(&self) -> &str {
        match self {
            Command::ActivateCommandProtocol { .. } => COMMAND_PROTOCOL_ROUTING_KEY,
            Command::LockAcquire { .. }
            | Command::LockAcquireV2 { .. }
            | Command::LockAcquireAttempt { .. }
            | Command::LockRenew { .. }
            | Command::LockRelease { .. }
            | Command::LockCancel { .. }
            | Command::LockCancelAttempt { .. }
            | Command::SemaphoreAcquire { .. }
            | Command::SemaphoreAcquireV2 { .. }
            | Command::SemaphoreAcquireAttempt { .. }
            | Command::SemaphoreRenew { .. }
            | Command::SemaphoreRelease { .. }
            | Command::SemaphoreCancel { .. }
            | Command::SemaphoreCancelAttempt { .. } => LOCK_DOMAIN,
            Command::KvPut { key, .. }
            | Command::KvDelete { key }
            | Command::CounterAdd { key, .. }
            | Command::CounterSet { key, .. }
            | Command::RateLimitCheck { key, .. }
            | Command::IdempotencyClaim { key, .. }
            | Command::IdempotencyComplete { key, .. }
            | Command::IdempotencyRenew { key, .. }
            | Command::IdempotencyAbandon { key, .. } => key,
            Command::BarrierCreate { name, .. } | Command::BarrierArrive { name, .. } => name,
            Command::TaskCreate { name, .. }
            | Command::TaskClaim { name, .. }
            | Command::TaskProgress { name, .. }
            | Command::TaskComplete { name, .. }
            | Command::TaskFail { name, .. }
            | Command::TaskCancel { name } => name,
            Command::EffectPrepare { name, .. }
            | Command::EffectApprove { name, .. }
            | Command::EffectCommit { name, .. }
            | Command::EffectAbort { name } => name,
            Command::HandoffOffer { name, .. }
            | Command::HandoffAccept { name, .. }
            | Command::HandoffReject { name, .. } => name,
            Command::DecisionPropose { name, .. } | Command::DecisionVote { name, .. } => name,
            Command::BudgetSet { name, .. }
            | Command::BudgetReserve { name, .. }
            | Command::BudgetCommit { name, .. }
            | Command::BudgetRelease { name, .. } => name,
            Command::ClaimAssert { name, .. }
            | Command::ClaimSupport { name, .. }
            | Command::ClaimContest { name, .. }
            | Command::ClaimResolve { name, .. }
            | Command::ClaimSupersede { name, .. } => name,
            Command::ScheduleUpsert { name, .. }
            | Command::ScheduleRecordRun { name, .. }
            | Command::ScheduleClaimFire { name, .. }
            | Command::ScheduleRecordResult { name, .. } => name,
            Command::ElectionCampaign { name, .. }
            | Command::ElectionRenew { name, .. }
            | Command::ElectionResign { name, .. } => name,
            Command::ServiceRegister { .. }
            | Command::ServiceHeartbeat { .. }
            | Command::ServiceDeregister { .. } => SERVICE_DOMAIN,
        }
    }

    /// Short, stable label for this command's operation kind — used as the `op`
    /// attribute on telemetry spans/events so traces group by primitive.
    pub fn kind(&self) -> &'static str {
        match self {
            Command::ActivateCommandProtocol { .. } => "raft.command_protocol.activate",
            Command::KvPut { .. } => "kv.put",
            Command::KvDelete { .. } => "kv.delete",
            Command::CounterAdd { .. } => "counter.add",
            Command::CounterSet { .. } => "counter.set",
            Command::BarrierCreate { .. } => "barrier.create",
            Command::BarrierArrive { .. } => "barrier.arrive",
            Command::TaskCreate { .. } => "task.create",
            Command::TaskClaim { .. } => "task.claim",
            Command::TaskProgress { .. } => "task.progress",
            Command::TaskComplete { .. } => "task.complete",
            Command::TaskFail { .. } => "task.fail",
            Command::TaskCancel { .. } => "task.cancel",
            Command::EffectPrepare { .. } => "effect.prepare",
            Command::EffectApprove { .. } => "effect.approve",
            Command::EffectCommit { .. } => "effect.commit",
            Command::EffectAbort { .. } => "effect.abort",
            Command::HandoffOffer { .. } => "handoff.offer",
            Command::HandoffAccept { .. } => "handoff.accept",
            Command::HandoffReject { .. } => "handoff.reject",
            Command::DecisionPropose { .. } => "decision.propose",
            Command::DecisionVote { .. } => "decision.vote",
            Command::BudgetSet { .. } => "budget.set",
            Command::BudgetReserve { .. } => "budget.reserve",
            Command::BudgetCommit { .. } => "budget.commit",
            Command::BudgetRelease { .. } => "budget.release",
            Command::ClaimAssert { .. } => "claim.assert",
            Command::ClaimSupport { .. } => "claim.support",
            Command::ClaimContest { .. } => "claim.contest",
            Command::ClaimResolve { .. } => "claim.resolve",
            Command::ClaimSupersede { .. } => "claim.supersede",
            Command::LockAcquire { .. }
            | Command::LockAcquireV2 { .. }
            | Command::LockAcquireAttempt { .. } => "lock.acquire",
            Command::LockRenew { .. } => "lock.renew",
            Command::LockRelease { .. } => "lock.release",
            Command::LockCancel { .. } | Command::LockCancelAttempt { .. } => "lock.cancel",
            Command::SemaphoreAcquire { .. }
            | Command::SemaphoreAcquireV2 { .. }
            | Command::SemaphoreAcquireAttempt { .. } => "semaphore.acquire",
            Command::SemaphoreRenew { .. } => "semaphore.renew",
            Command::SemaphoreRelease { .. } => "semaphore.release",
            Command::SemaphoreCancel { .. } | Command::SemaphoreCancelAttempt { .. } => {
                "semaphore.cancel"
            }
            Command::RateLimitCheck { .. } => "ratelimit.check",
            Command::IdempotencyClaim { .. } => "idempotency.claim",
            Command::IdempotencyComplete { .. } => "idempotency.complete",
            Command::IdempotencyRenew { .. } => "idempotency.renew",
            Command::IdempotencyAbandon { .. } => "idempotency.abandon",
            Command::ScheduleUpsert { .. } => "schedule.upsert",
            Command::ScheduleRecordRun { .. } => "schedule.record_run",
            Command::ScheduleClaimFire { .. } => "schedule.claim_fire",
            Command::ScheduleRecordResult { .. } => "schedule.record_result",
            Command::ElectionCampaign { .. } => "election.campaign",
            Command::ElectionRenew { .. } => "election.renew",
            Command::ElectionResign { .. } => "election.resign",
            Command::ServiceRegister { .. } => "service.register",
            Command::ServiceHeartbeat { .. } => "service.heartbeat",
            Command::ServiceDeregister { .. } => "service.deregister",
        }
    }

    /// Convert availability-compatible V2 acquire requests to their historical
    /// command shapes while a mixed-version cluster is still in phase one.
    /// Operations with no safe legacy expression stay unavailable until the
    /// replicated protocol activation commits.
    pub(crate) fn for_active_protocol(self, active: u16) -> Result<Self, u16> {
        if active >= CURRENT_COMMAND_PROTOCOL {
            return Ok(self);
        }
        match self {
            Command::LockAcquireV2 {
                keys,
                holder,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => Ok(Command::LockAcquire {
                keys,
                holder,
                ttl_ms,
                wait,
                wait_timeout_ms,
            }),
            Command::SemaphoreAcquireV2 {
                key,
                holder,
                limit,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => Ok(Command::SemaphoreAcquire {
                key,
                holder,
                limit,
                ttl_ms,
                wait,
                wait_timeout_ms,
            }),
            Command::LockAcquireAttempt { .. }
            | Command::LockRenew { .. }
            | Command::LockCancel { .. }
            | Command::LockCancelAttempt { .. }
            | Command::SemaphoreAcquireAttempt { .. }
            | Command::SemaphoreRenew { .. }
            | Command::SemaphoreCancel { .. }
            | Command::SemaphoreCancelAttempt { .. }
            | Command::ActivateCommandProtocol { .. } => Err(CURRENT_COMMAND_PROTOCOL),
            command => Ok(command),
        }
    }

    pub(crate) fn requires_current_protocol(&self) -> bool {
        match self {
            Command::ActivateCommandProtocol { version } => *version == CURRENT_COMMAND_PROTOCOL,
            Command::LockAcquireV2 { .. }
            | Command::LockAcquireAttempt { .. }
            | Command::LockRenew { .. }
            | Command::LockCancel { .. }
            | Command::LockCancelAttempt { .. }
            | Command::SemaphoreAcquireV2 { .. }
            | Command::SemaphoreAcquireAttempt { .. }
            | Command::SemaphoreRenew { .. }
            | Command::SemaphoreCancel { .. }
            | Command::SemaphoreCancelAttempt { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitAlgorithm {
    TokenBucket,
    SlidingWindow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySemantics {
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleTarget {
    Webhook { url: String },
    Queue { name: String },
    Grpc { endpoint: String },
}

/// Result produced by applying a committed command.
#[derive(Debug, Clone, Serialize)]
pub struct ApplyResult {
    pub revision: u64,
    pub output: Value,
}

/// A single versioned KV entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvEntry {
    pub value: String,
    pub mod_revision: u64,
    pub expires_at_ms: Option<u64>,
}

/// A distributed counter: a signed 64-bit value with a monotonic revision, used
/// for success/failure thresholds, quotas, and fan-in tallies. Every mutation
/// stamps `mod_revision`, so callers can compare-and-set against a known value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterEntry {
    pub value: i64,
    pub mod_revision: u64,
}

/// How a barrier decides it has resolved from its participants' arrivals.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BarrierPolicy {
    /// Every one of `expected` participants must arrive.
    All,
    /// Any `required` distinct participants suffice.
    Quorum { required: u32 },
    /// The first successful (non-veto) arrival resolves it.
    FirstSuccess,
    /// Every `expected` participant must arrive, but any veto aborts it.
    AnyVeto,
    /// Resolves at the deadline using whoever arrived (needs `deadline_ms`).
    BestByDeadline,
    /// Arrivals carry weights; resolves when their sum reaches `required_weight`.
    WeightedQuorum { required_weight: u64 },
}

/// The lifecycle state of a barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BarrierStatus {
    Pending,
    Satisfied,
    Vetoed,
    TimedOut,
}

impl BarrierStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(self, BarrierStatus::Pending)
    }
}

/// One participant's arrival at a barrier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarrierArrival {
    pub participant: String,
    pub weight: u64,
    pub veto: bool,
    pub arrived_ms: u64,
}

/// Read view of a barrier: its policy, who has arrived, and whether it resolved.
#[derive(Debug, Clone, Serialize)]
pub struct BarrierState {
    pub name: String,
    pub policy: BarrierPolicy,
    pub expected: u32,
    pub deadline_ms: Option<u64>,
    pub status: BarrierStatus,
    pub arrivals: Vec<BarrierArrival>,
    /// Distinct non-veto arrivals and their summed weight (the satisfaction tally).
    pub arrived_count: u32,
    pub arrived_weight: u64,
}

/// Internal barrier record: the durable facts (arrivals); status is derived.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BarrierRecord {
    policy: BarrierPolicy,
    expected: u32,
    deadline_ms: Option<u64>,
    /// participant → arrival (a repeat arrival updates in place, so it is idempotent).
    arrivals: HashMap<String, BarrierArrival>,
}

impl BarrierRecord {
    /// Derive the barrier's status at time `now` from its arrivals. Monotonic:
    /// once satisfied it stays satisfied as more participants arrive; a veto
    /// under `AnyVeto` aborts; a passed deadline times out an unmet barrier.
    fn status(&self, now: u64) -> BarrierStatus {
        if matches!(self.policy, BarrierPolicy::AnyVeto)
            && self.arrivals.values().any(|arrival| arrival.veto)
        {
            return BarrierStatus::Vetoed;
        }
        let count = self.arrivals.values().filter(|a| !a.veto).count() as u64;
        // Weights are caller-supplied, so the tally saturates rather than
        // wrapping: in release a wrap would silently *unsatisfy* a met barrier
        // (and in debug it would panic inside `apply`, on every replica).
        let weight: u64 = self
            .arrivals
            .values()
            .filter(|a| !a.veto)
            .fold(0u64, |acc, a| acc.saturating_add(a.weight));
        let deadline_passed = self.deadline_ms.is_some_and(|deadline| now >= deadline);

        let met = match &self.policy {
            BarrierPolicy::All | BarrierPolicy::AnyVeto => count >= self.expected as u64,
            BarrierPolicy::Quorum { required } => count >= *required as u64,
            BarrierPolicy::FirstSuccess => count >= 1,
            BarrierPolicy::WeightedQuorum { required_weight } => weight >= *required_weight,
            BarrierPolicy::BestByDeadline => deadline_passed && count >= 1,
        };
        if met {
            BarrierStatus::Satisfied
        } else if deadline_passed {
            BarrierStatus::TimedOut
        } else {
            BarrierStatus::Pending
        }
    }

    fn view(&self, name: &str, now: u64) -> BarrierState {
        let mut arrivals: Vec<BarrierArrival> = self.arrivals.values().cloned().collect();
        arrivals.sort_by(|a, b| {
            a.arrived_ms
                .cmp(&b.arrived_ms)
                .then(a.participant.cmp(&b.participant))
        });
        let arrived_count = arrivals.iter().filter(|a| !a.veto).count() as u32;
        let arrived_weight = arrivals.iter().filter(|a| !a.veto).map(|a| a.weight).sum();
        BarrierState {
            name: name.to_string(),
            policy: self.policy.clone(),
            expected: self.expected,
            deadline_ms: self.deadline_ms,
            status: self.status(now),
            arrivals,
            arrived_count,
            arrived_weight,
        }
    }
}

/// Read view of one lock **member key**: who holds it, the whole set held with it
/// (the acquired union), and who is queued behind it.
#[derive(Debug, Clone, Serialize)]
pub struct LockState {
    pub key: String,
    pub holder: Option<String>,
    pub fencing_token: Option<u64>,
    pub lease_expires_ms: Option<u64>,
    /// Every member key held together by the current holder (the union grant).
    pub held_keys: Vec<String>,
    /// Holders queued on a set that includes this key, in FIFO order.
    pub wait_queue: Vec<LockWaiter>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LockWaiter {
    pub holder: String,
    /// The full key set this waiter is trying to acquire.
    pub keys: Vec<String>,
    pub requested_ms: u64,
    /// Absolute replicated deadline after which this queue entry is no longer
    /// eligible for promotion. `None` only represents a legacy snapshot entry.
    pub wait_expires_ms: Option<u64>,
}

/// One held union-lock grant, as surfaced by the observability inventory: who
/// holds which key set, under what fencing token, until when.
#[derive(Debug, Clone, Serialize)]
pub struct LockHolding {
    pub holder: String,
    pub keys: Vec<String>,
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
}

/// A whole-coordinator view of the lock primitive: every active grant plus the
/// single FIFO wait queue behind them. Built for `/v1/observe/locks`.
#[derive(Debug, Clone, Serialize)]
pub struct LockInventory {
    pub held: Vec<LockHolding>,
    pub wait_queue: Vec<LockWaiter>,
}

/// One held union-lock acquisition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockGrant {
    holder: String,
    keys: Vec<String>,
    fencing_token: u64,
    lease_expires_ms: u64,
    #[serde(default)]
    request_id: Option<String>,
}

/// One queued union-lock request awaiting its whole key set.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedLock {
    holder: String,
    keys: Vec<String>,
    ttl_ms: u64,
    requested_ms: u64,
    /// Absent in snapshots written before queue deadlines were introduced.
    #[serde(default)]
    wait_expires_ms: Option<u64>,
    #[serde(default)]
    request_id: Option<String>,
}

/// The multi-key lock table: which member key is held by which grant, the grants
/// themselves, and the FIFO wait queue of whole requests.
#[derive(Default, Serialize, Deserialize)]
struct LockManager {
    /// member key → owning grant's fencing token.
    held: HashMap<String, u64>,
    /// fencing token → grant.
    grants: HashMap<u64, LockGrant>,
    /// FIFO queue of requests waiting for their full union to be free. Indexed by
    /// `(holder, key-set)` so an "already queued?" check and a cancel/lease-expiry
    /// removal from the middle of the queue are O(1) (see [`crate::indexed_queue`]).
    queue: IndexedQueue<(String, Vec<String>), QueuedLock>,
}

/// Read view of a counting semaphore.
#[derive(Debug, Clone, Serialize)]
pub struct SemaphoreState {
    pub key: String,
    pub limit: u32,
    pub holders: Vec<SemaphoreHolder>,
    /// Free permits right now (`limit - holders`, floored at 0).
    pub available: u32,
    pub wait_queue: Vec<LockWaiter>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemaphoreHolder {
    pub holder: String,
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemaphoreSlot {
    holder: String,
    fencing_token: u64,
    lease_expires_ms: u64,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedPermit {
    holder: String,
    ttl_ms: u64,
    requested_ms: u64,
    /// Absent in snapshots written before queue deadlines were introduced.
    #[serde(default)]
    wait_expires_ms: Option<u64>,
    #[serde(default)]
    request_id: Option<String>,
}

/// A counting semaphore: up to `limit` permits, plus a FIFO queue for the rest.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Semaphore {
    limit: u32,
    holders: Vec<SemaphoreSlot>,
    /// FIFO queue of waiters, indexed by holder for O(1) dedup and removal.
    queue: IndexedQueue<String, QueuedPermit>,
}

/// Distributed rate-limit snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct RateLimitSnapshot {
    pub key: String,
    pub tenant: String,
    pub algorithm: RateLimitAlgorithm,
    pub allowed: bool,
    pub remaining: u32,
    pub reset_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RateLimitRecord {
    algorithm: RateLimitAlgorithm,
    limit: u32,
    window_ms: u64,
    tokens: f64,
    updated_ms: u64,
    events: VecDeque<u64>,
    last_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyStatus {
    Claimed,
    Completed,
}

/// One active idempotency record. While `Claimed` it lives only until the short
/// in-flight `lease_expires_ms` (so an abandoned claim frees the key quickly);
/// `complete` then extends `lease_expires_ms` by `retention_ms` so a `Completed`
/// record — and its replayable result — survives for the full retention window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub key: String,
    pub owner: String,
    pub fencing_token: u64,
    pub status: IdempotencyStatus,
    pub first_seen_ms: u64,
    pub lease_expires_ms: u64,
    /// How long the record lives *after* completion (ms). Captured at claim so
    /// `complete` can extend the lease from the in-flight window to the full
    /// retention window without the state machine reading the wall clock.
    pub retention_ms: u64,
    pub metadata: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}

/// The current holder of a named election.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leadership {
    pub leader: String,
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
    /// Campaign TTL in ms, retained so a renew without an explicit TTL reuses it.
    pub ttl_ms: u64,
    /// Candidate facts (address/region/version/…) — lets observers discover the
    /// current leader's endpoint, not just its id.
    pub metadata: HashMap<String, String>,
}

/// One named election and its current leadership — a row of the election
/// inventory surfaced by `/v1/observe/elections`.
#[derive(Debug, Clone, Serialize)]
pub struct ElectionEntry {
    pub name: String,
    #[serde(flatten)]
    pub leadership: Leadership,
}

/// A KV entry paired with its key — one row of a prefix listing.
#[derive(Debug, Clone, Serialize)]
pub struct KvListItem {
    pub key: String,
    #[serde(flatten)]
    pub entry: KvEntry,
}

/// One service in a discovery listing: its name and how many live instances it has.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceSummary {
    pub service: String,
    pub instances: usize,
}

/// A scheduled job definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub name: String,
    pub cron: Option<String>,
    pub one_shot_at_ms: Option<u64>,
    pub target: ScheduleTarget,
    pub delivery: DeliverySemantics,
    pub max_retries: u32,
    pub enabled: bool,
    /// The next time (epoch ms) this schedule should fire — the durable cursor the
    /// firing loop reads and the state machine advances on each claim. `None` when
    /// the schedule is exhausted (a delivered one-shot, or a cron that won't fire).
    pub next_fire_ms: Option<u64>,
}

/// Lifecycle of one fire's delivery, recorded durably in the schedule's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Claimed and being delivered (or interrupted before the result was recorded).
    Pending,
    /// Delivered successfully.
    Delivered,
    /// Gave up after exhausting retries.
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRun {
    pub fire_id: String,
    pub fired_at_ms: u64,
    pub attempts: u32,
    pub duplicate: bool,
    pub target: ScheduleTarget,
    pub status: RunStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleRecord {
    definition: Schedule,
    history: Vec<ScheduleRun>,
}

/// One registered service instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    pub instance_id: String,
    pub address: String,
    pub lease_expires_ms: u64,
    pub metadata: HashMap<String, String>,
}

/// The lifecycle of a durable task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

/// Read view of a durable task: a claimable unit of work whose exclusive owner
/// holds a fencing token. Progress/complete/fail must present that token, so a
/// stale worker that lost the claim cannot commit newer state.
#[derive(Debug, Clone, Serialize)]
pub struct TaskState {
    pub name: String,
    pub task_type: String,
    pub payload: Value,
    pub status: TaskStatus,
    pub owner: Option<String>,
    pub fencing_token: u64,
    pub progress: u32,
    pub checkpoint: Value,
    pub result: Option<Value>,
    pub lease_expires_ms: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRecord {
    task_type: String,
    payload: Value,
    status: TaskStatus,
    owner: Option<String>,
    fencing_token: u64,
    lease_ttl_ms: u64,
    lease_expires_ms: Option<u64>,
    progress: u32,
    checkpoint: Value,
    result: Option<Value>,
    deadline_ms: Option<u64>,
    generation: u64,
}

impl TaskRecord {
    /// A task is claimable when it is not terminal and either unowned/pending or
    /// its owner's lease has expired (so abandoned work is reassigned).
    fn claimable(&self, now: u64) -> bool {
        !self.status.is_terminal()
            && (self.status == TaskStatus::Pending
                || self.lease_expires_ms.is_none_or(|expires| expires <= now))
    }

    /// True when `worker` holds the current, unexpired claim under `token`.
    fn owned_now(&self, now: u64, worker: &str, token: u64) -> bool {
        !self.status.is_terminal()
            && self.owner.as_deref() == Some(worker)
            && self.fencing_token == token
            && self.lease_expires_ms.is_some_and(|expires| expires > now)
    }

    fn view(&self, name: &str) -> TaskState {
        TaskState {
            name: name.to_string(),
            task_type: self.task_type.clone(),
            payload: self.payload.clone(),
            status: self.status,
            owner: self.owner.clone(),
            fencing_token: self.fencing_token,
            progress: self.progress,
            checkpoint: self.checkpoint.clone(),
            result: self.result.clone(),
            lease_expires_ms: self.lease_expires_ms,
            deadline_ms: self.deadline_ms,
            generation: self.generation,
        }
    }
}

/// The lifecycle of an approval-escrow effect. Preparing, authorizing, and
/// executing a dangerous action are separated so a risky side effect cannot be
/// performed until it is approved, and is committed at most once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Prepared,
    Approved,
    Committed,
    Aborted,
}

impl EffectStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, EffectStatus::Committed | EffectStatus::Aborted)
    }
}

/// Read view of a prepared effect.
#[derive(Debug, Clone, Serialize)]
pub struct EffectState {
    pub name: String,
    pub effect_type: String,
    pub payload: Value,
    pub risk: String,
    pub idempotency_key: String,
    pub status: EffectStatus,
    pub required_approvals: u32,
    pub approvals: Vec<String>,
    pub result: Option<Value>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EffectRecord {
    effect_type: String,
    payload: Value,
    risk: String,
    idempotency_key: String,
    status: EffectStatus,
    required_approvals: u32,
    approvals: std::collections::BTreeSet<String>,
    result: Option<Value>,
    generation: u64,
}

impl EffectRecord {
    fn view(&self, name: &str) -> EffectState {
        EffectState {
            name: name.to_string(),
            effect_type: self.effect_type.clone(),
            payload: self.payload.clone(),
            risk: self.risk.clone(),
            idempotency_key: self.idempotency_key.clone(),
            status: self.status,
            required_approvals: self.required_approvals,
            approvals: self.approvals.iter().cloned().collect(),
            result: self.result.clone(),
            generation: self.generation,
        }
    }
}

/// The lifecycle of an ownership handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffStatus {
    Offered,
    Accepted,
    Rejected,
    Expired,
}

/// Read view of an atomic ownership handoff. While `Offered`, the original owner
/// (`from`, holding `from_token`) still owns the resource; on `Accepted`, `to`
/// receives a strictly higher `to_token`, so the resource's fencing rejects the
/// old owner — there is never a moment when both or neither own it.
#[derive(Debug, Clone, Serialize)]
pub struct HandoffState {
    pub name: String,
    pub resource: String,
    pub from: String,
    pub to: String,
    pub from_token: u64,
    pub to_token: Option<u64>,
    pub status: HandoffStatus,
    pub context: Value,
    pub expires_ms: Option<u64>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HandoffRecord {
    resource: String,
    from: String,
    to: String,
    from_token: u64,
    to_token: Option<u64>,
    status: HandoffStatus,
    context: Value,
    expires_ms: Option<u64>,
    generation: u64,
}

impl HandoffRecord {
    /// Lazily expire an offer whose deadline has passed.
    fn effective_status(&self, now: u64) -> HandoffStatus {
        if self.status == HandoffStatus::Offered && self.expires_ms.is_some_and(|e| now >= e) {
            HandoffStatus::Expired
        } else {
            self.status
        }
    }

    fn view(&self, name: &str, now: u64) -> HandoffState {
        HandoffState {
            name: name.to_string(),
            resource: self.resource.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            from_token: self.from_token,
            to_token: self.to_token,
            status: self.effective_status(now),
            context: self.context.clone(),
            expires_ms: self.expires_ms,
            generation: self.generation,
        }
    }
}

/// How a decision resolves from its weighted votes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecisionPolicy {
    /// Once `min_votes` are cast, the option with the most total weight wins.
    Plurality { min_votes: u32 },
    /// The first option whose summed weight reaches `required_weight` wins.
    Threshold { required_weight: u64 },
    /// Once `min_votes` are cast, resolves only if all voters chose one option.
    Unanimous { min_votes: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionStatus {
    Open,
    Resolved,
    Vetoed,
    TimedOut,
}

/// A per-option weight tally.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionTally {
    pub option: String,
    pub weight: u64,
}

/// One agent's vote in a decision.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionVoteView {
    pub voter: String,
    /// The chosen option, or `None` to abstain.
    pub option: Option<String>,
    pub confidence: f32,
    pub weight: u64,
    pub veto: bool,
    pub evidence: Vec<String>,
}

/// Read view of a decision, with its outcome derived at read time.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionState {
    pub name: String,
    pub question: String,
    pub options: Vec<String>,
    pub policy: DecisionPolicy,
    pub status: DecisionStatus,
    pub winner: Option<String>,
    /// Per-option summed weight, sorted by option for determinism.
    pub tallies: Vec<DecisionTally>,
    pub votes: Vec<DecisionVoteView>,
    pub deadline_ms: Option<u64>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionVoteRecord {
    option: Option<String>,
    confidence: f32,
    weight: u64,
    veto: bool,
    evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionRecord {
    question: String,
    options: Vec<String>,
    policy: DecisionPolicy,
    votes: std::collections::BTreeMap<String, DecisionVoteRecord>,
    deadline_ms: Option<u64>,
    generation: u64,
}

impl DecisionRecord {
    /// Per-option weight from non-veto, non-abstain votes (sorted by option).
    fn tallies(&self) -> Vec<(String, u64)> {
        let mut sums: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for vote in self.votes.values() {
            if vote.veto {
                continue;
            }
            if let Some(option) = &vote.option {
                // Saturating: vote weights are caller-supplied, and a wrapped
                // tally would hand the decision to the wrong option.
                let sum = sums.entry(option.clone()).or_default();
                *sum = sum.saturating_add(vote.weight);
            }
        }
        sums.into_iter().collect()
    }

    /// Resolve to `(status, winner)`. A veto aborts; otherwise the policy decides,
    /// with ties broken by the lexicographically smallest option. A passed
    /// deadline forces a plurality outcome (or `TimedOut` with no usable votes).
    fn resolve(&self, now: u64) -> (DecisionStatus, Option<String>) {
        if self.votes.values().any(|vote| vote.veto) {
            return (DecisionStatus::Vetoed, None);
        }
        let tallies = self.tallies();
        let cast = self
            .votes
            .values()
            .filter(|v| !v.veto && v.option.is_some())
            .count() as u32;
        let deadline_passed = self.deadline_ms.is_some_and(|deadline| now >= deadline);
        // Highest-weight option, ties → smallest option name (tallies are sorted).
        let top = tallies
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
            .cloned();

        match &self.policy {
            DecisionPolicy::Plurality { min_votes } => {
                if cast >= *min_votes || (deadline_passed && cast > 0) {
                    match top {
                        Some((option, _)) => (DecisionStatus::Resolved, Some(option)),
                        None => (DecisionStatus::Open, None),
                    }
                } else if deadline_passed {
                    (DecisionStatus::TimedOut, None)
                } else {
                    (DecisionStatus::Open, None)
                }
            }
            DecisionPolicy::Threshold { required_weight } => {
                let met = tallies
                    .iter()
                    .find(|(_, weight)| *weight >= *required_weight);
                if let Some((option, _)) = met {
                    (DecisionStatus::Resolved, Some(option.clone()))
                } else if deadline_passed {
                    (DecisionStatus::TimedOut, None)
                } else {
                    (DecisionStatus::Open, None)
                }
            }
            DecisionPolicy::Unanimous { min_votes } => {
                let unanimous = tallies.len() == 1;
                if cast >= *min_votes && unanimous {
                    (DecisionStatus::Resolved, top.map(|(option, _)| option))
                } else if deadline_passed {
                    (DecisionStatus::TimedOut, None)
                } else {
                    (DecisionStatus::Open, None)
                }
            }
        }
    }

    fn view(&self, name: &str, now: u64) -> DecisionState {
        let (status, winner) = self.resolve(now);
        let mut votes: Vec<DecisionVoteView> = self
            .votes
            .iter()
            .map(|(voter, vote)| DecisionVoteView {
                voter: voter.clone(),
                option: vote.option.clone(),
                confidence: vote.confidence,
                weight: vote.weight,
                veto: vote.veto,
                evidence: vote.evidence.clone(),
            })
            .collect();
        votes.sort_by(|a, b| a.voter.cmp(&b.voter));
        DecisionState {
            name: name.to_string(),
            question: self.question.clone(),
            options: self.options.clone(),
            policy: self.policy.clone(),
            status,
            winner,
            tallies: self
                .tallies()
                .into_iter()
                .map(|(option, weight)| DecisionTally { option, weight })
                .collect(),
            votes,
            deadline_ms: self.deadline_ms,
            generation: self.generation,
        }
    }
}

/// The status of a single budget reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationStatus {
    Held,
    Committed,
    Released,
}

/// A three-dimensional budget limit. `None` means unlimited on that axis.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BudgetAmount {
    #[serde(default)]
    pub usd_micros: Option<u64>,
    #[serde(default)]
    pub tokens: Option<u64>,
    #[serde(default)]
    pub tool_calls: Option<u64>,
}

impl BudgetAmount {
    fn zero() -> Self {
        BudgetAmount {
            usd_micros: Some(0),
            tokens: Some(0),
            tool_calls: Some(0),
        }
    }
}

/// Read view of one reservation against a budget.
#[derive(Debug, Clone, Serialize)]
pub struct ReservationView {
    pub id: String,
    pub holder: String,
    pub reserved: BudgetAmount,
    pub spent: BudgetAmount,
    pub status: ReservationStatus,
}

/// Read view of a budget: its ceiling, what is reserved and spent, and the
/// remaining headroom on each axis.
#[derive(Debug, Clone, Serialize)]
pub struct BudgetState {
    pub name: String,
    pub limit: BudgetAmount,
    pub reserved: BudgetAmount,
    pub spent: BudgetAmount,
    pub available: BudgetAmount,
    pub reservations: Vec<ReservationView>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReservationRecord {
    holder: String,
    reserved: BudgetAmount,
    spent: BudgetAmount,
    status: ReservationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BudgetRecord {
    limit: BudgetAmount,
    reservations: std::collections::BTreeMap<String, ReservationRecord>,
    generation: u64,
}

impl ReservationRecord {
    /// What this reservation currently consumes on `axis`: the full reservation
    /// while `Held`, the actual spend once `Committed`, and nothing when released.
    fn consumed_on(&self, axis: impl Fn(&BudgetAmount) -> Option<u64>) -> u64 {
        match self.status {
            ReservationStatus::Held => axis(&self.reserved).unwrap_or(0),
            ReservationStatus::Committed => axis(&self.spent).unwrap_or(0),
            ReservationStatus::Released => 0,
        }
    }
}

impl BudgetRecord {
    /// Budget consumed right now: held reservations block their full amount;
    /// committed ones consume only what was actually spent; released free it.
    fn consumed(&self) -> BudgetAmount {
        // Saturating: reservation amounts are caller-supplied, and a wrapped sum
        // would report a full budget as empty (or vice versa).
        let sum = |axis: fn(&BudgetAmount) -> Option<u64>| -> u64 {
            self.reservations
                .values()
                .fold(0u64, |acc, r| acc.saturating_add(r.consumed_on(axis)))
        };
        BudgetAmount {
            usd_micros: Some(sum(|b| b.usd_micros)),
            tokens: Some(sum(|b| b.tokens)),
            tool_calls: Some(sum(|b| b.tool_calls)),
        }
    }

    /// Actual committed spend (excludes still-held reservations).
    fn spent(&self) -> BudgetAmount {
        let sum = |axis: fn(&BudgetAmount) -> Option<u64>| -> u64 {
            self.reservations
                .values()
                .filter(|r| r.status == ReservationStatus::Committed)
                .fold(0u64, |acc, r| {
                    acc.saturating_add(axis(&r.spent).unwrap_or(0))
                })
        };
        BudgetAmount {
            usd_micros: Some(sum(|b| b.usd_micros)),
            tokens: Some(sum(|b| b.tokens)),
            tool_calls: Some(sum(|b| b.tool_calls)),
        }
    }

    /// Would reserving `amount` keep every limited axis within the ceiling?
    fn fits(&self, amount: &BudgetAmount) -> bool {
        let consumed = self.consumed();
        let axis_ok = |limit: Option<u64>, current: Option<u64>, add: Option<u64>| match limit {
            None => true, // unlimited axis
            Some(cap) => current.unwrap_or(0).saturating_add(add.unwrap_or(0)) <= cap,
        };
        axis_ok(
            self.limit.usd_micros,
            consumed.usd_micros,
            amount.usd_micros,
        ) && axis_ok(self.limit.tokens, consumed.tokens, amount.tokens)
            && axis_ok(
                self.limit.tool_calls,
                consumed.tool_calls,
                amount.tool_calls,
            )
    }

    fn available(&self) -> BudgetAmount {
        let consumed = self.consumed();
        let axis = |limit: Option<u64>, used: Option<u64>| {
            limit.map(|cap| cap.saturating_sub(used.unwrap_or(0)))
        };
        BudgetAmount {
            usd_micros: axis(self.limit.usd_micros, consumed.usd_micros),
            tokens: axis(self.limit.tokens, consumed.tokens),
            tool_calls: axis(self.limit.tool_calls, consumed.tool_calls),
        }
    }

    fn view(&self, name: &str) -> BudgetState {
        let mut reservations: Vec<ReservationView> = self
            .reservations
            .iter()
            .map(|(id, r)| ReservationView {
                id: id.clone(),
                holder: r.holder.clone(),
                reserved: r.reserved,
                spent: r.spent,
                status: r.status,
            })
            .collect();
        reservations.sort_by(|a, b| a.id.cmp(&b.id));
        BudgetState {
            name: name.to_string(),
            limit: self.limit,
            reserved: self.consumed(),
            spent: self.spent(),
            available: self.available(),
            reservations,
            generation: self.generation,
        }
    }
}

/// The lifecycle of a claim in the contestable ledger. Semantic similarity may
/// surface a claim, but only an authorized resolution moves it to `Accepted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Asserted,
    Contested,
    Accepted,
    Rejected,
    Superseded,
}

impl ClaimStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ClaimStatus::Accepted | ClaimStatus::Rejected | ClaimStatus::Superseded
        )
    }
}

/// One agent's contest of a claim.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimContest {
    pub agent: String,
    pub reason: String,
}

/// Read view of a versioned, contestable claim.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimState {
    pub name: String,
    pub subject: String,
    pub predicate: String,
    pub value: Value,
    pub confidence: f32,
    pub author: String,
    pub status: ClaimStatus,
    pub supporters: Vec<String>,
    pub contests: Vec<ClaimContest>,
    pub evidence: Vec<String>,
    pub valid_until_ms: Option<u64>,
    pub superseded_by: Option<String>,
    /// Bumped every time the asserted value changes.
    pub version: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClaimRecord {
    subject: String,
    predicate: String,
    value: Value,
    confidence: f32,
    author: String,
    status: ClaimStatus,
    supporters: std::collections::BTreeSet<String>,
    contests: std::collections::BTreeMap<String, String>,
    evidence: Vec<String>,
    valid_until_ms: Option<u64>,
    superseded_by: Option<String>,
    version: u64,
    generation: u64,
}

impl ClaimRecord {
    fn view(&self, name: &str) -> ClaimState {
        ClaimState {
            name: name.to_string(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            value: self.value.clone(),
            confidence: self.confidence,
            author: self.author.clone(),
            status: self.status,
            supporters: self.supporters.iter().cloned().collect(),
            contests: self
                .contests
                .iter()
                .map(|(agent, reason)| ClaimContest {
                    agent: agent.clone(),
                    reason: reason.clone(),
                })
                .collect(),
            evidence: self.evidence.clone(),
            valid_until_ms: self.valid_until_ms,
            superseded_by: self.superseded_by.clone(),
            version: self.version,
            generation: self.generation,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Store {
    command_protocol: u16,
    revision: u64,
    next_fencing_token: u64,
    kv: HashMap<String, KvEntry>,
    counters: HashMap<String, CounterEntry>,
    barriers: HashMap<String, BarrierRecord>,
    tasks: HashMap<String, TaskRecord>,
    effects: HashMap<String, EffectRecord>,
    handoffs: HashMap<String, HandoffRecord>,
    decisions: HashMap<String, DecisionRecord>,
    budgets: HashMap<String, BudgetRecord>,
    claims: HashMap<String, ClaimRecord>,
    locks: LockManager,
    semaphores: HashMap<String, Semaphore>,
    /// Recently cancelled acquisition identities. Fixed-size digests close the
    /// ordering race where cancel commits before an ambiguous acquire reaches
    /// the Raft log without retaining attacker-controlled keys in memory.
    lock_cancellations: CancellationLedger,
    semaphore_cancellations: CancellationLedger,
    rate_limits: HashMap<String, RateLimitRecord>,
    idempotency: HashMap<String, IdempotencyRecord>,
    elections: HashMap<String, Leadership>,
    schedules: HashMap<String, ScheduleRecord>,
    services: HashMap<String, HashMap<String, ServiceInstance>>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            command_protocol: LEGACY_COMMAND_PROTOCOL,
            revision: 0,
            next_fencing_token: 0,
            kv: HashMap::new(),
            counters: HashMap::new(),
            barriers: HashMap::new(),
            tasks: HashMap::new(),
            effects: HashMap::new(),
            handoffs: HashMap::new(),
            decisions: HashMap::new(),
            budgets: HashMap::new(),
            claims: HashMap::new(),
            locks: LockManager::default(),
            semaphores: HashMap::new(),
            lock_cancellations: CancellationLedger::default(),
            semaphore_cancellations: CancellationLedger::default(),
            rate_limits: HashMap::new(),
            idempotency: HashMap::new(),
            elections: HashMap::new(),
            schedules: HashMap::new(),
            services: HashMap::new(),
        }
    }
}

struct LockAcquireInput {
    keys: Vec<String>,
    holder: String,
    ttl_ms: u64,
    wait: bool,
    wait_timeout_ms: Option<u64>,
    request_id: Option<String>,
    legacy_semantics: bool,
}

struct SemaphoreAcquireInput {
    key: String,
    holder: String,
    limit: u32,
    ttl_ms: u64,
    wait: bool,
    wait_timeout_ms: Option<u64>,
    request_id: Option<String>,
    legacy_semantics: bool,
}

/// How far above this shard's fencing counter a handoff's caller-supplied
/// `from_token` may sit. Tokens are per-shard and a handoff may transfer a
/// resource fenced on another shard, so a `from_token` ahead of this counter is
/// legitimate — but it is also unauthenticated input that becomes the floor for
/// the next mint, so an unbounded one (`u64::MAX`) would burn the whole token
/// space in a single request and collapse every later token on the shard onto
/// the same value. A billion is far beyond any real cross-shard skew.
const MAX_HANDOFF_TOKEN_ADVANCE: u64 = 1_000_000_000;

const CANCELLATION_TOMBSTONE_TTL_MS: u64 = crate::validate::MAX_TTL_MS;
const MAX_CANCELLATION_TOMBSTONES_PER_TENANT: u32 = 10_000;
const MAX_CANCELLATION_TOMBSTONES_TOTAL: usize = 250_000;
/// Keep a slice of the global ledger available to tenants that have not yet
/// consumed a modest fair share. Without this reserve, 25 tenants at the hard
/// per-tenant cap could starve every other tenant until the 24-hour expiry.
const CANCELLATION_TOMBSTONE_GLOBAL_RESERVE: usize = 25_000;
const CANCELLATION_TOMBSTONE_FAIR_SHARE_PER_TENANT: u32 = 256;

#[derive(Debug, Clone, Copy)]
struct CancellationLimits {
    per_tenant: u32,
    total: usize,
    global_reserve: usize,
    fair_share_per_tenant: u32,
}

const CANCELLATION_LIMITS: CancellationLimits = CancellationLimits {
    per_tenant: MAX_CANCELLATION_TOMBSTONES_PER_TENANT,
    total: MAX_CANCELLATION_TOMBSTONES_TOTAL,
    global_reserve: CANCELLATION_TOMBSTONE_GLOBAL_RESERVE,
    fair_share_per_tenant: CANCELLATION_TOMBSTONE_FAIR_SHARE_PER_TENANT,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CancellationEntry {
    expires_at_ms: u64,
    tenant: String,
}

/// Replicated attempt-cancellation ledger. Both identity and tenant are SHA-256
/// digests, so each entry has a fixed upper bound regardless of union-key size.
/// The expiry index makes insertion/expiry O(log n); capacity is fail-closed and
/// never evicts another tenant's still-live safety record.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct CancellationLedger {
    entries: HashMap<String, CancellationEntry>,
    expirations: BTreeMap<u64, BTreeSet<String>>,
    tenant_counts: HashMap<String, u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RememberCancellation {
    Remembered,
    CapacityExceeded,
}

fn hash_segment(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn digest_string(mut hasher: Sha256) -> String {
    let digest = hasher.finalize_reset();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn acquisition_identity(kind: &str, holder: &str, keys: &[String], request_id: &str) -> String {
    let mut hasher = Sha256::new();
    hash_segment(&mut hasher, kind.as_bytes());
    hash_segment(&mut hasher, holder.as_bytes());
    for key in keys {
        hash_segment(&mut hasher, key.as_bytes());
    }
    hash_segment(&mut hasher, request_id.as_bytes());
    digest_string(hasher)
}

fn cancellation_tenant(holder: &str) -> String {
    let delimiter = fiducia_routing::ORG_SCOPE_DELIM;
    let scope = holder
        .strip_prefix(delimiter)
        .and_then(|rest| rest.find(delimiter).map(|end| &rest[..end]))
        .unwrap_or("legacy-unscoped");
    let mut hasher = Sha256::new();
    hash_segment(&mut hasher, scope.as_bytes());
    digest_string(hasher)
}

impl CancellationLedger {
    fn contains(&self, identity: &str) -> bool {
        self.entries.contains_key(identity)
    }

    fn remember(&mut self, identity: String, tenant: String, now: u64) -> RememberCancellation {
        self.remember_with_limits(identity, tenant, now, CANCELLATION_LIMITS)
    }

    fn remember_with_limits(
        &mut self,
        identity: String,
        tenant: String,
        now: u64,
        limits: CancellationLimits,
    ) -> RememberCancellation {
        if self.entries.contains_key(&identity) {
            return RememberCancellation::Remembered;
        }
        let tenant_count = self.tenant_counts.get(&tenant).copied().unwrap_or(0);
        let reserve_starts_at = limits.total.saturating_sub(limits.global_reserve);
        let consuming_reserved_capacity =
            self.entries.len() >= reserve_starts_at && tenant_count >= limits.fair_share_per_tenant;
        if self.entries.len() >= limits.total
            || tenant_count >= limits.per_tenant
            || consuming_reserved_capacity
        {
            return RememberCancellation::CapacityExceeded;
        }

        let expires_at_ms = now.saturating_add(CANCELLATION_TOMBSTONE_TTL_MS);
        self.entries.insert(
            identity.clone(),
            CancellationEntry {
                expires_at_ms,
                tenant: tenant.clone(),
            },
        );
        self.expirations
            .entry(expires_at_ms)
            .or_default()
            .insert(identity);
        *self.tenant_counts.entry(tenant).or_default() += 1;
        RememberCancellation::Remembered
    }

    fn expire(&mut self, now: u64) {
        let deadlines: Vec<u64> = self.expirations.range(..=now).map(|(at, _)| *at).collect();
        for deadline in deadlines {
            let Some(identities) = self.expirations.remove(&deadline) else {
                continue;
            };
            for identity in identities {
                let Some(entry) = self.entries.get(&identity) else {
                    continue;
                };
                if entry.expires_at_ms != deadline {
                    continue;
                }
                let tenant = entry.tenant.clone();
                self.entries.remove(&identity);
                if let Some(count) = self.tenant_counts.get_mut(&tenant) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.tenant_counts.remove(&tenant);
                    }
                }
            }
        }
    }

    fn validate(&self, label: &str) -> Result<(), String> {
        let mut counts = HashMap::<&str, u32>::new();
        for (identity, entry) in &self.entries {
            if !self
                .expirations
                .get(&entry.expires_at_ms)
                .is_some_and(|bucket| bucket.contains(identity))
            {
                return Err(format!(
                    "{label} cancellation '{identity}' is absent from expiry index"
                ));
            }
            *counts.entry(entry.tenant.as_str()).or_default() += 1;
        }
        for (tenant, count) in &self.tenant_counts {
            if counts.get(tenant.as_str()).copied().unwrap_or(0) != *count {
                return Err(format!("{label} cancellation tenant count is inconsistent"));
            }
        }
        if counts.len() != self.tenant_counts.len() {
            return Err(format!("{label} cancellation tenant index is incomplete"));
        }
        Ok(())
    }
}

struct RateLimitCheckInput {
    key: String,
    tenant: String,
    algorithm: RateLimitAlgorithm,
    limit: u32,
    window_ms: u64,
    refill_per_second: Option<f64>,
    cost: u32,
}

/// Applies committed commands and answers read queries.
pub struct StateMachine {
    store: Mutex<Store>,
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine {
            store: Mutex::new(Store::default()),
        }
    }

    /// Lock the store, recovering the guard from a poisoned mutex.
    ///
    /// One panic under the guard would otherwise poison the lock *forever*: every
    /// later apply and every read on this shard would panic too, while
    /// `CatchPanicLayer` keeps the process cheerfully answering 500s. Poisoning
    /// says "state may be half-updated", which is exactly the state the shard is
    /// in either way — refusing to look at it only converts one bad command into
    /// a permanently dead shard. Take the inner guard and keep serving.
    fn locked(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Serialize the complete applied state for Raft log compaction.
    pub fn snapshot(&self) -> Result<Vec<u8>, serde_json::Error> {
        let store = self.locked();
        let payload = serde_json::to_vec(&*store)?;
        if store.command_protocol < CURRENT_COMMAND_PROTOCOL {
            return Ok(payload);
        }
        let mut stamped = Vec::with_capacity(CURRENT_PROTOCOL_SNAPSHOT_MAGIC.len() + payload.len());
        stamped.extend_from_slice(CURRENT_PROTOCOL_SNAPSHOT_MAGIC);
        stamped.extend_from_slice(&payload);
        Ok(stamped)
    }

    /// Restore an atomically persisted state-machine snapshot.
    ///
    /// The deserialized state is invariant-checked before it becomes live (see
    /// [`Store::validate_restored`]): a snapshot that parses but would zero or
    /// regress the fencing counter, or carries an inconsistent lock table, is
    /// rejected here instead of silently serving wrong authority.
    pub fn restore(&self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        let (stamped, payload) =
            if let Some(payload) = bytes.strip_prefix(CURRENT_PROTOCOL_SNAPSHOT_MAGIC) {
                (true, payload)
            } else {
                (false, bytes)
            };
        let restored: Store = serde_json::from_slice(payload)?;
        if stamped != (restored.command_protocol >= CURRENT_COMMAND_PROTOCOL) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "state-machine command protocol and snapshot compatibility stamp disagree",
            ));
        }
        restored
            .validate_restored()
            .map_err(<serde_json::Error as serde::de::Error>::custom)?;
        *self.locked() = restored;
        Ok(())
    }

    pub fn command_protocol(&self) -> u16 {
        self.locked().command_protocol
    }

    #[allow(dead_code)]
    pub fn apply(&self, command: Command) -> ApplyResult {
        self.apply_at(command, now_ms())
    }

    /// Apply a committed command using the timestamp captured by its Raft log
    /// entry. Replicas and restart recovery must pass the same value so leases,
    /// TTLs, refill windows, and deadlines cannot move when an entry is replayed.
    pub fn apply_at(&self, command: Command, proposed_at_ms: u64) -> ApplyResult {
        let mut store = self.locked();
        let now = proposed_at_ms;
        let protocol_activation = matches!(command, Command::ActivateCommandProtocol { .. });
        // Recovery defense: builds that predate the explicit activation barrier
        // may already have committed one of these variants. That log is already
        // unreadable by V1, so stamp the materialized state V2 as well; otherwise
        // compaction could accidentally turn an incompatible log into a raw JSON
        // snapshot an older binary would accept.
        if command.requires_current_protocol() {
            store.command_protocol = CURRENT_COMMAND_PROTOCOL;
        }
        if !protocol_activation {
            store.expire_due(now);
            store.revision += 1;
        }
        let revision = store.revision;

        let output = match command {
            Command::ActivateCommandProtocol { version } => {
                if version == CURRENT_COMMAND_PROTOCOL
                    && store.command_protocol <= CURRENT_COMMAND_PROTOCOL
                {
                    store.command_protocol = version;
                    json!({
                        "activated": true,
                        "command_protocol": store.command_protocol,
                        "revision": revision,
                    })
                } else {
                    json!({
                        "activated": false,
                        "reason": "unsupported_command_protocol",
                        "requested": version,
                        "command_protocol": store.command_protocol,
                        "revision": revision,
                    })
                }
            }
            Command::KvPut {
                key,
                value,
                ttl_ms,
                prev_revision,
            } => store.apply_kv_put(revision, now, key, value, ttl_ms, prev_revision),
            Command::KvDelete { key } => {
                let existed = store.kv.remove(&key).is_some();
                json!({ "ok": true, "deleted": existed, "revision": revision })
            }
            Command::CounterAdd {
                key,
                delta,
                prev_revision,
            } => store.apply_counter_add(revision, key, delta, prev_revision),
            Command::CounterSet {
                key,
                value,
                prev_revision,
            } => store.apply_counter_set(revision, key, value, prev_revision),
            Command::BarrierCreate {
                name,
                policy,
                expected,
                deadline_ms,
            } => store.apply_barrier_create(now, name, policy, expected, deadline_ms),
            Command::BarrierArrive {
                name,
                participant,
                weight,
                veto,
            } => store.apply_barrier_arrive(now, name, participant, weight, veto),
            Command::TaskCreate {
                name,
                task_type,
                payload,
                deadline_ms,
            } => store.apply_task_create(name, task_type, payload, deadline_ms),
            Command::TaskClaim {
                name,
                worker,
                ttl_ms,
            } => store.apply_task_claim(now, name, worker, ttl_ms),
            Command::TaskProgress {
                name,
                worker,
                fencing_token,
                percent,
                checkpoint,
            } => store.apply_task_progress(now, name, worker, fencing_token, percent, checkpoint),
            Command::TaskComplete {
                name,
                worker,
                fencing_token,
                result,
            } => store.apply_task_complete(now, name, worker, fencing_token, result),
            Command::TaskFail {
                name,
                worker,
                fencing_token,
                retryable,
            } => store.apply_task_fail(now, name, worker, fencing_token, retryable),
            Command::TaskCancel { name } => store.apply_task_cancel(name),
            Command::EffectPrepare {
                name,
                effect_type,
                payload,
                risk,
                idempotency_key,
                required_approvals,
            } => store.apply_effect_prepare(
                name,
                effect_type,
                payload,
                risk,
                idempotency_key,
                required_approvals,
            ),
            Command::EffectApprove { name, principal } => {
                store.apply_effect_approve(name, principal)
            }
            Command::EffectCommit { name, result } => store.apply_effect_commit(name, result),
            Command::EffectAbort { name } => store.apply_effect_abort(name),
            Command::HandoffOffer {
                name,
                resource,
                from,
                to,
                from_token,
                context,
                ttl_ms,
            } => store
                .apply_handoff_offer(now, name, resource, from, to, from_token, context, ttl_ms),
            Command::HandoffAccept { name, to } => store.apply_handoff_accept(now, name, to),
            Command::HandoffReject { name, to } => store.apply_handoff_reject(now, name, to),
            Command::DecisionPropose {
                name,
                question,
                options,
                policy,
                deadline_ms,
            } => store.apply_decision_propose(now, name, question, options, policy, deadline_ms),
            Command::DecisionVote {
                name,
                voter,
                option,
                confidence,
                weight,
                veto,
                evidence,
            } => store
                .apply_decision_vote(now, name, voter, option, confidence, weight, veto, evidence),
            Command::BudgetSet { name, limit } => store.apply_budget_set(name, limit),
            Command::BudgetReserve {
                name,
                reservation_id,
                holder,
                amount,
            } => store.apply_budget_reserve(name, reservation_id, holder, amount),
            Command::BudgetCommit {
                name,
                reservation_id,
                actual,
            } => store.apply_budget_commit(name, reservation_id, actual),
            Command::BudgetRelease {
                name,
                reservation_id,
            } => store.apply_budget_release(name, reservation_id),
            Command::ClaimAssert {
                name,
                subject,
                predicate,
                value,
                confidence,
                author,
                evidence,
                valid_until_ms,
            } => store.apply_claim_assert(
                name,
                subject,
                predicate,
                value,
                confidence,
                author,
                evidence,
                valid_until_ms,
            ),
            Command::ClaimSupport { name, agent } => store.apply_claim_support(name, agent),
            Command::ClaimContest {
                name,
                agent,
                reason,
            } => store.apply_claim_contest(name, agent, reason),
            Command::ClaimResolve { name, accepted } => store.apply_claim_resolve(name, accepted),
            Command::ClaimSupersede {
                name,
                superseded_by,
            } => store.apply_claim_supersede(name, superseded_by),
            Command::LockAcquire {
                keys,
                holder,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_lock_acquire(
                revision,
                now,
                LockAcquireInput {
                    keys,
                    holder,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: None,
                    legacy_semantics: true,
                },
            ),
            Command::LockAcquireV2 {
                keys,
                holder,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_lock_acquire(
                revision,
                now,
                LockAcquireInput {
                    keys,
                    holder,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: None,
                    legacy_semantics: false,
                },
            ),
            Command::LockAcquireAttempt {
                keys,
                holder,
                request_id,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_lock_acquire(
                revision,
                now,
                LockAcquireInput {
                    keys,
                    holder,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: Some(request_id),
                    legacy_semantics: false,
                },
            ),
            Command::LockRenew {
                keys,
                holder,
                fencing_token,
                ttl_ms,
            } => store.apply_lock_renew(revision, now, keys, holder, fencing_token, ttl_ms),
            Command::LockRelease {
                holder,
                fencing_token,
            } => store.apply_lock_release(revision, now, holder, fencing_token),
            Command::LockCancel { keys, holder } => {
                store.apply_lock_cancel(revision, now, keys, holder, None)
            }
            Command::LockCancelAttempt {
                keys,
                holder,
                request_id,
            } => store.apply_lock_cancel(revision, now, keys, holder, Some(request_id)),
            Command::SemaphoreAcquire {
                key,
                holder,
                limit,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_semaphore_acquire(
                revision,
                now,
                SemaphoreAcquireInput {
                    key,
                    holder,
                    limit,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: None,
                    legacy_semantics: true,
                },
            ),
            Command::SemaphoreAcquireV2 {
                key,
                holder,
                limit,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_semaphore_acquire(
                revision,
                now,
                SemaphoreAcquireInput {
                    key,
                    holder,
                    limit,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: None,
                    legacy_semantics: false,
                },
            ),
            Command::SemaphoreAcquireAttempt {
                key,
                holder,
                request_id,
                limit,
                ttl_ms,
                wait,
                wait_timeout_ms,
            } => store.apply_semaphore_acquire(
                revision,
                now,
                SemaphoreAcquireInput {
                    key,
                    holder,
                    limit,
                    ttl_ms,
                    wait,
                    wait_timeout_ms,
                    request_id: Some(request_id),
                    legacy_semantics: false,
                },
            ),
            Command::SemaphoreRenew {
                key,
                holder,
                fencing_token,
                ttl_ms,
            } => store.apply_semaphore_renew(revision, now, key, holder, fencing_token, ttl_ms),
            Command::SemaphoreRelease {
                key,
                holder,
                fencing_token,
            } => store.apply_semaphore_release(revision, now, key, holder, fencing_token),
            Command::SemaphoreCancel { key, holder } => {
                store.apply_semaphore_cancel(revision, now, key, holder, None)
            }
            Command::SemaphoreCancelAttempt {
                key,
                holder,
                request_id,
            } => store.apply_semaphore_cancel(revision, now, key, holder, Some(request_id)),
            Command::RateLimitCheck {
                key,
                tenant,
                algorithm,
                limit,
                window_ms,
                refill_per_second,
                cost,
            } => store.apply_rate_limit_check(
                now,
                RateLimitCheckInput {
                    key,
                    tenant,
                    algorithm,
                    limit,
                    window_ms,
                    refill_per_second,
                    cost: cost.max(1),
                },
            ),
            Command::IdempotencyClaim {
                key,
                owner,
                ttl_ms,
                retention_ms,
                metadata,
            } => store.apply_idempotency_claim(
                revision,
                now,
                key,
                owner,
                ttl_ms,
                retention_ms,
                metadata,
            ),
            Command::IdempotencyComplete {
                key,
                owner,
                fencing_token,
                result,
            } => store.apply_idempotency_complete(revision, now, key, owner, fencing_token, result),
            Command::IdempotencyRenew {
                key,
                owner,
                fencing_token,
                ttl_ms,
            } => store.apply_idempotency_renew(revision, now, key, owner, fencing_token, ttl_ms),
            Command::IdempotencyAbandon {
                key,
                owner,
                fencing_token,
            } => store.apply_idempotency_abandon(revision, key, owner, fencing_token),
            Command::ScheduleUpsert {
                name,
                cron,
                one_shot_at_ms,
                target,
                delivery,
                max_retries,
                now_ms,
            } => store.apply_schedule_upsert(
                name,
                cron,
                one_shot_at_ms,
                target,
                delivery,
                max_retries,
                now_ms,
            ),
            Command::ScheduleRecordRun {
                name,
                fire_id,
                fired_at_ms,
            } => store.apply_schedule_record_run(name, fire_id, fired_at_ms),
            Command::ScheduleClaimFire { name, fire_id_ms } => {
                store.apply_schedule_claim_fire(name, fire_id_ms)
            }
            Command::ScheduleRecordResult {
                name,
                fire_id_ms,
                delivered,
                attempts,
                error,
            } => store.apply_schedule_record_result(name, fire_id_ms, delivered, attempts, error),
            Command::ElectionCampaign {
                name,
                candidate,
                ttl_ms,
                metadata,
            } => store.apply_election_campaign(revision, now, name, candidate, ttl_ms, metadata),
            Command::ElectionRenew {
                name,
                candidate,
                fencing_token,
                ttl_ms,
            } => store.apply_election_renew(now, name, candidate, fencing_token, ttl_ms),
            Command::ElectionResign {
                name,
                candidate,
                fencing_token,
            } => store.apply_election_resign(name, candidate, fencing_token),
            Command::ServiceRegister {
                service,
                instance_id,
                address,
                ttl_ms,
                metadata,
            } => store.apply_service_register(now, service, instance_id, address, ttl_ms, metadata),
            Command::ServiceHeartbeat {
                service,
                instance_id,
                ttl_ms,
            } => store.apply_service_heartbeat(now, service, instance_id, ttl_ms),
            Command::ServiceDeregister {
                service,
                instance_id,
            } => store.apply_service_deregister(service, instance_id),
        };

        ApplyResult { revision, output }
    }

    #[allow(dead_code)]
    pub fn revision(&self) -> u64 {
        self.locked().revision
    }

    pub fn kv_get(&self, key: &str) -> Option<KvEntry> {
        let now = now_ms();
        self.locked()
            .kv
            .get(key)
            .filter(|entry| entry.expires_at_ms.is_none_or(|expires| expires > now))
            .cloned()
    }

    /// Read a counter's current value and revision. Counters do not expire.
    pub fn counter_get(&self, key: &str) -> Option<CounterEntry> {
        self.locked().counters.get(key).cloned()
    }

    /// Read a barrier's current state, with its status derived at `now`.
    pub fn barrier_get(&self, name: &str) -> Option<BarrierState> {
        let store = self.locked();
        store
            .barriers
            .get(name)
            .map(|record| record.view(name, now_ms()))
    }

    /// Read a durable task's current state.
    pub fn task_get(&self, name: &str) -> Option<TaskState> {
        self.locked()
            .tasks
            .get(name)
            .map(|record| record.view(name))
    }

    /// Read an approval-escrow effect's current state.
    pub fn effect_get(&self, name: &str) -> Option<EffectState> {
        self.locked()
            .effects
            .get(name)
            .map(|record| record.view(name))
    }

    /// Read an ownership handoff's current state.
    pub fn handoff_get(&self, name: &str) -> Option<HandoffState> {
        let store = self.locked();
        store
            .handoffs
            .get(name)
            .map(|record| record.view(name, now_ms()))
    }

    /// Read a decision's current state, with its outcome derived at `now`.
    pub fn decision_get(&self, name: &str) -> Option<DecisionState> {
        let store = self.locked();
        store
            .decisions
            .get(name)
            .map(|record| record.view(name, now_ms()))
    }

    /// Read a budget's ceiling, consumption, and reservations.
    pub fn budget_get(&self, name: &str) -> Option<BudgetState> {
        self.locked()
            .budgets
            .get(name)
            .map(|record| record.view(name))
    }

    /// Read a claim's current state.
    pub fn claim_get(&self, name: &str) -> Option<ClaimState> {
        self.locked()
            .claims
            .get(name)
            .map(|record| record.view(name))
    }

    pub fn kv_prefix(&self, prefix: &str) -> Vec<(String, KvEntry)> {
        let now = now_ms();
        let store = self.locked();
        let mut entries: Vec<_> = store
            .kv
            .iter()
            .filter(|(key, entry)| {
                key.starts_with(prefix) && entry.expires_at_ms.is_none_or(|expires| expires > now)
            })
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
    }

    pub fn lock_get(&self, key: &str) -> LockState {
        self.locked().lock_snapshot_live_at(key, now_ms())
    }

    pub fn semaphore_get(&self, key: &str) -> SemaphoreState {
        self.locked().semaphore_snapshot_live_at(key, now_ms())
    }

    pub fn rate_limit_get(&self, tenant: &str, key: &str) -> Option<RateLimitSnapshot> {
        self.locked().rate_limit_snapshot(tenant, key)
    }

    pub fn idempotency_get(&self, key: &str) -> Option<IdempotencyRecord> {
        let now = now_ms();
        self.locked()
            .idempotency
            .get(key)
            .filter(|record| record.lease_expires_ms > now)
            .cloned()
    }

    pub fn election_get(&self, name: &str) -> Option<Leadership> {
        let now = now_ms();
        self.locked()
            .elections
            .get(name)
            .filter(|leadership| leadership.lease_expires_ms > now)
            .cloned()
    }

    pub fn schedule_get(&self, name: &str) -> Option<Schedule> {
        self.locked()
            .schedules
            .get(name)
            .map(|record| record.definition.clone())
    }

    pub fn schedule_history(&self, name: &str) -> Vec<ScheduleRun> {
        self.locked()
            .schedules
            .get(name)
            .map(|record| record.history.clone())
            .unwrap_or_default()
    }

    /// Every schedule definition on this shard (for the firing loop to scan).
    pub fn schedule_list(&self) -> Vec<Schedule> {
        self.locked()
            .schedules
            .values()
            .map(|record| record.definition.clone())
            .collect()
    }

    pub fn service_list(&self, service: &str) -> Vec<ServiceInstance> {
        let now = now_ms();
        let store = self.locked();
        store
            .services
            .get(service)
            .map(|instances| {
                instances
                    .values()
                    .filter(|instance| instance.lease_expires_ms > now)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every live KV entry whose key starts with `prefix` (this shard's slice of
    /// the keyspace). Serializable read off applied state; callers fan this out
    /// across shards and merge.
    pub fn kv_list(&self, prefix: &str) -> Vec<KvListItem> {
        let now = now_ms();
        let store = self.locked();
        store
            .kv
            .iter()
            .filter(|(key, entry)| {
                key.starts_with(prefix) && entry.expires_at_ms.is_none_or(|expires| expires > now)
            })
            .map(|(key, entry)| KvListItem {
                key: key.clone(),
                entry: entry.clone(),
            })
            .collect()
    }

    /// Every service that has at least one live instance on this shard, with the
    /// live-instance count. Callers fan this out across shards and sum.
    pub fn service_names(&self) -> Vec<ServiceSummary> {
        let now = now_ms();
        let store = self.locked();
        store
            .services
            .iter()
            .filter_map(|(service, instances)| {
                let live_instances = instances
                    .values()
                    .filter(|instance| instance.lease_expires_ms > now)
                    .count();
                (live_instances > 0).then(|| ServiceSummary {
                    service: service.clone(),
                    instances: live_instances,
                })
            })
            .collect()
    }

    /// Every live lock grant plus the FIFO wait queue — the observability view of
    /// the whole lock coordinator (all lock state lives on one shard, so this is a
    /// single-shard read). The view filters expired grants without promoting
    /// waiters or otherwise changing applied state. Grants are sorted by fencing
    /// token and the queue by request time, so output is deterministic for
    /// tests/diffs.
    pub fn lock_inventory(&self) -> LockInventory {
        self.locked().lock_inventory_live_at(now_ms())
    }

    /// A snapshot of every counting semaphore on this shard (holders, free
    /// permits, wait queue). Sorted by key for deterministic output.
    pub fn semaphore_inventory(&self) -> Vec<SemaphoreState> {
        self.locked().semaphore_inventory_live_at(now_ms())
    }

    /// Every named election with live leadership on this shard, sorted by name.
    /// Callers fan this out across shards (elections route by name) and merge.
    pub fn election_inventory(&self) -> Vec<ElectionEntry> {
        self.locked().election_inventory_live_at(now_ms())
    }
}

impl Store {
    /// Mint the next fencing token, or `None` once the counter is exhausted.
    ///
    /// Fail-closed on purpose: a saturating counter would hand every subsequent
    /// caller the *same* `u64::MAX`, and two holders with equal tokens is exactly
    /// the split-brain fencing exists to prevent. Refusing the acquisition keeps
    /// tokens strictly increasing; callers surface it as a failed operation.
    fn next_token(&mut self) -> Option<u64> {
        self.next_fencing_token = self.next_fencing_token.checked_add(1)?;
        Some(self.next_fencing_token)
    }

    /// Mint a fencing token strictly greater than `floor`, advancing this shard's
    /// counter so every later token also exceeds it. Used by handoffs, whose new
    /// owner's token must beat the `from_token` the previous owner presented even
    /// when that token was minted on a different shard's counter. `None` when the
    /// counter is exhausted (see [`Store::next_token`]).
    fn mint_token_above(&mut self, floor: u64) -> Option<u64> {
        self.next_fencing_token = self.next_fencing_token.max(floor).checked_add(1)?;
        Some(self.next_fencing_token)
    }

    /// The highest fencing token referenced anywhere in this state — the floor
    /// `next_fencing_token` must never be below, or a future mint would reissue
    /// a token some holder already presents.
    fn max_referenced_fencing_token(&self) -> u64 {
        let mut max = 0u64;
        for (&token, grant) in &self.locks.grants {
            max = max.max(token).max(grant.fencing_token);
        }
        for &token in self.locks.held.values() {
            max = max.max(token);
        }
        for semaphore in self.semaphores.values() {
            for slot in &semaphore.holders {
                max = max.max(slot.fencing_token);
            }
        }
        for record in self.idempotency.values() {
            max = max.max(record.fencing_token);
        }
        for leadership in self.elections.values() {
            max = max.max(leadership.fencing_token);
        }
        for task in self.tasks.values() {
            max = max.max(task.fencing_token);
        }
        for handoff in self.handoffs.values() {
            max = max.max(handoff.from_token);
            if let Some(to_token) = handoff.to_token {
                max = max.max(to_token);
            }
        }
        max
    }

    /// Validate a deserialized snapshot before it becomes live state.
    ///
    /// `Store` derives `#[serde(default)]`, so a snapshot missing
    /// `next_fencing_token` (a migrated/truncated/corrupted file that still
    /// parses) would silently deserialize the counter as 0 — and the node would
    /// re-mint tokens holders already present: fencing-token reuse, the exact
    /// split-brain fencing exists to prevent. This check refuses any state whose
    /// counter is below a token it references, and any lock table whose
    /// `held`/`grants` dual index is inconsistent (a resurrected or half-released
    /// lock). Callers surface the error (InstallSnapshot rejects; boot fail-stops)
    /// rather than serving from silently-wrong state.
    fn validate_restored(&self) -> Result<(), String> {
        if !matches!(
            self.command_protocol,
            LEGACY_COMMAND_PROTOCOL | CURRENT_COMMAND_PROTOCOL
        ) {
            return Err(format!(
                "unsupported state-machine command protocol {}",
                self.command_protocol
            ));
        }
        let floor = self.max_referenced_fencing_token();
        if self.next_fencing_token < floor {
            return Err(format!(
                "next_fencing_token {} is below the highest referenced fencing token {}; \
                 restoring would re-mint issued tokens",
                self.next_fencing_token, floor
            ));
        }
        for (key, token) in &self.locks.held {
            if !self.locks.grants.contains_key(token) {
                return Err(format!(
                    "held lock key '{key}' references missing grant token {token}"
                ));
            }
        }
        for (&token, grant) in &self.locks.grants {
            if grant.fencing_token != token {
                return Err(format!(
                    "lock grant indexed at token {token} carries fencing_token {}",
                    grant.fencing_token
                ));
            }
            for key in &grant.keys {
                if self.locks.held.get(key) != Some(&token) {
                    return Err(format!(
                        "lock grant {token} claims key '{key}' that is not held by it"
                    ));
                }
            }
        }
        self.lock_cancellations.validate("lock")?;
        self.semaphore_cancellations.validate("semaphore")?;
        Ok(())
    }

    fn expire_due(&mut self, now: u64) {
        self.kv.retain(|_, entry| {
            entry
                .expires_at_ms
                .map(|expires| expires > now)
                .unwrap_or(true)
        });
        self.lock_cancellations.expire(now);
        self.semaphore_cancellations.expire(now);
        // Queue deadlines are checked before any promotion. Otherwise a waiter
        // whose patience lease elapsed could be granted authority by this very
        // sweep. Legacy snapshot entries have no deadline and retain their old
        // indefinite-wait behavior.
        let expired_waiters: Vec<(String, Vec<String>)> = self
            .locks
            .queue
            .iter()
            .filter(|(_, waiter)| !waiter_is_live(waiter.wait_expires_ms, now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired_waiters {
            self.locks.queue.remove(&id);
        }

        // Expire any union-lock grants whose lease lapsed, freeing their member
        // keys, then promote whatever the freed keys or expired reservations now
        // unblock.
        let expired: Vec<u64> = self
            .locks
            .grants
            .iter()
            .filter(|(_, g)| g.lease_expires_ms <= now)
            .map(|(token, _)| *token)
            .collect();
        for token in expired {
            self.release_grant(token);
        }
        self.lock_promote(now);

        // Expire semaphore waiters before permits, then admit only live FIFO
        // successors. Collecting ids first keeps keyed removals O(1) each.
        for sem in self.semaphores.values_mut() {
            let expired_waiters: Vec<String> = sem
                .queue
                .iter()
                .filter(|(_, waiter)| !waiter_is_live(waiter.wait_expires_ms, now))
                .map(|(holder, _)| holder.clone())
                .collect();
            for holder in expired_waiters {
                sem.queue.remove(&holder);
            }
            sem.holders.retain(|slot| slot.lease_expires_ms > now);
        }
        self.semaphores_promote(now);
        self.elections
            .retain(|_, leadership| leadership.lease_expires_ms > now);
        self.idempotency
            .retain(|_, record| record.lease_expires_ms > now);
        for instances in self.services.values_mut() {
            instances.retain(|_, instance| instance.lease_expires_ms > now);
        }
        self.services.retain(|_, instances| !instances.is_empty());
    }

    fn apply_kv_put(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        value: String,
        ttl_ms: Option<u64>,
        prev_revision: Option<u64>,
    ) -> Value {
        let current_revision = self
            .kv
            .get(&key)
            .map(|entry| entry.mod_revision)
            .unwrap_or(0);
        if let Some(expected) = prev_revision {
            if current_revision != expected {
                return json!({
                    "ok": false,
                    "reason": "cas_mismatch",
                    "current_revision": current_revision,
                    "revision": revision,
                });
            }
        }
        let expires_at_ms = ttl_ms.map(|ttl| now.saturating_add(ttl));
        self.kv.insert(
            key.clone(),
            KvEntry {
                value,
                mod_revision: revision,
                expires_at_ms,
            },
        );
        json!({ "ok": true, "key": key, "revision": revision, "expires_at_ms": expires_at_ms })
    }

    fn apply_counter_add(
        &mut self,
        revision: u64,
        key: String,
        delta: i64,
        prev_revision: Option<u64>,
    ) -> Value {
        let current = self.counters.get(&key);
        let current_revision = current.map(|entry| entry.mod_revision).unwrap_or(0);
        if let Some(expected) = prev_revision {
            if current_revision != expected {
                return json!({
                    "ok": false,
                    "reason": "cas_mismatch",
                    "current_revision": current_revision,
                    "revision": revision,
                });
            }
        }
        let value = current
            .map(|entry| entry.value)
            .unwrap_or(0)
            .saturating_add(delta);
        self.counters.insert(
            key.clone(),
            CounterEntry {
                value,
                mod_revision: revision,
            },
        );
        json!({ "ok": true, "key": key, "value": value, "mod_revision": revision })
    }

    fn apply_counter_set(
        &mut self,
        revision: u64,
        key: String,
        value: i64,
        prev_revision: Option<u64>,
    ) -> Value {
        let current_revision = self
            .counters
            .get(&key)
            .map(|entry| entry.mod_revision)
            .unwrap_or(0);
        if let Some(expected) = prev_revision {
            if current_revision != expected {
                return json!({
                    "ok": false,
                    "reason": "cas_mismatch",
                    "current_revision": current_revision,
                    "revision": revision,
                });
            }
        }
        self.counters.insert(
            key.clone(),
            CounterEntry {
                value,
                mod_revision: revision,
            },
        );
        json!({ "ok": true, "key": key, "value": value, "mod_revision": revision })
    }

    fn apply_barrier_create(
        &mut self,
        now: u64,
        name: String,
        policy: BarrierPolicy,
        expected: u32,
        deadline_ms: Option<u64>,
    ) -> Value {
        // Reconfigure only while still pending; a resolved barrier is immutable.
        if self
            .barriers
            .get(&name)
            .is_some_and(|record| record.status(now).is_terminal())
        {
            let view = self.barriers.get(&name).unwrap().view(&name, now);
            return json!({ "ok": false, "reason": "already_resolved", "barrier": view });
        }
        let arrivals = self
            .barriers
            .remove(&name)
            .map(|record| record.arrivals)
            .unwrap_or_default();
        let record = BarrierRecord {
            policy,
            expected,
            deadline_ms,
            arrivals,
        };
        let view = record.view(&name, now);
        self.barriers.insert(name, record);
        json!({ "ok": true, "barrier": view })
    }

    fn apply_barrier_arrive(
        &mut self,
        now: u64,
        name: String,
        participant: String,
        weight: u64,
        veto: bool,
    ) -> Value {
        // Auto-create a single-participant `all` barrier if none exists, so a
        // plain fan-in needs no explicit create.
        let record = self
            .barriers
            .entry(name.clone())
            .or_insert_with(|| BarrierRecord {
                policy: BarrierPolicy::All,
                expected: 1,
                deadline_ms: None,
                arrivals: HashMap::new(),
            });
        record.arrivals.insert(
            participant.clone(),
            BarrierArrival {
                participant,
                weight,
                veto,
                arrived_ms: now,
            },
        );
        let view = record.view(&name, now);
        json!({ "ok": true, "resolved": view.status.is_terminal(), "barrier": view })
    }

    fn apply_task_create(
        &mut self,
        name: String,
        task_type: String,
        payload: Value,
        deadline_ms: Option<u64>,
    ) -> Value {
        if let Some(existing) = self.tasks.get(&name) {
            return json!({ "ok": true, "created": false, "task": existing.view(&name) });
        }
        let record = TaskRecord {
            task_type,
            payload,
            status: TaskStatus::Pending,
            owner: None,
            fencing_token: 0,
            lease_ttl_ms: 0,
            lease_expires_ms: None,
            progress: 0,
            checkpoint: Value::Null,
            result: None,
            deadline_ms,
            generation: 1,
        };
        let view = record.view(&name);
        self.tasks.insert(name, record);
        json!({ "ok": true, "created": true, "task": view })
    }

    fn apply_task_claim(&mut self, now: u64, name: String, worker: String, ttl_ms: u64) -> Value {
        let Some(token) = self.next_token() else {
            return json!({ "ok": false, "reason": "fencing_tokens_exhausted" });
        };
        let Some(record) = self.tasks.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "task": record.view(&name) });
        }
        if !record.claimable(now) {
            return json!({
                "ok": false,
                "reason": "already_claimed",
                "owner": record.owner,
                "task": record.view(&name),
            });
        }
        record.owner = Some(worker);
        record.fencing_token = token;
        record.lease_ttl_ms = ttl_ms;
        record.lease_expires_ms = Some(now.saturating_add(ttl_ms));
        record.status = TaskStatus::Claimed;
        record.generation += 1;
        json!({ "ok": true, "fencing_token": token, "task": record.view(&name) })
    }

    fn apply_task_progress(
        &mut self,
        now: u64,
        name: String,
        worker: String,
        fencing_token: u64,
        percent: u32,
        checkpoint: Value,
    ) -> Value {
        let Some(record) = self.tasks.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if !record.owned_now(now, &worker, fencing_token) {
            return json!({ "ok": false, "reason": "fenced", "task": record.view(&name) });
        }
        record.status = TaskStatus::Running;
        record.progress = percent.min(100);
        record.checkpoint = checkpoint;
        record.lease_expires_ms = Some(now.saturating_add(record.lease_ttl_ms));
        record.generation += 1;
        json!({ "ok": true, "task": record.view(&name) })
    }

    fn apply_task_complete(
        &mut self,
        now: u64,
        name: String,
        worker: String,
        fencing_token: u64,
        result: Value,
    ) -> Value {
        let Some(record) = self.tasks.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if !record.owned_now(now, &worker, fencing_token) {
            return json!({ "ok": false, "reason": "fenced", "task": record.view(&name) });
        }
        record.status = TaskStatus::Completed;
        record.progress = 100;
        record.result = Some(result);
        record.lease_expires_ms = None;
        record.generation += 1;
        json!({ "ok": true, "task": record.view(&name) })
    }

    fn apply_task_fail(
        &mut self,
        now: u64,
        name: String,
        worker: String,
        fencing_token: u64,
        retryable: bool,
    ) -> Value {
        let Some(record) = self.tasks.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if !record.owned_now(now, &worker, fencing_token) {
            return json!({ "ok": false, "reason": "fenced", "task": record.view(&name) });
        }
        if retryable {
            // Return to the claimable pool for another worker.
            record.status = TaskStatus::Pending;
            record.owner = None;
            record.lease_expires_ms = None;
        } else {
            record.status = TaskStatus::Failed;
        }
        record.generation += 1;
        json!({ "ok": true, "retryable": retryable, "task": record.view(&name) })
    }

    fn apply_task_cancel(&mut self, name: String) -> Value {
        let Some(record) = self.tasks.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "task": record.view(&name) });
        }
        record.status = TaskStatus::Cancelled;
        record.owner = None;
        record.lease_expires_ms = None;
        record.generation += 1;
        json!({ "ok": true, "task": record.view(&name) })
    }

    fn apply_effect_prepare(
        &mut self,
        name: String,
        effect_type: String,
        payload: Value,
        risk: String,
        idempotency_key: String,
        required_approvals: u32,
    ) -> Value {
        if let Some(existing) = self.effects.get(&name) {
            return json!({ "ok": true, "created": false, "effect": existing.view(&name) });
        }
        let status = if required_approvals == 0 {
            EffectStatus::Approved
        } else {
            EffectStatus::Prepared
        };
        let record = EffectRecord {
            effect_type,
            payload,
            risk,
            idempotency_key,
            status,
            required_approvals,
            approvals: std::collections::BTreeSet::new(),
            result: None,
            generation: 1,
        };
        let view = record.view(&name);
        self.effects.insert(name, record);
        json!({ "ok": true, "created": true, "effect": view })
    }

    fn apply_effect_approve(&mut self, name: String, principal: String) -> Value {
        let Some(record) = self.effects.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "effect": record.view(&name) });
        }
        record.approvals.insert(principal);
        if record.approvals.len() as u32 >= record.required_approvals {
            record.status = EffectStatus::Approved;
        }
        record.generation += 1;
        json!({ "ok": true, "effect": record.view(&name) })
    }

    fn apply_effect_commit(&mut self, name: String, result: Value) -> Value {
        let Some(record) = self.effects.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        match record.status {
            // Idempotent replay: already committed, do not re-execute.
            EffectStatus::Committed => {
                json!({ "ok": true, "committed": false, "effect": record.view(&name) })
            }
            EffectStatus::Aborted => {
                json!({ "ok": false, "reason": "aborted", "effect": record.view(&name) })
            }
            EffectStatus::Prepared => json!({
                "ok": false,
                "reason": "not_approved",
                "approvals": record.approvals.len(),
                "required_approvals": record.required_approvals,
                "effect": record.view(&name),
            }),
            EffectStatus::Approved => {
                record.status = EffectStatus::Committed;
                record.result = Some(result);
                record.generation += 1;
                json!({ "ok": true, "committed": true, "effect": record.view(&name) })
            }
        }
    }

    fn apply_effect_abort(&mut self, name: String) -> Value {
        let Some(record) = self.effects.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "effect": record.view(&name) });
        }
        record.status = EffectStatus::Aborted;
        record.generation += 1;
        json!({ "ok": true, "effect": record.view(&name) })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_handoff_offer(
        &mut self,
        now: u64,
        name: String,
        resource: String,
        from: String,
        to: String,
        from_token: u64,
        context: Value,
        ttl_ms: u64,
    ) -> Value {
        // `from_token` is caller-supplied and becomes the floor for the token the
        // acceptor is minted, so an unbounded one drags `next_fencing_token`
        // arbitrarily far forward — at `u64::MAX` every later mint on the shard
        // collides on one value and fencing stops fencing. A cross-shard token
        // may legitimately lead this shard's counter, so bound the lead rather
        // than requiring one this shard issued.
        if from_token
            > self
                .next_fencing_token
                .saturating_add(MAX_HANDOFF_TOKEN_ADVANCE)
        {
            return json!({ "ok": false, "reason": "implausible_from_token" });
        }
        // A pending offer for this name cannot be replaced; resolve it first.
        if let Some(existing) = self.handoffs.get(&name) {
            if existing.effective_status(now) == HandoffStatus::Offered {
                return json!({ "ok": false, "reason": "already_offered", "handoff": existing.view(&name, now) });
            }
        }
        let record = HandoffRecord {
            resource,
            from,
            to,
            from_token,
            to_token: None,
            status: HandoffStatus::Offered,
            context,
            expires_ms: Some(now.saturating_add(ttl_ms)),
            generation: 1,
        };
        let view = record.view(&name, now);
        self.handoffs.insert(name, record);
        json!({ "ok": true, "handoff": view })
    }

    fn apply_handoff_accept(&mut self, now: u64, name: String, to: String) -> Value {
        // Validate against an immutable borrow first, so a rejected accept does
        // not consume a fencing token; capture the presented `from_token`.
        let from_token = match self.handoffs.get(&name) {
            None => return json!({ "ok": false, "reason": "not_found" }),
            Some(record) if record.effective_status(now) != HandoffStatus::Offered => {
                return json!({ "ok": false, "reason": "not_offered", "handoff": record.view(&name, now) })
            }
            Some(record) if record.to != to => {
                return json!({ "ok": false, "reason": "not_recipient", "handoff": record.view(&name, now) })
            }
            Some(record) => record.from_token,
        };
        // Mint a token strictly above `from_token`. Fencing tokens are per-shard,
        // and the handoff may live on a different shard than the resource, so we
        // cannot rely on this shard's counter already exceeding `from_token`.
        let Some(token) = self.mint_token_above(from_token) else {
            return json!({ "ok": false, "reason": "fencing_tokens_exhausted" });
        };
        let record = self.handoffs.get_mut(&name).expect("handoff present");
        record.status = HandoffStatus::Accepted;
        record.to_token = Some(token);
        record.generation += 1;
        json!({ "ok": true, "to_token": token, "handoff": record.view(&name, now) })
    }

    fn apply_handoff_reject(&mut self, now: u64, name: String, to: String) -> Value {
        let Some(record) = self.handoffs.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.effective_status(now) != HandoffStatus::Offered {
            return json!({ "ok": false, "reason": "not_offered", "handoff": record.view(&name, now) });
        }
        if record.to != to {
            return json!({ "ok": false, "reason": "not_recipient", "handoff": record.view(&name, now) });
        }
        record.status = HandoffStatus::Rejected;
        record.generation += 1;
        json!({ "ok": true, "handoff": record.view(&name, now) })
    }

    fn apply_decision_propose(
        &mut self,
        now: u64,
        name: String,
        question: String,
        options: Vec<String>,
        policy: DecisionPolicy,
        deadline_ms: Option<u64>,
    ) -> Value {
        if let Some(existing) = self.decisions.get(&name) {
            return json!({ "ok": true, "created": false, "decision": existing.view(&name, now) });
        }
        let record = DecisionRecord {
            question,
            options,
            policy,
            votes: std::collections::BTreeMap::new(),
            deadline_ms,
            generation: 1,
        };
        let view = record.view(&name, now);
        self.decisions.insert(name, record);
        json!({ "ok": true, "created": true, "decision": view })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_decision_vote(
        &mut self,
        now: u64,
        name: String,
        voter: String,
        option: Option<String>,
        confidence: f32,
        weight: u64,
        veto: bool,
        evidence: Vec<String>,
    ) -> Value {
        let Some(record) = self.decisions.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        // A vote for an option outside the declared set is rejected (abstain and
        // veto need no option).
        if let Some(choice) = &option {
            if !record.options.contains(choice) {
                return json!({ "ok": false, "reason": "unknown_option", "decision": record.view(&name, now) });
            }
        }
        // Re-voting replaces the voter's prior vote (idempotent per voter).
        record.votes.insert(
            voter,
            DecisionVoteRecord {
                option,
                confidence,
                weight,
                veto,
                evidence,
            },
        );
        record.generation += 1;
        json!({ "ok": true, "decision": record.view(&name, now) })
    }

    fn apply_budget_set(&mut self, name: String, limit: BudgetAmount) -> Value {
        let record = self
            .budgets
            .entry(name.clone())
            .or_insert_with(|| BudgetRecord {
                limit,
                reservations: std::collections::BTreeMap::new(),
                generation: 0,
            });
        record.limit = limit;
        record.generation += 1;
        json!({ "ok": true, "budget": record.view(&name) })
    }

    fn apply_budget_reserve(
        &mut self,
        name: String,
        reservation_id: String,
        holder: String,
        amount: BudgetAmount,
    ) -> Value {
        let Some(record) = self.budgets.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        // Reserving the same id twice is idempotent (returns the existing hold).
        if record.reservations.contains_key(&reservation_id) {
            return json!({ "ok": true, "reserved": false, "budget": record.view(&name) });
        }
        if !record.fits(&amount) {
            return json!({ "ok": false, "reason": "insufficient_budget", "available": record.available(), "budget": record.view(&name) });
        }
        record.reservations.insert(
            reservation_id,
            ReservationRecord {
                holder,
                reserved: amount,
                spent: BudgetAmount::zero(),
                status: ReservationStatus::Held,
            },
        );
        record.generation += 1;
        json!({ "ok": true, "reserved": true, "budget": record.view(&name) })
    }

    fn apply_budget_commit(
        &mut self,
        name: String,
        reservation_id: String,
        actual: BudgetAmount,
    ) -> Value {
        let Some(record) = self.budgets.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        let Some(reservation) = record.reservations.get_mut(&reservation_id) else {
            return json!({ "ok": false, "reason": "reservation_not_found" });
        };
        match reservation.status {
            ReservationStatus::Committed => {
                json!({ "ok": true, "committed": false, "budget": record.view(&name) })
            }
            ReservationStatus::Released => {
                json!({ "ok": false, "reason": "released" })
            }
            ReservationStatus::Held => {
                // Actual spend is capped at what was reserved on each axis.
                let cap = |actual: Option<u64>, reserved: Option<u64>| match (actual, reserved) {
                    (Some(a), Some(r)) => Some(a.min(r)),
                    (Some(a), None) => Some(a),
                    (None, _) => Some(0),
                };
                reservation.spent = BudgetAmount {
                    usd_micros: cap(actual.usd_micros, reservation.reserved.usd_micros),
                    tokens: cap(actual.tokens, reservation.reserved.tokens),
                    tool_calls: cap(actual.tool_calls, reservation.reserved.tool_calls),
                };
                reservation.status = ReservationStatus::Committed;
                record.generation += 1;
                json!({ "ok": true, "committed": true, "budget": record.view(&name) })
            }
        }
    }

    fn apply_budget_release(&mut self, name: String, reservation_id: String) -> Value {
        let Some(record) = self.budgets.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        let Some(reservation) = record.reservations.get_mut(&reservation_id) else {
            return json!({ "ok": false, "reason": "reservation_not_found" });
        };
        if reservation.status == ReservationStatus::Committed {
            return json!({ "ok": false, "reason": "already_committed", "budget": record.view(&name) });
        }
        reservation.status = ReservationStatus::Released;
        record.generation += 1;
        json!({ "ok": true, "budget": record.view(&name) })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_claim_assert(
        &mut self,
        name: String,
        subject: String,
        predicate: String,
        value: Value,
        confidence: f32,
        author: String,
        evidence: Vec<String>,
        valid_until_ms: Option<u64>,
    ) -> Value {
        match self.claims.get_mut(&name) {
            Some(record) if !record.status.is_terminal() => {
                // Re-assertion by anyone updates the value and bumps the version,
                // resetting support/contests for the new assertion.
                record.subject = subject;
                record.predicate = predicate;
                record.value = value;
                record.confidence = confidence;
                record.author = author;
                record.evidence = evidence;
                record.valid_until_ms = valid_until_ms;
                record.status = ClaimStatus::Asserted;
                record.supporters.clear();
                record.contests.clear();
                record.version += 1;
                record.generation += 1;
                json!({ "ok": true, "created": false, "claim": record.view(&name) })
            }
            Some(record) => {
                json!({ "ok": false, "reason": "terminal", "claim": record.view(&name) })
            }
            None => {
                let record = ClaimRecord {
                    subject,
                    predicate,
                    value,
                    confidence,
                    author,
                    status: ClaimStatus::Asserted,
                    supporters: std::collections::BTreeSet::new(),
                    contests: std::collections::BTreeMap::new(),
                    evidence,
                    valid_until_ms,
                    superseded_by: None,
                    version: 1,
                    generation: 1,
                };
                let view = record.view(&name);
                self.claims.insert(name, record);
                json!({ "ok": true, "created": true, "claim": view })
            }
        }
    }

    fn apply_claim_support(&mut self, name: String, agent: String) -> Value {
        let Some(record) = self.claims.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "claim": record.view(&name) });
        }
        record.supporters.insert(agent);
        record.generation += 1;
        json!({ "ok": true, "claim": record.view(&name) })
    }

    fn apply_claim_contest(&mut self, name: String, agent: String, reason: String) -> Value {
        let Some(record) = self.claims.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "claim": record.view(&name) });
        }
        record.contests.insert(agent, reason);
        record.status = ClaimStatus::Contested;
        record.generation += 1;
        json!({ "ok": true, "claim": record.view(&name) })
    }

    fn apply_claim_resolve(&mut self, name: String, accepted: bool) -> Value {
        let Some(record) = self.claims.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status.is_terminal() {
            return json!({ "ok": false, "reason": "terminal", "claim": record.view(&name) });
        }
        record.status = if accepted {
            ClaimStatus::Accepted
        } else {
            ClaimStatus::Rejected
        };
        record.generation += 1;
        json!({ "ok": true, "claim": record.view(&name) })
    }

    fn apply_claim_supersede(&mut self, name: String, superseded_by: String) -> Value {
        let Some(record) = self.claims.get_mut(&name) else {
            return json!({ "ok": false, "reason": "not_found" });
        };
        if record.status == ClaimStatus::Superseded {
            return json!({ "ok": true, "superseded": false, "claim": record.view(&name) });
        }
        record.status = ClaimStatus::Superseded;
        record.superseded_by = Some(superseded_by);
        record.generation += 1;
        json!({ "ok": true, "superseded": true, "claim": record.view(&name) })
    }

    /// Acquire the **union** of `keys` (multi-key lock), all-or-nothing.
    fn apply_lock_acquire(&mut self, revision: u64, now: u64, input: LockAcquireInput) -> Value {
        let LockAcquireInput {
            keys,
            holder,
            ttl_ms,
            wait,
            wait_timeout_ms,
            request_id,
            legacy_semantics,
        } = input;
        let keys = canonical_keys(&keys);
        if keys.is_empty() {
            return json!({ "acquired": false, "reason": "no_keys", "revision": revision });
        }

        if request_id.as_ref().is_some_and(|request_id| {
            self.lock_cancellations
                .contains(&acquisition_identity("lock", &holder, &keys, request_id))
        }) {
            return json!({
                "acquired": false,
                "queued": false,
                "position": null,
                "wait_expires_ms": null,
                "keys": keys,
                "holder": holder,
                "conflicts": [],
                "revision": revision,
            });
        }

        // Historical log entries used an exact-holder re-acquire as renewal, so
        // preserve that behavior only while replaying the legacy command. New
        // command variants discover the original token without extending it;
        // only the token-bound renew command may extend new authority.
        if let Some(&tok) = self.locks.held.get(&keys[0]) {
            if let Some(grant) = self.locks.grants.get_mut(&tok) {
                if grant.holder == holder
                    && grant.keys == keys
                    && (legacy_semantics || grant.request_id == request_id)
                {
                    if legacy_semantics {
                        grant.lease_expires_ms =
                            grant.lease_expires_ms.max(now.saturating_add(ttl_ms));
                    }
                    return json!({
                        "acquired": true,
                        "queued": false,
                        "renewed": legacy_semantics,
                        "keys": keys,
                        "holder": holder,
                        "fencing_token": tok,
                        "lease_expires_ms": grant.lease_expires_ms,
                        "revision": revision,
                    });
                }
            }
        }

        // Grantable now iff no member key is held AND none is reserved by a
        // request already in the queue (FIFO fairness — we'd join the tail).
        let blocked_by_held = keys.iter().any(|k| self.locks.held.contains_key(k));
        let reserved: std::collections::HashSet<&str> = self
            .locks
            .queue
            .iter()
            .flat_map(|(_, q)| q.keys.iter().map(|k| k.as_str()))
            .collect();
        let blocked_by_queue = keys.iter().any(|k| reserved.contains(k.as_str()));

        if !blocked_by_held && !blocked_by_queue {
            let Some(token) = self.next_token() else {
                return json!({
                    "acquired": false,
                    "queued": false,
                    "reason": "fencing_tokens_exhausted",
                    "keys": keys,
                    "holder": holder,
                    "revision": revision,
                });
            };
            let lease_expires_ms = now.saturating_add(ttl_ms);
            self.install_grant(LockGrant {
                holder: holder.clone(),
                keys: keys.clone(),
                fencing_token: token,
                lease_expires_ms,
                request_id,
            });
            return json!({
                "acquired": true,
                "queued": false,
                "keys": keys,
                "holder": holder,
                "fencing_token": token,
                "lease_expires_ms": lease_expires_ms,
                "revision": revision,
            });
        }

        // Not grantable. Queue it (idempotently) when the caller wants to wait.
        // Identity is (holder, key-set), so the dedup and place-in-line are O(1).
        let id = (holder.clone(), keys.clone());
        let already = self
            .locks
            .queue
            .get(&id)
            .is_some_and(|queued| queued.request_id == request_id);
        let identity_in_use = self.locks.queue.contains(&id) && !already;
        if wait && !already && !identity_in_use {
            self.locks.queue.push_back(
                id.clone(),
                QueuedLock {
                    holder: holder.clone(),
                    keys: keys.clone(),
                    ttl_ms,
                    requested_ms: now,
                    wait_expires_ms: (!legacy_semantics).then(|| {
                        now.saturating_add(
                            wait_timeout_ms.unwrap_or(crate::validate::DEFAULT_WAIT_TIMEOUT_MS),
                        )
                    }),
                    request_id,
                },
            );
        }
        let position = (!identity_in_use)
            .then(|| self.locks.queue.position(&id).map(|idx| idx + 1))
            .flatten();
        let wait_expires_ms = (!identity_in_use)
            .then(|| {
                self.locks
                    .queue
                    .get(&id)
                    .and_then(|waiter| waiter.wait_expires_ms)
            })
            .flatten();
        let conflicts: Vec<String> = keys
            .iter()
            .filter(|k| self.locks.held.contains_key(*k))
            .cloned()
            .collect();
        json!({
            "acquired": false,
            // If this identity was already queued, a retry with `wait:false`
            // reports the durable truth without deleting, duplicating, or
            // extending its original queue entry.
            "queued": if legacy_semantics { wait && position.is_some() } else { position.is_some() },
            "position": position,
            "wait_expires_ms": wait_expires_ms,
            "keys": keys,
            "holder": holder,
            "conflicts": conflicts,
            "revision": revision,
        })
    }

    /// Renew an active union grant only when all authority-bearing fields match.
    fn apply_lock_renew(
        &mut self,
        revision: u64,
        now: u64,
        keys: Vec<String>,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Value {
        let keys = canonical_keys(&keys);
        let Some(grant) = self.locks.grants.get_mut(&fencing_token) else {
            return json!({ "renewed": false, "reason": "not_found", "revision": revision });
        };
        if grant.holder != holder {
            return json!({ "renewed": false, "reason": "not_holder", "revision": revision });
        }
        if grant.keys != keys {
            return json!({ "renewed": false, "reason": "key_mismatch", "revision": revision });
        }
        grant.lease_expires_ms = grant.lease_expires_ms.max(now.saturating_add(ttl_ms));
        json!({
            "renewed": true,
            "keys": keys,
            "holder": holder,
            "fencing_token": fencing_token,
            "lease_expires_ms": grant.lease_expires_ms,
            "revision": revision,
        })
    }

    /// Release a union grant by its fencing token, freeing all member keys and
    /// promoting whatever waiters that unblocks.
    fn apply_lock_release(
        &mut self,
        revision: u64,
        now: u64,
        holder: String,
        fencing_token: u64,
    ) -> Value {
        let Some(grant) = self.locks.grants.get(&fencing_token) else {
            return json!({ "released": false, "reason": "not_found", "revision": revision });
        };
        if grant.holder != holder {
            return json!({ "released": false, "reason": "not_holder", "revision": revision });
        }
        let keys = grant.keys.clone();
        self.release_grant(fencing_token);
        let promoted = self.lock_promote(now);
        json!({
            "released": true,
            "keys": keys,
            "promoted": promoted,
            "revision": revision,
        })
    }

    /// Idempotently cancel one exact queued union-lock identity. Removing an
    /// earlier reservation may make later disjoint/overlapping entries
    /// grantable, so promotion is part of this same committed command.
    fn apply_lock_cancel(
        &mut self,
        revision: u64,
        now: u64,
        keys: Vec<String>,
        holder: String,
        request_id: Option<String>,
    ) -> Value {
        let keys = canonical_keys(&keys);
        let id = (holder.clone(), keys.clone());
        if let Some(request_id) = &request_id {
            // The command-level expiry sweep runs before this method and may
            // have promoted this exact attempt. Returning its fencing authority
            // is more important than allocating a cancellation tombstone: an
            // aborting client must be able to release a raced grant even when
            // the bounded tombstone ledger is full.
            if let Some(grant) = self.locks.grants.values().find(|grant| {
                grant.holder == holder
                    && grant.keys == keys
                    && grant.request_id.as_ref() == Some(request_id)
            }) {
                return json!({
                    "cancelled": false,
                    "acquired": true,
                    "keys": keys,
                    "holder": holder,
                    "fencing_token": grant.fencing_token,
                    "lease_expires_ms": grant.lease_expires_ms,
                    "promoted": [],
                    "revision": revision,
                });
            }
            let identity = acquisition_identity("lock", &holder, &keys, request_id);
            if self
                .lock_cancellations
                .remember(identity, cancellation_tenant(&holder), now)
                == RememberCancellation::CapacityExceeded
            {
                return json!({
                    "cancelled": false,
                    "acquired": false,
                    "reason": "cancellation_capacity",
                    "keys": keys,
                    "holder": holder,
                    "promoted": [],
                    "revision": revision,
                });
            }
        }
        let queued_matches = self
            .locks
            .queue
            .get(&id)
            .is_some_and(|queued| request_id.is_none() || queued.request_id == request_id);
        let cancelled = queued_matches && self.locks.queue.remove(&id).is_some();
        if !cancelled {
            // Cancellation can race the release/expiry that promoted this exact
            // waiter. Report the acquired authority so an aborting client can
            // immediately release it; cancellation itself must never release a
            // live grant behind the holder's back.
            if let Some(grant) = self.locks.grants.values().find(|grant| {
                grant.holder == holder
                    && grant.keys == keys
                    && (request_id.is_none() || grant.request_id == request_id)
            }) {
                return json!({
                    "cancelled": false,
                    "acquired": true,
                    "keys": keys,
                    "holder": holder,
                    "fencing_token": grant.fencing_token,
                    "lease_expires_ms": grant.lease_expires_ms,
                    "promoted": [],
                    "revision": revision,
                });
            }
            if request_id.is_some() {
                return json!({
                    "cancelled": true,
                    "acquired": false,
                    "keys": keys,
                    "holder": holder,
                    "promoted": [],
                    "revision": revision,
                });
            }
            return json!({
                "cancelled": false,
                "acquired": false,
                "reason": "not_found",
                "keys": keys,
                "holder": holder,
                "promoted": [],
                "revision": revision,
            });
        }
        let promoted = self.lock_promote(now);
        json!({
            "cancelled": true,
            "acquired": false,
            "keys": keys,
            "holder": holder,
            "promoted": promoted,
            "revision": revision,
        })
    }

    /// Insert a grant and mark every member key held by it.
    fn install_grant(&mut self, grant: LockGrant) {
        for key in &grant.keys {
            self.locks.held.insert(key.clone(), grant.fencing_token);
        }
        self.locks.grants.insert(grant.fencing_token, grant);
    }

    /// Remove a grant and free its member keys (no promotion).
    fn release_grant(&mut self, fencing_token: u64) {
        if let Some(grant) = self.locks.grants.remove(&fencing_token) {
            for key in &grant.keys {
                if self.locks.held.get(key) == Some(&fencing_token) {
                    self.locks.held.remove(key);
                }
            }
        }
    }

    /// Index of the first queue entry whose whole key set is free, treating the
    /// key sets of earlier still-queued entries as reserved (so a later request
    /// can't barge ahead of an earlier overlapping one — FIFO, no starvation).
    fn lock_first_grantable(&self) -> Option<(String, Vec<String>)> {
        let mut reserved: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (id, q) in self.locks.queue.iter() {
            let blocked = q
                .keys
                .iter()
                .any(|k| self.locks.held.contains_key(k) || reserved.contains(k.as_str()));
            if !blocked {
                return Some(id.clone());
            }
            for k in &q.keys {
                reserved.insert(k.as_str());
            }
        }
        None
    }

    /// Grant every queued request that can now be satisfied; returns who was
    /// promoted and the token they were granted.
    fn lock_promote(&mut self, now: u64) -> Vec<Value> {
        let mut promoted = Vec::new();
        while let Some(id) = self.lock_first_grantable() {
            // Mint before dequeuing: an exhausted counter must leave the waiter
            // in line rather than dropping it.
            let Some(token) = self.next_token() else {
                break;
            };
            let waiter = self.locks.queue.remove(&id).expect("key from scan");
            let lease_expires_ms = now.saturating_add(waiter.ttl_ms);
            promoted.push(json!({
                "holder": waiter.holder,
                "keys": waiter.keys,
                "fencing_token": token,
                "lease_expires_ms": lease_expires_ms,
            }));
            self.install_grant(LockGrant {
                holder: waiter.holder,
                keys: waiter.keys,
                fencing_token: token,
                lease_expires_ms,
                request_id: waiter.request_id,
            });
        }
        promoted
    }

    // --- counting semaphores ---------------------------------------------

    fn apply_semaphore_acquire(
        &mut self,
        revision: u64,
        now: u64,
        input: SemaphoreAcquireInput,
    ) -> Value {
        let SemaphoreAcquireInput {
            key,
            holder,
            limit,
            ttl_ms,
            wait,
            wait_timeout_ms,
            request_id,
            legacy_semantics,
        } = input;
        if request_id.as_ref().is_some_and(|request_id| {
            self.semaphore_cancellations.contains(&acquisition_identity(
                "semaphore",
                &holder,
                std::slice::from_ref(&key),
                request_id,
            ))
        }) {
            return json!({
                "acquired": false,
                "queued": false,
                "position": null,
                "wait_expires_ms": null,
                "key": key,
                "holder": holder,
                "limit": limit,
                "available": 0,
                "revision": revision,
            });
        }
        if !legacy_semantics {
            if let Some(existing) = self.semaphores.get(&key) {
                // Capacity is part of the semaphore's identity. Letting any
                // acquire silently raise it would break callers relying on the
                // original exclusion bound; changing it requires a future
                // fenced/admin API.
                if existing.limit != limit {
                    return json!({
                        "acquired": false,
                        "queued": false,
                        "reason": "limit_mismatch",
                        "key": key,
                        "holder": holder,
                        "requested_limit": limit,
                        "limit": existing.limit,
                        "revision": revision,
                    });
                }
            }
        }
        let sem = self
            .semaphores
            .entry(key.clone())
            .or_insert_with(|| Semaphore {
                limit: if legacy_semantics {
                    limit.max(1)
                } else {
                    limit
                },
                holders: Vec::new(),
                queue: IndexedQueue::new(),
            });
        if legacy_semantics {
            // Preserve the exact behavior of historical log entries. Newly
            // accepted HTTP requests use the versioned immutable-limit command.
            sem.limit = limit.max(1);
        }

        // Idempotent re-acquire returns the existing permit rather than consuming
        // a second one, but does not extend it. Holder is a queue identity, not
        // fenced renewal authority; use SemaphoreRenew with the token to extend.
        if let Some(index) = sem.holders.iter().position(|slot| {
            slot.holder == holder && (legacy_semantics || slot.request_id == request_id)
        }) {
            let slot = &sem.holders[index];
            let token = slot.fencing_token;
            let lease_expires_ms = slot.lease_expires_ms;
            let available = sem.limit.saturating_sub(sem.holders.len() as u32);
            return json!({
                "acquired": true,
                "queued": false,
                "renewed": false,
                "key": key,
                "holder": holder,
                "fencing_token": token,
                "lease_expires_ms": lease_expires_ms,
                "available": available,
                "limit": sem.limit,
                "revision": revision,
            });
        }

        let has_capacity = (sem.holders.len() as u32) < sem.limit;
        let queue_empty = sem.queue.is_empty();
        if has_capacity && queue_empty {
            let Some(token) = self.next_token() else {
                return json!({
                    "acquired": false,
                    "queued": false,
                    "reason": "fencing_tokens_exhausted",
                    "key": key,
                    "holder": holder,
                    "revision": revision,
                });
            };
            let lease_expires_ms = now.saturating_add(ttl_ms);
            let sem = self.semaphores.get_mut(&key).expect("just inserted");
            sem.holders.push(SemaphoreSlot {
                holder: holder.clone(),
                fencing_token: token,
                lease_expires_ms,
                request_id,
            });
            let available = sem.limit.saturating_sub(sem.holders.len() as u32);
            return json!({
                "acquired": true,
                "queued": false,
                "key": key,
                "holder": holder,
                "fencing_token": token,
                "lease_expires_ms": lease_expires_ms,
                "available": available,
                "limit": sem.limit,
                "revision": revision,
            });
        }

        let already = sem
            .queue
            .get(&holder)
            .is_some_and(|queued| queued.request_id == request_id);
        let identity_in_use = sem.queue.contains(&holder) && !already;
        if wait && !already && !identity_in_use {
            sem.queue.push_back(
                holder.clone(),
                QueuedPermit {
                    holder: holder.clone(),
                    ttl_ms,
                    requested_ms: now,
                    wait_expires_ms: (!legacy_semantics).then(|| {
                        now.saturating_add(
                            wait_timeout_ms.unwrap_or(crate::validate::DEFAULT_WAIT_TIMEOUT_MS),
                        )
                    }),
                    request_id,
                },
            );
        }
        let position = (!identity_in_use)
            .then(|| sem.queue.position(&holder).map(|idx| idx + 1))
            .flatten();
        let wait_expires_ms = (!identity_in_use)
            .then(|| {
                sem.queue
                    .get(&holder)
                    .and_then(|waiter| waiter.wait_expires_ms)
            })
            .flatten();
        json!({
            "acquired": false,
            "queued": if legacy_semantics { wait && position.is_some() } else { position.is_some() },
            "position": position,
            "wait_expires_ms": wait_expires_ms,
            "key": key,
            "holder": holder,
            "limit": sem.limit,
            "available": 0,
            "revision": revision,
        })
    }

    fn apply_semaphore_renew(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Value {
        let Some(sem) = self.semaphores.get_mut(&key) else {
            return json!({ "renewed": false, "reason": "not_found", "revision": revision });
        };
        let Some(slot) = sem
            .holders
            .iter_mut()
            .find(|slot| slot.fencing_token == fencing_token)
        else {
            return json!({ "renewed": false, "reason": "not_found", "revision": revision });
        };
        if slot.holder != holder {
            return json!({ "renewed": false, "reason": "not_holder", "revision": revision });
        }
        slot.lease_expires_ms = slot.lease_expires_ms.max(now.saturating_add(ttl_ms));
        json!({
            "renewed": true,
            "key": key,
            "holder": holder,
            "fencing_token": fencing_token,
            "lease_expires_ms": slot.lease_expires_ms,
            "revision": revision,
        })
    }

    fn apply_semaphore_release(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        fencing_token: u64,
    ) -> Value {
        let Some(sem) = self.semaphores.get_mut(&key) else {
            return json!({ "released": false, "reason": "not_found", "revision": revision });
        };
        let before = sem.holders.len();
        sem.holders
            .retain(|slot| !(slot.fencing_token == fencing_token && slot.holder == holder));
        if sem.holders.len() == before {
            return json!({ "released": false, "reason": "not_holder", "revision": revision });
        }
        let promoted = self.semaphore_promote(&key, now);
        json!({
            "released": true,
            "key": key,
            "promoted": promoted,
            "revision": revision,
        })
    }

    fn apply_semaphore_cancel(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        request_id: Option<String>,
    ) -> Value {
        if let Some(request_id) = &request_id {
            if let Some(slot) = self.semaphores.get(&key).and_then(|sem| {
                sem.holders.iter().find(|slot| {
                    slot.holder == holder && slot.request_id.as_ref() == Some(request_id)
                })
            }) {
                return json!({
                    "cancelled": false,
                    "acquired": true,
                    "key": key,
                    "holder": holder,
                    "fencing_token": slot.fencing_token,
                    "lease_expires_ms": slot.lease_expires_ms,
                    "promoted": [],
                    "revision": revision,
                });
            }
            let identity =
                acquisition_identity("semaphore", &holder, std::slice::from_ref(&key), request_id);
            if self
                .semaphore_cancellations
                .remember(identity, cancellation_tenant(&holder), now)
                == RememberCancellation::CapacityExceeded
            {
                return json!({
                    "cancelled": false,
                    "acquired": false,
                    "reason": "cancellation_capacity",
                    "key": key,
                    "holder": holder,
                    "promoted": [],
                    "revision": revision,
                });
            }
        }
        let queued_matches = self
            .semaphores
            .get(&key)
            .and_then(|sem| sem.queue.get(&holder))
            .is_some_and(|queued| request_id.is_none() || queued.request_id == request_id);
        let cancelled = queued_matches
            && self
                .semaphores
                .get_mut(&key)
                .and_then(|sem| sem.queue.remove(&holder))
                .is_some();
        if !cancelled {
            if let Some(slot) = self.semaphores.get(&key).and_then(|sem| {
                sem.holders.iter().find(|slot| {
                    slot.holder == holder && (request_id.is_none() || slot.request_id == request_id)
                })
            }) {
                return json!({
                    "cancelled": false,
                    "acquired": true,
                    "key": key,
                    "holder": holder,
                    "fencing_token": slot.fencing_token,
                    "lease_expires_ms": slot.lease_expires_ms,
                    "promoted": [],
                    "revision": revision,
                });
            }
            if request_id.is_some() {
                return json!({
                    "cancelled": true,
                    "acquired": false,
                    "key": key,
                    "holder": holder,
                    "promoted": [],
                    "revision": revision,
                });
            }
            return json!({
                "cancelled": false,
                "acquired": false,
                "reason": "not_found",
                "key": key,
                "holder": holder,
                "promoted": [],
                "revision": revision,
            });
        }
        let promoted = self.semaphore_promote(&key, now);
        json!({
            "cancelled": true,
            "acquired": false,
            "key": key,
            "holder": holder,
            "promoted": promoted,
            "revision": revision,
        })
    }

    /// Admit FIFO waiters of one semaphore up to its limit.
    fn semaphore_promote(&mut self, key: &str, now: u64) -> Vec<Value> {
        let mut promoted = Vec::new();
        while let Some(sem) = self.semaphores.get(key) {
            if (sem.holders.len() as u32) >= sem.limit || sem.queue.is_empty() {
                break;
            }
            // Mint before popping, so an exhausted counter leaves the waiter queued.
            let Some(token) = self.next_token() else {
                break;
            };
            let sem = self.semaphores.get_mut(key).expect("checked above");
            let (_, waiter) = sem.queue.pop_front().expect("non-empty checked");
            let lease_expires_ms = now.saturating_add(waiter.ttl_ms);
            sem.holders.push(SemaphoreSlot {
                holder: waiter.holder.clone(),
                fencing_token: token,
                lease_expires_ms,
                request_id: waiter.request_id,
            });
            promoted.push(json!({
                "holder": waiter.holder,
                "fencing_token": token,
                "lease_expires_ms": lease_expires_ms,
            }));
        }
        promoted
    }

    /// Admit waiters across every semaphore (used after a TTL sweep).
    fn semaphores_promote(&mut self, now: u64) {
        let keys: Vec<String> = self.semaphores.keys().cloned().collect();
        for key in keys {
            self.semaphore_promote(&key, now);
        }
    }

    fn apply_rate_limit_check(&mut self, now: u64, input: RateLimitCheckInput) -> Value {
        let RateLimitCheckInput {
            key,
            tenant,
            algorithm,
            limit,
            window_ms,
            refill_per_second,
            cost,
        } = input;
        let store_key = rate_limit_store_key(&tenant, &key);
        let record = self
            .rate_limits
            .entry(store_key)
            .or_insert_with(|| RateLimitRecord {
                algorithm: algorithm.clone(),
                limit,
                window_ms,
                tokens: limit as f64,
                updated_ms: now,
                events: VecDeque::new(),
                last_allowed: true,
            });
        record.algorithm = algorithm.clone();
        record.limit = limit;
        record.window_ms = window_ms;

        let (allowed, remaining, reset_ms) = match algorithm {
            RateLimitAlgorithm::TokenBucket => {
                let refill =
                    refill_per_second.unwrap_or(limit as f64 / (window_ms.max(1) as f64 / 1000.0));
                let elapsed = now.saturating_sub(record.updated_ms) as f64 / 1000.0;
                // Clamp, never just `min`: a non-finite accumulator (from a
                // legacy log entry whose `refill_per_second` predates
                // `validate::rate_limit_params`) serializes as JSON `null`, and
                // the shard's next snapshot would then fail to restore — breaking
                // compaction, InstallSnapshot, and boot recovery.
                record.tokens = clamp_tokens(record.tokens + elapsed * refill, limit);
                record.updated_ms = now;
                if record.tokens >= cost as f64 {
                    record.tokens -= cost as f64;
                    (true, record.tokens.floor() as u32, now)
                } else {
                    let missing = cost as f64 - record.tokens;
                    let wait_ms = ((missing / refill.max(0.000_001)) * 1000.0).ceil() as u64;
                    (
                        false,
                        record.tokens.floor() as u32,
                        now.saturating_add(wait_ms),
                    )
                }
            }
            RateLimitAlgorithm::SlidingWindow => {
                let cutoff = now.saturating_sub(window_ms);
                while record
                    .events
                    .front()
                    .map(|ts| *ts <= cutoff)
                    .unwrap_or(false)
                {
                    record.events.pop_front();
                }
                let available = limit.saturating_sub(record.events.len() as u32);
                if available >= cost {
                    for _ in 0..cost {
                        record.events.push_back(now);
                    }
                    (true, available - cost, now.saturating_add(window_ms))
                } else {
                    let reset = record
                        .events
                        .front()
                        .copied()
                        .unwrap_or(now)
                        .saturating_add(window_ms);
                    (false, available, reset)
                }
            }
        };
        record.last_allowed = allowed;
        json!({
            "allowed": allowed,
            "remaining": remaining,
            "reset_ms": reset_ms,
            "key": key,
            "tenant": tenant,
            "algorithm": record.algorithm,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_idempotency_claim(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        owner: String,
        ttl_ms: u64,
        retention_ms: Option<u64>,
        metadata: HashMap<String, String>,
    ) -> Value {
        if key.trim().is_empty() {
            return json!({ "claimed": false, "duplicate": false, "reason": "empty_key", "revision": revision });
        }
        if let Some(record) = self.idempotency.get(&key) {
            return json!({
                "claimed": false,
                "duplicate": true,
                "key": key,
                "record": record,
                "revision": revision,
            });
        }

        let Some(token) = self.next_token() else {
            return json!({ "claimed": false, "duplicate": false, "reason": "fencing_tokens_exhausted", "revision": revision });
        };
        // Held only for the short in-flight window while `Claimed`; `complete`
        // extends it to `retention_ms`. A retention shorter than the in-flight
        // lease would let a completed record expire early, so floor it at `ttl_ms`.
        let ttl_ms = ttl_ms.max(1);
        let retention_ms = retention_ms.unwrap_or(ttl_ms).max(ttl_ms);
        let record = IdempotencyRecord {
            key: key.clone(),
            owner,
            fencing_token: token,
            status: IdempotencyStatus::Claimed,
            first_seen_ms: now,
            lease_expires_ms: now.saturating_add(ttl_ms),
            retention_ms,
            metadata,
            result: None,
        };
        self.idempotency.insert(key.clone(), record.clone());
        json!({
            "claimed": true,
            "duplicate": false,
            "key": key,
            "record": record,
            "fencing_token": token,
            "lease_expires_ms": record.lease_expires_ms,
            "revision": revision,
        })
    }

    fn apply_idempotency_complete(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        owner: String,
        fencing_token: u64,
        result: Option<Value>,
    ) -> Value {
        let Some(record) = self.idempotency.get_mut(&key) else {
            return json!({ "completed": false, "reason": "not_found", "key": key, "revision": revision });
        };
        if record.owner != owner || record.fencing_token != fencing_token {
            return json!({ "completed": false, "reason": "not_holder", "key": key, "revision": revision });
        }
        if record.status == IdempotencyStatus::Completed {
            return json!({
                "completed": true,
                "duplicate": true,
                "key": key,
                "record": record,
                "revision": revision,
            });
        }
        record.status = IdempotencyStatus::Completed;
        record.result = result;
        // Extend the lease from the short in-flight window to the full retention
        // window, measured from completion, so the replayable record survives.
        record.lease_expires_ms = now.saturating_add(record.retention_ms.max(1));
        json!({
            "completed": true,
            "duplicate": false,
            "key": key,
            "record": record,
            "revision": revision,
        })
    }

    fn apply_idempotency_renew(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        owner: String,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Value {
        let Some(record) = self.idempotency.get_mut(&key) else {
            return json!({ "renewed": false, "reason": "not_found", "key": key, "revision": revision });
        };
        if record.owner != owner || record.fencing_token != fencing_token {
            return json!({ "renewed": false, "reason": "not_holder", "key": key, "revision": revision });
        }
        if record.status == IdempotencyStatus::Completed {
            // Nothing to renew: the retention lease already governs a completed record.
            return json!({
                "renewed": false,
                "reason": "already_completed",
                "key": key,
                "record": record,
                "revision": revision,
            });
        }
        record.lease_expires_ms = now.saturating_add(ttl_ms.max(1));
        json!({
            "renewed": true,
            "key": key,
            "lease_expires_ms": record.lease_expires_ms,
            "record": record,
            "revision": revision,
        })
    }

    fn apply_idempotency_abandon(
        &mut self,
        revision: u64,
        key: String,
        owner: String,
        fencing_token: u64,
    ) -> Value {
        let Some(record) = self.idempotency.get(&key) else {
            return json!({ "abandoned": false, "reason": "not_found", "key": key, "revision": revision });
        };
        if record.owner != owner || record.fencing_token != fencing_token {
            return json!({ "abandoned": false, "reason": "not_holder", "key": key, "revision": revision });
        }
        if record.status == IdempotencyStatus::Completed {
            // A completed record's result must survive for replay; never drop it.
            return json!({ "abandoned": false, "reason": "already_completed", "key": key, "revision": revision });
        }
        self.idempotency.remove(&key);
        json!({ "abandoned": true, "key": key, "revision": revision })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_schedule_upsert(
        &mut self,
        name: String,
        cron: Option<String>,
        one_shot_at_ms: Option<u64>,
        target: ScheduleTarget,
        delivery: DeliverySemantics,
        max_retries: u32,
        now_ms: u64,
    ) -> Value {
        // The initial cursor: a one-shot fires at its time; a cron fires at its
        // first occurrence after now. (`now_ms` is committed in the command, so
        // every replica computes the same cursor — the state machine stays
        // deterministic and never reads the wall clock itself.)
        let next_fire_ms = next_fire_for(&cron, one_shot_at_ms, now_ms);
        let definition = Schedule {
            name: name.clone(),
            cron,
            one_shot_at_ms,
            target,
            delivery,
            max_retries,
            enabled: true,
            next_fire_ms,
        };
        self.schedules
            .entry(name.clone())
            .and_modify(|record| record.definition = definition.clone())
            .or_insert_with(|| ScheduleRecord {
                definition,
                history: Vec::new(),
            });
        json!({ "scheduled": true, "name": name, "next_fire_ms": next_fire_ms })
    }

    fn apply_schedule_record_run(
        &mut self,
        name: String,
        fire_id: String,
        fired_at_ms: u64,
    ) -> Value {
        let Some(record) = self.schedules.get_mut(&name) else {
            return json!({ "recorded": false, "reason": "not_found", "name": name });
        };
        let duplicate = matches!(record.definition.delivery, DeliverySemantics::ExactlyOnce)
            && record.history.iter().any(|run| run.fire_id == fire_id);
        if !duplicate {
            record.history.push(ScheduleRun {
                fire_id: fire_id.clone(),
                fired_at_ms,
                attempts: 1,
                duplicate: false,
                target: record.definition.target.clone(),
                status: RunStatus::Delivered,
                error: None,
            });
            trim_history(record);
        }
        json!({ "recorded": !duplicate, "duplicate": duplicate, "name": name, "fire_id": fire_id })
    }

    /// Claim one due fire for delivery. Succeeds only if `fire_id_ms` is the
    /// schedule's current expected next fire (the cursor) — so a second claim of
    /// the same fire fails after the cursor advances, which is the dedup that makes
    /// firing leader-elected with no duplicates. Records a `Pending` run and
    /// advances the cursor to the following occurrence.
    fn apply_schedule_claim_fire(&mut self, name: String, fire_id_ms: u64) -> Value {
        let Some(record) = self.schedules.get_mut(&name) else {
            return json!({ "claimed": false, "reason": "not_found", "name": name });
        };
        if !record.definition.enabled {
            return json!({ "claimed": false, "reason": "disabled", "name": name });
        }
        if record.definition.next_fire_ms != Some(fire_id_ms) {
            return json!({
                "claimed": false,
                "reason": "stale_or_duplicate",
                "name": name,
                "expected": record.definition.next_fire_ms,
            });
        }
        record.history.push(ScheduleRun {
            fire_id: fire_id_ms.to_string(),
            fired_at_ms: fire_id_ms,
            attempts: 0,
            duplicate: false,
            target: record.definition.target.clone(),
            status: RunStatus::Pending,
            error: None,
        });
        trim_history(record);
        // Advance the cursor: one-shots are now exhausted; crons step to the next.
        let next = if record.definition.one_shot_at_ms.is_some() {
            None
        } else {
            next_fire_for(&record.definition.cron, None, fire_id_ms)
        };
        record.definition.next_fire_ms = next;
        json!({
            "claimed": true,
            "name": name,
            "fire_id_ms": fire_id_ms,
            "target": record.definition.target,
            "delivery": record.definition.delivery,
            "max_retries": record.definition.max_retries,
            "next_fire_ms": next,
        })
    }

    /// Record the delivery outcome of a claimed fire (after retries).
    fn apply_schedule_record_result(
        &mut self,
        name: String,
        fire_id_ms: u64,
        delivered: bool,
        attempts: u32,
        error: Option<String>,
    ) -> Value {
        let Some(record) = self.schedules.get_mut(&name) else {
            return json!({ "recorded": false, "reason": "not_found", "name": name });
        };
        let fire_id = fire_id_ms.to_string();
        if let Some(run) = record
            .history
            .iter_mut()
            .rev()
            .find(|r| r.fire_id == fire_id)
        {
            run.status = if delivered {
                RunStatus::Delivered
            } else {
                RunStatus::Failed
            };
            run.attempts = attempts;
            run.error = error;
            json!({ "recorded": true, "name": name, "fire_id_ms": fire_id_ms, "delivered": delivered })
        } else {
            json!({ "recorded": false, "reason": "no_run", "name": name, "fire_id_ms": fire_id_ms })
        }
    }

    fn apply_election_campaign(
        &mut self,
        revision: u64,
        now: u64,
        name: String,
        candidate: String,
        ttl_ms: u64,
        metadata: HashMap<String, String>,
    ) -> Value {
        if let Some(existing) = self.elections.get(&name) {
            // Idempotent re-campaign: the current leader retrying (lost response)
            // gets its existing win back, not won:false. Others still lose.
            if existing.leader == candidate {
                return json!({ "won": true, "name": name, "leadership": existing, "revision": revision });
            }
            return json!({ "won": false, "name": name, "leader": existing });
        }
        let Some(token) = self.next_token() else {
            return json!({ "won": false, "name": name, "reason": "fencing_tokens_exhausted", "revision": revision });
        };
        let leadership = Leadership {
            leader: candidate.clone(),
            fencing_token: token,
            lease_expires_ms: now.saturating_add(ttl_ms),
            ttl_ms,
            metadata,
        };
        self.elections.insert(name.clone(), leadership.clone());
        json!({ "won": true, "name": name, "leadership": leadership, "revision": revision })
    }

    fn apply_election_renew(
        &mut self,
        now: u64,
        name: String,
        candidate: String,
        fencing_token: u64,
        ttl_ms: Option<u64>,
    ) -> Value {
        let Some(leadership) = self.elections.get_mut(&name) else {
            return json!({ "renewed": false, "reason": "not_found", "name": name });
        };
        if leadership.leader != candidate || leadership.fencing_token != fencing_token {
            return json!({ "renewed": false, "reason": "not_leader", "name": name });
        }
        // Honor an explicit TTL, else reuse the TTL the leader campaigned with.
        let ttl = ttl_ms.unwrap_or(leadership.ttl_ms);
        leadership.ttl_ms = ttl;
        leadership.lease_expires_ms = now.saturating_add(ttl);
        json!({ "renewed": true, "name": name, "leadership": leadership })
    }

    fn apply_election_resign(
        &mut self,
        name: String,
        candidate: String,
        fencing_token: u64,
    ) -> Value {
        let ok = self
            .elections
            .get(&name)
            .map(|leadership| {
                leadership.leader == candidate && leadership.fencing_token == fencing_token
            })
            .unwrap_or(false);
        if ok {
            self.elections.remove(&name);
        }
        json!({ "resigned": ok, "name": name })
    }

    fn apply_service_register(
        &mut self,
        now: u64,
        service: String,
        instance_id: String,
        address: String,
        ttl_ms: u64,
        metadata: HashMap<String, String>,
    ) -> Value {
        let instance = ServiceInstance {
            instance_id: instance_id.clone(),
            address,
            lease_expires_ms: now.saturating_add(ttl_ms),
            metadata,
        };
        self.services
            .entry(service.clone())
            .or_default()
            .insert(instance_id.clone(), instance.clone());
        json!({ "registered": true, "service": service, "instance": instance })
    }

    fn apply_service_heartbeat(
        &mut self,
        now: u64,
        service: String,
        instance_id: String,
        ttl_ms: Option<u64>,
    ) -> Value {
        let Some(instance) = self
            .services
            .get_mut(&service)
            .and_then(|instances| instances.get_mut(&instance_id))
        else {
            return json!({ "heartbeat": false, "reason": "not_found", "service": service, "instance_id": instance_id });
        };
        instance.lease_expires_ms = now.saturating_add(ttl_ms.unwrap_or(30_000));
        json!({ "heartbeat": true, "service": service, "instance": instance })
    }

    fn apply_service_deregister(&mut self, service: String, instance_id: String) -> Value {
        let removed = self
            .services
            .get_mut(&service)
            .map(|instances| instances.remove(&instance_id).is_some())
            .unwrap_or(false);
        if self
            .services
            .get(&service)
            .map(|instances| instances.is_empty())
            .unwrap_or(false)
        {
            self.services.remove(&service);
        }
        json!({ "deregistered": removed, "service": service, "instance_id": instance_id })
    }

    /// Construct a point-in-time lock view without expiring a grant or
    /// promoting its queue. Promotion mints fencing tokens, so it is strictly a
    /// committed `apply_at` concern.
    fn lock_snapshot_live_at(&self, key: &str, now: u64) -> LockState {
        let grant = self
            .locks
            .held
            .get(key)
            .and_then(|token| self.locks.grants.get(token))
            .filter(|grant| grant.lease_expires_ms > now);
        let wait_queue = self
            .locks
            .queue
            .iter()
            .filter(|(_, q)| {
                waiter_is_live(q.wait_expires_ms, now) && q.keys.iter().any(|k| k == key)
            })
            .map(|(_, q)| LockWaiter {
                holder: q.holder.clone(),
                keys: q.keys.clone(),
                requested_ms: q.requested_ms,
                wait_expires_ms: q.wait_expires_ms,
            })
            .collect();
        LockState {
            key: key.to_string(),
            holder: grant.map(|g| g.holder.clone()),
            fencing_token: grant.map(|g| g.fencing_token),
            lease_expires_ms: grant.map(|g| g.lease_expires_ms),
            held_keys: grant.map(|g| g.keys.clone()).unwrap_or_default(),
            wait_queue,
        }
    }

    /// Construct a point-in-time semaphore view without expiring permits or
    /// admitting waiters. A timed-out permit is absent from the view, while its
    /// queued successor remains queued until a committed command sweeps it.
    fn semaphore_snapshot_live_at(&self, key: &str, now: u64) -> SemaphoreState {
        let Some(sem) = self.semaphores.get(key) else {
            return SemaphoreState {
                key: key.to_string(),
                limit: 0,
                holders: Vec::new(),
                available: 0,
                wait_queue: Vec::new(),
            };
        };
        SemaphoreState {
            key: key.to_string(),
            limit: sem.limit,
            available: sem.limit.saturating_sub(
                sem.holders
                    .iter()
                    .filter(|slot| slot.lease_expires_ms > now)
                    .count() as u32,
            ),
            holders: sem
                .holders
                .iter()
                .filter(|slot| slot.lease_expires_ms > now)
                .map(|slot| SemaphoreHolder {
                    holder: slot.holder.clone(),
                    fencing_token: slot.fencing_token,
                    lease_expires_ms: slot.lease_expires_ms,
                })
                .collect(),
            wait_queue: sem
                .queue
                .iter()
                .filter(|(_, q)| waiter_is_live(q.wait_expires_ms, now))
                .map(|(_, q)| LockWaiter {
                    holder: q.holder.clone(),
                    keys: vec![key.to_string()],
                    requested_ms: q.requested_ms,
                    wait_expires_ms: q.wait_expires_ms,
                })
                .collect(),
        }
    }

    fn lock_inventory_live_at(&self, now: u64) -> LockInventory {
        let mut held: Vec<LockHolding> = self
            .locks
            .grants
            .values()
            .filter(|grant| grant.lease_expires_ms > now)
            .map(|grant| LockHolding {
                holder: grant.holder.clone(),
                keys: grant.keys.clone(),
                fencing_token: grant.fencing_token,
                lease_expires_ms: grant.lease_expires_ms,
            })
            .collect();
        held.sort_by_key(|holding| holding.fencing_token);
        let wait_queue = self
            .locks
            .queue
            .iter()
            .filter(|(_, queued)| waiter_is_live(queued.wait_expires_ms, now))
            .map(|(_, queued)| LockWaiter {
                holder: queued.holder.clone(),
                keys: queued.keys.clone(),
                requested_ms: queued.requested_ms,
                wait_expires_ms: queued.wait_expires_ms,
            })
            .collect();
        LockInventory { held, wait_queue }
    }

    fn semaphore_inventory_live_at(&self, now: u64) -> Vec<SemaphoreState> {
        let mut keys: Vec<_> = self.semaphores.keys().collect();
        keys.sort();
        keys.into_iter()
            .map(|key| self.semaphore_snapshot_live_at(key, now))
            .collect()
    }

    fn election_inventory_live_at(&self, now: u64) -> Vec<ElectionEntry> {
        let mut out: Vec<_> = self
            .elections
            .iter()
            .filter(|(_, leadership)| leadership.lease_expires_ms > now)
            .map(|(name, leadership)| ElectionEntry {
                name: name.clone(),
                leadership: leadership.clone(),
            })
            .collect();
        out.sort_by(|left, right| left.name.cmp(&right.name));
        out
    }

    fn rate_limit_snapshot(&self, tenant: &str, key: &str) -> Option<RateLimitSnapshot> {
        let record = self.rate_limits.get(&rate_limit_store_key(tenant, key))?;
        Some(RateLimitSnapshot {
            key: key.to_string(),
            tenant: tenant.to_string(),
            algorithm: record.algorithm.clone(),
            allowed: record.last_allowed,
            remaining: match record.algorithm {
                RateLimitAlgorithm::TokenBucket => record.tokens.floor() as u32,
                RateLimitAlgorithm::SlidingWindow => {
                    record.limit.saturating_sub(record.events.len() as u32)
                }
            },
            reset_ms: record.updated_ms.saturating_add(record.window_ms),
        })
    }
}

/// Force a token-bucket level into `0.0..=limit`, mapping NaN/±inf to a finite
/// value. Only finite floats survive a JSON round trip, and the limiter record
/// is snapshot state.
fn clamp_tokens(tokens: f64, limit: u32) -> f64 {
    if tokens.is_nan() {
        return 0.0;
    }
    tokens.clamp(0.0, limit as f64)
}

fn rate_limit_store_key(tenant: &str, key: &str) -> String {
    format!("{tenant}:{key}")
}

/// Sort + dedup a key set so `{A,B}` and `{B,A,B}` are the same union, and so
/// conflict/grant checks are order-independent.
fn canonical_keys(keys: &[String]) -> Vec<String> {
    let mut out: Vec<String> = keys.iter().filter(|k| !k.is_empty()).cloned().collect();
    out.sort();
    out.dedup();
    out
}

/// Historical snapshots did not carry queue deadlines. Preserve those entries
/// on restore (`None`), while every newly queued request has a bounded deadline.
fn waiter_is_live(wait_expires_ms: Option<u64>, now: u64) -> bool {
    wait_expires_ms.map(|expires| expires > now).unwrap_or(true)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn valid_cron_expression(value: &str) -> bool {
    crate::cron::CronSchedule::parse(value).is_ok()
}

/// The next fire time for a schedule given a cursor: a one-shot fires once at its
/// appointed time; a cron at its first occurrence strictly after `after_ms`.
fn next_fire_for(cron: &Option<String>, one_shot_at_ms: Option<u64>, after_ms: u64) -> Option<u64> {
    if let Some(at) = one_shot_at_ms {
        return Some(at);
    }
    cron.as_deref()
        .and_then(|expr| crate::cron::CronSchedule::parse(expr).ok())
        .and_then(|c| c.next_after(after_ms))
}

/// Cap on retained run history per schedule (bounds memory + the WAL; full
/// retention is the durability follow-up in future-work.md).
const MAX_SCHEDULE_HISTORY: usize = 100;

fn trim_history(record: &mut ScheduleRecord) {
    let len = record.history.len();
    if len > MAX_SCHEDULE_HISTORY {
        record.history.drain(0..len - MAX_SCHEDULE_HISTORY);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill_tenant_cancellation_capacity(ledger: &mut CancellationLedger, tenant: &str, now: u64) {
        for index in 0..MAX_CANCELLATION_TOMBSTONES_PER_TENANT {
            assert_eq!(
                ledger.remember(format!("{index:064x}"), tenant.to_string(), now),
                RememberCancellation::Remembered
            );
        }
    }

    fn acquire(sm: &StateMachine, keys: &[&str], holder: &str, wait: bool) -> Value {
        sm.apply(Command::LockAcquireV2 {
            keys: keys.iter().map(|s| s.to_string()).collect(),
            holder: holder.to_string(),
            ttl_ms: 30_000,
            wait,
            wait_timeout_ms: None,
        })
        .output
    }

    fn semaphore_acquire(
        sm: &StateMachine,
        key: &str,
        holder: &str,
        limit: u32,
        wait: bool,
    ) -> Value {
        sm.apply(Command::SemaphoreAcquireV2 {
            key: key.to_string(),
            holder: holder.to_string(),
            limit,
            ttl_ms: 30_000,
            wait,
            wait_timeout_ms: None,
        })
        .output
    }

    // Proves whether a lost-response RETRY of an acquire is safe server-side —
    // i.e. whether the client's Idempotency-Key work (fiducia-clients #8) has any
    // server semantics to lean on, or is cosmetic. A retried acquire by the SAME
    // holder must not consume a second permit / grant a second token.
    #[test]
    fn semaphore_reacquire_by_same_holder_is_idempotent() {
        let sm = StateMachine::new();
        let first = semaphore_acquire(&sm, "pool", "holder-a", 3, false);
        assert_eq!(first["acquired"], true);
        let token1 = first["fencing_token"].clone();

        // Simulate the client retrying because the first response was lost.
        let retry = semaphore_acquire(&sm, "pool", "holder-a", 3, false);

        let held_by_a = sm
            .semaphore_get("pool")
            .holders
            .iter()
            .filter(|h| h.holder == "holder-a")
            .count();
        assert_eq!(
            held_by_a, 1,
            "a holder's retried acquire must not consume a second permit"
        );
        assert_eq!(
            retry["fencing_token"], token1,
            "retry should return the original fencing token"
        );
        assert_eq!(retry["renewed"], false);
    }

    #[test]
    fn semaphore_acquire_cannot_silently_change_the_configured_limit() {
        let sm = StateMachine::new();
        let first = semaphore_acquire(&sm, "pool", "holder-a", 1, false);
        assert_eq!(first["acquired"], true);

        let raised = semaphore_acquire(&sm, "pool", "holder-b", 2, false);
        assert_eq!(raised["acquired"], false);
        assert_eq!(raised["reason"], "limit_mismatch");
        assert_eq!(raised["limit"], 1);
        assert_eq!(raised["requested_limit"], 2);
        assert_eq!(sm.semaphore_get("pool").holders.len(), 1);
        assert_eq!(sm.semaphore_get("pool").limit, 1);
    }

    #[test]
    fn lock_reacquire_by_same_holder_returns_the_same_grant() {
        let sm = StateMachine::new();
        let first = acquire(&sm, &["orders/42"], "worker-a", false);
        assert_eq!(first["acquired"], true);
        let token1 = first["fencing_token"].clone();

        // Lost-response retry by the same holder that already holds the lock.
        let retry = acquire(&sm, &["orders/42"], "worker-a", false);
        assert_eq!(
            retry["acquired"], true,
            "re-acquiring a lock you already hold should succeed idempotently"
        );
        assert_eq!(
            retry["fencing_token"], token1,
            "retry should return the original fencing token"
        );
        assert_eq!(retry["renewed"], false);
    }

    #[test]
    fn lock_reacquire_does_not_renew_without_the_fencing_token() {
        let sm = StateMachine::new();
        let command = || Command::LockAcquireV2 {
            keys: vec!["customers/acme".to_string()],
            holder: "billing-request-7".to_string(),
            ttl_ms: 100,
            wait: false,
            wait_timeout_ms: None,
        };
        let first = sm.apply_at(command(), 1_000).output;
        assert_eq!(first["lease_expires_ms"], 1_100);
        let token = first["fencing_token"].clone();

        let retry = sm.apply_at(command(), 1_050).output;
        assert_eq!(retry["acquired"], true);
        assert_eq!(retry["renewed"], false);
        assert_eq!(retry["fencing_token"], token);
        assert_eq!(retry["lease_expires_ms"], 1_100);
        let fencing_token = token.as_u64().expect("numeric fencing token");
        assert_eq!(
            sm.store
                .lock()
                .unwrap()
                .locks
                .grants
                .get(&fencing_token)
                .expect("idempotently returned grant")
                .lease_expires_ms,
            1_100
        );
    }

    #[test]
    fn explicit_lock_renew_is_token_holder_and_exact_key_set_bound() {
        let sm = StateMachine::new();
        let first = sm.apply_at(
            Command::LockAcquire {
                keys: vec!["b".to_string(), "a".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 100,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        let token = first.output["fencing_token"].as_u64().unwrap();

        let wrong_holder = sm.apply_at(
            Command::LockRenew {
                keys: vec!["a".to_string(), "b".to_string()],
                holder: "intruder".to_string(),
                fencing_token: token,
                ttl_ms: 500,
            },
            1_010,
        );
        assert_eq!(wrong_holder.output["renewed"], false);
        assert_eq!(wrong_holder.output["reason"], "not_holder");

        let wrong_keys = sm.apply_at(
            Command::LockRenew {
                keys: vec!["a".to_string()],
                holder: "owner".to_string(),
                fencing_token: token,
                ttl_ms: 500,
            },
            1_020,
        );
        assert_eq!(wrong_keys.output["renewed"], false);
        assert_eq!(wrong_keys.output["reason"], "key_mismatch");

        let renewed = sm.apply_at(
            Command::LockRenew {
                keys: vec!["a".to_string(), "b".to_string()],
                holder: "owner".to_string(),
                fencing_token: token,
                ttl_ms: 500,
            },
            1_050,
        );
        assert_eq!(renewed.output["renewed"], true);
        assert_eq!(renewed.output["fencing_token"], token);
        assert_eq!(renewed.output["lease_expires_ms"], 1_550);
    }

    #[test]
    fn semaphore_reacquire_requires_explicit_token_bound_renewal_to_extend() {
        let sm = StateMachine::new();
        let first = sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "pool".to_string(),
                holder: "worker".to_string(),
                limit: 1,
                ttl_ms: 100,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        let token = first.output["fencing_token"].as_u64().unwrap();

        let retry = sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "pool".to_string(),
                holder: "worker".to_string(),
                limit: 1,
                ttl_ms: 200,
                wait: false,
                wait_timeout_ms: None,
            },
            1_050,
        );
        assert_eq!(retry.output["renewed"], false);
        assert_eq!(retry.output["fencing_token"], token);
        assert_eq!(retry.output["lease_expires_ms"], 1_100);

        let wrong_holder = sm.apply_at(
            Command::SemaphoreRenew {
                key: "pool".to_string(),
                holder: "intruder".to_string(),
                fencing_token: token,
                ttl_ms: 500,
            },
            1_060,
        );
        assert_eq!(wrong_holder.output["renewed"], false);
        assert_eq!(wrong_holder.output["reason"], "not_holder");

        let renewed = sm.apply_at(
            Command::SemaphoreRenew {
                key: "pool".to_string(),
                holder: "worker".to_string(),
                fencing_token: token,
                ttl_ms: 500,
            },
            1_090,
        );
        assert_eq!(renewed.output["renewed"], true);
        assert_eq!(renewed.output["fencing_token"], token);
        assert_eq!(renewed.output["lease_expires_ms"], 1_590);
    }

    #[test]
    fn queued_lock_retry_does_not_duplicate_or_extend_its_deadline() {
        let sm = StateMachine::new();
        let held = sm.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["resource".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 10_000,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        let token = held.output["fencing_token"].as_u64().unwrap();
        let queued = sm.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["resource".to_string()],
                holder: "waiter".to_string(),
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(100),
            },
            1_001,
        );
        assert_eq!(queued.output["wait_expires_ms"], 1_101);

        let retry = sm.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["resource".to_string()],
                holder: "waiter".to_string(),
                ttl_ms: 9_000,
                wait: true,
                wait_timeout_ms: Some(9_000),
            },
            1_050,
        );
        assert_eq!(retry.output["queued"], true);
        assert_eq!(retry.output["position"], 1);
        assert_eq!(retry.output["wait_expires_ms"], 1_101);
        assert_eq!(sm.store.lock().unwrap().locks.queue.len(), 1);

        // The sweep runs before release/promotion, so the timed-out waiter can
        // never receive a grant in the same committed command.
        let released = sm.apply_at(
            Command::LockRelease {
                holder: "owner".to_string(),
                fencing_token: token,
            },
            1_101,
        );
        assert_eq!(released.output["promoted"], json!([]));
        assert!(sm.store.lock().unwrap().locks.queue.is_empty());
    }

    #[test]
    fn queued_semaphore_retry_does_not_duplicate_or_extend_its_deadline() {
        let sm = StateMachine::new();
        let held = sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "pool".to_string(),
                holder: "owner".to_string(),
                limit: 1,
                ttl_ms: 10_000,
                wait: false,
                wait_timeout_ms: None,
            },
            2_000,
        );
        let token = held.output["fencing_token"].as_u64().unwrap();
        let queued = sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "pool".to_string(),
                holder: "waiter".to_string(),
                limit: 1,
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(100),
            },
            2_001,
        );
        assert_eq!(queued.output["wait_expires_ms"], 2_101);
        let retry = sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "pool".to_string(),
                holder: "waiter".to_string(),
                limit: 1,
                ttl_ms: 9_000,
                wait: true,
                wait_timeout_ms: Some(9_000),
            },
            2_050,
        );
        assert_eq!(retry.output["wait_expires_ms"], 2_101);
        assert_eq!(sm.store.lock().unwrap().semaphores["pool"].queue.len(), 1);

        let released = sm.apply_at(
            Command::SemaphoreRelease {
                key: "pool".to_string(),
                holder: "owner".to_string(),
                fencing_token: token,
            },
            2_101,
        );
        assert_eq!(released.output["promoted"], json!([]));
        assert!(sm.store.lock().unwrap().semaphores["pool"].queue.is_empty());
    }

    #[test]
    fn queued_lock_and_semaphore_cancellation_is_committed_and_idempotent() {
        let sm = StateMachine::new();
        acquire(&sm, &["lock"], "lock-owner", false);
        acquire(&sm, &["lock"], "lock-waiter", true);
        semaphore_acquire(&sm, "pool", "permit-owner", 1, false);
        semaphore_acquire(&sm, "pool", "permit-waiter", 1, true);

        let lock_cancel = || {
            sm.apply(Command::LockCancel {
                keys: vec!["lock".to_string()],
                holder: "lock-waiter".to_string(),
            })
            .output
        };
        assert_eq!(lock_cancel()["cancelled"], true);
        let missing_lock = lock_cancel();
        assert_eq!(missing_lock["cancelled"], false);
        assert_eq!(missing_lock["acquired"], false);
        assert_eq!(missing_lock["reason"], "not_found");

        let semaphore_cancel = || {
            sm.apply(Command::SemaphoreCancel {
                key: "pool".to_string(),
                holder: "permit-waiter".to_string(),
            })
            .output
        };
        assert_eq!(semaphore_cancel()["cancelled"], true);
        let missing_permit = semaphore_cancel();
        assert_eq!(missing_permit["cancelled"], false);
        assert_eq!(missing_permit["acquired"], false);
        assert_eq!(missing_permit["reason"], "not_found");
        assert!(sm.lock_get("lock").wait_queue.is_empty());
        assert!(sm.semaphore_get("pool").wait_queue.is_empty());
    }

    #[test]
    fn cancellation_tombstone_suppresses_a_late_ambiguous_acquire() {
        let sm = StateMachine::new();
        let base = 10_000;

        let cancelled = sm.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["orders/42".to_string(), "inventory/7".to_string()],
                holder: "worker-a".to_string(),
                request_id: "unique-attempt-a".to_string(),
            },
            base,
        );
        assert_eq!(cancelled.output["cancelled"], true);
        let late = sm.apply_at(
            Command::LockAcquireAttempt {
                keys: vec!["inventory/7".to_string(), "orders/42".to_string()],
                holder: "worker-a".to_string(),
                request_id: "unique-attempt-a".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: Some(30_000),
            },
            base + 1,
        );
        assert_eq!(late.output["acquired"], false);
        assert_eq!(late.output["queued"], false);
        assert!(sm.lock_get("orders/42").holder.is_none());
        assert!(sm.lock_get("orders/42").wait_queue.is_empty());

        let cancelled = sm.apply_at(
            Command::SemaphoreCancelAttempt {
                key: "pool".to_string(),
                holder: "worker-b".to_string(),
                request_id: "unique-attempt-b".to_string(),
            },
            base + 2,
        );
        assert_eq!(cancelled.output["cancelled"], true);
        let late = sm.apply_at(
            Command::SemaphoreAcquireAttempt {
                key: "pool".to_string(),
                holder: "worker-b".to_string(),
                request_id: "unique-attempt-b".to_string(),
                limit: 1,
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: Some(30_000),
            },
            base + 3,
        );
        assert_eq!(late.output["acquired"], false);
        assert_eq!(late.output["queued"], false);
        assert!(sm.semaphore_get("pool").holders.is_empty());
        assert!(sm.semaphore_get("pool").wait_queue.is_empty());

        let snapshot = sm.snapshot().unwrap();
        let restored = StateMachine::new();
        restored.restore(&snapshot).unwrap();
        let replayed_late = restored.apply_at(
            Command::LockAcquireAttempt {
                keys: vec!["orders/42".to_string(), "inventory/7".to_string()],
                holder: "worker-a".to_string(),
                request_id: "unique-attempt-a".to_string(),
                ttl_ms: 30_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base + 4,
        );
        assert_eq!(replayed_late.output["acquired"], false);
    }

    #[test]
    fn cancellation_is_scoped_to_one_request_id_not_the_stable_holder() {
        let sm = StateMachine::new();
        sm.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["resource".to_string()],
                holder: "\u{1}org-a\u{1}worker".to_string(),
                request_id: "attempt-a".to_string(),
            },
            1_000,
        );

        let distinct = sm.apply_at(
            Command::LockAcquireAttempt {
                keys: vec!["resource".to_string()],
                holder: "\u{1}org-a\u{1}worker".to_string(),
                request_id: "attempt-b".to_string(),
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            },
            1_001,
        );
        assert_eq!(distinct.output["acquired"], true);

        let store = sm.store.lock().unwrap();
        assert_eq!(store.lock_cancellations.entries.len(), 1);
        assert!(store
            .lock_cancellations
            .entries
            .keys()
            .all(|digest| digest.len() == 64));
        assert_eq!(store.lock_cancellations.tenant_counts.len(), 1);
    }

    #[test]
    fn cancellation_capacity_never_hides_raced_lock_or_semaphore_authority() {
        let sm = StateMachine::new();
        let delimiter = fiducia_routing::ORG_SCOPE_DELIM;
        let lock_holder = format!("{delimiter}org-lock{delimiter}waiter");
        let permit_holder = format!("{delimiter}org-permit{delimiter}waiter");
        {
            let mut store = sm.store.lock().unwrap();
            let lock_tenant = cancellation_tenant(&lock_holder);
            fill_tenant_cancellation_capacity(&mut store.lock_cancellations, &lock_tenant, 0);
            let permit_tenant = cancellation_tenant(&permit_holder);
            fill_tenant_cancellation_capacity(
                &mut store.semaphore_cancellations,
                &permit_tenant,
                0,
            );
        }

        sm.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["capacity-lock".to_string()],
                holder: "lock-owner".to_string(),
                ttl_ms: 10,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        sm.apply_at(
            Command::LockAcquireAttempt {
                keys: vec!["capacity-lock".to_string()],
                holder: lock_holder.clone(),
                request_id: "raced-lock-attempt".to_string(),
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            1_001,
        );
        let lock_cancel = sm.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["capacity-lock".to_string()],
                holder: lock_holder,
                request_id: "raced-lock-attempt".to_string(),
            },
            1_010,
        );
        assert_eq!(lock_cancel.output["acquired"], true);
        assert_eq!(lock_cancel.output["cancelled"], false);
        assert!(lock_cancel.output["fencing_token"].as_u64().unwrap() > 1);
        assert_ne!(
            lock_cancel.output["reason"], "cancellation_capacity",
            "a full safety ledger must not conceal authority the caller now owns"
        );

        sm.apply_at(
            Command::SemaphoreAcquireV2 {
                key: "capacity-pool".to_string(),
                holder: "permit-owner".to_string(),
                limit: 1,
                ttl_ms: 10,
                wait: false,
                wait_timeout_ms: None,
            },
            2_000,
        );
        sm.apply_at(
            Command::SemaphoreAcquireAttempt {
                key: "capacity-pool".to_string(),
                holder: permit_holder.clone(),
                request_id: "raced-permit-attempt".to_string(),
                limit: 1,
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            2_001,
        );
        let permit_cancel = sm.apply_at(
            Command::SemaphoreCancelAttempt {
                key: "capacity-pool".to_string(),
                holder: permit_holder,
                request_id: "raced-permit-attempt".to_string(),
            },
            2_010,
        );
        assert_eq!(permit_cancel.output["acquired"], true);
        assert_eq!(permit_cancel.output["cancelled"], false);
        assert_ne!(permit_cancel.output["reason"], "cancellation_capacity");
    }

    #[test]
    fn cancellation_capacity_is_fail_closed_and_preserves_the_waiter() {
        let sm = StateMachine::new();
        let delimiter = fiducia_routing::ORG_SCOPE_DELIM;
        let holder = format!("{delimiter}org-full{delimiter}waiter");
        {
            let mut store = sm.store.lock().unwrap();
            fill_tenant_cancellation_capacity(
                &mut store.lock_cancellations,
                &cancellation_tenant(&holder),
                0,
            );
        }
        sm.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["busy".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 5_000,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        sm.apply_at(
            Command::LockAcquireAttempt {
                keys: vec!["busy".to_string()],
                holder: holder.clone(),
                request_id: "still-queued".to_string(),
                ttl_ms: 5_000,
                wait: true,
                wait_timeout_ms: Some(5_000),
            },
            1_001,
        );
        let cancel = sm.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["busy".to_string()],
                holder: holder.clone(),
                request_id: "still-queued".to_string(),
            },
            1_002,
        );
        assert_eq!(cancel.output["reason"], "cancellation_capacity");
        assert_eq!(cancel.output["cancelled"], false);
        let store = sm.store.lock().unwrap();
        assert!(store
            .locks
            .queue
            .get(&(holder, vec!["busy".to_string()]))
            .is_some());
    }

    #[test]
    fn cancellation_capacity_survives_snapshot_and_recovers_after_expiry() {
        let original = StateMachine::new();
        let delimiter = fiducia_routing::ORG_SCOPE_DELIM;
        let holder = format!("{delimiter}org-snapshot{delimiter}worker");
        let tenant = cancellation_tenant(&holder);
        {
            let mut store = original.store.lock().unwrap();
            fill_tenant_cancellation_capacity(&mut store.lock_cancellations, &tenant, 0);
        }

        let restored = StateMachine::new();
        restored.restore(&original.snapshot().unwrap()).unwrap();
        let before_expiry = restored.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["snapshot-capacity".to_string()],
                holder: holder.clone(),
                request_id: "after-snapshot".to_string(),
            },
            CANCELLATION_TOMBSTONE_TTL_MS - 1,
        );
        assert_eq!(before_expiry.output["reason"], "cancellation_capacity");
        assert_eq!(
            restored
                .store
                .lock()
                .unwrap()
                .lock_cancellations
                .entries
                .len(),
            MAX_CANCELLATION_TOMBSTONES_PER_TENANT as usize
        );

        let after_expiry = restored.apply_at(
            Command::LockCancelAttempt {
                keys: vec!["snapshot-capacity".to_string()],
                holder,
                request_id: "after-snapshot".to_string(),
            },
            CANCELLATION_TOMBSTONE_TTL_MS,
        );
        assert_eq!(after_expiry.output["cancelled"], true);
        assert!(after_expiry.output.get("reason").is_none());
        let store = restored.store.lock().unwrap();
        assert_eq!(store.lock_cancellations.entries.len(), 1);
        assert_eq!(store.lock_cancellations.tenant_counts[&tenant], 1);
        store.lock_cancellations.validate("lock").unwrap();
    }

    #[test]
    fn cancellation_global_reserve_protects_a_new_tenants_fair_share() {
        let limits = CancellationLimits {
            per_tenant: 10,
            total: 10,
            global_reserve: 4,
            fair_share_per_tenant: 2,
        };
        let mut ledger = CancellationLedger::default();
        for index in 0..6 {
            assert_eq!(
                ledger.remember_with_limits(
                    format!("bully-{index}"),
                    "bully".to_string(),
                    0,
                    limits,
                ),
                RememberCancellation::Remembered
            );
        }
        assert_eq!(
            ledger.remember_with_limits(
                "bully-reserved".to_string(),
                "bully".to_string(),
                0,
                limits,
            ),
            RememberCancellation::CapacityExceeded
        );
        for index in 0..2 {
            assert_eq!(
                ledger.remember_with_limits(
                    format!("new-{index}"),
                    "new-tenant".to_string(),
                    0,
                    limits,
                ),
                RememberCancellation::Remembered
            );
        }
        assert_eq!(
            ledger.remember_with_limits(
                "new-over-share".to_string(),
                "new-tenant".to_string(),
                0,
                limits,
            ),
            RememberCancellation::CapacityExceeded
        );
        for index in 0..2 {
            assert_eq!(
                ledger.remember_with_limits(
                    format!("other-{index}"),
                    "other-tenant".to_string(),
                    0,
                    limits,
                ),
                RememberCancellation::Remembered
            );
        }
        assert_eq!(ledger.entries.len(), limits.total);
        assert_eq!(
            ledger.remember_with_limits(
                "hard-cap".to_string(),
                "third-tenant".to_string(),
                0,
                limits,
            ),
            RememberCancellation::CapacityExceeded
        );
        assert_eq!(
            ledger.remember_with_limits("new-0".to_string(), "new-tenant".to_string(), 0, limits,),
            RememberCancellation::Remembered,
            "idempotent retries remain accepted even at the hard cap"
        );
    }

    #[test]
    fn cancellation_reports_authority_when_promotion_won_the_race() {
        let sm = StateMachine::new();
        sm.apply_at(
            Command::LockAcquire {
                keys: vec!["lock".to_string()],
                holder: "lock-owner".to_string(),
                ttl_ms: 10,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        sm.apply_at(
            Command::LockAcquire {
                keys: vec!["lock".to_string()],
                holder: "lock-waiter".to_string(),
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            1_001,
        );
        // The cancel command's pre-apply sweep expires the owner and promotes
        // the still-live waiter. Cancel must return that raced authority rather
        // than claiming the queue entry was simply absent.
        let lock_cancel = sm.apply_at(
            Command::LockCancel {
                keys: vec!["lock".to_string()],
                holder: "lock-waiter".to_string(),
            },
            1_010,
        );
        assert_eq!(lock_cancel.output["cancelled"], false);
        assert_eq!(lock_cancel.output["acquired"], true);
        let promoted_lock_token = lock_cancel.output["fencing_token"].as_u64().unwrap();
        assert!(promoted_lock_token > 1);
        assert_eq!(
            sm.store.lock().unwrap().locks.grants[&promoted_lock_token].holder,
            "lock-waiter"
        );

        sm.apply_at(
            Command::SemaphoreAcquire {
                key: "pool".to_string(),
                holder: "permit-owner".to_string(),
                limit: 1,
                ttl_ms: 10,
                wait: false,
                wait_timeout_ms: None,
            },
            2_000,
        );
        sm.apply_at(
            Command::SemaphoreAcquire {
                key: "pool".to_string(),
                holder: "permit-waiter".to_string(),
                limit: 1,
                ttl_ms: 1_000,
                wait: true,
                wait_timeout_ms: Some(1_000),
            },
            2_001,
        );
        let semaphore_cancel = sm.apply_at(
            Command::SemaphoreCancel {
                key: "pool".to_string(),
                holder: "permit-waiter".to_string(),
            },
            2_010,
        );
        assert_eq!(semaphore_cancel.output["cancelled"], false);
        assert_eq!(semaphore_cancel.output["acquired"], true);
        assert!(semaphore_cancel.output["fencing_token"].as_u64().unwrap() > promoted_lock_token);
        assert_eq!(
            sm.store.lock().unwrap().semaphores["pool"].holders[0].holder,
            "permit-waiter"
        );
    }

    #[test]
    fn election_recampaign_by_current_leader_is_idempotent() {
        let sm = StateMachine::new();
        let campaign = |candidate: &str| {
            sm.apply(Command::ElectionCampaign {
                name: "primary".to_string(),
                candidate: candidate.to_string(),
                ttl_ms: 30_000,
                metadata: HashMap::new(),
            })
            .output
        };
        let first = campaign("node-a");
        assert_eq!(first["won"], true);
        let token1 = first["leadership"]["fencing_token"].clone();

        // Lost-response retry by the current leader.
        let retry = campaign("node-a");
        assert_eq!(
            retry["won"], true,
            "current leader re-campaigning should win idempotently"
        );
        assert_eq!(retry["leadership"]["fencing_token"], token1);

        // A different candidate still loses.
        assert_eq!(campaign("node-b")["won"], false);
    }

    #[test]
    fn lock_inventory_lists_every_grant_and_the_whole_wait_queue() {
        let sm = StateMachine::new();
        // Two independent grants on disjoint key sets, plus one waiter blocked on
        // a set that overlaps the first grant.
        acquire(&sm, &["orders/42"], "worker-a", false);
        acquire(&sm, &["inventory/sku-7"], "worker-b", false);
        let queued = acquire(&sm, &["orders/42", "inventory/sku-9"], "worker-c", true);
        assert_eq!(queued["queued"], true);

        let inv = sm.lock_inventory();
        assert_eq!(inv.held.len(), 2, "two active grants");
        // Sorted by fencing token, so worker-a (acquired first) comes first.
        assert_eq!(inv.held[0].holder, "worker-a");
        assert_eq!(inv.held[1].holder, "worker-b");
        assert!(inv.held[0].fencing_token < inv.held[1].fencing_token);
        assert_eq!(inv.wait_queue.len(), 1, "one waiter");
        assert_eq!(inv.wait_queue[0].holder, "worker-c");
        assert_eq!(
            inv.wait_queue[0].keys,
            vec!["inventory/sku-9".to_string(), "orders/42".to_string()]
        );
    }

    #[test]
    fn semaphore_inventory_snapshots_each_semaphore_sorted_by_key() {
        let sm = StateMachine::new();
        let acquire_permit = |key: &str, holder: &str| {
            sm.apply(Command::SemaphoreAcquire {
                key: key.to_string(),
                holder: holder.to_string(),
                limit: 1,
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            })
        };
        acquire_permit("db/pool-z", "h1"); // holds the single permit
        acquire_permit("db/pool-z", "h2"); // queues
        acquire_permit("db/pool-a", "h3"); // holds the single permit

        let inv = sm.semaphore_inventory();
        assert_eq!(inv.len(), 2);
        // Sorted by key: pool-a before pool-z.
        assert_eq!(inv[0].key, "db/pool-a");
        assert_eq!(inv[1].key, "db/pool-z");
        assert_eq!(inv[1].holders.len(), 1);
        assert_eq!(inv[1].available, 0);
        assert_eq!(inv[1].wait_queue.len(), 1);
    }

    #[test]
    fn follower_observation_of_an_expired_semaphore_is_pure_and_never_mints() {
        // This models a follower serving a local inventory fan-out. The first
        // permit has expired, while the second holder is queued. A read may
        // hide the expired permit, but must not promote the waiter or alter the
        // applied snapshot: promotion mints a fencing token and belongs only in
        // an ordered Raft apply.
        let sm = StateMachine::new();
        let expired_at = now_ms().saturating_sub(10_000);
        let held = sm.apply_at(
            Command::SemaphoreAcquire {
                key: "pool".to_string(),
                holder: "expired-holder".to_string(),
                limit: 1,
                ttl_ms: 1,
                wait: true,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(held.output["fencing_token"], 1);
        let queued = sm.apply_at(
            Command::SemaphoreAcquire {
                key: "pool".to_string(),
                holder: "waiter".to_string(),
                limit: 1,
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            expired_at,
        );
        assert_eq!(queued.output["queued"], true);

        let before = sm.snapshot().unwrap();
        let inventory = sm.semaphore_inventory();
        assert!(
            inventory[0].holders.is_empty(),
            "expired permits are not live"
        );
        assert_eq!(inventory[0].wait_queue[0].holder, "waiter");
        assert_eq!(
            sm.snapshot().unwrap(),
            before,
            "a local read must not mutate applied state or mint a fencing token"
        );
    }

    #[test]
    fn election_inventory_lists_current_leaders_sorted_by_name() {
        let sm = StateMachine::new();
        let campaign = |name: &str, candidate: &str| {
            sm.apply(Command::ElectionCampaign {
                name: name.to_string(),
                candidate: candidate.to_string(),
                ttl_ms: 30_000,
                metadata: HashMap::new(),
            })
        };
        campaign("scheduler", "node-z");
        campaign("gc-leader", "node-a");

        let inv = sm.election_inventory();
        assert_eq!(inv.len(), 2);
        assert_eq!(inv[0].name, "gc-leader");
        assert_eq!(inv[0].leadership.leader, "node-a");
        assert_eq!(inv[1].name, "scheduler");
        assert_eq!(inv[1].leadership.leader, "node-z");
    }

    #[test]
    fn expired_election_get_and_inventory_are_live_but_do_not_sweep_state() {
        let sm = StateMachine::new();
        let expired_at = now_ms().saturating_sub(10_000);
        sm.apply_at(
            Command::ElectionCampaign {
                name: "expired-election".to_string(),
                candidate: "node-a".to_string(),
                ttl_ms: 1,
                metadata: HashMap::new(),
            },
            expired_at,
        );

        let before = sm.snapshot().unwrap();
        assert!(sm.election_get("expired-election").is_none());
        assert!(sm.election_inventory().is_empty());
        assert_eq!(
            sm.snapshot().unwrap(),
            before,
            "an expired lease is hidden by reads, not deleted outside the Raft log"
        );
    }

    #[test]
    fn single_key_lock_queues_and_transfers_with_monotonic_fencing_tokens() {
        let sm = StateMachine::new();
        let first = acquire(&sm, &["orders/checkout"], "worker-a", false);
        assert_eq!(first["acquired"], true);
        let token = first["fencing_token"].as_u64().unwrap();

        let second = acquire(&sm, &["orders/checkout"], "worker-b", true);
        assert_eq!(second["queued"], true);
        assert_eq!(second["position"], 1);

        let release = sm.apply(Command::LockRelease {
            holder: "worker-a".to_string(),
            fencing_token: token,
        });
        assert_eq!(release.output["released"], true);
        // worker-b is promoted with a strictly newer fencing token.
        let promoted = &release.output["promoted"][0];
        assert_eq!(promoted["holder"], "worker-b");
        assert!(promoted["fencing_token"].as_u64().unwrap() > token);
        // ...and now holds the key.
        assert_eq!(
            sm.lock_get("orders/checkout").holder.as_deref(),
            Some("worker-b")
        );
    }

    #[test]
    fn multi_key_union_lock_conflicts_on_any_shared_member() {
        let sm = StateMachine::new();
        // Hold the union {a, b}.
        let g1 = acquire(&sm, &["a", "b"], "holder-1", false);
        assert_eq!(g1["acquired"], true);

        // {b, c} overlaps on b → must conflict and queue.
        let g2 = acquire(&sm, &["b", "c"], "holder-2", true);
        assert_eq!(g2["acquired"], false);
        assert_eq!(g2["queued"], true);
        assert_eq!(g2["conflicts"], json!(["b"]));

        // {d, e} is disjoint → grants immediately even while {a,b} is held.
        let g3 = acquire(&sm, &["d", "e"], "holder-3", false);
        assert_eq!(g3["acquired"], true);

        // Release {a,b}; holder-2's {b,c} is now grantable and is promoted.
        let token1 = g1["fencing_token"].as_u64().unwrap();
        let rel = sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: token1,
        });
        assert_eq!(rel.output["promoted"][0]["holder"], "holder-2");
        assert_eq!(sm.lock_get("c").holder.as_deref(), Some("holder-2"));
        assert_eq!(sm.lock_get("b").holder.as_deref(), Some("holder-2"));
    }

    #[test]
    fn union_queue_is_fifo_fair_and_deadlock_free() {
        let sm = StateMachine::new();
        // holder-1 holds {x}. Two waiters queue behind it: {x,y} then {y}.
        let g1 = acquire(&sm, &["x"], "holder-1", false);
        let w_xy = acquire(&sm, &["x", "y"], "holder-2", true);
        assert_eq!(w_xy["queued"], true);
        // {y} alone is free, BUT holder-2 ({x,y}) is ahead and reserves y, so a
        // later {y} request must wait behind it (no barging → no starvation).
        let w_y = acquire(&sm, &["y"], "holder-3", true);
        assert_eq!(w_y["queued"], true);
        assert_eq!(w_y["position"], 2);

        // Release {x}: holder-2 ({x,y}) is promoted first (FIFO); holder-3 still waits.
        let rel = sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: g1["fencing_token"].as_u64().unwrap(),
        });
        assert_eq!(rel.output["promoted"][0]["holder"], "holder-2");
        assert_eq!(sm.lock_get("y").holder.as_deref(), Some("holder-2"));
        assert_eq!(sm.lock_get("y").wait_queue[0].holder, "holder-3");
    }

    #[test]
    fn wait_queue_is_recreated_by_replaying_the_log_after_a_restart() {
        // The wait queue is derived state inside the replicated state machine, so a
        // node that goes down recovers it by **replaying its committed log** — there
        // is nothing extra to persist. Apply a command log to one machine, then
        // replay the identical log into a fresh one (the restart) and confirm the
        // rebuilt machine holds the same grant and the same FIFO wait queue order.
        let log = vec![
            Command::LockAcquire {
                keys: vec!["x".to_string()],
                holder: "holder-1".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            Command::LockAcquire {
                keys: vec!["x".to_string()],
                holder: "holder-2".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            Command::LockAcquire {
                keys: vec!["x".to_string()],
                holder: "holder-3".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
        ];

        let crashed = StateMachine::new();
        for command in &log {
            crashed.apply(command.clone());
        }
        // holder-1 holds {x}; holder-2 then holder-3 wait behind it.
        let before = crashed.lock_get("x");
        assert_eq!(before.holder.as_deref(), Some("holder-1"));
        assert_eq!(before.wait_queue.len(), 2);

        // Restart: a fresh state machine replays the same committed log.
        let recovered = StateMachine::new();
        for command in &log {
            recovered.apply(command.clone());
        }

        let after = recovered.lock_get("x");
        assert_eq!(after.holder.as_deref(), Some("holder-1"), "grant recovered");
        let recovered_waiters: Vec<String> =
            after.wait_queue.iter().map(|w| w.holder.clone()).collect();
        assert_eq!(
            recovered_waiters,
            vec!["holder-2".to_string(), "holder-3".to_string()],
            "the FIFO wait queue is rebuilt in order by log replay"
        );
    }

    #[test]
    fn union_lock_canonicalizes_keys_and_releases_every_member() {
        let sm = StateMachine::new();
        let grant = acquire(&sm, &["z", "x", "x", "", "y"], "holder-1", false);
        assert_eq!(grant["acquired"], true);
        assert_eq!(grant["keys"], json!(["x", "y", "z"]));

        for key in ["x", "y", "z"] {
            let state = sm.lock_get(key);
            assert_eq!(state.holder.as_deref(), Some("holder-1"));
            assert_eq!(state.held_keys, vec!["x", "y", "z"]);
        }

        let release = sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: grant["fencing_token"].as_u64().unwrap(),
        });
        assert_eq!(release.output["released"], true);

        for key in ["x", "y", "z"] {
            assert!(sm.lock_get(key).holder.is_none());
        }
    }

    #[test]
    fn no_wait_union_conflict_leaves_free_members_unheld_and_unqueued() {
        let sm = StateMachine::new();
        let first = acquire(&sm, &["x", "y"], "holder-1", false);
        assert_eq!(first["acquired"], true);

        let no_wait = acquire(&sm, &["y", "z"], "holder-2", false);
        assert_eq!(no_wait["acquired"], false);
        assert_eq!(no_wait["queued"], false);
        assert_eq!(no_wait["conflicts"], json!(["y"]));
        assert!(sm.lock_get("z").holder.is_none());
        assert!(sm.lock_get("y").wait_queue.is_empty());

        sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: first["fencing_token"].as_u64().unwrap(),
        });

        let retry = acquire(&sm, &["y", "z"], "holder-3", false);
        assert_eq!(retry["acquired"], true);
        assert_eq!(sm.lock_get("z").holder.as_deref(), Some("holder-3"));
    }

    #[test]
    fn stale_lock_holder_cannot_release_after_fifo_promotion() {
        let sm = StateMachine::new();
        let first = acquire(&sm, &["orders"], "holder-1", false);
        let token1 = first["fencing_token"].as_u64().unwrap();
        let queued = acquire(&sm, &["orders"], "holder-2", true);
        assert_eq!(queued["queued"], true);

        let release = sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: token1,
        });
        let token2 = release.output["promoted"][0]["fencing_token"]
            .as_u64()
            .unwrap();
        assert!(token2 > token1);

        let stale = sm.apply(Command::LockRelease {
            holder: "holder-1".to_string(),
            fencing_token: token1,
        });
        assert_eq!(stale.output["released"], false);
        assert_eq!(stale.output["reason"], "not_found");
        assert_eq!(sm.lock_get("orders").holder.as_deref(), Some("holder-2"));
    }

    #[test]
    fn committed_apply_promotes_an_expired_lock_grant_with_a_new_token() {
        let sm = StateMachine::new();
        let base = now_ms();
        let first = sm
            .apply_at(
                Command::LockAcquire {
                    keys: vec!["lease-key".to_string()],
                    holder: "holder-1".to_string(),
                    ttl_ms: 1,
                    wait: false,
                    wait_timeout_ms: None,
                },
                base,
            )
            .output;
        let token1 = first["fencing_token"].as_u64().unwrap();
        let queued = sm.apply_at(
            Command::LockAcquire {
                keys: vec!["lease-key".to_string()],
                holder: "holder-2".to_string(),
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            base,
        );
        assert_eq!(queued.output["queued"], true);

        // A subsequent committed entry sweeps the expired grant and admits the
        // waiter. A read may observe the expiry, but must never perform this
        // state transition or mint its token.
        sm.apply_at(
            Command::CounterAdd {
                key: "trigger-expiry-sweep".to_string(),
                delta: 1,
                prev_revision: None,
            },
            base.saturating_add(1),
        );

        let state = sm.lock_get("lease-key");
        assert_eq!(state.holder.as_deref(), Some("holder-2"));
        assert!(state.fencing_token.unwrap() > token1);
        assert!(state.wait_queue.is_empty());
    }

    #[test]
    fn semaphore_caps_concurrent_holders_and_admits_in_fifo() {
        let sm = StateMachine::new();
        // limit = 2: first two acquire, third is capped out and queues.
        let a = semaphore_acquire(&sm, "db-pool", "a", 2, false);
        let b = semaphore_acquire(&sm, "db-pool", "b", 2, false);
        let c = semaphore_acquire(&sm, "db-pool", "c", 2, true);
        assert_eq!(a["acquired"], true);
        assert_eq!(b["acquired"], true);
        assert_eq!(c["acquired"], false);
        assert_eq!(c["queued"], true);
        assert_eq!(sm.semaphore_get("db-pool").available, 0);

        // a releases its permit → c is admitted.
        let rel = sm.apply(Command::SemaphoreRelease {
            key: "db-pool".to_string(),
            holder: "a".to_string(),
            fencing_token: a["fencing_token"].as_u64().unwrap(),
        });
        assert_eq!(rel.output["promoted"][0]["holder"], "c");
        let state = sm.semaphore_get("db-pool");
        assert_eq!(state.holders.len(), 2);
        assert!(state.holders.iter().any(|h| h.holder == "c"));
    }

    #[test]
    fn semaphore_no_wait_over_cap_does_not_queue_or_consume_permit() {
        let sm = StateMachine::new();
        let first = semaphore_acquire(&sm, "pool", "holder-1", 1, false);
        assert_eq!(first["acquired"], true);

        let no_wait = semaphore_acquire(&sm, "pool", "holder-2", 1, false);
        assert_eq!(no_wait["acquired"], false);
        assert_eq!(no_wait["queued"], false);
        let capped = sm.semaphore_get("pool");
        assert_eq!(capped.holders.len(), 1);
        assert!(capped.wait_queue.is_empty());

        sm.apply(Command::SemaphoreRelease {
            key: "pool".to_string(),
            holder: "holder-1".to_string(),
            fencing_token: first["fencing_token"].as_u64().unwrap(),
        });

        let retry = semaphore_acquire(&sm, "pool", "holder-3", 1, false);
        assert_eq!(retry["acquired"], true);
        assert_eq!(sm.semaphore_get("pool").holders[0].holder, "holder-3");
    }

    #[test]
    fn committed_apply_promotes_an_expired_semaphore_permit_in_fifo_order() {
        let sm = StateMachine::new();
        let base = now_ms();
        let first = sm
            .apply_at(
                Command::SemaphoreAcquire {
                    key: "lease-pool".to_string(),
                    holder: "holder-1".to_string(),
                    limit: 1,
                    ttl_ms: 1,
                    wait: false,
                    wait_timeout_ms: None,
                },
                base,
            )
            .output;
        let token1 = first["fencing_token"].as_u64().unwrap();
        let queued = sm.apply_at(
            Command::SemaphoreAcquire {
                key: "lease-pool".to_string(),
                holder: "holder-2".to_string(),
                limit: 1,
                ttl_ms: 30_000,
                wait: true,
                wait_timeout_ms: None,
            },
            base,
        );
        assert_eq!(queued.output["queued"], true);

        sm.apply_at(
            Command::CounterAdd {
                key: "trigger-expiry-sweep".to_string(),
                delta: 1,
                prev_revision: None,
            },
            base.saturating_add(1),
        );

        let state = sm.semaphore_get("lease-pool");
        assert_eq!(state.holders.len(), 1);
        assert_eq!(state.holders[0].holder, "holder-2");
        assert!(state.holders[0].fencing_token > token1);
        assert!(state.wait_queue.is_empty());
    }

    #[test]
    fn token_bucket_check_is_atomic() {
        let sm = StateMachine::new();
        for _ in 0..2 {
            let out = sm.apply(Command::RateLimitCheck {
                key: "checkout".to_string(),
                tenant: "tenant-a".to_string(),
                algorithm: RateLimitAlgorithm::TokenBucket,
                limit: 2,
                window_ms: 60_000,
                refill_per_second: Some(0.01),
                cost: 1,
            });
            assert_eq!(out.output["allowed"], true);
        }
        let denied = sm.apply(Command::RateLimitCheck {
            key: "checkout".to_string(),
            tenant: "tenant-a".to_string(),
            algorithm: RateLimitAlgorithm::TokenBucket,
            limit: 2,
            window_ms: 60_000,
            refill_per_second: Some(0.01),
            cost: 1,
        });
        assert_eq!(denied.output["allowed"], false);
    }

    #[test]
    fn sliding_window_check_is_atomic_per_tenant_key() {
        let sm = StateMachine::new();
        let first = sm.apply(Command::RateLimitCheck {
            key: "checkout".to_string(),
            tenant: "tenant-a".to_string(),
            algorithm: RateLimitAlgorithm::SlidingWindow,
            limit: 1,
            window_ms: 60_000,
            refill_per_second: None,
            cost: 1,
        });
        let second = sm.apply(Command::RateLimitCheck {
            key: "checkout".to_string(),
            tenant: "tenant-a".to_string(),
            algorithm: RateLimitAlgorithm::SlidingWindow,
            limit: 1,
            window_ms: 60_000,
            refill_per_second: None,
            cost: 1,
        });
        let other_tenant = sm.apply(Command::RateLimitCheck {
            key: "checkout".to_string(),
            tenant: "tenant-b".to_string(),
            algorithm: RateLimitAlgorithm::SlidingWindow,
            limit: 1,
            window_ms: 60_000,
            refill_per_second: None,
            cost: 1,
        });

        assert_eq!(first.output["allowed"], true);
        assert_eq!(second.output["allowed"], false);
        assert_eq!(other_tenant.output["allowed"], true);
    }

    #[test]
    fn idempotency_claim_dedupes_until_ttl_expires() {
        let sm = StateMachine::new();
        let first = sm.apply(Command::IdempotencyClaim {
            key: "stripe-webhook/event_123".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 50,
            retention_ms: None,
            metadata: HashMap::new(),
        });
        let token = first.output["fencing_token"].as_u64().unwrap();
        let duplicate = sm.apply(Command::IdempotencyClaim {
            key: "stripe-webhook/event_123".to_string(),
            owner: "worker-b".to_string(),
            ttl_ms: 50,
            retention_ms: None,
            metadata: HashMap::new(),
        });

        assert_eq!(first.output["claimed"], true);
        assert_eq!(duplicate.output["claimed"], false);
        assert_eq!(duplicate.output["duplicate"], true);
        assert_eq!(duplicate.output["record"]["owner"], "worker-a");
        assert_eq!(duplicate.output["record"]["fencing_token"], token);

        std::thread::sleep(std::time::Duration::from_millis(80));
        let retry_after_ttl = sm.apply(Command::IdempotencyClaim {
            key: "stripe-webhook/event_123".to_string(),
            owner: "worker-b".to_string(),
            ttl_ms: 50,
            retention_ms: None,
            metadata: HashMap::new(),
        });

        assert_eq!(retry_after_ttl.output["claimed"], true);
        assert_eq!(retry_after_ttl.output["duplicate"], false);
        assert_ne!(retry_after_ttl.output["fencing_token"], token);
    }

    #[test]
    fn idempotency_claim_preserves_exact_nonblank_key() {
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: " event with spaces ".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: HashMap::new(),
        });
        let blank = sm.apply(Command::IdempotencyClaim {
            key: "   ".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: HashMap::new(),
        });

        assert_eq!(claimed.output["claimed"], true);
        assert_eq!(claimed.output["key"], " event with spaces ");
        assert!(sm
            .idempotency_get(" event with spaces ")
            .is_some_and(|record| record.key == " event with spaces "));
        assert!(sm.idempotency_get("event with spaces").is_none());
        assert_eq!(blank.output["claimed"], false);
        assert_eq!(blank.output["reason"], "empty_key");
    }

    #[test]
    fn idempotency_complete_requires_holder_and_replays_result() {
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: "orders/fulfill/123".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: [("source".to_string(), "stripe".to_string())]
                .into_iter()
                .collect(),
        });
        let token = claimed.output["fencing_token"].as_u64().unwrap();
        let stale = sm.apply(Command::IdempotencyComplete {
            key: "orders/fulfill/123".to_string(),
            owner: "worker-b".to_string(),
            fencing_token: token,
            result: None,
        });
        let completed = sm.apply(Command::IdempotencyComplete {
            key: "orders/fulfill/123".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            result: Some(json!({ "order_id": "123", "status": "fulfilled" })),
        });
        let duplicate = sm.apply(Command::IdempotencyClaim {
            key: "orders/fulfill/123".to_string(),
            owner: "worker-c".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: HashMap::new(),
        });

        assert_eq!(stale.output["completed"], false);
        assert_eq!(stale.output["reason"], "not_holder");
        assert_eq!(completed.output["completed"], true);
        assert_eq!(completed.output["record"]["status"], "completed");
        assert_eq!(duplicate.output["duplicate"], true);
        assert_eq!(
            duplicate.output["record"]["result"],
            json!({ "order_id": "123", "status": "fulfilled" })
        );
    }

    #[test]
    fn idempotency_complete_rejects_missing_key_without_creating_record() {
        let sm = StateMachine::new();
        let missing = sm.apply(Command::IdempotencyComplete {
            key: "missing-key".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: 1,
            result: Some(json!({ "ok": true })),
        });

        assert_eq!(missing.output["completed"], false);
        assert_eq!(missing.output["reason"], "not_found");
        assert!(sm.idempotency_get("missing-key").is_none());
    }

    #[test]
    fn idempotency_complete_is_idempotent_for_same_holder_and_token() {
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: "orders/fulfill/456".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: HashMap::new(),
        });
        let token = claimed.output["fencing_token"].as_u64().unwrap();
        let first = sm.apply(Command::IdempotencyComplete {
            key: "orders/fulfill/456".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            result: Some(json!({ "status": "fulfilled" })),
        });
        let second = sm.apply(Command::IdempotencyComplete {
            key: "orders/fulfill/456".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            result: Some(json!({ "status": "ignored" })),
        });

        assert_eq!(first.output["completed"], true);
        assert_eq!(first.output["duplicate"], false);
        assert_eq!(second.output["completed"], true);
        assert_eq!(second.output["duplicate"], true);
        assert_eq!(
            second.output["record"]["result"],
            json!({ "status": "fulfilled" })
        );
    }

    #[test]
    fn idempotency_claim_metadata_round_trips_through_read_model() {
        let sm = StateMachine::new();
        sm.apply(Command::IdempotencyClaim {
            key: "webhook/event_789".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 30_000,
            retention_ms: None,
            metadata: [
                ("provider".to_string(), "stripe".to_string()),
                (
                    "event_type".to_string(),
                    "checkout.session.completed".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        });

        let record = sm.idempotency_get("webhook/event_789").expect("record");
        assert_eq!(
            record.metadata.get("provider").map(String::as_str),
            Some("stripe")
        );
        assert_eq!(
            record.metadata.get("event_type").map(String::as_str),
            Some("checkout.session.completed")
        );
        assert_eq!(record.status, IdempotencyStatus::Claimed);
    }

    #[test]
    fn idempotency_abandoned_claim_expires_at_inflight_lease_not_retention() {
        // The poisoning fix: a claim that is never completed must free the key at
        // the *short* in-flight lease, even though its retention window is long.
        // Otherwise a holder that dies between claim and complete would lock the
        // key for the whole retention window.
        let sm = StateMachine::new();
        let first = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/abandoned".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        assert_eq!(first.output["claimed"], true);

        std::thread::sleep(std::time::Duration::from_millis(70));

        // The claim was abandoned (never completed); it must be re-claimable.
        let reclaim = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/abandoned".to_string(),
            owner: "worker-b".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        assert_eq!(reclaim.output["claimed"], true);
        assert_eq!(reclaim.output["duplicate"], false);
        assert_ne!(
            reclaim.output["fencing_token"],
            first.output["fencing_token"]
        );
    }

    #[test]
    fn idempotency_complete_extends_lease_to_retention_window() {
        // A completed record must outlive the short in-flight lease so duplicates
        // can still replay it deep into the retention window.
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/completed".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        let token = claimed.output["fencing_token"].as_u64().unwrap();
        let completed = sm.apply(Command::IdempotencyComplete {
            key: "orders/charge/completed".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            result: Some(json!({ "charge": "ok" })),
        });
        assert_eq!(completed.output["completed"], true);

        // Past the in-flight lease, but well within retention.
        std::thread::sleep(std::time::Duration::from_millis(70));

        let duplicate = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/completed".to_string(),
            owner: "worker-b".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        assert_eq!(duplicate.output["duplicate"], true);
        assert_eq!(duplicate.output["record"]["status"], "completed");
        assert_eq!(
            duplicate.output["record"]["result"],
            json!({ "charge": "ok" })
        );
    }

    #[test]
    fn idempotency_renew_extends_inflight_lease_only_for_holder() {
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: "jobs/long-running".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        let token = claimed.output["fencing_token"].as_u64().unwrap();

        // A non-holder cannot renew.
        let intruder = sm.apply(Command::IdempotencyRenew {
            key: "jobs/long-running".to_string(),
            owner: "worker-b".to_string(),
            fencing_token: token,
            ttl_ms: 5_000,
        });
        assert_eq!(intruder.output["renewed"], false);
        assert_eq!(intruder.output["reason"], "not_holder");

        // The holder renews well past the original in-flight lease.
        let renewed = sm.apply(Command::IdempotencyRenew {
            key: "jobs/long-running".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            ttl_ms: 5_000,
        });
        assert_eq!(renewed.output["renewed"], true);

        std::thread::sleep(std::time::Duration::from_millis(70));

        // Still held after the original 40ms lease because it was renewed.
        let duplicate = sm.apply(Command::IdempotencyClaim {
            key: "jobs/long-running".to_string(),
            owner: "worker-c".to_string(),
            ttl_ms: 40,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        assert_eq!(duplicate.output["duplicate"], true);
        assert_eq!(duplicate.output["record"]["fencing_token"], token);

        // Renew is rejected once completed — the retention lease governs then.
        sm.apply(Command::IdempotencyComplete {
            key: "jobs/long-running".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            result: None,
        });
        let after_complete = sm.apply(Command::IdempotencyRenew {
            key: "jobs/long-running".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
            ttl_ms: 5_000,
        });
        assert_eq!(after_complete.output["renewed"], false);
        assert_eq!(after_complete.output["reason"], "already_completed");
    }

    #[test]
    fn idempotency_abandon_frees_claimed_key_but_never_a_completed_one() {
        let sm = StateMachine::new();
        let claimed = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/transient".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 120_000,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        let token = claimed.output["fencing_token"].as_u64().unwrap();

        // A non-holder cannot abandon.
        let intruder = sm.apply(Command::IdempotencyAbandon {
            key: "orders/charge/transient".to_string(),
            owner: "worker-b".to_string(),
            fencing_token: token,
        });
        assert_eq!(intruder.output["abandoned"], false);
        assert_eq!(intruder.output["reason"], "not_holder");

        // The holder abandons after a transient failure; the key is freed at once.
        let abandoned = sm.apply(Command::IdempotencyAbandon {
            key: "orders/charge/transient".to_string(),
            owner: "worker-a".to_string(),
            fencing_token: token,
        });
        assert_eq!(abandoned.output["abandoned"], true);
        assert!(sm.idempotency_get("orders/charge/transient").is_none());

        // A retry re-claims immediately (no waiting out the in-flight lease).
        let reclaim = sm.apply(Command::IdempotencyClaim {
            key: "orders/charge/transient".to_string(),
            owner: "worker-c".to_string(),
            ttl_ms: 120_000,
            retention_ms: Some(3_600_000),
            metadata: HashMap::new(),
        });
        assert_eq!(reclaim.output["claimed"], true);

        // A completed record can never be abandoned — its result must survive.
        let token2 = reclaim.output["fencing_token"].as_u64().unwrap();
        sm.apply(Command::IdempotencyComplete {
            key: "orders/charge/transient".to_string(),
            owner: "worker-c".to_string(),
            fencing_token: token2,
            result: Some(json!({ "charge": "ok" })),
        });
        let after_complete = sm.apply(Command::IdempotencyAbandon {
            key: "orders/charge/transient".to_string(),
            owner: "worker-c".to_string(),
            fencing_token: token2,
        });
        assert_eq!(after_complete.output["abandoned"], false);
        assert_eq!(after_complete.output["reason"], "already_completed");
        assert!(sm
            .idempotency_get("orders/charge/transient")
            .is_some_and(|r| r.status == IdempotencyStatus::Completed));
    }

    #[test]
    fn kv_put_uses_compare_and_swap_revision() {
        let sm = StateMachine::new();
        let created = sm.apply(Command::KvPut {
            key: "flags/new-checkout".to_string(),
            value: "on".to_string(),
            ttl_ms: None,
            prev_revision: Some(0),
        });
        let revision = created.output["revision"].as_u64().unwrap();
        let stale = sm.apply(Command::KvPut {
            key: "flags/new-checkout".to_string(),
            value: "off".to_string(),
            ttl_ms: None,
            prev_revision: Some(0),
        });
        let updated = sm.apply(Command::KvPut {
            key: "flags/new-checkout".to_string(),
            value: "off".to_string(),
            ttl_ms: None,
            prev_revision: Some(revision),
        });

        assert_eq!(created.output["ok"], true);
        assert_eq!(stale.output["ok"], false);
        assert_eq!(stale.output["reason"], "cas_mismatch");
        assert_eq!(updated.output["ok"], true);
        assert_eq!(sm.kv_get("flags/new-checkout").unwrap().value, "off");
    }

    #[test]
    fn counter_add_creates_accumulates_and_compare_and_sets() {
        let sm = StateMachine::new();
        // Absent counter is read as None (callers treat that as 0).
        assert!(sm.counter_get("rollout/v2/failures").is_none());

        // First add creates it at 0 then applies the delta.
        let first = sm.apply(Command::CounterAdd {
            key: "rollout/v2/failures".to_string(),
            delta: 1,
            prev_revision: None,
        });
        assert_eq!(first.output["ok"], true);
        assert_eq!(first.output["value"], 1);

        // A negative delta accumulates.
        let second = sm.apply(Command::CounterAdd {
            key: "rollout/v2/failures".to_string(),
            delta: 4,
            prev_revision: None,
        });
        assert_eq!(second.output["value"], 5);
        let revision = second.output["mod_revision"].as_u64().unwrap();

        // A compare-and-set add against a stale revision is rejected.
        let stale = sm.apply(Command::CounterAdd {
            key: "rollout/v2/failures".to_string(),
            delta: 100,
            prev_revision: Some(0),
        });
        assert_eq!(stale.output["ok"], false);
        assert_eq!(stale.output["reason"], "cas_mismatch");
        assert_eq!(sm.counter_get("rollout/v2/failures").unwrap().value, 5);

        // Set with the correct revision resets it to 0.
        let reset = sm.apply(Command::CounterSet {
            key: "rollout/v2/failures".to_string(),
            value: 0,
            prev_revision: Some(revision),
        });
        assert_eq!(reset.output["ok"], true);
        assert_eq!(sm.counter_get("rollout/v2/failures").unwrap().value, 0);
    }

    #[test]
    fn barrier_quorum_resolves_on_second_distinct_arrival() {
        let sm = StateMachine::new();
        sm.apply(Command::BarrierCreate {
            name: "release/reviewers".to_string(),
            policy: BarrierPolicy::Quorum { required: 2 },
            expected: 3,
            deadline_ms: None,
        });
        let first = sm.apply(Command::BarrierArrive {
            name: "release/reviewers".to_string(),
            participant: "reviewer-a".to_string(),
            weight: 1,
            veto: false,
        });
        assert_eq!(first.output["resolved"], false);
        // A repeat arrival by the same participant is idempotent (still 1 distinct).
        let repeat = sm.apply(Command::BarrierArrive {
            name: "release/reviewers".to_string(),
            participant: "reviewer-a".to_string(),
            weight: 1,
            veto: false,
        });
        assert_eq!(repeat.output["resolved"], false);
        let second = sm.apply(Command::BarrierArrive {
            name: "release/reviewers".to_string(),
            participant: "reviewer-b".to_string(),
            weight: 1,
            veto: false,
        });
        assert_eq!(second.output["resolved"], true);
        assert_eq!(second.output["barrier"]["status"], "satisfied");
        assert_eq!(second.output["barrier"]["arrived_count"], 2);
    }

    #[test]
    fn barrier_any_veto_aborts() {
        let sm = StateMachine::new();
        sm.apply(Command::BarrierCreate {
            name: "deploy/safety".to_string(),
            policy: BarrierPolicy::AnyVeto,
            expected: 2,
            deadline_ms: None,
        });
        sm.apply(Command::BarrierArrive {
            name: "deploy/safety".to_string(),
            participant: "compliance".to_string(),
            weight: 1,
            veto: true,
        });
        assert_eq!(
            sm.barrier_get("deploy/safety").unwrap().status,
            BarrierStatus::Vetoed
        );
    }

    #[test]
    fn barrier_weighted_quorum_sums_weights() {
        let sm = StateMachine::new();
        sm.apply(Command::BarrierCreate {
            name: "vote".to_string(),
            policy: BarrierPolicy::WeightedQuorum { required_weight: 5 },
            expected: 0,
            deadline_ms: None,
        });
        sm.apply(Command::BarrierArrive {
            name: "vote".to_string(),
            participant: "specialist".to_string(),
            weight: 3,
            veto: false,
        });
        assert_eq!(
            sm.barrier_get("vote").unwrap().status,
            BarrierStatus::Pending
        );
        sm.apply(Command::BarrierArrive {
            name: "vote".to_string(),
            participant: "generalist".to_string(),
            weight: 2,
            veto: false,
        });
        assert_eq!(
            sm.barrier_get("vote").unwrap().status,
            BarrierStatus::Satisfied
        );
    }

    #[test]
    fn barrier_arrive_auto_creates_single_participant_all() {
        let sm = StateMachine::new();
        let out = sm.apply(Command::BarrierArrive {
            name: "adhoc".to_string(),
            participant: "solo".to_string(),
            weight: 1,
            veto: false,
        });
        // Auto-created `all` with expected=1 → one arrival satisfies it.
        assert_eq!(out.output["resolved"], true);
    }

    #[test]
    fn task_claim_is_exclusive_and_fences_stale_workers() {
        let sm = StateMachine::new();
        let task = "repo/acme/api/issue/482".to_string();
        sm.apply(Command::TaskCreate {
            name: task.clone(),
            task_type: "implement".to_string(),
            payload: json!({ "issue": 482 }),
            deadline_ms: None,
        });

        // Worker A claims and receives a fencing token.
        let claim = sm.apply(Command::TaskClaim {
            name: task.clone(),
            worker: "agent-a".to_string(),
            ttl_ms: 60_000,
        });
        assert_eq!(claim.output["ok"], true);
        let token = claim.output["fencing_token"].as_u64().unwrap();

        // Worker B cannot claim an actively-owned task.
        let contended = sm.apply(Command::TaskClaim {
            name: task.clone(),
            worker: "agent-b".to_string(),
            ttl_ms: 60_000,
        });
        assert_eq!(contended.output["ok"], false);
        assert_eq!(contended.output["reason"], "already_claimed");

        // A progress write with the wrong (stale) token is fenced.
        let stale = sm.apply(Command::TaskProgress {
            name: task.clone(),
            worker: "agent-a".to_string(),
            fencing_token: token + 1,
            percent: 50,
            checkpoint: json!({}),
        });
        assert_eq!(stale.output["ok"], false);
        assert_eq!(stale.output["reason"], "fenced");

        // The current token holder makes progress, then completes.
        sm.apply(Command::TaskProgress {
            name: task.clone(),
            worker: "agent-a".to_string(),
            fencing_token: token,
            percent: 50,
            checkpoint: json!({ "step": "coded" }),
        });
        let done = sm.apply(Command::TaskComplete {
            name: task.clone(),
            worker: "agent-a".to_string(),
            fencing_token: token,
            result: json!({ "pr": 991 }),
        });
        assert_eq!(done.output["ok"], true);
        assert_eq!(sm.task_get(&task).unwrap().status, TaskStatus::Completed);

        // A claim on a completed task is rejected as terminal.
        let after = sm.apply(Command::TaskClaim {
            name: task,
            worker: "agent-c".to_string(),
            ttl_ms: 1_000,
        });
        assert_eq!(after.output["reason"], "terminal");
    }

    #[test]
    fn task_retryable_failure_returns_to_the_claimable_pool() {
        let sm = StateMachine::new();
        sm.apply(Command::TaskCreate {
            name: "backfill".to_string(),
            task_type: "job".to_string(),
            payload: Value::Null,
            deadline_ms: None,
        });
        let token = sm
            .apply(Command::TaskClaim {
                name: "backfill".to_string(),
                worker: "w1".to_string(),
                ttl_ms: 60_000,
            })
            .output["fencing_token"]
            .as_u64()
            .unwrap();
        sm.apply(Command::TaskFail {
            name: "backfill".to_string(),
            worker: "w1".to_string(),
            fencing_token: token,
            retryable: true,
        });
        // Back to Pending, so another worker can claim it (with a higher token).
        assert_eq!(sm.task_get("backfill").unwrap().status, TaskStatus::Pending);
        let reclaim = sm.apply(Command::TaskClaim {
            name: "backfill".to_string(),
            worker: "w2".to_string(),
            ttl_ms: 60_000,
        });
        assert_eq!(reclaim.output["ok"], true);
        assert!(reclaim.output["fencing_token"].as_u64().unwrap() > token);
    }

    #[test]
    fn effect_requires_approval_then_commits_exactly_once() {
        let sm = StateMachine::new();
        let name = "invoice-882/payment".to_string();
        sm.apply(Command::EffectPrepare {
            name: name.clone(),
            effect_type: "send_payment".to_string(),
            payload: json!({ "amount_usd": 500 }),
            risk: "high".to_string(),
            idempotency_key: "invoice-882:payment".to_string(),
            required_approvals: 2,
        });

        // Cannot commit before it is approved.
        let early = sm.apply(Command::EffectCommit {
            name: name.clone(),
            result: json!({}),
        });
        assert_eq!(early.output["ok"], false);
        assert_eq!(early.output["reason"], "not_approved");

        // One approval is not enough (requires 2), and a duplicate counts once.
        sm.apply(Command::EffectApprove {
            name: name.clone(),
            principal: "finance-a".to_string(),
        });
        sm.apply(Command::EffectApprove {
            name: name.clone(),
            principal: "finance-a".to_string(),
        });
        assert_eq!(sm.effect_get(&name).unwrap().status, EffectStatus::Prepared);

        // A second distinct approval moves it to Approved.
        sm.apply(Command::EffectApprove {
            name: name.clone(),
            principal: "finance-b".to_string(),
        });
        assert_eq!(sm.effect_get(&name).unwrap().status, EffectStatus::Approved);

        // First commit executes; a duplicate commit replays without re-executing.
        let first = sm.apply(Command::EffectCommit {
            name: name.clone(),
            result: json!({ "confirmation": "pay_123" }),
        });
        assert_eq!(first.output["committed"], true);
        let replay = sm.apply(Command::EffectCommit {
            name: name.clone(),
            result: json!({ "confirmation": "pay_999" }),
        });
        assert_eq!(replay.output["ok"], true);
        assert_eq!(replay.output["committed"], false);
        // The originally recorded result is preserved, not overwritten.
        assert_eq!(
            sm.effect_get(&name).unwrap().result.unwrap()["confirmation"],
            "pay_123"
        );
    }

    #[test]
    fn handoff_transfers_ownership_atomically_with_a_higher_token() {
        let sm = StateMachine::new();
        // research-agent really owns the task, holding a fencing token minted by
        // the node's single monotonic counter.
        sm.apply(Command::TaskCreate {
            name: "task:ticket-482".to_string(),
            task_type: "research".to_string(),
            payload: Value::Null,
            deadline_ms: None,
        });
        let from_token = sm
            .apply(Command::TaskClaim {
                name: "task:ticket-482".to_string(),
                worker: "research-agent".to_string(),
                ttl_ms: 60_000,
            })
            .output["fencing_token"]
            .as_u64()
            .unwrap();

        let offer = sm.apply(Command::HandoffOffer {
            name: "ticket-482/handoff".to_string(),
            resource: "task:ticket-482".to_string(),
            from: "research-agent".to_string(),
            to: "legal-agent".to_string(),
            from_token,
            context: json!({ "summary": "needs legal review" }),
            ttl_ms: 30_000,
        });
        assert_eq!(offer.output["ok"], true);
        // While offered, ownership has not moved.
        assert_eq!(
            sm.handoff_get("ticket-482/handoff").unwrap().status,
            HandoffStatus::Offered
        );

        // Only the offered recipient may accept.
        let wrong = sm.apply(Command::HandoffAccept {
            name: "ticket-482/handoff".to_string(),
            to: "random-agent".to_string(),
        });
        assert_eq!(wrong.output["reason"], "not_recipient");

        // The recipient accepts and receives a strictly higher fencing token.
        let accept = sm.apply(Command::HandoffAccept {
            name: "ticket-482/handoff".to_string(),
            to: "legal-agent".to_string(),
        });
        assert_eq!(accept.output["ok"], true);
        let to_token = accept.output["to_token"].as_u64().unwrap();
        assert!(
            to_token > from_token,
            "new owner's token must exceed the old owner's"
        );
        assert_eq!(
            sm.handoff_get("ticket-482/handoff").unwrap().status,
            HandoffStatus::Accepted
        );

        // A second accept on the resolved handoff is rejected.
        let again = sm.apply(Command::HandoffAccept {
            name: "ticket-482/handoff".to_string(),
            to: "legal-agent".to_string(),
        });
        assert_eq!(again.output["reason"], "not_offered");
    }

    #[test]
    fn handoff_accept_token_exceeds_a_from_token_minted_on_another_shard() {
        // Fencing tokens are per-shard. A handoff may live on a different shard
        // than the resource it transfers, so its `from_token` can be far ahead of
        // this shard's counter. The accept must still mint a strictly higher
        // token (regression: this shard's fresh counter would return 1).
        let sm = StateMachine::new();
        sm.apply(Command::HandoffOffer {
            name: "cross/shard".to_string(),
            resource: "task:elsewhere".to_string(),
            from: "owner-a".to_string(),
            to: "owner-b".to_string(),
            from_token: 1_000_000,
            context: Value::Null,
            ttl_ms: 30_000,
        });
        let accept = sm.apply(Command::HandoffAccept {
            name: "cross/shard".to_string(),
            to: "owner-b".to_string(),
        });
        assert!(accept.output["to_token"].as_u64().unwrap() > 1_000_000);
    }

    #[test]
    fn decision_resolves_by_weight_and_vetoes_abort() {
        let sm = StateMachine::new();
        let propose = |name: &str| {
            sm.apply(Command::DecisionPropose {
                name: name.to_string(),
                question: "Is this deploy safe?".to_string(),
                options: vec!["approve".to_string(), "reject".to_string()],
                policy: DecisionPolicy::Plurality { min_votes: 3 },
                deadline_ms: None,
            });
        };
        let vote = |name: &str, voter: &str, option: Option<&str>, weight: u64, veto: bool| {
            sm.apply(Command::DecisionVote {
                name: name.to_string(),
                voter: voter.to_string(),
                option: option.map(str::to_string),
                confidence: 0.9,
                weight,
                veto,
                evidence: vec![],
            })
        };

        propose("deploy/safe");
        vote("deploy/safe", "a", Some("approve"), 1, false);
        vote("deploy/safe", "b", Some("reject"), 1, false);
        // Not enough votes yet (min_votes = 3).
        assert_eq!(
            sm.decision_get("deploy/safe").unwrap().status,
            DecisionStatus::Open
        );
        // A heavier third vote for approve resolves it to approve.
        vote("deploy/safe", "c", Some("approve"), 5, false);
        let resolved = sm.decision_get("deploy/safe").unwrap();
        assert_eq!(resolved.status, DecisionStatus::Resolved);
        assert_eq!(resolved.winner.as_deref(), Some("approve"));

        // A vote for an option outside the declared set is rejected.
        let bad = vote("deploy/safe", "d", Some("maybe"), 1, false);
        assert_eq!(bad.output["reason"], "unknown_option");

        // Any veto aborts a separate decision.
        propose("release/gate");
        vote("release/gate", "compliance", None, 1, true);
        assert_eq!(
            sm.decision_get("release/gate").unwrap().status,
            DecisionStatus::Vetoed
        );
    }

    #[test]
    fn budget_reservations_cannot_oversubscribe_and_commit_frees_the_difference() {
        let sm = StateMachine::new();
        // A $1.00 (1_000_000 micros) workflow budget, unlimited tokens/tool_calls.
        sm.apply(Command::BudgetSet {
            name: "org/acme/wf/42".to_string(),
            limit: BudgetAmount {
                usd_micros: Some(1_000_000),
                tokens: None,
                tool_calls: None,
            },
        });
        let reserve = |id: &str, usd: u64| {
            sm.apply(Command::BudgetReserve {
                name: "org/acme/wf/42".to_string(),
                reservation_id: id.to_string(),
                holder: format!("agent-{id}"),
                amount: BudgetAmount {
                    usd_micros: Some(usd),
                    tokens: None,
                    tool_calls: None,
                },
            })
        };

        // Two agents each reserve $0.60 — the second cannot oversubscribe the $1.00.
        assert_eq!(reserve("a", 600_000).output["reserved"], true);
        let denied = reserve("b", 600_000);
        assert_eq!(denied.output["ok"], false);
        assert_eq!(denied.output["reason"], "insufficient_budget");

        // Agent A actually spends only $0.20; committing frees the other $0.40.
        sm.apply(Command::BudgetCommit {
            name: "org/acme/wf/42".to_string(),
            reservation_id: "a".to_string(),
            actual: BudgetAmount {
                usd_micros: Some(200_000),
                tokens: None,
                tool_calls: None,
            },
        });
        let budget = sm.budget_get("org/acme/wf/42").unwrap();
        assert_eq!(budget.spent.usd_micros, Some(200_000));
        assert_eq!(budget.available.usd_micros, Some(800_000));

        // Now agent B's $0.60 reservation fits.
        assert_eq!(reserve("b", 600_000).output["reserved"], true);

        // Releasing an uncommitted reservation returns its headroom.
        sm.apply(Command::BudgetRelease {
            name: "org/acme/wf/42".to_string(),
            reservation_id: "b".to_string(),
        });
        assert_eq!(
            sm.budget_get("org/acme/wf/42")
                .unwrap()
                .available
                .usd_micros,
            Some(800_000)
        );
    }

    #[test]
    fn claim_is_contestable_versioned_and_authoritatively_resolved() {
        let sm = StateMachine::new();
        let name = "customer/219/refund_eligible".to_string();
        let created = sm.apply(Command::ClaimAssert {
            name: name.clone(),
            subject: "customer:219".to_string(),
            predicate: "eligible_for_refund".to_string(),
            value: json!(true),
            confidence: 0.91,
            author: "billing-agent".to_string(),
            evidence: vec!["ticket:88".to_string()],
            valid_until_ms: None,
        });
        assert_eq!(created.output["created"], true);
        assert_eq!(sm.claim_get(&name).unwrap().status, ClaimStatus::Asserted);

        // Another agent supports, then a third contests it.
        sm.apply(Command::ClaimSupport {
            name: name.clone(),
            agent: "audit-agent".to_string(),
        });
        sm.apply(Command::ClaimContest {
            name: name.clone(),
            agent: "fraud-agent".to_string(),
            reason: "chargeback on file".to_string(),
        });
        let contested = sm.claim_get(&name).unwrap();
        assert_eq!(contested.status, ClaimStatus::Contested);
        assert_eq!(contested.supporters, vec!["audit-agent".to_string()]);
        assert_eq!(contested.contests.len(), 1);

        // Re-asserting a new value bumps the version and clears prior support.
        let reasserted = sm.apply(Command::ClaimAssert {
            name: name.clone(),
            subject: "customer:219".to_string(),
            predicate: "eligible_for_refund".to_string(),
            value: json!(false),
            confidence: 0.7,
            author: "fraud-agent".to_string(),
            evidence: vec!["chargeback:12".to_string()],
            valid_until_ms: None,
        });
        assert_eq!(reasserted.output["claim"]["version"], 2);
        assert_eq!(sm.claim_get(&name).unwrap().supporters.len(), 0);

        // An authorized process resolves it (accepted, terminal). Further
        // mutations are rejected.
        sm.apply(Command::ClaimResolve {
            name: name.clone(),
            accepted: true,
        });
        assert_eq!(sm.claim_get(&name).unwrap().status, ClaimStatus::Accepted);
        let late = sm.apply(Command::ClaimSupport {
            name: name.clone(),
            agent: "x".to_string(),
        });
        assert_eq!(late.output["reason"], "terminal");
    }

    #[test]
    fn kv_prefix_lists_matching_keys_in_order() {
        let sm = StateMachine::new();
        for (key, value) in [
            ("flags/new-checkout", "on"),
            ("flags/search", "off"),
            ("config/theme", "blue"),
        ] {
            sm.apply(Command::KvPut {
                key: key.to_string(),
                value: value.to_string(),
                ttl_ms: None,
                prev_revision: None,
            });
        }

        let entries = sm.kv_prefix("flags/");
        let keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();

        assert_eq!(keys, vec!["flags/new-checkout", "flags/search"]);
        assert_eq!(entries[0].1.value, "on");
    }

    #[test]
    fn election_uses_fencing_tokens_for_campaign_renew_and_resign() {
        let sm = StateMachine::new();
        let metadata = HashMap::from([
            ("region".to_string(), "us-east".to_string()),
            ("address".to_string(), "https://node-a.internal".to_string()),
        ]);
        let won = sm.apply(Command::ElectionCampaign {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            ttl_ms: 120_000,
            metadata,
        });
        let token = won.output["leadership"]["fencing_token"].as_u64().unwrap();
        let initial_expiry = won.output["leadership"]["lease_expires_ms"]
            .as_u64()
            .unwrap();
        let stale_renew = sm.apply(Command::ElectionRenew {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token + 1,
            ttl_ms: None,
        });
        let renewed = sm.apply(Command::ElectionRenew {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token,
            ttl_ms: None,
        });
        let stored_metadata = sm.election_get("scheduler").unwrap().metadata;
        let resigned = sm.apply(Command::ElectionResign {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token,
        });

        assert_eq!(won.output["won"], true);
        assert_eq!(won.output["leadership"]["metadata"]["region"], "us-east");
        assert_eq!(stored_metadata["address"], "https://node-a.internal");
        assert_eq!(stale_renew.output["renewed"], false);
        assert_eq!(renewed.output["renewed"], true);
        let renewed_expiry = renewed.output["leadership"]["lease_expires_ms"]
            .as_u64()
            .unwrap();
        assert!(renewed_expiry >= initial_expiry.saturating_sub(1_000));
        assert_eq!(resigned.output["resigned"], true);
        assert!(sm.election_get("scheduler").is_none());
    }

    #[test]
    fn exactly_once_schedule_run_dedupes_fire_id() {
        let sm = StateMachine::new();
        sm.apply(Command::ScheduleUpsert {
            name: "nightly".to_string(),
            cron: Some("0 0 * * *".to_string()),
            one_shot_at_ms: None,
            target: ScheduleTarget::Webhook {
                url: "https://example.com/hook".to_string(),
            },
            delivery: DeliverySemantics::ExactlyOnce,
            max_retries: 3,
            now_ms: 0,
        });
        let first = sm.apply(Command::ScheduleRecordRun {
            name: "nightly".to_string(),
            fire_id: "2026-06-27T00:00Z".to_string(),
            fired_at_ms: 1,
        });
        let second = sm.apply(Command::ScheduleRecordRun {
            name: "nightly".to_string(),
            fire_id: "2026-06-27T00:00Z".to_string(),
            fired_at_ms: 2,
        });

        assert_eq!(first.output["recorded"], true);
        assert_eq!(second.output["duplicate"], true);
        assert_eq!(sm.schedule_history("nightly").len(), 1);
    }

    #[test]
    fn claim_fire_advances_the_cursor_and_dedups_a_second_claim() {
        let sm = StateMachine::new();
        // Daily at midnight, anchored at epoch 0 → first fire is day 1 midnight.
        sm.apply(Command::ScheduleUpsert {
            name: "nightly".to_string(),
            cron: Some("0 0 * * *".to_string()),
            one_shot_at_ms: None,
            target: ScheduleTarget::Webhook {
                url: "https://example.com/hook".to_string(),
            },
            delivery: DeliverySemantics::AtLeastOnce,
            max_retries: 3,
            now_ms: 0,
        });
        let day = 86_400_000u64;
        let first_fire = sm.schedule_get("nightly").unwrap().next_fire_ms.unwrap();
        assert_eq!(first_fire, day, "first daily fire is day 1 midnight");

        // Claiming it succeeds and advances the cursor to the next occurrence.
        let claim = sm
            .apply(Command::ScheduleClaimFire {
                name: "nightly".to_string(),
                fire_id_ms: first_fire,
            })
            .output;
        assert_eq!(claim["claimed"], true);
        assert_eq!(
            sm.schedule_get("nightly").unwrap().next_fire_ms,
            Some(2 * day),
            "cursor advanced to the following day",
        );

        // Claiming the SAME fire again fails — this is the no-duplicate-fires dedup.
        let dup = sm
            .apply(Command::ScheduleClaimFire {
                name: "nightly".to_string(),
                fire_id_ms: first_fire,
            })
            .output;
        assert_eq!(dup["claimed"], false);
        assert_eq!(dup["reason"], "stale_or_duplicate");

        // The claim recorded a Pending run; recording the result resolves it.
        let history = sm.schedule_history("nightly");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].status, RunStatus::Pending);
        sm.apply(Command::ScheduleRecordResult {
            name: "nightly".to_string(),
            fire_id_ms: first_fire,
            delivered: true,
            attempts: 2,
            error: None,
        });
        let resolved = &sm.schedule_history("nightly")[0];
        assert_eq!(resolved.status, RunStatus::Delivered);
        assert_eq!(resolved.attempts, 2);
    }

    #[test]
    fn one_shot_fires_once_then_is_exhausted() {
        let sm = StateMachine::new();
        sm.apply(Command::ScheduleUpsert {
            name: "once".to_string(),
            cron: None,
            one_shot_at_ms: Some(5_000),
            target: ScheduleTarget::Queue {
                name: "https://queue.local/ingress".to_string(),
            },
            delivery: DeliverySemantics::ExactlyOnce,
            max_retries: 0,
            now_ms: 0,
        });
        assert_eq!(sm.schedule_get("once").unwrap().next_fire_ms, Some(5_000));
        let claim = sm
            .apply(Command::ScheduleClaimFire {
                name: "once".to_string(),
                fire_id_ms: 5_000,
            })
            .output;
        assert_eq!(claim["claimed"], true);
        // A one-shot has no next fire once claimed.
        assert_eq!(sm.schedule_get("once").unwrap().next_fire_ms, None);
        let dup = sm
            .apply(Command::ScheduleClaimFire {
                name: "once".to_string(),
                fire_id_ms: 5_000,
            })
            .output;
        assert_eq!(dup["claimed"], false, "exhausted one-shot can't re-fire");
    }

    #[test]
    fn service_registration_carries_metadata_and_heartbeat_ttl() {
        let sm = StateMachine::new();
        let mut metadata = HashMap::new();
        metadata.insert("region".to_string(), "us-east-1".to_string());
        let registered = sm.apply(Command::ServiceRegister {
            service: "api".to_string(),
            instance_id: "i-1".to_string(),
            address: "http://10.0.0.1:8080".to_string(),
            ttl_ms: 10_000,
            metadata,
        });
        let initial_expiry = registered.output["instance"]["lease_expires_ms"]
            .as_u64()
            .unwrap();
        let heartbeat = sm.apply(Command::ServiceHeartbeat {
            service: "api".to_string(),
            instance_id: "i-1".to_string(),
            ttl_ms: Some(60_000),
        });
        let instances = sm.service_list("api");

        assert_eq!(registered.output["registered"], true);
        assert_eq!(heartbeat.output["heartbeat"], true);
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].metadata["region"], "us-east-1");
        assert!(instances[0].lease_expires_ms >= initial_expiry);
    }

    #[test]
    fn election_campaign_publishes_candidate_metadata() {
        let sm = StateMachine::new();
        let mut metadata = HashMap::new();
        metadata.insert("address".to_string(), "10.2.4.18:8080".to_string());
        metadata.insert("region".to_string(), "us-east-1".to_string());
        sm.apply(Command::ElectionCampaign {
            name: "invoice-reconciler/leader".to_string(),
            candidate: "node-3".to_string(),
            ttl_ms: 15_000,
            metadata,
        });
        let held = sm.election_get("invoice-reconciler/leader").unwrap();
        assert_eq!(held.leader, "node-3");
        assert_eq!(held.metadata["address"], "10.2.4.18:8080");
        assert_eq!(held.metadata["region"], "us-east-1");
        assert_eq!(held.ttl_ms, 15_000);
    }

    #[test]
    fn election_renew_reuses_campaign_ttl_when_unspecified() {
        let sm = StateMachine::new();
        let won = sm.apply(Command::ElectionCampaign {
            name: "leader".to_string(),
            candidate: "node-a".to_string(),
            ttl_ms: 90_000,
            metadata: HashMap::new(),
        });
        let token = won.output["leadership"]["fencing_token"].as_u64().unwrap();
        let campaign_expiry = won.output["leadership"]["lease_expires_ms"]
            .as_u64()
            .unwrap();
        // Renew with no explicit TTL: must reuse the 90s campaign TTL, not snap
        // back to the old hard-coded 30s default (which would shrink the lease).
        let renewed = sm.apply(Command::ElectionRenew {
            name: "leader".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token,
            ttl_ms: None,
        });
        let renew_expiry = renewed.output["leadership"]["lease_expires_ms"]
            .as_u64()
            .unwrap();
        assert_eq!(renewed.output["renewed"], true);
        assert!(
            renew_expiry >= campaign_expiry,
            "renew {renew_expiry} must not shrink lease below campaign {campaign_expiry}"
        );
    }

    #[test]
    fn election_failover_after_lease_expiry() {
        // Drive the store directly so we control `now` deterministically.
        let mut store = Store::default();
        let won_a = store.apply_election_campaign(
            1,
            1_000,
            "leader".to_string(),
            "node-a".to_string(),
            5_000, // lease_expires at 6_000
            HashMap::new(),
        );
        assert_eq!(won_a["won"], true);
        // Before expiry, a rival campaign loses.
        let lost = store.apply_election_campaign(
            2,
            5_000,
            "leader".to_string(),
            "node-b".to_string(),
            5_000,
            HashMap::new(),
        );
        assert_eq!(lost["won"], false);
        // After the lease lapses, expire_due reaps it and the rival wins.
        store.expire_due(7_000);
        let won_b = store.apply_election_campaign(
            3,
            7_000,
            "leader".to_string(),
            "node-b".to_string(),
            5_000,
            HashMap::new(),
        );
        assert_eq!(won_b["won"], true);
        assert_eq!(won_b["leadership"]["leader"], "node-b");
    }

    #[test]
    fn kv_list_returns_only_prefixed_live_entries() {
        let sm = StateMachine::new();
        for key in ["app/a", "app/b", "other/c"] {
            sm.apply(Command::KvPut {
                key: key.to_string(),
                value: "v".to_string(),
                ttl_ms: None,
                prev_revision: None,
            });
        }
        let listed = sm.kv_list("app/");
        let mut keys: Vec<String> = listed.into_iter().map(|item| item.key).collect();
        keys.sort();
        assert_eq!(keys, vec!["app/a".to_string(), "app/b".to_string()]);
    }

    #[test]
    fn service_names_summarizes_live_instance_counts() {
        let sm = StateMachine::new();
        for id in ["i-1", "i-2"] {
            sm.apply(Command::ServiceRegister {
                service: "router".to_string(),
                instance_id: id.to_string(),
                address: format!("10.0.0.{id}:8080"),
                ttl_ms: 30_000,
                metadata: HashMap::new(),
            });
        }
        let summaries = sm.service_names();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].service, "router");
        assert_eq!(summaries[0].instances, 2);
    }

    #[test]
    fn expired_service_instances_leave_no_stale_service_name() {
        let sm = StateMachine::new();
        sm.apply(Command::ServiceRegister {
            service: "api".to_string(),
            instance_id: "a-1".to_string(),
            address: "http://10.0.0.1:8080".to_string(),
            ttl_ms: 0,
            metadata: HashMap::new(),
        });

        assert!(sm.service_list("api").is_empty());
        assert!(sm.service_names().is_empty());
    }

    #[test]
    fn service_discovery_routes_to_the_registry_coordination_domain() {
        for command in [
            Command::ServiceRegister {
                service: "api".to_string(),
                instance_id: "a-1".to_string(),
                address: "http://10.0.0.1:8080".to_string(),
                ttl_ms: 30_000,
                metadata: HashMap::new(),
            },
            Command::ServiceHeartbeat {
                service: "api".to_string(),
                instance_id: "a-1".to_string(),
                ttl_ms: None,
            },
            Command::ServiceDeregister {
                service: "api".to_string(),
                instance_id: "a-1".to_string(),
            },
        ] {
            assert_eq!(command.routing_key(), SERVICE_DOMAIN);
        }
    }

    #[test]
    fn replay_uses_the_original_proposal_time_for_leases() {
        let command = Command::LockAcquire {
            keys: vec!["replay-safe".to_string()],
            holder: "worker-a".to_string(),
            ttl_ms: 5_000,
            wait: false,
            wait_timeout_ms: None,
        };
        let original = StateMachine::new();
        let first = original.apply_at(command.clone(), 10_000);
        let restarted = StateMachine::new();
        let replay = restarted.apply_at(command, 10_000);
        assert_eq!(
            first.output["lease_expires_ms"],
            replay.output["lease_expires_ms"]
        );
        assert_eq!(replay.output["lease_expires_ms"], 15_000);
    }

    #[test]
    fn snapshot_restores_fencing_tokens_and_wait_queues() {
        let original = StateMachine::new();
        let base = now_ms();
        original.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["snapshot-lock".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base,
        );
        original.apply_at(
            Command::LockAcquireV2 {
                keys: vec!["snapshot-lock".to_string()],
                holder: "waiter".to_string(),
                ttl_ms: 300_000,
                wait: true,
                wait_timeout_ms: Some(120_000),
            },
            base + 1,
        );
        let bytes = original.snapshot().unwrap();
        let restored = StateMachine::new();
        restored.restore(&bytes).unwrap();
        let inventory = restored.lock_inventory();
        assert_eq!(inventory.held.len(), 1);
        assert_eq!(inventory.wait_queue.len(), 1);
        assert_eq!(inventory.held[0].fencing_token, 1);
        assert_eq!(inventory.wait_queue[0].holder, "waiter");
        assert_eq!(
            inventory.wait_queue[0].wait_expires_ms,
            Some(base + 120_001),
            "the replicated queue deadline survives snapshot restart"
        );
    }

    #[test]
    fn legacy_snapshots_without_queue_deadlines_restore_compatibly() {
        let original = StateMachine::new();
        let base = now_ms();
        original.apply_at(
            Command::LockAcquire {
                keys: vec!["legacy-lock".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base,
        );
        original.apply_at(
            Command::LockAcquire {
                keys: vec!["legacy-lock".to_string()],
                holder: "waiter".to_string(),
                ttl_ms: 300_000,
                wait: true,
                wait_timeout_ms: Some(120_000),
            },
            base + 1,
        );
        original.apply_at(
            Command::SemaphoreAcquire {
                key: "legacy-pool".to_string(),
                holder: "owner".to_string(),
                limit: 1,
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base + 2,
        );
        original.apply_at(
            Command::SemaphoreAcquire {
                key: "legacy-pool".to_string(),
                holder: "waiter".to_string(),
                limit: 1,
                ttl_ms: 300_000,
                wait: true,
                wait_timeout_ms: Some(120_000),
            },
            base + 3,
        );

        let mut value: serde_json::Value =
            serde_json::from_slice(&original.snapshot().unwrap()).unwrap();
        value["locks"]["queue"][0][1]
            .as_object_mut()
            .unwrap()
            .remove("wait_expires_ms");
        value["semaphores"]["legacy-pool"]["queue"][0][1]
            .as_object_mut()
            .unwrap()
            .remove("wait_expires_ms");

        let restored = StateMachine::new();
        restored
            .restore(&serde_json::to_vec(&value).unwrap())
            .unwrap();
        assert_eq!(restored.lock_inventory().wait_queue.len(), 1);
        assert_eq!(
            restored.lock_inventory().wait_queue[0].wait_expires_ms,
            None
        );
        assert_eq!(
            restored.semaphore_inventory()[0].wait_queue[0].wait_expires_ms,
            None
        );
    }

    #[test]
    fn historical_acquire_log_entries_preserve_indefinite_waits() {
        let sm = StateMachine::new();
        sm.apply_at(
            Command::LockAcquire {
                keys: vec!["legacy-log".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 10_000,
                wait: false,
                wait_timeout_ms: None,
            },
            5_000,
        );
        let historical: Command = serde_json::from_value(json!({
            "op": "lock_acquire",
            "keys": ["legacy-log"],
            "holder": "waiter",
            "ttl_ms": 10_000,
            "wait": true
        }))
        .unwrap();
        let queued = sm.apply_at(historical, 5_001);
        assert_eq!(queued.output["wait_expires_ms"], Value::Null);

        sm.apply_at(
            Command::SemaphoreAcquire {
                key: "legacy-pool-log".to_string(),
                holder: "owner".to_string(),
                limit: 1,
                ttl_ms: 10_000,
                wait: false,
                wait_timeout_ms: None,
            },
            6_000,
        );
        let historical: Command = serde_json::from_value(json!({
            "op": "semaphore_acquire",
            "key": "legacy-pool-log",
            "holder": "waiter",
            "limit": 1,
            "ttl_ms": 10_000,
            "wait": true
        }))
        .unwrap();
        let queued = sm.apply_at(historical, 6_001);
        assert_eq!(queued.output["wait_expires_ms"], Value::Null);
    }

    #[test]
    fn historical_acquire_log_entries_preserve_renew_and_limit_reconfiguration() {
        let sm = StateMachine::new();
        let first = sm.apply_at(
            Command::LockAcquire {
                keys: vec!["legacy-lock".to_string()],
                holder: "holder".to_string(),
                ttl_ms: 100,
                wait: false,
                wait_timeout_ms: None,
            },
            1_000,
        );
        let retry = sm.apply_at(
            Command::LockAcquire {
                keys: vec!["legacy-lock".to_string()],
                holder: "holder".to_string(),
                ttl_ms: 500,
                wait: false,
                wait_timeout_ms: None,
            },
            1_050,
        );
        assert_eq!(retry.output["fencing_token"], first.output["fencing_token"]);
        assert_eq!(retry.output["renewed"], true);
        assert_eq!(retry.output["lease_expires_ms"], 1_550);

        sm.apply_at(
            Command::SemaphoreAcquire {
                key: "legacy-pool".to_string(),
                holder: "holder-a".to_string(),
                limit: 1,
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            },
            2_000,
        );
        let reconfigured = sm.apply_at(
            Command::SemaphoreAcquire {
                key: "legacy-pool".to_string(),
                holder: "holder-b".to_string(),
                limit: 2,
                ttl_ms: 1_000,
                wait: false,
                wait_timeout_ms: None,
            },
            2_001,
        );
        assert_eq!(reconfigured.output["acquired"], true);
        let store = sm.store.lock().unwrap();
        assert_eq!(store.semaphores["legacy-pool"].limit, 2);
        assert_eq!(store.semaphores["legacy-pool"].holders.len(), 2);
    }

    #[test]
    fn protocol_activation_stamps_snapshots_for_fail_closed_downgrades() {
        let state = StateMachine::new();
        let legacy = state.snapshot().unwrap();
        assert!(serde_json::from_slice::<serde_json::Value>(&legacy).is_ok());
        assert_eq!(state.command_protocol(), LEGACY_COMMAND_PROTOCOL);

        let activated = state.apply_at(
            Command::ActivateCommandProtocol {
                version: CURRENT_COMMAND_PROTOCOL,
            },
            1_000,
        );
        assert_eq!(activated.output["activated"], true);
        assert_eq!(
            activated.revision, 0,
            "internal activation is not a user revision"
        );
        let stamped = state.snapshot().unwrap();
        assert!(stamped.starts_with(CURRENT_PROTOCOL_SNAPSHOT_MAGIC));
        assert!(
            serde_json::from_slice::<serde_json::Value>(&stamped).is_err(),
            "the previous release's raw-JSON snapshot parser must fail closed"
        );

        let restored = StateMachine::new();
        restored.restore(&stamped).unwrap();
        assert_eq!(restored.command_protocol(), CURRENT_COMMAND_PROTOCOL);

        let raw_v2 = stamped
            .strip_prefix(CURRENT_PROTOCOL_SNAPSHOT_MAGIC)
            .unwrap();
        assert!(
            StateMachine::new().restore(raw_v2).is_err(),
            "a V2 state claiming no compatibility envelope is corrupt"
        );
    }

    #[test]
    fn snapshot_restore_keeps_the_fencing_counter_ahead_of_prior_grants() {
        let original = StateMachine::new();
        let base = now_ms();
        let first = original.apply_at(
            Command::LockAcquire {
                keys: vec!["before-restart".to_string()],
                holder: "owner-a".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base,
        );
        let first_token = first.output["fencing_token"].as_u64().unwrap();

        let restored = StateMachine::new();
        restored.restore(&original.snapshot().unwrap()).unwrap();
        let next = restored.apply_at(
            Command::LockAcquire {
                keys: vec!["after-restart".to_string()],
                holder: "owner-b".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            base.saturating_add(1),
        );
        assert!(
            next.output["fencing_token"].as_u64().unwrap() > first_token,
            "a restored state machine must not reissue a fencing token"
        );
    }

    #[test]
    fn restore_rejects_a_zeroed_or_missing_fencing_counter() {
        // A state that has minted a token: one held lock (fencing token 1).
        let original = StateMachine::new();
        original.apply_at(
            Command::LockAcquire {
                keys: vec!["guarded".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            now_ms(),
        );
        let bytes = original.snapshot().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Sanity: the untouched snapshot restores.
        StateMachine::new().restore(&bytes).unwrap();

        // Counter regressed below a referenced token (bit-flip / buggy writer):
        // refused, or a future mint would reissue token 1 (split-brain).
        value["next_fencing_token"] = serde_json::json!(0);
        let regressed = serde_json::to_vec(&value).unwrap();
        let err = StateMachine::new().restore(&regressed).unwrap_err();
        assert!(
            err.to_string().contains("re-mint"),
            "unexpected error: {err}"
        );

        // Counter field absent entirely (migrated/truncated snapshot): serde's
        // `#[serde(default)]` would silently zero it — the validator refuses.
        value.as_object_mut().unwrap().remove("next_fencing_token");
        let missing = serde_json::to_vec(&value).unwrap();
        assert!(StateMachine::new().restore(&missing).is_err());
    }

    #[test]
    fn restore_rejects_an_inconsistent_lock_table() {
        let original = StateMachine::new();
        original.apply_at(
            Command::LockAcquire {
                keys: vec!["guarded".to_string()],
                holder: "owner".to_string(),
                ttl_ms: 300_000,
                wait: false,
                wait_timeout_ms: None,
            },
            now_ms(),
        );
        let bytes = original.snapshot().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Drop the grant but keep the held-key entry pointing at it: a
        // half-released lock. Must be refused, not served.
        value["locks"]["grants"].as_object_mut().unwrap().clear();
        let torn = serde_json::to_vec(&value).unwrap();
        let err = StateMachine::new().restore(&torn).unwrap_err();
        assert!(
            err.to_string().contains("missing grant"),
            "unexpected error: {err}"
        );
    }
}
