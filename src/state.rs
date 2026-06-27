//! The replicated state machine.
//!
//! Every mutation exposed by Fiducia is represented as a [`Command`] and is applied
//! in committed-log order. In this single-node skeleton the log is local, but the
//! state-machine semantics are the same ones the replicated path will use.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Every mutation in the system, as it travels through the replicated log.
///
/// Read operations never become commands. They are served directly off applied
/// state after the request has reached the shard leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Command {
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
    },
    /// Release a held lock by its fencing token, freeing every member key at once
    /// and promoting the next grantable waiter(s).
    LockRelease {
        holder: String,
        fencing_token: u64,
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
    },
    /// Release one held permit by its fencing token, admitting the next waiter.
    SemaphoreRelease {
        key: String,
        holder: String,
        fencing_token: u64,
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

    // --- Cron / scheduling -------------------------------------------------
    ScheduleUpsert {
        name: String,
        cron: Option<String>,
        one_shot_at_ms: Option<u64>,
        target: ScheduleTarget,
        delivery: DeliverySemantics,
        max_retries: u32,
    },
    ScheduleRecordRun {
        name: String,
        fire_id: String,
        fired_at_ms: u64,
    },

    // --- Leader election ---------------------------------------------------
    ElectionCampaign {
        name: String,
        candidate: String,
        ttl_ms: u64,
    },
    ElectionRenew {
        name: String,
        candidate: String,
        fencing_token: u64,
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
            Command::LockAcquire { .. }
            | Command::LockRelease { .. }
            | Command::SemaphoreAcquire { .. }
            | Command::SemaphoreRelease { .. } => LOCK_DOMAIN,
            Command::KvPut { key, .. }
            | Command::KvDelete { key }
            | Command::RateLimitCheck { key, .. } => key,
            Command::ScheduleUpsert { name, .. } | Command::ScheduleRecordRun { name, .. } => name,
            Command::ElectionCampaign { name, .. }
            | Command::ElectionRenew { name, .. }
            | Command::ElectionResign { name, .. } => name,
            Command::ServiceRegister { .. }
            | Command::ServiceHeartbeat { .. }
            | Command::ServiceDeregister { .. } => SERVICE_DOMAIN,
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
#[derive(Debug, Clone, Serialize)]
pub struct KvEntry {
    pub value: String,
    pub mod_revision: u64,
    pub expires_at_ms: Option<u64>,
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
}

/// One held union-lock acquisition.
#[derive(Debug, Clone)]
struct LockGrant {
    holder: String,
    keys: Vec<String>,
    fencing_token: u64,
    lease_expires_ms: u64,
}

/// One queued union-lock request awaiting its whole key set.
#[derive(Debug, Clone)]
struct QueuedLock {
    holder: String,
    keys: Vec<String>,
    ttl_ms: u64,
    requested_ms: u64,
}

/// The multi-key lock table: which member key is held by which grant, the grants
/// themselves, and the FIFO wait queue of whole requests.
#[derive(Default)]
struct LockManager {
    /// member key → owning grant's fencing token.
    held: HashMap<String, u64>,
    /// fencing token → grant.
    grants: HashMap<u64, LockGrant>,
    /// FIFO queue of requests waiting for their full union to be free.
    queue: VecDeque<QueuedLock>,
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

#[derive(Debug, Clone)]
struct SemaphoreSlot {
    holder: String,
    fencing_token: u64,
    lease_expires_ms: u64,
}

#[derive(Debug, Clone)]
struct QueuedPermit {
    holder: String,
    ttl_ms: u64,
    requested_ms: u64,
}

/// A counting semaphore: up to `limit` permits, plus a FIFO queue for the rest.
#[derive(Debug, Clone)]
struct Semaphore {
    limit: u32,
    holders: Vec<SemaphoreSlot>,
    queue: VecDeque<QueuedPermit>,
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

#[derive(Debug, Clone)]
struct RateLimitRecord {
    algorithm: RateLimitAlgorithm,
    limit: u32,
    window_ms: u64,
    tokens: f64,
    updated_ms: u64,
    events: VecDeque<u64>,
    last_allowed: bool,
}

/// The current holder of a named election.
#[derive(Debug, Clone, Serialize)]
pub struct Leadership {
    pub leader: String,
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
}

/// A scheduled job definition.
#[derive(Debug, Clone, Serialize)]
pub struct Schedule {
    pub name: String,
    pub cron: Option<String>,
    pub one_shot_at_ms: Option<u64>,
    pub target: ScheduleTarget,
    pub delivery: DeliverySemantics,
    pub max_retries: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRun {
    pub fire_id: String,
    pub fired_at_ms: u64,
    pub attempts: u32,
    pub duplicate: bool,
    pub target: ScheduleTarget,
}

#[derive(Debug, Clone)]
struct ScheduleRecord {
    definition: Schedule,
    history: Vec<ScheduleRun>,
}

/// One registered service instance.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceInstance {
    pub instance_id: String,
    pub address: String,
    pub lease_expires_ms: u64,
    pub metadata: HashMap<String, String>,
}

#[derive(Default)]
struct Store {
    revision: u64,
    next_fencing_token: u64,
    kv: HashMap<String, KvEntry>,
    locks: LockManager,
    semaphores: HashMap<String, Semaphore>,
    rate_limits: HashMap<String, RateLimitRecord>,
    elections: HashMap<String, Leadership>,
    schedules: HashMap<String, ScheduleRecord>,
    services: HashMap<String, HashMap<String, ServiceInstance>>,
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

    pub fn apply(&self, command: Command) -> ApplyResult {
        let mut store = self.store.lock().unwrap();
        let now = now_ms();
        store.expire_due(now);
        store.revision += 1;
        let revision = store.revision;

        let output = match command {
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
            Command::LockAcquire {
                keys,
                holder,
                ttl_ms,
                wait,
            } => store.apply_lock_acquire(revision, now, keys, holder, ttl_ms, wait),
            Command::LockRelease {
                holder,
                fencing_token,
            } => store.apply_lock_release(revision, now, holder, fencing_token),
            Command::SemaphoreAcquire {
                key,
                holder,
                limit,
                ttl_ms,
                wait,
            } => store.apply_semaphore_acquire(revision, now, key, holder, limit, ttl_ms, wait),
            Command::SemaphoreRelease {
                key,
                holder,
                fencing_token,
            } => store.apply_semaphore_release(revision, now, key, holder, fencing_token),
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
                key,
                tenant,
                algorithm,
                limit,
                window_ms,
                refill_per_second,
                cost.max(1),
            ),
            Command::ScheduleUpsert {
                name,
                cron,
                one_shot_at_ms,
                target,
                delivery,
                max_retries,
            } => store.apply_schedule_upsert(
                name,
                cron,
                one_shot_at_ms,
                target,
                delivery,
                max_retries,
            ),
            Command::ScheduleRecordRun {
                name,
                fire_id,
                fired_at_ms,
            } => store.apply_schedule_record_run(name, fire_id, fired_at_ms),
            Command::ElectionCampaign {
                name,
                candidate,
                ttl_ms,
            } => store.apply_election_campaign(revision, now, name, candidate, ttl_ms),
            Command::ElectionRenew {
                name,
                candidate,
                fencing_token,
            } => store.apply_election_renew(now, name, candidate, fencing_token),
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
        self.store.lock().unwrap().revision
    }

    pub fn kv_get(&self, key: &str) -> Option<KvEntry> {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store.kv.get(key).cloned()
    }

    pub fn lock_get(&self, key: &str) -> LockState {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store.lock_snapshot(key)
    }

    pub fn semaphore_get(&self, key: &str) -> SemaphoreState {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store.semaphore_snapshot(key)
    }

    pub fn rate_limit_get(&self, tenant: &str, key: &str) -> Option<RateLimitSnapshot> {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store.rate_limit_snapshot(tenant, key)
    }

    pub fn election_get(&self, name: &str) -> Option<Leadership> {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store.elections.get(name).cloned()
    }

    pub fn schedule_get(&self, name: &str) -> Option<Schedule> {
        self.store
            .lock()
            .unwrap()
            .schedules
            .get(name)
            .map(|record| record.definition.clone())
    }

    pub fn schedule_history(&self, name: &str) -> Vec<ScheduleRun> {
        self.store
            .lock()
            .unwrap()
            .schedules
            .get(name)
            .map(|record| record.history.clone())
            .unwrap_or_default()
    }

    pub fn service_list(&self, service: &str) -> Vec<ServiceInstance> {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        store
            .services
            .get(service)
            .map(|instances| instances.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn service_names(&self) -> Vec<String> {
        let mut store = self.store.lock().unwrap();
        store.expire_due(now_ms());
        let mut names: Vec<String> = store.services.keys().cloned().collect();
        names.sort();
        names
    }
}

impl Store {
    fn next_token(&mut self) -> u64 {
        self.next_fencing_token = self.next_fencing_token.saturating_add(1);
        self.next_fencing_token
    }

    fn expire_due(&mut self, now: u64) {
        self.kv.retain(|_, entry| {
            entry
                .expires_at_ms
                .map(|expires| expires > now)
                .unwrap_or(true)
        });
        // Expire any union-lock grants whose lease lapsed, freeing their member
        // keys, then promote whatever the freed keys now unblock.
        let expired: Vec<u64> = self
            .locks
            .grants
            .iter()
            .filter(|(_, g)| g.lease_expires_ms <= now)
            .map(|(token, _)| *token)
            .collect();
        if !expired.is_empty() {
            for token in expired {
                self.release_grant(token);
            }
            self.lock_promote(now);
        }
        // Expire semaphore permits, then admit whoever was waiting.
        for sem in self.semaphores.values_mut() {
            let before = sem.holders.len();
            sem.holders.retain(|slot| slot.lease_expires_ms > now);
            if sem.holders.len() != before {
                // A slot freed up; admit FIFO waiters up to the limit below.
            }
        }
        self.semaphores_promote(now);
        self.elections
            .retain(|_, leadership| leadership.lease_expires_ms > now);
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

    /// Acquire the **union** of `keys` (multi-key lock), all-or-nothing.
    fn apply_lock_acquire(
        &mut self,
        revision: u64,
        now: u64,
        keys: Vec<String>,
        holder: String,
        ttl_ms: u64,
        wait: bool,
    ) -> Value {
        let keys = canonical_keys(&keys);
        if keys.is_empty() {
            return json!({ "acquired": false, "reason": "no_keys", "revision": revision });
        }

        // Grantable now iff no member key is held AND none is reserved by a
        // request already in the queue (FIFO fairness — we'd join the tail).
        let blocked_by_held = keys.iter().any(|k| self.locks.held.contains_key(k));
        let reserved: std::collections::HashSet<&str> = self
            .locks
            .queue
            .iter()
            .flat_map(|q| q.keys.iter().map(|k| k.as_str()))
            .collect();
        let blocked_by_queue = keys.iter().any(|k| reserved.contains(k.as_str()));

        if !blocked_by_held && !blocked_by_queue {
            let token = self.next_token();
            let lease_expires_ms = now.saturating_add(ttl_ms);
            self.install_grant(LockGrant {
                holder: holder.clone(),
                keys: keys.clone(),
                fencing_token: token,
                lease_expires_ms,
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
        let already = self
            .locks
            .queue
            .iter()
            .any(|q| q.holder == holder && q.keys == keys);
        if wait && !already {
            self.locks.queue.push_back(QueuedLock {
                holder: holder.clone(),
                keys: keys.clone(),
                ttl_ms,
                requested_ms: now,
            });
        }
        let position = self
            .locks
            .queue
            .iter()
            .position(|q| q.holder == holder && q.keys == keys)
            .map(|idx| idx + 1);
        let conflicts: Vec<String> = keys
            .iter()
            .filter(|k| self.locks.held.contains_key(*k))
            .cloned()
            .collect();
        json!({
            "acquired": false,
            "queued": wait && position.is_some(),
            "position": position,
            "keys": keys,
            "holder": holder,
            "conflicts": conflicts,
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
    fn lock_first_grantable(&self) -> Option<usize> {
        let mut reserved: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (idx, q) in self.locks.queue.iter().enumerate() {
            let blocked = q
                .keys
                .iter()
                .any(|k| self.locks.held.contains_key(k) || reserved.contains(k.as_str()));
            if !blocked {
                return Some(idx);
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
        while let Some(idx) = self.lock_first_grantable() {
            let waiter = self.locks.queue.remove(idx).expect("index from scan");
            let token = self.next_token();
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
            });
        }
        promoted
    }

    // --- counting semaphores ---------------------------------------------

    fn apply_semaphore_acquire(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        limit: u32,
        ttl_ms: u64,
        wait: bool,
    ) -> Value {
        let sem = self
            .semaphores
            .entry(key.clone())
            .or_insert_with(|| Semaphore {
                limit: limit.max(1),
                holders: Vec::new(),
                queue: VecDeque::new(),
            });
        // Let callers re-tune the cap; shrinking just stops new grants until it
        // drains back under the new limit.
        sem.limit = limit.max(1);

        let has_capacity = (sem.holders.len() as u32) < sem.limit;
        let queue_empty = sem.queue.is_empty();
        if has_capacity && queue_empty {
            let token = self.next_token();
            let lease_expires_ms = now.saturating_add(ttl_ms);
            let sem = self.semaphores.get_mut(&key).expect("just inserted");
            sem.holders.push(SemaphoreSlot {
                holder: holder.clone(),
                fencing_token: token,
                lease_expires_ms,
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

        let already = sem.queue.iter().any(|q| q.holder == holder);
        if wait && !already {
            sem.queue.push_back(QueuedPermit {
                holder: holder.clone(),
                ttl_ms,
                requested_ms: now,
            });
        }
        let position = sem
            .queue
            .iter()
            .position(|q| q.holder == holder)
            .map(|idx| idx + 1);
        json!({
            "acquired": false,
            "queued": wait && position.is_some(),
            "position": position,
            "key": key,
            "holder": holder,
            "limit": sem.limit,
            "available": 0,
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

    /// Admit FIFO waiters of one semaphore up to its limit.
    fn semaphore_promote(&mut self, key: &str, now: u64) -> Vec<Value> {
        let mut promoted = Vec::new();
        loop {
            let Some(sem) = self.semaphores.get(key) else {
                break;
            };
            if (sem.holders.len() as u32) >= sem.limit || sem.queue.is_empty() {
                break;
            }
            let token = self.next_token();
            let sem = self.semaphores.get_mut(key).expect("checked above");
            let waiter = sem.queue.pop_front().expect("non-empty checked");
            let lease_expires_ms = now.saturating_add(waiter.ttl_ms);
            sem.holders.push(SemaphoreSlot {
                holder: waiter.holder.clone(),
                fencing_token: token,
                lease_expires_ms,
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

    fn apply_rate_limit_check(
        &mut self,
        now: u64,
        key: String,
        tenant: String,
        algorithm: RateLimitAlgorithm,
        limit: u32,
        window_ms: u64,
        refill_per_second: Option<f64>,
        cost: u32,
    ) -> Value {
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
                record.tokens = (record.tokens + elapsed * refill).min(limit as f64);
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

    fn apply_schedule_upsert(
        &mut self,
        name: String,
        cron: Option<String>,
        one_shot_at_ms: Option<u64>,
        target: ScheduleTarget,
        delivery: DeliverySemantics,
        max_retries: u32,
    ) -> Value {
        let definition = Schedule {
            name: name.clone(),
            cron,
            one_shot_at_ms,
            target,
            delivery,
            max_retries,
            enabled: true,
        };
        self.schedules
            .entry(name.clone())
            .and_modify(|record| record.definition = definition.clone())
            .or_insert_with(|| ScheduleRecord {
                definition,
                history: Vec::new(),
            });
        json!({ "scheduled": true, "name": name })
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
            });
        }
        json!({ "recorded": !duplicate, "duplicate": duplicate, "name": name, "fire_id": fire_id })
    }

    fn apply_election_campaign(
        &mut self,
        revision: u64,
        now: u64,
        name: String,
        candidate: String,
        ttl_ms: u64,
    ) -> Value {
        if self.elections.contains_key(&name) {
            return json!({ "won": false, "name": name, "leader": self.elections.get(&name) });
        }
        let token = self.next_token();
        let leadership = Leadership {
            leader: candidate.clone(),
            fencing_token: token,
            lease_expires_ms: now.saturating_add(ttl_ms),
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
    ) -> Value {
        let Some(leadership) = self.elections.get_mut(&name) else {
            return json!({ "renewed": false, "reason": "not_found", "name": name });
        };
        if leadership.leader != candidate || leadership.fencing_token != fencing_token {
            return json!({ "renewed": false, "reason": "not_leader", "name": name });
        }
        leadership.lease_expires_ms = now.saturating_add(30_000);
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

    fn lock_snapshot(&self, key: &str) -> LockState {
        let grant = self
            .locks
            .held
            .get(key)
            .and_then(|token| self.locks.grants.get(token));
        let wait_queue = self
            .locks
            .queue
            .iter()
            .filter(|q| q.keys.iter().any(|k| k == key))
            .map(|q| LockWaiter {
                holder: q.holder.clone(),
                keys: q.keys.clone(),
                requested_ms: q.requested_ms,
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

    fn semaphore_snapshot(&self, key: &str) -> SemaphoreState {
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
            available: sem.limit.saturating_sub(sem.holders.len() as u32),
            holders: sem
                .holders
                .iter()
                .map(|slot| SemaphoreHolder {
                    holder: slot.holder.clone(),
                    fencing_token: slot.fencing_token,
                    lease_expires_ms: slot.lease_expires_ms,
                })
                .collect(),
            wait_queue: sem
                .queue
                .iter()
                .map(|q| LockWaiter {
                    holder: q.holder.clone(),
                    keys: vec![key.to_string()],
                    requested_ms: q.requested_ms,
                })
                .collect(),
        }
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn valid_cron_expression(value: &str) -> bool {
    value.split_whitespace().count() == 5
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acquire(sm: &StateMachine, keys: &[&str], holder: &str, wait: bool) -> Value {
        sm.apply(Command::LockAcquire {
            keys: keys.iter().map(|s| s.to_string()).collect(),
            holder: holder.to_string(),
            ttl_ms: 30_000,
            wait,
        })
        .output
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
    fn semaphore_caps_concurrent_holders_and_admits_in_fifo() {
        let sm = StateMachine::new();
        let acq = |holder: &str, wait: bool| {
            sm.apply(Command::SemaphoreAcquire {
                key: "db-pool".to_string(),
                holder: holder.to_string(),
                limit: 2,
                ttl_ms: 30_000,
                wait,
            })
            .output
        };
        // limit = 2: first two acquire, third is capped out and queues.
        let a = acq("a", false);
        let b = acq("b", false);
        let c = acq("c", true);
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
    fn election_uses_fencing_tokens_for_campaign_renew_and_resign() {
        let sm = StateMachine::new();
        let won = sm.apply(Command::ElectionCampaign {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            ttl_ms: 30_000,
        });
        let token = won.output["leadership"]["fencing_token"].as_u64().unwrap();
        let stale_renew = sm.apply(Command::ElectionRenew {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token + 1,
        });
        let renewed = sm.apply(Command::ElectionRenew {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token,
        });
        let resigned = sm.apply(Command::ElectionResign {
            name: "scheduler".to_string(),
            candidate: "node-a".to_string(),
            fencing_token: token,
        });

        assert_eq!(won.output["won"], true);
        assert_eq!(stale_renew.output["renewed"], false);
        assert_eq!(renewed.output["renewed"], true);
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
    fn service_names_are_sorted_and_pruned_when_empty() {
        let sm = StateMachine::new();
        sm.apply(Command::ServiceRegister {
            service: "worker".to_string(),
            instance_id: "w-1".to_string(),
            address: "http://10.0.0.2:8080".to_string(),
            ttl_ms: 30_000,
            metadata: HashMap::new(),
        });
        sm.apply(Command::ServiceRegister {
            service: "api".to_string(),
            instance_id: "a-1".to_string(),
            address: "http://10.0.0.1:8080".to_string(),
            ttl_ms: 30_000,
            metadata: HashMap::new(),
        });

        assert_eq!(
            sm.service_names(),
            vec!["api".to_string(), "worker".to_string()]
        );

        sm.apply(Command::ServiceDeregister {
            service: "api".to_string(),
            instance_id: "a-1".to_string(),
        });

        assert_eq!(sm.service_names(), vec!["worker".to_string()]);
        assert!(sm.service_list("api").is_empty());
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
}
