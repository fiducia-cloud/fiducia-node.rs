//! The replicated state machine.
//!
//! Every mutation exposed by Fiducia is represented as a [`Command`] and is applied
//! in committed-log order. In this single-node skeleton the log is local, but the
//! state-machine semantics are the same ones the replicated path will use.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const LOCK_COORDINATION_ROUTING_KEY: &str = "__fiducia_lock_coordination__";
const MAX_COMPOSITE_LOCK_KEYS: usize = 5;

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

    // --- Mutual-exclusion locks -------------------------------------------
    LockAcquire {
        key: String,
        holder: String,
        ttl_ms: u64,
        wait: bool,
        max: u32,
    },
    LockAcquireMany {
        keys: Vec<String>,
        holder: String,
        ttl_ms: u64,
        wait: bool,
    },
    LockRelease {
        key: String,
        holder: String,
        fencing_token: u64,
    },
    LockReleaseMany {
        lock_id: String,
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

impl Command {
    /// Key used to route this command to its owning shard.
    pub fn routing_key(&self) -> &str {
        match self {
            Command::KvPut { key, .. }
            | Command::KvDelete { key }
            | Command::RateLimitCheck { key, .. } => key,
            Command::LockAcquire { .. }
            | Command::LockAcquireMany { .. }
            | Command::LockRelease { .. }
            | Command::LockReleaseMany { .. } => LOCK_COORDINATION_ROUTING_KEY,
            Command::ScheduleUpsert { name, .. } | Command::ScheduleRecordRun { name, .. } => name,
            Command::ElectionCampaign { name, .. }
            | Command::ElectionRenew { name, .. }
            | Command::ElectionResign { name, .. } => name,
            Command::ServiceRegister { service, .. }
            | Command::ServiceHeartbeat { service, .. }
            | Command::ServiceDeregister { service, .. } => service,
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

/// Current lock state for a key.
#[derive(Debug, Clone, Serialize)]
pub struct LockState {
    pub key: String,
    pub holder: Option<String>,
    pub fencing_token: Option<u64>,
    pub lease_expires_ms: Option<u64>,
    pub holders: Vec<LockHolderState>,
    pub max_holders: u32,
    pub available: u32,
    pub wait_queue: Vec<LockWaiter>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LockHolderState {
    pub holder: String,
    pub lock_id: String,
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
    pub keys: Vec<String>,
    pub exclusive: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LockWaiter {
    pub holder: String,
    pub requested_ms: u64,
    pub keys: Vec<String>,
    pub max_holders: u32,
}

#[derive(Debug, Clone)]
struct QueuedLockWaiter {
    holder: String,
    requested_ms: u64,
    ttl_ms: u64,
    max_holders: u32,
}

#[derive(Debug, Clone)]
struct ActiveLockHolder {
    holder: String,
    lock_id: String,
    fencing_token: u64,
    lease_expires_ms: u64,
    keys: Vec<String>,
    exclusive: bool,
}

#[derive(Debug, Clone)]
struct LockRecord {
    max_holders: u32,
    holders: Vec<ActiveLockHolder>,
    wait_queue: VecDeque<QueuedLockWaiter>,
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
    locks: HashMap<String, LockRecord>,
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
                key,
                holder,
                ttl_ms,
                wait,
                max,
            } => store.apply_lock_acquire(revision, now, key, holder, ttl_ms, wait, max),
            Command::LockAcquireMany {
                keys,
                holder,
                ttl_ms,
                wait,
            } => store.apply_lock_acquire_many(revision, now, keys, holder, ttl_ms, wait),
            Command::LockRelease {
                key,
                holder,
                fencing_token,
            } => store.apply_lock_release(revision, now, key, holder, fencing_token),
            Command::LockReleaseMany { lock_id } => {
                store.apply_lock_release_many(revision, now, lock_id)
            }
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
        let expired_composite_locks: HashSet<String> = self
            .locks
            .values()
            .flat_map(|lock| lock.holders.iter())
            .filter(|holder| holder.keys.len() > 1 && holder.lease_expires_ms <= now)
            .map(|holder| holder.lock_id.clone())
            .collect();
        self.locks.retain(|_, lock| {
            lock.holders.retain(|holder| {
                holder.lease_expires_ms > now && !expired_composite_locks.contains(&holder.lock_id)
            });
            !lock.holders.is_empty() || !lock.wait_queue.is_empty()
        });
        self.elections
            .retain(|_, leadership| leadership.lease_expires_ms > now);
        for instances in self.services.values_mut() {
            instances.retain(|_, instance| instance.lease_expires_ms > now);
        }
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

    fn apply_lock_acquire(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        ttl_ms: u64,
        wait: bool,
        max: u32,
    ) -> Value {
        let max_holders = max.max(1);
        if self.can_grant_single(&key, &holder, max_holders) {
            let grant = self.grant_single_lock(
                revision,
                now,
                key.clone(),
                holder.clone(),
                ttl_ms,
                max_holders,
            );
            return json!({
                "acquired": true,
                "queued": false,
                "key": key,
                "holder": holder,
                "lock_id": grant.lock_id,
                "fencing_token": grant.fencing_token,
                "lease_expires_ms": grant.lease_expires_ms,
                "holders": self.lock_holder_count(&key),
                "max": self.lock_max_holders(&key),
                "available": self.lock_available(&key),
                "revision": revision,
            });
        }

        let lock = self.lock_entry(&key, max_holders);
        if wait && !lock.wait_queue.iter().any(|queued| queued.holder == holder) {
            lock.wait_queue.push_back(QueuedLockWaiter {
                holder: holder.clone(),
                requested_ms: now,
                ttl_ms,
                max_holders,
            });
        }
        let position = lock
            .wait_queue
            .iter()
            .position(|queued| queued.holder == holder)
            .map(|idx| idx + 1);
        json!({
            "acquired": false,
            "queued": wait,
            "position": position,
            "key": key,
            "holder": holder,
            "current_holder": lock.holders.first().map(|holder| holder.holder.clone()),
            "holders": lock.holders.len(),
            "max": lock.max_holders,
            "available": lock.max_holders.saturating_sub(lock.holders.len() as u32),
            "revision": revision,
        })
    }

    fn apply_lock_acquire_many(
        &mut self,
        revision: u64,
        now: u64,
        keys: Vec<String>,
        holder: String,
        ttl_ms: u64,
        wait: bool,
    ) -> Value {
        let keys = match canonical_lock_keys(keys) {
            Ok(keys) => keys,
            Err(reason) => {
                return json!({
                    "acquired": false,
                    "queued": false,
                    "reason": reason,
                    "revision": revision,
                });
            }
        };
        let conflict_keys = self.composite_conflicts(&keys);
        if !conflict_keys.is_empty() {
            return json!({
                "acquired": false,
                "queued": false,
                "reason": "contended",
                "wait_supported": false,
                "keys": keys,
                "holder": holder,
                "conflict_keys": conflict_keys,
                "requested_wait": wait,
                "revision": revision,
            });
        }

        let first_token = self.next_token();
        let lock_id = lock_id_for(revision, first_token);
        let mut fencing_tokens = serde_json::Map::new();
        for (idx, key) in keys.iter().enumerate() {
            let fencing_token = if idx == 0 {
                first_token
            } else {
                self.next_token()
            };
            let lease_expires_ms = now.saturating_add(ttl_ms);
            fencing_tokens.insert(key.clone(), json!(fencing_token));
            let active = ActiveLockHolder {
                holder: holder.clone(),
                lock_id: lock_id.clone(),
                fencing_token,
                lease_expires_ms,
                keys: keys.clone(),
                exclusive: true,
            };
            let lock = self.lock_entry(key, 1);
            lock.holders.push(active);
        }

        json!({
            "acquired": true,
            "queued": false,
            "keys": keys,
            "holder": holder,
            "lock_id": lock_id,
            "fencing_tokens": Value::Object(fencing_tokens),
            "lease_expires_ms": now.saturating_add(ttl_ms),
            "revision": revision,
        })
    }

    fn apply_lock_release(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        fencing_token: u64,
    ) -> Value {
        let Some(existing) = self.locks.get(&key) else {
            return json!({ "released": false, "reason": "not_found", "revision": revision });
        };
        let Some(position) = existing
            .holders
            .iter()
            .position(|active| active.holder == holder && active.fencing_token == fencing_token)
        else {
            return json!({ "released": false, "reason": "not_holder", "revision": revision });
        };
        if existing.holders[position].keys.len() > 1 {
            return json!({ "released": false, "reason": "composite_release_required", "lock_id": existing.holders[position].lock_id, "revision": revision });
        }

        if let Some(lock) = self.locks.get_mut(&key) {
            lock.holders.remove(position);
        }
        let transfers = self.grant_queued_single(&key, now, revision);
        let transferred_to = transfers
            .first()
            .and_then(|transfer| transfer.get("holder"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let next_token = transfers
            .first()
            .and_then(|transfer| transfer.get("fencing_token"))
            .and_then(Value::as_u64);
        let next_expires = transfers
            .first()
            .and_then(|transfer| transfer.get("lease_expires_ms"))
            .and_then(Value::as_u64);

        json!({
            "released": true,
            "key": key,
            "transferred_to": transferred_to,
            "transfers": transfers,
            "fencing_token": next_token,
            "lease_expires_ms": next_expires,
            "holders": self.lock_holder_count(&key),
            "max": self.lock_max_holders(&key),
            "available": self.lock_available(&key),
            "revision": revision,
        })
    }

    fn apply_lock_release_many(&mut self, revision: u64, now: u64, lock_id: String) -> Value {
        let mut released_keys = Vec::new();
        for (key, lock) in self.locks.iter_mut() {
            let before = lock.holders.len();
            lock.holders.retain(|active| active.lock_id != lock_id);
            if lock.holders.len() != before {
                released_keys.push(key.clone());
            }
        }
        if released_keys.is_empty() {
            return json!({ "released": false, "reason": "not_found", "lock_id": lock_id, "revision": revision });
        }

        released_keys.sort();
        let mut transfers = Vec::new();
        for key in &released_keys {
            transfers.extend(self.grant_queued_single(key, now, revision));
        }

        json!({
            "released": true,
            "lock_id": lock_id,
            "keys": released_keys,
            "transfers": transfers,
            "revision": revision,
        })
    }

    fn lock_entry(&mut self, key: &str, max_holders: u32) -> &mut LockRecord {
        self.locks
            .entry(key.to_string())
            .or_insert_with(|| LockRecord {
                max_holders: max_holders.max(1),
                holders: Vec::new(),
                wait_queue: VecDeque::new(),
            })
    }

    fn can_grant_single(&self, key: &str, holder: &str, max_holders: u32) -> bool {
        let Some(lock) = self.locks.get(key) else {
            return true;
        };
        if lock.wait_queue.front().is_some() {
            return false;
        }
        if lock
            .holders
            .iter()
            .any(|active| active.exclusive || active.holder == holder)
        {
            return false;
        }
        let capacity = if lock.holders.is_empty() {
            max_holders.max(1)
        } else {
            lock.max_holders
        };
        (lock.holders.len() as u32) < capacity
    }

    fn grant_single_lock(
        &mut self,
        revision: u64,
        now: u64,
        key: String,
        holder: String,
        ttl_ms: u64,
        max_holders: u32,
    ) -> ActiveLockHolder {
        let fencing_token = self.next_token();
        let lease_expires_ms = now.saturating_add(ttl_ms);
        let active = ActiveLockHolder {
            holder,
            lock_id: lock_id_for(revision, fencing_token),
            fencing_token,
            lease_expires_ms,
            keys: vec![key.clone()],
            exclusive: false,
        };
        let lock = self.lock_entry(&key, max_holders);
        if lock.holders.is_empty() {
            lock.max_holders = max_holders.max(1);
        }
        lock.holders.push(active.clone());
        active
    }

    fn grant_queued_single(&mut self, key: &str, now: u64, revision: u64) -> Vec<Value> {
        let mut transfers = Vec::new();
        loop {
            let can_grant = self
                .locks
                .get(key)
                .map(|lock| {
                    !lock.holders.iter().any(|active| active.exclusive)
                        && (lock.holders.len() as u32) < lock.max_holders
                        && lock.wait_queue.front().is_some()
                })
                .unwrap_or(false);
            if !can_grant {
                break;
            }
            let waiter = self
                .locks
                .get_mut(key)
                .and_then(|lock| lock.wait_queue.pop_front());
            let Some(waiter) = waiter else {
                break;
            };
            let grant = self.grant_single_lock(
                revision,
                now,
                key.to_string(),
                waiter.holder,
                waiter.ttl_ms,
                waiter.max_holders,
            );
            transfers.push(json!({
                "key": key,
                "holder": grant.holder,
                "lock_id": grant.lock_id,
                "fencing_token": grant.fencing_token,
                "lease_expires_ms": grant.lease_expires_ms,
            }));
        }
        transfers
    }

    fn composite_conflicts(&self, keys: &[String]) -> Vec<String> {
        keys.iter()
            .filter(|key| {
                self.locks
                    .get(key.as_str())
                    .map(|lock| !lock.holders.is_empty() || !lock.wait_queue.is_empty())
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    fn lock_holder_count(&self, key: &str) -> usize {
        self.locks
            .get(key)
            .map(|lock| lock.holders.len())
            .unwrap_or(0)
    }

    fn lock_max_holders(&self, key: &str) -> u32 {
        self.locks
            .get(key)
            .map(|lock| lock.max_holders)
            .unwrap_or(1)
    }

    fn lock_available(&self, key: &str) -> u32 {
        self.locks
            .get(key)
            .map(|lock| lock.max_holders.saturating_sub(lock.holders.len() as u32))
            .unwrap_or(1)
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
        json!({ "deregistered": removed, "service": service, "instance_id": instance_id })
    }

    fn lock_snapshot(&self, key: &str) -> LockState {
        let Some(lock) = self.locks.get(key) else {
            return LockState {
                key: key.to_string(),
                holder: None,
                fencing_token: None,
                lease_expires_ms: None,
                holders: Vec::new(),
                max_holders: 1,
                available: 1,
                wait_queue: Vec::new(),
            };
        };
        let holders: Vec<LockHolderState> = lock
            .holders
            .iter()
            .map(|holder| LockHolderState {
                holder: holder.holder.clone(),
                lock_id: holder.lock_id.clone(),
                fencing_token: holder.fencing_token,
                lease_expires_ms: holder.lease_expires_ms,
                keys: holder.keys.clone(),
                exclusive: holder.exclusive,
            })
            .collect();
        let first = holders.first();
        LockState {
            key: key.to_string(),
            holder: first.map(|holder| holder.holder.clone()),
            fencing_token: first.map(|holder| holder.fencing_token),
            lease_expires_ms: first.map(|holder| holder.lease_expires_ms),
            holders,
            max_holders: lock.max_holders,
            available: lock.max_holders.saturating_sub(lock.holders.len() as u32),
            wait_queue: lock
                .wait_queue
                .iter()
                .map(|waiter| LockWaiter {
                    holder: waiter.holder.clone(),
                    requested_ms: waiter.requested_ms,
                    keys: vec![key.to_string()],
                    max_holders: waiter.max_holders,
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

fn canonical_lock_keys(mut keys: Vec<String>) -> Result<Vec<String>, &'static str> {
    keys.retain(|key| !key.trim().is_empty());
    keys.sort();
    keys.dedup();
    if keys.is_empty() {
        return Err("empty_keys");
    }
    if keys.len() > MAX_COMPOSITE_LOCK_KEYS {
        return Err("too_many_keys");
    }
    Ok(keys)
}

fn lock_id_for(revision: u64, fencing_token: u64) -> String {
    format!("lck_{revision}_{fencing_token}")
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

    #[test]
    fn lock_acquire_queues_and_transfers_with_fencing_tokens() {
        let sm = StateMachine::new();
        let first = sm.apply(Command::LockAcquire {
            key: "orders/checkout".to_string(),
            holder: "worker-a".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        assert_eq!(first.output["acquired"], true);
        let token = first.output["fencing_token"].as_u64().unwrap();

        let second = sm.apply(Command::LockAcquire {
            key: "orders/checkout".to_string(),
            holder: "worker-b".to_string(),
            ttl_ms: 30_000,
            wait: true,
            max: 1,
        });
        assert_eq!(second.output["queued"], true);
        assert_eq!(second.output["position"], 1);

        let release = sm.apply(Command::LockRelease {
            key: "orders/checkout".to_string(),
            holder: "worker-a".to_string(),
            fencing_token: token,
        });
        assert_eq!(release.output["released"], true);
        assert_eq!(release.output["transferred_to"], "worker-b");
        assert!(release.output["fencing_token"].as_u64().unwrap() > token);
    }

    #[test]
    fn semaphore_allows_multiple_holders_up_to_cap() {
        let sm = StateMachine::new();
        let first = sm.apply(Command::LockAcquire {
            key: "deploys".to_string(),
            holder: "worker-a".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 2,
        });
        let second = sm.apply(Command::LockAcquire {
            key: "deploys".to_string(),
            holder: "worker-b".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 2,
        });
        let third = sm.apply(Command::LockAcquire {
            key: "deploys".to_string(),
            holder: "worker-c".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 2,
        });

        assert_eq!(first.output["acquired"], true);
        assert_eq!(second.output["acquired"], true);
        assert_eq!(third.output["acquired"], false);
        assert_eq!(third.output["max"], 2);
        assert_eq!(sm.lock_get("deploys").holders.len(), 2);
        assert_eq!(sm.lock_get("deploys").available, 0);
    }

    #[test]
    fn multi_key_lock_blocks_any_overlapping_key_until_released() {
        let sm = StateMachine::new();
        let composite = sm.apply(Command::LockAcquireMany {
            keys: vec!["inventory".to_string(), "orders".to_string()],
            holder: "worker-a".to_string(),
            ttl_ms: 30_000,
            wait: false,
        });
        assert_eq!(composite.output["acquired"], true);
        assert_eq!(composite.output["keys"][0], "inventory");
        assert_eq!(composite.output["keys"][1], "orders");
        assert!(composite.output["fencing_tokens"]["inventory"]
            .as_u64()
            .is_some());
        assert!(composite.output["fencing_tokens"]["orders"]
            .as_u64()
            .is_some());

        let blocked = sm.apply(Command::LockAcquire {
            key: "orders".to_string(),
            holder: "worker-b".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        let unrelated = sm.apply(Command::LockAcquire {
            key: "payments".to_string(),
            holder: "worker-c".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        assert_eq!(blocked.output["acquired"], false);
        assert_eq!(unrelated.output["acquired"], true);

        let lock_id = composite.output["lock_id"].as_str().unwrap().to_string();
        let release = sm.apply(Command::LockReleaseMany { lock_id });
        assert_eq!(release.output["released"], true);
        assert_eq!(release.output["keys"][0], "inventory");
        assert_eq!(release.output["keys"][1], "orders");

        let after_release = sm.apply(Command::LockAcquire {
            key: "orders".to_string(),
            holder: "worker-b".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        assert_eq!(after_release.output["acquired"], true);
    }

    #[test]
    fn expired_multi_key_lock_releases_every_member_key() {
        let sm = StateMachine::new();
        let composite = sm.apply(Command::LockAcquireMany {
            keys: vec!["inventory".to_string(), "orders".to_string()],
            holder: "worker-a".to_string(),
            ttl_ms: 1,
            wait: false,
        });
        assert_eq!(composite.output["acquired"], true);
        std::thread::sleep(std::time::Duration::from_millis(5));

        let blocked = sm.apply(Command::LockAcquire {
            key: "orders".to_string(),
            holder: "worker-b".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        assert_eq!(blocked.output["acquired"], true);

        let other_member = sm.apply(Command::LockAcquire {
            key: "inventory".to_string(),
            holder: "worker-c".to_string(),
            ttl_ms: 30_000,
            wait: false,
            max: 1,
        });
        assert_eq!(other_member.output["acquired"], true);
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
}
