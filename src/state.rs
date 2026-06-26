//! The replicated state machine (skeleton).
//!
//! Committed [`Command`]s from the [`crate::consensus`] log are applied here, in
//! order, to produce the authoritative coordination state: the config KV store,
//! the set of held elections, and the service registry. Because every node
//! applies the same committed log in the same order, every node converges on the
//! same state — that is the whole point of routing mutations through Raft.
//!
//! This is a skeleton: the maps exist and [`StateMachine::apply`] dispatches on
//! the command, but the per-command logic, the monotonic revision counter, TTL
//! expiry, and watch-event emission are left as `TODO`s.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Every mutation in the system, as it travels through the replicated log.
///
/// Read operations never become commands — they are served directly off applied
/// state (subject to a leader lease for linearizability, once clustered).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Command {
    // --- Config KV ---------------------------------------------------------
    KvPut { key: String, value: String, ttl_ms: Option<u64> },
    KvDelete { key: String },

    // --- Leader election ---------------------------------------------------
    ElectionCampaign { name: String, candidate: String, ttl_ms: u64 },
    ElectionRenew { name: String, candidate: String, fencing_token: u64 },
    ElectionResign { name: String, candidate: String, fencing_token: u64 },

    // --- Service discovery -------------------------------------------------
    ServiceRegister { service: String, instance_id: String, address: String, ttl_ms: u64 },
    ServiceHeartbeat { service: String, instance_id: String },
    ServiceDeregister { service: String, instance_id: String },
}

impl Command {
    /// The key used to route this command to its owning shard.
    ///
    /// Commands that touch the same logical object must hash to the same shard
    /// so they are ordered by one Raft group: KV by its key, an election by its
    /// name, a service instance by its service name. Service operations route by
    /// service (not instance) so a service's whole instance set lives in one
    /// shard and `GET /v1/services/{service}` is a single-shard read.
    pub fn routing_key(&self) -> &str {
        match self {
            Command::KvPut { key, .. } => key,
            Command::KvDelete { key } => key,
            Command::ElectionCampaign { name, .. } => name,
            Command::ElectionRenew { name, .. } => name,
            Command::ElectionResign { name, .. } => name,
            Command::ServiceRegister { service, .. } => service,
            Command::ServiceHeartbeat { service, .. } => service,
            Command::ServiceDeregister { service, .. } => service,
        }
    }
}

/// A single versioned KV entry.
#[derive(Debug, Clone, Serialize)]
pub struct KvEntry {
    pub value: String,
    /// Revision at which this key was last written.
    pub mod_revision: u64,
    /// Absolute expiry (ms since epoch), if a TTL was set.
    pub expires_at_ms: Option<u64>,
}

/// The current holder of a named election.
#[derive(Debug, Clone, Serialize)]
pub struct Leadership {
    pub leader: String,
    /// Monotonic token a holder must present to renew/resign — and that
    /// downstream resources use to fence stale leaders.
    pub fencing_token: u64,
    pub lease_expires_ms: u64,
}

/// One registered service instance.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceInstance {
    pub instance_id: String,
    pub address: String,
    pub lease_expires_ms: u64,
}

/// The applied coordination state. All fields move together under one lock in
/// the skeleton; a real build would shard or use copy-on-write for read scaling.
#[derive(Default)]
struct Store {
    /// Monotonic revision, bumped on every applied mutation (etcd-style mvcc).
    revision: u64,
    kv: HashMap<String, KvEntry>,
    elections: HashMap<String, Leadership>,
    services: HashMap<String, HashMap<String, ServiceInstance>>,
}

/// Applies committed commands and answers read queries.
pub struct StateMachine {
    store: Mutex<Store>,
    // TODO(watch): a broadcast channel of change events lives here, fed by
    // `apply` and consumed by the SSE `*/watch` endpoints.
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine {
            store: Mutex::new(Store::default()),
        }
    }

    /// Apply one committed command to the state machine.
    ///
    /// Returns the revision produced by the mutation. The skeleton bumps the
    /// revision and matches each command but leaves the mutation bodies as
    /// `TODO`s.
    pub fn apply(&self, command: Command) -> u64 {
        let mut store = self.store.lock().unwrap();
        store.revision += 1;
        let _rev = store.revision;

        match command {
            Command::KvPut { .. } => { /* TODO: upsert KvEntry, emit Put event */ }
            Command::KvDelete { .. } => { /* TODO: remove key, emit Delete event */ }
            Command::ElectionCampaign { .. } => { /* TODO: grant leadership if free/expired */ }
            Command::ElectionRenew { .. } => { /* TODO: extend lease if token matches */ }
            Command::ElectionResign { .. } => { /* TODO: clear leadership if token matches */ }
            Command::ServiceRegister { .. } => { /* TODO: upsert instance */ }
            Command::ServiceHeartbeat { .. } => { /* TODO: extend instance lease */ }
            Command::ServiceDeregister { .. } => { /* TODO: remove instance */ }
        }

        store.revision
    }

    /// Current global revision (for status/debugging).
    pub fn revision(&self) -> u64 {
        self.store.lock().unwrap().revision
    }

    // TODO(reads): typed read helpers used by the GET handlers, e.g.
    //   pub fn kv_get(&self, key: &str) -> Option<KvEntry>
    //   pub fn election_get(&self, name: &str) -> Option<Leadership>
    //   pub fn service_list(&self, service: &str) -> Vec<ServiceInstance>
    // plus a background sweeper that expires TTL'd KV/leases/instances and
    // emits the corresponding watch events.
}
