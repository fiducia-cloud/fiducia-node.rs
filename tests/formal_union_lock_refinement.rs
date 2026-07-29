#![allow(dead_code)]

// Compile the production state machine directly into this integration-test crate.
// This avoids a second implementation adapter while the binary-only crate is
// being split into a reusable library. The reference model below is independent;
// only the system under test comes from these source modules.
#[path = "../src/cron.rs"]
mod cron;
#[path = "../src/indexed_queue.rs"]
mod indexed_queue;
#[path = "../src/state.rs"]
mod state;
#[path = "../src/validate.rs"]
mod validate;

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde_json::Value;
use sha2::{Digest, Sha256};
use state::{Command, StateMachine};

const BASE_TIME_MS: u64 = 4_000_000_000_000;
const HOLD_TTL_MS: u64 = 4_000;
const WAIT_TTL_MS: u64 = 2_000;
const RETRY_TTL_MS: u64 = 9_000;
const RETRY_WAIT_TTL_MS: u64 = 9_000;

const MAX_DEPTH: usize = 5;
const MAX_STATES: usize = 25_000;
const MAX_TRANSITIONS: usize = 400_000;
const ITF_MAX_TOKEN: u64 = 4;
const ITF_TIME_UNIT_MS: u64 = validate::MAX_TTL_MS / 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Attempt {
    holder: u8,
    keys: u8,
    request_id: u8,
}

impl Attempt {
    const fn new(holder: u8, keys: u8, request_id: u8) -> Self {
        Self {
            holder,
            keys,
            request_id,
        }
    }

    fn holder_name(self) -> String {
        format!("h{}", self.holder)
    }

    fn request_name(self) -> String {
        format!("r{}", self.request_id)
    }

    fn key_names(self) -> Vec<String> {
        let mut keys = Vec::new();
        if self.keys & 0b01 != 0 {
            keys.push("k1".to_string());
        }
        if self.keys & 0b10 != 0 {
            keys.push("k2".to_string());
        }
        keys
    }

    fn from_wire(holder: &str, keys: &[String], request_id: &str) -> Self {
        let holder = match holder {
            "h1" => 1,
            "h2" => 2,
            "h3" => 3,
            other => panic!("unexpected holder in formal projection: {other}"),
        };
        let request_id = match request_id {
            "r1" => 1,
            "r2" => 2,
            other => panic!("unexpected request id in formal projection: {other}"),
        };
        let mut key_mask = 0u8;
        for key in keys {
            key_mask |= match key.as_str() {
                "k1" => 0b01,
                "k2" => 0b10,
                other => panic!("unexpected key in formal projection: {other}"),
            };
        }
        assert_ne!(key_mask, 0, "formal attempts must contain at least one key");
        Self::new(holder, key_mask, request_id)
    }

    fn same_queue_identity(self, other: Self) -> bool {
        self.holder == other.holder && self.keys == other.keys
    }
}

const ATTEMPTS: [Attempt; 4] = [
    Attempt::new(1, 0b01, 1),
    // Same holder/key identity but a different request id exercises queue
    // collision and delayed-cancel behavior.
    Attempt::new(1, 0b01, 2),
    Attempt::new(2, 0b11, 1),
    Attempt::new(3, 0b10, 1),
];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Grant {
    attempt: Attempt,
    token: u64,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Waiter {
    attempt: Attempt,
    ttl_ms: u64,
    requested_ms: u64,
    wait_expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Cancellation {
    attempt: Attempt,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PublicGrant {
    holder: u8,
    keys: u8,
    token: u64,
    expires_at_ms: u64,
}

impl PublicGrant {
    fn from_grant(grant: &Grant) -> Self {
        Self {
            holder: grant.attempt.holder,
            keys: grant.attempt.keys,
            token: grant.token,
            expires_at_ms: grant.expires_at_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Model {
    now_ms: u64,
    last_token: u64,
    grants: Vec<Grant>,
    queue: Vec<Waiter>,
    cancellations: Vec<Cancellation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Acquire {
        attempt: Attempt,
        wait: bool,
        ttl_ms: u64,
        wait_timeout_ms: u64,
    },
    Cancel {
        attempt: Attempt,
    },
    Renew {
        holder: u8,
        keys: u8,
        token: u64,
        ttl_ms: u64,
    },
    Release {
        holder: u8,
        token: u64,
    },
    AdvanceToNextDeadline,
    SnapshotRoundTrip,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Acquire {
        acquired: bool,
        queued: bool,
        position: Option<u64>,
        token: Option<u64>,
        lease_expires_at_ms: Option<u64>,
        wait_expires_at_ms: Option<u64>,
    },
    Cancel {
        cancelled: bool,
        acquired: bool,
        token: Option<u64>,
        lease_expires_at_ms: Option<u64>,
        promoted: Vec<PublicGrant>,
    },
    Renew {
        renewed: bool,
        lease_expires_at_ms: Option<u64>,
    },
    Release {
        released: bool,
        promoted: Vec<PublicGrant>,
    },
    Advanced,
    SnapshotRoundTrip,
}

impl Model {
    fn initial() -> Self {
        Self {
            now_ms: BASE_TIME_MS,
            last_token: 0,
            grants: Vec::new(),
            queue: Vec::new(),
            cancellations: Vec::new(),
        }
    }

    fn actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        for attempt in ATTEMPTS {
            actions.push(Action::Acquire {
                attempt,
                wait: true,
                ttl_ms: HOLD_TTL_MS,
                wait_timeout_ms: WAIT_TTL_MS,
            });
            actions.push(Action::Acquire {
                attempt,
                wait: false,
                ttl_ms: RETRY_TTL_MS,
                wait_timeout_ms: RETRY_WAIT_TTL_MS,
            });
            actions.push(Action::Cancel { attempt });
        }

        for grant in &self.grants {
            actions.push(Action::Renew {
                holder: grant.attempt.holder,
                keys: grant.attempt.keys,
                token: grant.token,
                ttl_ms: RETRY_TTL_MS,
            });
            actions.push(Action::Renew {
                holder: different_holder(grant.attempt.holder),
                keys: grant.attempt.keys,
                token: grant.token,
                ttl_ms: RETRY_TTL_MS,
            });
            actions.push(Action::Release {
                holder: grant.attempt.holder,
                token: grant.token,
            });
            actions.push(Action::Release {
                holder: different_holder(grant.attempt.holder),
                token: grant.token,
            });
        }

        if self.next_deadline().is_some() {
            actions.push(Action::AdvanceToNextDeadline);
        }
        actions.push(Action::SnapshotRoundTrip);
        actions
    }

    fn apply(&mut self, action: &Action) -> Outcome {
        match action {
            Action::SnapshotRoundTrip => return Outcome::SnapshotRoundTrip,
            Action::AdvanceToNextDeadline => {
                self.now_ms = self
                    .next_deadline()
                    .expect("deadline action is generated only when one exists");
                self.expire_due();
                self.assert_invariants();
                return Outcome::Advanced;
            }
            _ => self.expire_due(),
        }

        let outcome = match *action {
            Action::Acquire {
                attempt,
                wait,
                ttl_ms,
                wait_timeout_ms,
            } => self.acquire(attempt, wait, ttl_ms, wait_timeout_ms),
            Action::Cancel { attempt } => self.cancel(attempt),
            Action::Renew {
                holder,
                keys,
                token,
                ttl_ms,
            } => self.renew(holder, keys, token, ttl_ms),
            Action::Release { holder, token } => self.release(holder, token),
            Action::AdvanceToNextDeadline | Action::SnapshotRoundTrip => unreachable!(),
        };
        self.assert_invariants();
        outcome
    }

    fn acquire(
        &mut self,
        attempt: Attempt,
        wait: bool,
        ttl_ms: u64,
        wait_timeout_ms: u64,
    ) -> Outcome {
        if self.has_active_cancellation(attempt) {
            return Outcome::Acquire {
                acquired: false,
                queued: false,
                position: None,
                token: None,
                lease_expires_at_ms: None,
                wait_expires_at_ms: None,
            };
        }

        if let Some(grant) = self.grants.iter().find(|grant| grant.attempt == attempt) {
            return Outcome::Acquire {
                acquired: true,
                queued: false,
                position: None,
                token: Some(grant.token),
                lease_expires_at_ms: Some(grant.expires_at_ms),
                wait_expires_at_ms: None,
            };
        }

        let blocked_by_grant = self
            .grants
            .iter()
            .any(|grant| overlaps(grant.attempt.keys, attempt.keys));
        let blocked_by_queue = self
            .queue
            .iter()
            .any(|waiter| overlaps(waiter.attempt.keys, attempt.keys));

        if !blocked_by_grant && !blocked_by_queue {
            let Some(token) = self.last_token.checked_add(1) else {
                return Outcome::Acquire {
                    acquired: false,
                    queued: false,
                    position: None,
                    token: None,
                    lease_expires_at_ms: None,
                    wait_expires_at_ms: None,
                };
            };
            self.last_token = token;
            let expires_at_ms = self.now_ms.saturating_add(ttl_ms);
            self.grants.push(Grant {
                attempt,
                token,
                expires_at_ms,
            });
            self.grants.sort_by_key(|grant| grant.token);
            return Outcome::Acquire {
                acquired: true,
                queued: false,
                position: None,
                token: Some(token),
                lease_expires_at_ms: Some(expires_at_ms),
                wait_expires_at_ms: None,
            };
        }

        let exact_index = self
            .queue
            .iter()
            .position(|waiter| waiter.attempt == attempt);
        let identity_in_use = exact_index.is_none()
            && self
                .queue
                .iter()
                .any(|waiter| waiter.attempt.same_queue_identity(attempt));

        if wait && exact_index.is_none() && !identity_in_use {
            self.queue.push(Waiter {
                attempt,
                ttl_ms,
                requested_ms: self.now_ms,
                wait_expires_at_ms: self.now_ms.saturating_add(wait_timeout_ms),
            });
        }

        let position = if identity_in_use {
            None
        } else {
            self.queue
                .iter()
                .position(|waiter| waiter.attempt == attempt)
                .map(|index| index as u64 + 1)
        };
        let wait_expires_at_ms = if identity_in_use {
            None
        } else {
            self.queue
                .iter()
                .find(|waiter| waiter.attempt == attempt)
                .map(|waiter| waiter.wait_expires_at_ms)
        };

        Outcome::Acquire {
            acquired: false,
            queued: position.is_some(),
            position,
            token: None,
            lease_expires_at_ms: None,
            wait_expires_at_ms,
        }
    }

    fn cancel(&mut self, attempt: Attempt) -> Outcome {
        if let Some(grant) = self.grants.iter().find(|grant| grant.attempt == attempt) {
            return Outcome::Cancel {
                cancelled: false,
                acquired: true,
                token: Some(grant.token),
                lease_expires_at_ms: Some(grant.expires_at_ms),
                promoted: Vec::new(),
            };
        }

        if !self.has_active_cancellation(attempt) {
            self.cancellations.push(Cancellation {
                attempt,
                expires_at_ms: self.now_ms.saturating_add(validate::MAX_TTL_MS),
            });
            self.cancellations.sort_by_key(|item| item.attempt);
        }

        let cancelled = self
            .queue
            .iter()
            .position(|waiter| waiter.attempt == attempt)
            .map(|index| {
                self.queue.remove(index);
                true
            })
            .unwrap_or(false);
        let promoted = if cancelled {
            self.promote()
        } else {
            Vec::new()
        };

        Outcome::Cancel {
            // Attempt-scoped cancellation is idempotently successful even when
            // the delayed acquire has not arrived yet.
            cancelled: true,
            acquired: false,
            token: None,
            lease_expires_at_ms: None,
            promoted,
        }
    }

    fn renew(&mut self, holder: u8, keys: u8, token: u64, ttl_ms: u64) -> Outcome {
        let Some(grant) = self.grants.iter_mut().find(|grant| grant.token == token) else {
            return Outcome::Renew {
                renewed: false,
                lease_expires_at_ms: None,
            };
        };
        if grant.attempt.holder != holder || grant.attempt.keys != keys {
            return Outcome::Renew {
                renewed: false,
                lease_expires_at_ms: None,
            };
        }
        grant.expires_at_ms = grant.expires_at_ms.max(self.now_ms.saturating_add(ttl_ms));
        Outcome::Renew {
            renewed: true,
            lease_expires_at_ms: Some(grant.expires_at_ms),
        }
    }

    fn release(&mut self, holder: u8, token: u64) -> Outcome {
        let Some(index) = self.grants.iter().position(|grant| grant.token == token) else {
            return Outcome::Release {
                released: false,
                promoted: Vec::new(),
            };
        };
        if self.grants[index].attempt.holder != holder {
            return Outcome::Release {
                released: false,
                promoted: Vec::new(),
            };
        }
        self.grants.remove(index);
        Outcome::Release {
            released: true,
            promoted: self.promote(),
        }
    }

    fn expire_due(&mut self) {
        self.cancellations
            .retain(|item| item.expires_at_ms > self.now_ms);
        self.queue
            .retain(|waiter| waiter.wait_expires_at_ms > self.now_ms);
        self.grants
            .retain(|grant| grant.expires_at_ms > self.now_ms);
        self.promote();
    }

    fn promote(&mut self) -> Vec<PublicGrant> {
        let mut promoted = Vec::new();
        while let Some(index) = self.first_grantable_waiter() {
            // Production deliberately leaves the waiter in line when the fencing
            // counter is exhausted.
            let Some(token) = self.last_token.checked_add(1) else {
                break;
            };
            self.last_token = token;
            let waiter = self.queue.remove(index);
            let grant = Grant {
                attempt: waiter.attempt,
                token,
                expires_at_ms: self.now_ms.saturating_add(waiter.ttl_ms),
            };
            promoted.push(PublicGrant::from_grant(&grant));
            self.grants.push(grant);
            self.grants.sort_by_key(|item| item.token);
        }
        promoted
    }

    fn first_grantable_waiter(&self) -> Option<usize> {
        let mut reserved = 0u8;
        for (index, waiter) in self.queue.iter().enumerate() {
            let blocked_by_grant = self
                .grants
                .iter()
                .any(|grant| overlaps(grant.attempt.keys, waiter.attempt.keys));
            let blocked_by_queue = overlaps(reserved, waiter.attempt.keys);
            if !blocked_by_grant && !blocked_by_queue {
                return Some(index);
            }
            reserved |= waiter.attempt.keys;
        }
        None
    }

    fn has_active_cancellation(&self, attempt: Attempt) -> bool {
        self.cancellations
            .iter()
            .any(|item| item.attempt == attempt && item.expires_at_ms > self.now_ms)
    }

    fn next_deadline(&self) -> Option<u64> {
        self.grants
            .iter()
            .map(|grant| grant.expires_at_ms)
            .chain(self.queue.iter().map(|waiter| waiter.wait_expires_at_ms))
            .chain(self.cancellations.iter().map(|item| item.expires_at_ms))
            .filter(|deadline| *deadline > self.now_ms)
            .min()
    }

    fn assert_invariants(&self) {
        assert!(
            self.grants
                .windows(2)
                .all(|window| window[0].token < window[1].token),
            "grant tokens must remain unique and sorted"
        );
        assert!(
            self.grants.iter().all(|grant| {
                grant.token <= self.last_token && grant.expires_at_ms > self.now_ms
            }),
            "live grants must reference issued, unexpired tokens"
        );
        for (index, left) in self.grants.iter().enumerate() {
            for right in self.grants.iter().skip(index + 1) {
                assert!(
                    !overlaps(left.attempt.keys, right.attempt.keys),
                    "live union grants overlap: {left:?} and {right:?}"
                );
            }
        }
        for (index, left) in self.queue.iter().enumerate() {
            assert!(
                left.wait_expires_at_ms > self.now_ms,
                "expired waiter remained observable: {left:?}"
            );
            for right in self.queue.iter().skip(index + 1) {
                assert!(
                    !left.attempt.same_queue_identity(right.attempt),
                    "duplicate queue identity: {left:?} and {right:?}"
                );
            }
            assert!(
                !self
                    .grants
                    .iter()
                    .any(|grant| grant.attempt == left.attempt),
                "attempt is both queued and granted: {left:?}"
            );
        }
        for cancellation in &self.cancellations {
            assert!(
                cancellation.expires_at_ms > self.now_ms,
                "expired cancellation remained observable: {cancellation:?}"
            );
            assert!(
                !self
                    .grants
                    .iter()
                    .any(|grant| grant.attempt == cancellation.attempt),
                "cancelled attempt is granted: {cancellation:?}"
            );
            assert!(
                !self
                    .queue
                    .iter()
                    .any(|waiter| waiter.attempt == cancellation.attempt),
                "cancelled attempt is queued: {cancellation:?}"
            );
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Projection {
    last_token: u64,
    grants: Vec<Grant>,
    queue: Vec<Waiter>,
    cancellations: BTreeMap<String, u64>,
    held: BTreeMap<String, u64>,
}

impl Projection {
    fn from_model(model: &Model) -> Self {
        let cancellations = model
            .cancellations
            .iter()
            .map(|item| (cancellation_identity(item.attempt), item.expires_at_ms))
            .collect();
        let mut held = BTreeMap::new();
        for grant in &model.grants {
            for key in grant.attempt.key_names() {
                let previous = held.insert(key, grant.token);
                assert!(previous.is_none(), "model granted one key twice");
            }
        }
        Self {
            last_token: model.last_token,
            grants: model.grants.clone(),
            queue: model.queue.clone(),
            cancellations,
            held,
        }
    }

    fn from_machine(machine: &StateMachine) -> Self {
        let value = snapshot_value(machine);
        let last_token = value["next_fencing_token"]
            .as_u64()
            .expect("snapshot next_fencing_token");
        let locks = value["locks"].as_object().expect("snapshot locks object");

        let mut grants = locks["grants"]
            .as_object()
            .expect("snapshot grants object")
            .iter()
            .map(|(index_token, value)| {
                let holder = required_string(value, "holder");
                let keys = required_string_array(value, "keys");
                let request_id = required_string(value, "request_id");
                let token = required_u64(value, "fencing_token");
                assert_eq!(
                    index_token.parse::<u64>().expect("numeric grant index"),
                    token,
                    "grant index and embedded token diverged"
                );
                Grant {
                    attempt: Attempt::from_wire(&holder, &keys, &request_id),
                    token,
                    expires_at_ms: required_u64(value, "lease_expires_ms"),
                }
            })
            .collect::<Vec<_>>();
        grants.sort_by_key(|grant| grant.token);

        let queue = locks["queue"]
            .as_array()
            .expect("snapshot queue array")
            .iter()
            .map(|pair| {
                let pair = pair.as_array().expect("serialized queue pair");
                assert_eq!(pair.len(), 2, "serialized queue pair arity");
                let value = &pair[1];
                let holder = required_string(value, "holder");
                let keys = required_string_array(value, "keys");
                let request_id = required_string(value, "request_id");
                Waiter {
                    attempt: Attempt::from_wire(&holder, &keys, &request_id),
                    ttl_ms: required_u64(value, "ttl_ms"),
                    requested_ms: required_u64(value, "requested_ms"),
                    wait_expires_at_ms: required_u64(value, "wait_expires_ms"),
                }
            })
            .collect();

        let held = locks["held"]
            .as_object()
            .expect("snapshot held object")
            .iter()
            .map(|(key, token)| {
                (
                    key.clone(),
                    token.as_u64().expect("held fencing token is u64"),
                )
            })
            .collect();

        let cancellations = value["lock_cancellations"]["entries"]
            .as_object()
            .expect("snapshot cancellation entries")
            .iter()
            .map(|(identity, entry)| (identity.clone(), required_u64(entry, "expires_at_ms")))
            .collect();

        Self {
            last_token,
            grants,
            queue,
            cancellations,
            held,
        }
    }
}

#[derive(Default, Debug)]
struct Coverage {
    immediate_grant: bool,
    queued: bool,
    grant_retry_no_extension: bool,
    queue_retry_no_extension: bool,
    no_wait_conflict: bool,
    same_identity_collision: bool,
    cancel_absent: bool,
    cancel_queued: bool,
    cancel_active: bool,
    delayed_acquire_blocked: bool,
    promotion: bool,
    grant_expiry: bool,
    waiter_expiry: bool,
    cancellation_expiry: bool,
    disjoint_bypass: bool,
    wrong_renew_rejected: bool,
    wrong_release_rejected: bool,
    snapshot_round_trip: bool,
}

impl Coverage {
    fn observe(&mut self, parent: &Model, action: &Action, outcome: &Outcome, child: &Model) {
        match (action, outcome) {
            (
                Action::Acquire { attempt, wait, .. },
                Outcome::Acquire {
                    acquired,
                    queued,
                    position,
                    ..
                },
            ) => {
                let existing_grant = parent.grants.iter().find(|grant| grant.attempt == *attempt);
                let existing_waiter = parent
                    .queue
                    .iter()
                    .find(|waiter| waiter.attempt == *attempt);
                if *acquired && existing_grant.is_none() {
                    self.immediate_grant = true;
                }
                if *queued && child.queue.len() > parent.queue.len() {
                    self.queued = true;
                }
                if existing_grant.is_some() && *acquired && parent == child {
                    self.grant_retry_no_extension = true;
                }
                if existing_waiter.is_some() && *queued && parent == child {
                    self.queue_retry_no_extension = true;
                }
                if !*wait && !*acquired && !*queued && parent == child {
                    self.no_wait_conflict = true;
                }
                if parent.queue.iter().any(|waiter| {
                    waiter.attempt.same_queue_identity(*attempt)
                        && waiter.attempt.request_id != attempt.request_id
                }) && !*queued
                    && position.is_none()
                {
                    self.same_identity_collision = true;
                }
                if parent.has_active_cancellation(*attempt) && !*acquired && !*queued {
                    self.delayed_acquire_blocked = true;
                }
            }
            (
                Action::Cancel { attempt },
                Outcome::Cancel {
                    cancelled,
                    acquired,
                    ..
                },
            ) => {
                let had_grant = parent.grants.iter().any(|grant| grant.attempt == *attempt);
                let had_waiter = parent.queue.iter().any(|waiter| waiter.attempt == *attempt);
                if had_grant && *acquired && !*cancelled {
                    self.cancel_active = true;
                } else if had_waiter
                    && *cancelled
                    && !child.queue.iter().any(|waiter| waiter.attempt == *attempt)
                {
                    self.cancel_queued = true;
                } else if !had_grant
                    && !had_waiter
                    && *cancelled
                    && child.has_active_cancellation(*attempt)
                {
                    self.cancel_absent = true;
                }
            }
            (
                Action::Renew {
                    holder,
                    keys,
                    token,
                    ..
                },
                Outcome::Renew { renewed, .. },
            ) => {
                let authorized = parent.grants.iter().any(|grant| {
                    grant.token == *token
                        && grant.attempt.holder == *holder
                        && grant.attempt.keys == *keys
                });
                if !authorized && !*renewed {
                    self.wrong_renew_rejected = true;
                }
            }
            (Action::Release { holder, token }, Outcome::Release { released, .. }) => {
                let authorized = parent
                    .grants
                    .iter()
                    .any(|grant| grant.token == *token && grant.attempt.holder == *holder);
                if !authorized && !*released {
                    self.wrong_release_rejected = true;
                }
            }
            (Action::SnapshotRoundTrip, Outcome::SnapshotRoundTrip) => {
                self.snapshot_round_trip = true;
            }
            _ => {}
        }

        if parent.queue.iter().any(|waiter| {
            child
                .grants
                .iter()
                .any(|grant| grant.attempt == waiter.attempt)
        }) {
            self.promotion = true;
        }
        if matches!(action, Action::AdvanceToNextDeadline) {
            if parent.grants.iter().any(|grant| {
                !child
                    .grants
                    .iter()
                    .any(|candidate| candidate.attempt == grant.attempt)
                    && !child
                        .queue
                        .iter()
                        .any(|candidate| candidate.attempt == grant.attempt)
            }) {
                self.grant_expiry = true;
            }
            if parent.queue.iter().any(|waiter| {
                !child
                    .queue
                    .iter()
                    .any(|candidate| candidate.attempt == waiter.attempt)
                    && !child
                        .grants
                        .iter()
                        .any(|candidate| candidate.attempt == waiter.attempt)
            }) {
                self.waiter_expiry = true;
            }
            if parent.cancellations.iter().any(|item| {
                !child
                    .cancellations
                    .iter()
                    .any(|candidate| candidate.attempt == item.attempt)
            }) {
                self.cancellation_expiry = true;
            }
        }
        if child.queue.len() == 1
            && child.grants.len() == 2
            && !overlaps(child.grants[0].attempt.keys, child.grants[1].attempt.keys)
        {
            self.disjoint_bypass = true;
        }
    }

    fn assert_complete(&self) {
        assert!(self.immediate_grant, "coverage: immediate grant");
        assert!(self.queued, "coverage: queued conflict");
        assert!(
            self.grant_retry_no_extension,
            "coverage: granted retry does not extend"
        );
        assert!(
            self.queue_retry_no_extension,
            "coverage: queued retry does not extend"
        );
        assert!(self.no_wait_conflict, "coverage: no-wait conflict");
        assert!(
            self.same_identity_collision,
            "coverage: queue identity collision"
        );
        assert!(self.cancel_absent, "coverage: cancel-before-acquire");
        assert!(self.cancel_queued, "coverage: queued cancellation");
        assert!(self.cancel_active, "coverage: cancellation/promotion race");
        assert!(
            self.delayed_acquire_blocked,
            "coverage: tombstone blocks delayed acquire"
        );
        assert!(self.promotion, "coverage: queue promotion");
        assert!(self.grant_expiry, "coverage: grant expiry");
        assert!(self.waiter_expiry, "coverage: waiter expiry");
        assert!(self.cancellation_expiry, "coverage: cancellation expiry");
        assert!(self.disjoint_bypass, "coverage: disjoint queue bypass");
        assert!(
            self.wrong_renew_rejected,
            "coverage: wrong renewal authority"
        );
        assert!(
            self.wrong_release_rejected,
            "coverage: wrong release authority"
        );
        assert!(self.snapshot_round_trip, "coverage: snapshot round trip");
    }
}

struct Frontier {
    model: Model,
    snapshot: Vec<u8>,
    trace: Vec<Action>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ItfGrant {
    attempt: Attempt,
    token: u64,
    expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ItfWaiter {
    attempt: Attempt,
    ttl: u64,
    wait_expires_at: u64,
    seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ItfCancellation {
    attempt: Attempt,
    expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ItfState {
    now: u64,
    next_token: u64,
    grants: Vec<ItfGrant>,
    queue: Vec<ItfWaiter>,
    cancellations: Vec<ItfCancellation>,
}

#[derive(Debug, PartialEq, Eq)]
struct ItfProjection {
    last_token: u64,
    grants: Vec<Grant>,
    queue: Vec<(Attempt, u64, u64)>,
    cancellations: BTreeMap<String, u64>,
    held: BTreeMap<String, u64>,
}

impl ItfState {
    fn from_trace_state(state: &Value) -> Self {
        let state = state.get("s").expect("ITF state variable 's'");
        let mut grants = itf_set(&state["grants"])
            .iter()
            .map(|grant| ItfGrant {
                attempt: itf_attempt(&grant["attempt"]),
                token: itf_u64(&grant["token"]),
                expires_at: itf_u64(&grant["expires_at"]),
            })
            .collect::<Vec<_>>();
        grants.sort_by_key(|grant| grant.token);

        let mut queue = itf_set(&state["queue"])
            .iter()
            .map(|waiter| ItfWaiter {
                attempt: itf_attempt(&waiter["attempt"]),
                ttl: itf_u64(&waiter["ttl"]),
                wait_expires_at: itf_u64(&waiter["wait_expires_at"]),
                seq: itf_u64(&waiter["seq"]),
            })
            .collect::<Vec<_>>();
        queue.sort_by_key(|waiter| waiter.seq);

        let mut cancellations = itf_set(&state["cancellations"])
            .iter()
            .map(|cancellation| ItfCancellation {
                attempt: itf_attempt(&cancellation["attempt"]),
                expires_at: itf_u64(&cancellation["expires_at"]),
            })
            .collect::<Vec<_>>();
        cancellations.sort_by_key(|item| item.attempt);

        let next_token = itf_u64(&state["next_token"]);
        let minted_tokens = itf_set(&state["minted_tokens"])
            .iter()
            .map(itf_u64)
            .collect::<HashSet<_>>();
        let expected_tokens = (1..next_token).collect::<HashSet<_>>();
        assert_eq!(
            minted_tokens, expected_tokens,
            "ITF minted-token history must be contiguous"
        );

        Self {
            now: itf_u64(&state["now"]),
            next_token,
            grants,
            queue,
            cancellations,
        }
    }

    fn promote_to_quiescence(&mut self) {
        while self.next_token <= ITF_MAX_TOKEN {
            let mut reserved = 0u8;
            let mut grantable = None;
            for (index, waiter) in self.queue.iter().enumerate() {
                let held = self
                    .grants
                    .iter()
                    .any(|grant| overlaps(grant.attempt.keys, waiter.attempt.keys));
                if !held && !overlaps(reserved, waiter.attempt.keys) {
                    grantable = Some(index);
                    break;
                }
                reserved |= waiter.attempt.keys;
            }
            let Some(index) = grantable else {
                break;
            };
            let waiter = self.queue.remove(index);
            self.grants.push(ItfGrant {
                attempt: waiter.attempt,
                token: self.next_token,
                expires_at: self.now + waiter.ttl,
            });
            self.next_token += 1;
            self.grants.sort_by_key(|grant| grant.token);
        }
    }

    fn projection(&self) -> ItfProjection {
        let grants = self
            .grants
            .iter()
            .map(|grant| Grant {
                attempt: grant.attempt,
                token: grant.token,
                expires_at_ms: itf_time_ms(grant.expires_at),
            })
            .collect::<Vec<_>>();
        let queue = self
            .queue
            .iter()
            .map(|waiter| {
                (
                    waiter.attempt,
                    waiter.ttl * ITF_TIME_UNIT_MS,
                    itf_time_ms(waiter.wait_expires_at),
                )
            })
            .collect();
        let cancellations = self
            .cancellations
            .iter()
            .map(|item| {
                (
                    cancellation_identity(item.attempt),
                    itf_time_ms(item.expires_at),
                )
            })
            .collect();
        let mut held = BTreeMap::new();
        for grant in &grants {
            for key in grant.attempt.key_names() {
                let previous = held.insert(key, grant.token);
                assert!(previous.is_none(), "ITF state grants one key twice");
            }
        }
        ItfProjection {
            last_token: self.next_token - 1,
            grants,
            queue,
            cancellations,
            held,
        }
    }
}

impl ItfProjection {
    fn from_machine(machine: &StateMachine) -> Self {
        let projection = Projection::from_machine(machine);
        let last_token = if projection.last_token == u64::MAX {
            ITF_MAX_TOKEN
        } else {
            projection.last_token
        };
        let queue = projection
            .queue
            .into_iter()
            .map(|waiter| (waiter.attempt, waiter.ttl_ms, waiter.wait_expires_at_ms))
            .collect();
        Self {
            last_token,
            grants: projection.grants,
            queue,
            cancellations: projection.cancellations,
            held: projection.held,
        }
    }
}

#[test]
fn generated_itf_traces_replay_against_production() {
    let Some(trace_dir) = std::env::var_os("FIDUCIA_ITF_TRACE_DIR") else {
        assert_ne!(
            std::env::var("FIDUCIA_REQUIRE_ITF_REPLAY").as_deref(),
            Ok("1"),
            "FIDUCIA_ITF_TRACE_DIR is required for the formal conformance profile"
        );
        eprintln!("ITF replay skipped; run `nix develop -c agent-check formal-refinement`");
        return;
    };
    let trace_dir = PathBuf::from(trace_dir);
    let mut traces = std::fs::read_dir(&trace_dir)
        .unwrap_or_else(|error| panic!("read ITF directory {}: {error}", trace_dir.display()))
        .map(|entry| entry.expect("read ITF directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().contains(".itf."))
        })
        .collect::<Vec<_>>();
    traces.sort();
    assert!(
        !traces.is_empty(),
        "no generated ITF traces found in {}",
        trace_dir.display()
    );

    let mut replayed_states = 0usize;
    for trace in &traces {
        replayed_states += replay_itf_trace(trace);
    }
    eprintln!(
        "replayed {} generated ITF traces ({} states) against production",
        traces.len(),
        replayed_states
    );
}

fn replay_itf_trace(path: &Path) -> usize {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|error| panic!("read ITF trace {}: {error}", path.display()));
    let trace: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("decode ITF trace {}: {error}", path.display()));
    assert_eq!(
        trace["#meta"]["format"],
        "ITF",
        "trace format: {}",
        path.display()
    );
    assert_eq!(
        trace["#meta"]["status"],
        "ok",
        "trace status: {}",
        path.display()
    );
    let states = trace["states"]
        .as_array()
        .unwrap_or_else(|| panic!("ITF states array: {}", path.display()));
    assert!(
        states.len() >= 2,
        "ITF trace is too short: {}",
        path.display()
    );

    let machine = StateMachine::new();
    let initial = ItfState::from_trace_state(&states[0]);
    assert_eq!(
        ItfProjection::from_machine(&machine),
        initial.projection(),
        "initial ITF state diverged: {}",
        path.display()
    );

    for pair in states.windows(2) {
        replay_itf_transition(&machine, &pair[0], &pair[1], path);
        let mut expected = ItfState::from_trace_state(&pair[1]);
        expected.promote_to_quiescence();
        if expected.next_token > ITF_MAX_TOKEN {
            force_production_token_exhaustion(&machine);
        }
        assert_eq!(
            ItfProjection::from_machine(&machine),
            expected.projection(),
            "ITF state diverged at index {} in {} after action {}",
            pair[1]["#meta"]["index"],
            path.display(),
            pair[1]["mbt::actionTaken"]
        );
    }
    states.len()
}

fn replay_itf_transition(machine: &StateMachine, previous: &Value, next: &Value, path: &Path) {
    let action = next["mbt::actionTaken"]
        .as_str()
        .unwrap_or_else(|| panic!("missing MBT action in {}", path.display()));
    let previous_state = ItfState::from_trace_state(previous);
    let now_ms = itf_time_ms(previous_state.now);
    let holder = || itf_pick_u64(next, "holder") as u8;
    let keys = || itf_pick_keys(next);
    let request_id = || itf_pick_u64(next, "request_id") as u8;
    let attempt = || Attempt::new(holder(), keys(), request_id());
    let ttl_ms = || itf_pick_u64(next, "ttl") * ITF_TIME_UNIT_MS;
    let wait_timeout_ms = || itf_pick_u64(next, "wait_ttl") * ITF_TIME_UNIT_MS;

    match action {
        "acquire_now" => {
            let attempt = attempt();
            machine.apply_at(
                Command::LockAcquireAttempt {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    request_id: attempt.request_name(),
                    ttl_ms: ttl_ms(),
                    wait: false,
                    wait_timeout_ms: Some(wait_timeout_ms()),
                },
                now_ms,
            );
        }
        "enqueue_wait" | "retry_attempt" => {
            let attempt = attempt();
            machine.apply_at(
                Command::LockAcquireAttempt {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    request_id: attempt.request_name(),
                    ttl_ms: ttl_ms(),
                    wait: true,
                    wait_timeout_ms: Some(wait_timeout_ms()),
                },
                now_ms,
            );
        }
        "renew" => {
            let attempt = attempt();
            machine.apply_at(
                Command::LockRenew {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    fencing_token: itf_pick_u64(next, "token"),
                    ttl_ms: ttl_ms(),
                },
                now_ms,
            );
        }
        "release" => {
            machine.apply_at(
                Command::LockRelease {
                    holder: Attempt::new(holder(), 0b01, 1).holder_name(),
                    fencing_token: itf_pick_u64(next, "token"),
                },
                now_ms,
            );
        }
        "cancel_active_retry" | "cancel_queued" | "cancel_absent" => {
            let attempt = attempt();
            machine.apply_at(
                Command::LockCancelAttempt {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    request_id: attempt.request_name(),
                },
                now_ms,
            );
        }
        "tick" => {
            let next_now = itf_u64(&next["s"]["now"]);
            machine.apply_at(
                Command::KvDelete {
                    key: "__itf_clock_tick__".to_string(),
                },
                itf_time_ms(next_now),
            );
        }
        "snapshot_round_trip" => {
            let snapshot = machine.snapshot().expect("ITF snapshot");
            machine.restore(&snapshot).expect("ITF snapshot restore");
        }
        "promote_head" | "promote_after_blocked_head" | "idle" => {}
        other => panic!("unsupported ITF action '{other}' in {}", path.display()),
    }
}

fn force_production_token_exhaustion(machine: &StateMachine) {
    if Projection::from_machine(machine).last_token == u64::MAX {
        return;
    }
    let snapshot = mutate_snapshot(
        &machine.snapshot().expect("token exhaustion snapshot"),
        |value| {
            value["next_fencing_token"] = Value::from(u64::MAX);
        },
    );
    machine
        .restore(&snapshot)
        .expect("restore token exhaustion snapshot");
}

fn itf_u64(value: &Value) -> u64 {
    value["#bigint"]
        .as_str()
        .unwrap_or_else(|| panic!("expected ITF bigint, found {value}"))
        .parse()
        .unwrap_or_else(|error| panic!("parse ITF bigint {value}: {error}"))
}

fn itf_set(value: &Value) -> &[Value] {
    value["#set"]
        .as_array()
        .unwrap_or_else(|| panic!("expected ITF set, found {value}"))
}

fn itf_attempt(value: &Value) -> Attempt {
    Attempt::new(
        itf_u64(&value["holder"]) as u8,
        itf_set(&value["keys"])
            .iter()
            .fold(0u8, |mask, key| mask | (1 << (itf_u64(key) - 1))),
        itf_u64(&value["request_id"]) as u8,
    )
}

fn itf_pick_u64(state: &Value, name: &str) -> u64 {
    itf_u64(&state["mbt::nondetPicks"][name]["value"])
}

fn itf_pick_keys(state: &Value) -> u8 {
    itf_set(&state["mbt::nondetPicks"]["lock_keys"]["value"])
        .iter()
        .fold(0u8, |mask, key| mask | (1 << (itf_u64(key) - 1)))
}

fn itf_time_ms(time: u64) -> u64 {
    BASE_TIME_MS + time * ITF_TIME_UNIT_MS
}

#[test]
fn bounded_union_lock_refinement_matches_reference_model() {
    let initial_model = Model::initial();
    let initial_machine = StateMachine::new();
    assert_machine_matches(&initial_machine, &initial_model, &[]);

    let mut seen = HashSet::new();
    seen.insert(initial_model.clone());
    let mut frontier = VecDeque::new();
    frontier.push_back(Frontier {
        model: initial_model,
        snapshot: initial_machine.snapshot().expect("initial snapshot"),
        trace: Vec::new(),
    });

    let mut transitions = 0usize;
    let mut coverage = Coverage::default();

    while let Some(node) = frontier.pop_front() {
        if node.trace.len() >= MAX_DEPTH {
            continue;
        }
        for action in node.model.actions() {
            transitions += 1;
            assert!(
                transitions <= MAX_TRANSITIONS,
                "formal refinement exceeded {MAX_TRANSITIONS} transitions; states={} trace={:#?}",
                seen.len(),
                node.trace
            );

            let machine = StateMachine::new();
            machine
                .restore(&node.snapshot)
                .expect("restore explored state");
            let actual_outcome = execute_production(&machine, &node.model, &action);

            let mut expected_model = node.model.clone();
            let expected_outcome = expected_model.apply(&action);
            let mut trace = node.trace.clone();
            trace.push(action.clone());

            assert_eq!(
                actual_outcome, expected_outcome,
                "observable output diverged after trace:\n{trace:#?}"
            );
            assert!(
                expected_model.last_token >= node.model.last_token,
                "fencing token regressed after trace: {trace:#?}"
            );
            assert_machine_matches(&machine, &expected_model, &trace);
            coverage.observe(&node.model, &action, &actual_outcome, &expected_model);

            if seen.insert(expected_model.clone()) {
                assert!(
                    seen.len() <= MAX_STATES,
                    "formal refinement exceeded {MAX_STATES} states after trace: {trace:#?}"
                );
                frontier.push_back(Frontier {
                    model: expected_model,
                    snapshot: machine.snapshot().expect("child snapshot"),
                    trace,
                });
            }
        }
    }

    coverage.assert_complete();
    eprintln!(
        "bounded union-lock refinement explored {} states and {} transitions through depth {}",
        seen.len(),
        transitions,
        MAX_DEPTH
    );
}

#[test]
fn fencing_token_exhaustion_fails_closed_without_dropping_waiters() {
    let first = ATTEMPTS[0];
    let waiter = ATTEMPTS[2];
    let machine = StateMachine::new();

    let grant = machine.apply_at(
        Command::LockAcquireAttempt {
            keys: first.key_names(),
            holder: first.holder_name(),
            request_id: first.request_name(),
            ttl_ms: HOLD_TTL_MS,
            wait: true,
            wait_timeout_ms: Some(WAIT_TTL_MS),
        },
        BASE_TIME_MS,
    );
    assert_eq!(grant.output["fencing_token"], 1);
    let queued = machine.apply_at(
        Command::LockAcquireAttempt {
            keys: waiter.key_names(),
            holder: waiter.holder_name(),
            request_id: waiter.request_name(),
            ttl_ms: HOLD_TTL_MS,
            wait: true,
            wait_timeout_ms: Some(WAIT_TTL_MS),
        },
        BASE_TIME_MS,
    );
    assert_eq!(queued.output["queued"], true);

    let exhausted_snapshot = mutate_snapshot(&machine.snapshot().expect("snapshot"), |value| {
        value["next_fencing_token"] = Value::from(u64::MAX);
    });
    let exhausted = StateMachine::new();
    exhausted
        .restore(&exhausted_snapshot)
        .expect("restore exhausted fencing counter");

    let release = exhausted.apply_at(
        Command::LockRelease {
            holder: first.holder_name(),
            fencing_token: 1,
        },
        BASE_TIME_MS,
    );
    assert_eq!(release.output["released"], true);
    assert_eq!(release.output["promoted"], Value::Array(Vec::new()));
    let projection = Projection::from_machine(&exhausted);
    assert!(projection.grants.is_empty());
    assert_eq!(projection.queue.len(), 1, "waiter must remain durable");
    assert_eq!(projection.queue[0].attempt, waiter);
    assert_eq!(projection.last_token, u64::MAX);

    let empty = StateMachine::new();
    let exhausted_empty_snapshot =
        mutate_snapshot(&empty.snapshot().expect("empty snapshot"), |value| {
            value["next_fencing_token"] = Value::from(u64::MAX)
        });
    empty
        .restore(&exhausted_empty_snapshot)
        .expect("restore empty exhausted counter");
    let acquire = empty.apply_at(
        Command::LockAcquireAttempt {
            keys: first.key_names(),
            holder: first.holder_name(),
            request_id: first.request_name(),
            ttl_ms: HOLD_TTL_MS,
            wait: true,
            wait_timeout_ms: Some(WAIT_TTL_MS),
        },
        BASE_TIME_MS,
    );
    assert_eq!(acquire.output["acquired"], false);
    assert_eq!(acquire.output["queued"], false);
    assert_eq!(
        acquire.output["reason"],
        Value::String("fencing_tokens_exhausted".to_string())
    );
    let projection = Projection::from_machine(&empty);
    assert!(projection.grants.is_empty());
    assert!(projection.queue.is_empty());
    assert_eq!(projection.last_token, u64::MAX);
}

fn execute_production(machine: &StateMachine, model: &Model, action: &Action) -> Outcome {
    match *action {
        Action::Acquire {
            attempt,
            wait,
            ttl_ms,
            wait_timeout_ms,
        } => {
            let result = machine.apply_at(
                Command::LockAcquireAttempt {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    request_id: attempt.request_name(),
                    ttl_ms,
                    wait,
                    wait_timeout_ms: Some(wait_timeout_ms),
                },
                model.now_ms,
            );
            Outcome::Acquire {
                acquired: required_bool(&result.output, "acquired"),
                queued: required_bool(&result.output, "queued"),
                position: optional_u64(&result.output, "position"),
                token: optional_u64(&result.output, "fencing_token"),
                lease_expires_at_ms: optional_u64(&result.output, "lease_expires_ms"),
                wait_expires_at_ms: optional_u64(&result.output, "wait_expires_ms"),
            }
        }
        Action::Cancel { attempt } => {
            let result = machine.apply_at(
                Command::LockCancelAttempt {
                    keys: attempt.key_names(),
                    holder: attempt.holder_name(),
                    request_id: attempt.request_name(),
                },
                model.now_ms,
            );
            Outcome::Cancel {
                cancelled: required_bool(&result.output, "cancelled"),
                acquired: required_bool(&result.output, "acquired"),
                token: optional_u64(&result.output, "fencing_token"),
                lease_expires_at_ms: optional_u64(&result.output, "lease_expires_ms"),
                promoted: parse_promoted(&result.output),
            }
        }
        Action::Renew {
            holder,
            keys,
            token,
            ttl_ms,
        } => {
            let result = machine.apply_at(
                Command::LockRenew {
                    keys: Attempt::new(holder, keys, 1).key_names(),
                    holder: Attempt::new(holder, keys, 1).holder_name(),
                    fencing_token: token,
                    ttl_ms,
                },
                model.now_ms,
            );
            Outcome::Renew {
                renewed: required_bool(&result.output, "renewed"),
                lease_expires_at_ms: optional_u64(&result.output, "lease_expires_ms"),
            }
        }
        Action::Release { holder, token } => {
            let result = machine.apply_at(
                Command::LockRelease {
                    holder: Attempt::new(holder, 0b01, 1).holder_name(),
                    fencing_token: token,
                },
                model.now_ms,
            );
            Outcome::Release {
                released: required_bool(&result.output, "released"),
                promoted: parse_promoted(&result.output),
            }
        }
        Action::AdvanceToNextDeadline => {
            let deadline = model
                .next_deadline()
                .expect("deadline action is generated only when one exists");
            machine.apply_at(
                Command::KvDelete {
                    key: "__formal_clock_tick__".to_string(),
                },
                deadline,
            );
            Outcome::Advanced
        }
        Action::SnapshotRoundTrip => {
            let snapshot = machine.snapshot().expect("snapshot round trip encode");
            machine
                .restore(&snapshot)
                .expect("snapshot round trip restore");
            Outcome::SnapshotRoundTrip
        }
    }
}

fn assert_machine_matches(machine: &StateMachine, model: &Model, trace: &[Action]) {
    model.assert_invariants();
    assert_eq!(
        Projection::from_machine(machine),
        Projection::from_model(model),
        "canonical state diverged after trace:\n{trace:#?}"
    );
}

fn parse_promoted(output: &Value) -> Vec<PublicGrant> {
    let Some(items) = output.get("promoted").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            let holder = required_string(item, "holder");
            let keys = required_string_array(item, "keys");
            // Promoted output intentionally omits the private request id.
            let attempt = Attempt::from_wire(&holder, &keys, "r1");
            PublicGrant {
                holder: attempt.holder,
                keys: attempt.keys,
                token: required_u64(item, "fencing_token"),
                expires_at_ms: required_u64(item, "lease_expires_ms"),
            }
        })
        .collect()
}

fn snapshot_value(machine: &StateMachine) -> Value {
    let bytes = machine.snapshot().expect("state-machine snapshot");
    decode_snapshot(&bytes)
}

fn decode_snapshot(bytes: &[u8]) -> Value {
    let json_start = bytes
        .iter()
        .position(|byte| *byte == b'{')
        .expect("snapshot JSON object");
    serde_json::from_slice(&bytes[json_start..]).expect("decode snapshot JSON")
}

fn mutate_snapshot(bytes: &[u8], mutate: impl FnOnce(&mut Value)) -> Vec<u8> {
    let json_start = bytes
        .iter()
        .position(|byte| *byte == b'{')
        .expect("snapshot JSON object");
    let mut value: Value =
        serde_json::from_slice(&bytes[json_start..]).expect("decode snapshot for mutation");
    mutate(&mut value);
    let mut output = bytes[..json_start].to_vec();
    output.extend(serde_json::to_vec(&value).expect("encode mutated snapshot"));
    output
}

fn cancellation_identity(attempt: Attempt) -> String {
    let mut hasher = Sha256::new();
    hash_segment(&mut hasher, b"lock");
    hash_segment(&mut hasher, attempt.holder_name().as_bytes());
    for key in attempt.key_names() {
        hash_segment(&mut hasher, key.as_bytes());
    }
    hash_segment(&mut hasher, attempt.request_name().as_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("write digest");
    }
    encoded
}

fn hash_segment(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn overlaps(left: u8, right: u8) -> bool {
    left & right != 0
}

fn different_holder(holder: u8) -> u8 {
    if holder == 1 {
        2
    } else {
        1
    }
}

fn required_bool(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("missing boolean field '{field}' in {value}"))
}

fn optional_u64(value: &Value, field: &str) -> Option<u64> {
    value.get(field).and_then(Value::as_u64)
}

fn required_u64(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("missing u64 field '{field}' in {value}"))
}

fn required_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field '{field}' in {value}"))
        .to_string()
}

fn required_string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing string array field '{field}' in {value}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("non-string in '{field}': {item}"))
                .to_string()
        })
        .collect()
}
