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
