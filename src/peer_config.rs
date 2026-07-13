//! Shared bounds and timeouts for Raft's HTTP peer plane.
//!
//! The inbound listener, outbound transport, and leader batching logic all read
//! these helpers so an operator cannot accidentally advertise a body size that
//! the sender ignores. Values are parsed defensively and clamped away from zero.

use std::{sync::OnceLock, time::Duration};

pub const DEFAULT_MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_APPEND_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_APPEND_MAX_ENTRIES: usize = 64;
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_SNAPSHOT_TIMEOUT_MS: u64 = 120_000;

const _: () = {
    assert!(DEFAULT_APPEND_MAX_ENTRIES > 0);
    assert!(DEFAULT_APPEND_MAX_BYTES < DEFAULT_MAX_BODY_BYTES);
    assert!(DEFAULT_RPC_TIMEOUT_MS < DEFAULT_SNAPSHOT_TIMEOUT_MS);
};

static MAX_BODY_BYTES: OnceLock<usize> = OnceLock::new();
static APPEND_MAX_BYTES: OnceLock<usize> = OnceLock::new();
static APPEND_MAX_ENTRIES: OnceLock<usize> = OnceLock::new();
static RPC_TIMEOUT: OnceLock<Duration> = OnceLock::new();
static SNAPSHOT_TIMEOUT: OnceLock<Duration> = OnceLock::new();

fn usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

pub fn max_body_bytes() -> usize {
    *MAX_BODY_BYTES
        .get_or_init(|| usize_env("FIDUCIA_RAFT_PEER_MAX_BODY_BYTES", DEFAULT_MAX_BODY_BYTES))
}

pub fn append_max_entries() -> usize {
    *APPEND_MAX_ENTRIES.get_or_init(|| {
        usize_env(
            "FIDUCIA_RAFT_APPEND_MAX_ENTRIES",
            DEFAULT_APPEND_MAX_ENTRIES,
        )
    })
}

pub fn append_max_bytes() -> usize {
    *APPEND_MAX_BYTES.get_or_init(|| {
        usize_env("FIDUCIA_RAFT_APPEND_MAX_BYTES", DEFAULT_APPEND_MAX_BYTES).min(max_body_bytes())
    })
}

pub fn rpc_timeout() -> Duration {
    *RPC_TIMEOUT.get_or_init(|| {
        Duration::from_millis(u64_env(
            "FIDUCIA_RAFT_RPC_TIMEOUT_MS",
            DEFAULT_RPC_TIMEOUT_MS,
        ))
    })
}

pub fn snapshot_timeout() -> Duration {
    *SNAPSHOT_TIMEOUT.get_or_init(|| {
        Duration::from_millis(u64_env(
            "FIDUCIA_RAFT_SNAPSHOT_TIMEOUT_MS",
            DEFAULT_SNAPSHOT_TIMEOUT_MS,
        ))
    })
}
