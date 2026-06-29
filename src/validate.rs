//! Input bounds for coordination writes — rejected **before** a command reaches
//! the Raft log.
//!
//! The request-body byte cap ([`crate::MAX_BODY_BYTES`]) stops a single huge
//! payload, but it does not stop a *small* payload from being expensive: a 1 MB
//! body can still carry tens of thousands of union-lock keys, and every accepted
//! command is replicated to every replica and kept in the log **forever**. The
//! lock/semaphore primitives make this sharper still — all of their state lives
//! on one coordinator shard ([`crate::state::LOCK_DOMAIN`]), so one abusive
//! request degrades the whole lock service, not just one key's shard.
//!
//! These checks run in the HTTP handlers, before `Node::propose`, so a rejected
//! request never enters the log. Validation cannot live in the state machine:
//! `apply` runs *after* commit, by which point the bloat is already replicated.
//! The limits are generous for real coordination workloads (a union lock is a
//! handful of keys, a lease is seconds-to-minutes) and only bite obvious abuse.

use std::collections::HashMap;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Max member keys in one union lock. Real unions are tiny; thousands is abuse.
pub const MAX_LOCK_KEYS: usize = 256;
/// Max bytes for a single coordination key (lock/semaphore key).
pub const MAX_KEY_BYTES: usize = 1024;
/// Max bytes for a holder / candidate / instance identifier.
pub const MAX_HOLDER_BYTES: usize = 512;
/// Max bytes for an election or service *name*.
pub const MAX_NAME_BYTES: usize = 1024;
/// Max bytes for a service instance address.
pub const MAX_ADDRESS_BYTES: usize = 2048;
/// Max lease TTL: 24h. Leases auto-expire to free abandoned holders, so an
/// unbounded TTL is just a way to hold a resource forever; cap it.
pub const MAX_TTL_MS: u64 = 24 * 60 * 60 * 1000;
/// Sanity ceiling on a semaphore's concurrent-holder limit.
pub const MAX_SEMAPHORE_LIMIT: u32 = 1_000_000;
/// Max key/value pairs in a metadata map (discovery / election candidate facts).
pub const MAX_METADATA_ENTRIES: usize = 64;
/// Max bytes for a single metadata key.
pub const MAX_METADATA_KEY_BYTES: usize = 256;
/// Max bytes for a single metadata value.
pub const MAX_METADATA_VALUE_BYTES: usize = 4096;

/// A rejected write. Renders as `400 Bad Request` with a stable machine code and
/// a human-readable detail, matching the node's other error bodies.
#[derive(Debug)]
pub struct Rejection {
    code: &'static str,
    detail: String,
}

impl Rejection {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Rejection {
            code,
            detail: detail.into(),
        }
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": self.code, "detail": self.detail })),
        )
            .into_response()
    }
}

fn check_str(label: &str, value: &str, max: usize, allow_empty: bool) -> Result<(), Rejection> {
    if !allow_empty && value.is_empty() {
        return Err(Rejection::new("empty_field", format!("{label} must not be empty")));
    }
    if value.len() > max {
        return Err(Rejection::new(
            "field_too_long",
            format!("{label} exceeds {max} bytes ({} given)", value.len()),
        ));
    }
    Ok(())
}

fn check_ttl(ttl_ms: u64) -> Result<(), Rejection> {
    if ttl_ms > MAX_TTL_MS {
        return Err(Rejection::new(
            "ttl_too_large",
            format!("ttl_ms exceeds the {MAX_TTL_MS} ms (24h) ceiling"),
        ));
    }
    Ok(())
}

fn check_metadata(metadata: &HashMap<String, String>) -> Result<(), Rejection> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(Rejection::new(
            "too_much_metadata",
            format!("metadata has {} entries; max is {MAX_METADATA_ENTRIES}", metadata.len()),
        ));
    }
    for (k, v) in metadata {
        check_str("metadata key", k, MAX_METADATA_KEY_BYTES, false)?;
        check_str("metadata value", v, MAX_METADATA_VALUE_BYTES, true)?;
    }
    Ok(())
}

/// Validate a (possibly multi-key) lock acquire: key count, each key's length,
/// the optional holder, and the optional TTL.
pub fn lock_acquire(
    keys: &[String],
    holder: &Option<String>,
    ttl_ms: Option<u64>,
) -> Result<(), Rejection> {
    if keys.len() > MAX_LOCK_KEYS {
        return Err(Rejection::new(
            "too_many_keys",
            format!("union lock has {} keys; max is {MAX_LOCK_KEYS}", keys.len()),
        ));
    }
    for key in keys {
        check_str("lock key", key, MAX_KEY_BYTES, false)?;
    }
    if let Some(holder) = holder {
        check_str("holder", holder, MAX_HOLDER_BYTES, true)?;
    }
    if let Some(ttl) = ttl_ms {
        check_ttl(ttl)?;
    }
    Ok(())
}

/// Validate a semaphore acquire: key, optional holder, holder limit, optional TTL.
pub fn semaphore_acquire(
    key: &str,
    holder: &Option<String>,
    limit: u32,
    ttl_ms: Option<u64>,
) -> Result<(), Rejection> {
    check_str("semaphore key", key, MAX_KEY_BYTES, false)?;
    if let Some(holder) = holder {
        check_str("holder", holder, MAX_HOLDER_BYTES, true)?;
    }
    if limit > MAX_SEMAPHORE_LIMIT {
        return Err(Rejection::new(
            "limit_too_large",
            format!("semaphore limit {limit} exceeds {MAX_SEMAPHORE_LIMIT}"),
        ));
    }
    if let Some(ttl) = ttl_ms {
        check_ttl(ttl)?;
    }
    Ok(())
}

/// Validate a service registration: name, instance id, address, TTL, metadata.
pub fn service_register(
    service: &str,
    instance_id: &str,
    address: &str,
    ttl_ms: u64,
    metadata: &HashMap<String, String>,
) -> Result<(), Rejection> {
    check_str("service", service, MAX_NAME_BYTES, false)?;
    check_str("instance_id", instance_id, MAX_HOLDER_BYTES, false)?;
    check_str("address", address, MAX_ADDRESS_BYTES, false)?;
    check_ttl(ttl_ms)?;
    check_metadata(metadata)
}

/// Validate an election campaign: name, candidate, TTL, candidate metadata.
pub fn election_campaign(
    name: &str,
    candidate: &str,
    ttl_ms: u64,
    metadata: &HashMap<String, String>,
) -> Result<(), Rejection> {
    check_str("election name", name, MAX_NAME_BYTES, false)?;
    check_str("candidate", candidate, MAX_HOLDER_BYTES, false)?;
    check_ttl(ttl_ms)?;
    check_metadata(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn big(n: usize) -> String {
        "x".repeat(n)
    }

    #[test]
    fn lock_acquire_accepts_a_normal_union() {
        let keys = vec!["orders/42".to_string(), "inv/7".to_string()];
        assert!(lock_acquire(&keys, &Some("worker-a".to_string()), Some(30_000)).is_ok());
    }

    #[test]
    fn lock_acquire_rejects_too_many_keys() {
        let keys: Vec<String> = (0..MAX_LOCK_KEYS + 1).map(|i| format!("k{i}")).collect();
        let err = lock_acquire(&keys, &None, None).unwrap_err();
        assert_eq!(err.code, "too_many_keys");
    }

    #[test]
    fn lock_acquire_rejects_an_oversized_key() {
        let keys = vec![big(MAX_KEY_BYTES + 1)];
        assert_eq!(lock_acquire(&keys, &None, None).unwrap_err().code, "field_too_long");
    }

    #[test]
    fn lock_acquire_rejects_an_empty_key() {
        let keys = vec![String::new()];
        assert_eq!(lock_acquire(&keys, &None, None).unwrap_err().code, "empty_field");
    }

    #[test]
    fn ttl_ceiling_is_enforced() {
        let keys = vec!["k".to_string()];
        assert_eq!(
            lock_acquire(&keys, &None, Some(MAX_TTL_MS + 1)).unwrap_err().code,
            "ttl_too_large"
        );
        assert!(lock_acquire(&keys, &None, Some(MAX_TTL_MS)).is_ok());
    }

    #[test]
    fn semaphore_limit_ceiling_is_enforced() {
        let err = semaphore_acquire("k", &None, MAX_SEMAPHORE_LIMIT + 1, None).unwrap_err();
        assert_eq!(err.code, "limit_too_large");
    }

    #[test]
    fn metadata_entry_count_is_capped() {
        let md: HashMap<String, String> = (0..MAX_METADATA_ENTRIES + 1)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        let err = election_campaign("leader", "node-a", 30_000, &md).unwrap_err();
        assert_eq!(err.code, "too_much_metadata");
    }
}
