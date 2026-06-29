//! Trusted-hop authentication for the node's **internal** HTTP planes.
//!
//! The node has no per-request user authz of its own: it trusts the
//! `x-fiducia-org-id` that the load balancer injects after verifying the caller.
//! That trust is only sound if the only things that can reach the node are the
//! LB (for `/v1`) and peer nodes (for `/raft`). If a node's port is reachable
//! directly — a misconfigured NetworkPolicy, a shared VPC, an SSRF — an attacker
//! could forge `x-fiducia-org-id` and act as any org, or forge `AppendEntries`
//! and corrupt a shard's log.
//!
//! This middleware closes that gap with a shared cluster secret. When
//! `FIDUCIA_INTERNAL_SECRET` is set, every `/v1` and `/raft` request must carry a
//! matching [`INTERNAL_AUTH_HEADER`] (compared in constant time); the LB and the
//! peer transport attach it. When the secret is **unset**, the guard is a no-op —
//! so single-node/dev and the in-process loopback tests are byte-identical. It is
//! a coarse "are you inside the trust boundary" gate, **not** a replacement for
//! the LB's user-level auth; it is the cheap, deployable complement to a proper
//! mTLS / NetworkPolicy posture (see future-work #1).

use std::sync::OnceLock;

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Header carrying the shared internal secret between trusted hops.
pub const INTERNAL_AUTH_HEADER: &str = "x-fiducia-internal-auth";

/// The configured secret, read once from `FIDUCIA_INTERNAL_SECRET`. `None` (unset
/// or blank) disables the guard.
static SECRET: OnceLock<Option<String>> = OnceLock::new();

fn configured() -> &'static Option<String> {
    SECRET.get_or_init(|| {
        std::env::var("FIDUCIA_INTERNAL_SECRET")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

/// The shared secret to attach to outbound peer (`/raft`) RPCs, if configured.
pub fn secret() -> Option<&'static str> {
    configured().as_deref()
}

/// Force-initialize the secret and log the resulting posture once at startup, so
/// an operator can see whether the node is enforcing the trust boundary.
pub fn init_and_log() {
    if configured().is_some() {
        tracing::info!("internal-auth: enforcing FIDUCIA_INTERNAL_SECRET on /v1 and /raft");
    } else {
        tracing::warn!(
            "internal-auth: FIDUCIA_INTERNAL_SECRET is unset — /v1 and /raft accept traffic from \
             ANY caller. Set it (shared with the load balancer and peer nodes) so only trusted \
             hops are accepted, or ensure the node port is unreachable from outside the cluster."
        );
    }
}

/// Axum middleware guarding an internal plane. A no-op when no secret is set.
pub async fn guard(request: Request, next: Next) -> Response {
    let provided = request
        .headers()
        .get(INTERNAL_AUTH_HEADER)
        .and_then(|v| v.to_str().ok());
    if !authorized(configured().as_deref(), provided) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "unauthorized",
                "detail": "missing or invalid internal auth; this endpoint is cluster-internal"
            })),
        )
            .into_response();
    }
    next.run(request).await
}

/// Pure authorization decision (kept separate from the HTTP middleware so it can
/// be unit-tested without env/process state).
///
/// * `expected = None`  → guard disabled, always allowed.
/// * `expected = Some`  → `provided` must be present and constant-time-equal.
pub fn authorized(expected: Option<&str>, provided: Option<&str>) -> bool {
    match expected {
        None => true,
        Some(secret) => provided
            .map(|p| constant_time_eq(p.as_bytes(), secret.as_bytes()))
            .unwrap_or(false),
    }
}

/// Length-then-content compare that doesn't short-circuit on the first differing
/// byte, so the secret can't be recovered a byte at a time via response timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_guard_allows_everything() {
        assert!(authorized(None, None));
        assert!(authorized(None, Some("whatever")));
    }

    #[test]
    fn enabled_guard_requires_exact_secret() {
        assert!(authorized(Some("s3cret"), Some("s3cret")));
        assert!(!authorized(Some("s3cret"), Some("s3cre7")));
        assert!(!authorized(Some("s3cret"), Some("s3cret-extra"))); // length mismatch
        assert!(!authorized(Some("s3cret"), None)); // header absent
    }

    #[test]
    fn constant_time_eq_matches_std_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
