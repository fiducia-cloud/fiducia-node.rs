//! Mandatory per-org namespacing for the client (`/v1`) coordination plane.
//!
//! Every tenant's coordination state (KV, locks, semaphores, counters, …) must be
//! isolated: one org can never see or touch another's keys. The node's design
//! already trusts the `x-fiducia-org-id` the load balancer injects (behind the
//! internal-auth secret — see [`crate::internal_auth`]); this module turns that
//! trust into *enforcement + isolation*:
//!
//!   1. [`require_org`] rejects any state-touching `/v1` request that does not
//!      carry a valid org header, and stashes the validated [`OrgScope`] in the
//!      request extensions. There is no "no-org" coordination access.
//!   2. [`OrgScope::scope`] / [`OrgScope::unscope`] namespace a caller's key into
//!      a private keyspace (`\x01<org>\x01<key>`) and back, so the state machine
//!      stores every org's data under a disjoint prefix. The delimiter is the SOH
//!      control byte, which valid keys can never contain (the validators reject
//!      control characters), so no caller key can forge another org's prefix.
//!
//! Read-only node introspection (`/v1/status`, `/v1/observe/*`) is *not* tenant
//! data and is exempt.
//!
//! ## Status — enforcement live, per-primitive isolation in progress
//!
//! [`require_org`] is wired on `/v1`, so **no coordination request without a
//! valid org is accepted**. Full data isolation additionally requires every
//! primitive handler to route its key through [`OrgScope::scope`] (and
//! [`OrgScope::unscope`] on list/watch responses), plus scoping the inventory
//! reads (`LockInventory`, `ServiceList`, `KvList`, …) that today fan out across
//! all orgs. That wiring is being rolled out primitive-by-primitive; until a
//! given primitive calls `scope`, it is gated (org required) but not yet
//! namespaced. Do **not** treat the plane as fully isolated until every
//! primitive is scoped and covered by a cross-org test.

use axum::{
    extract::{FromRequestParts, Request},
    http::{request::Parts, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Header carrying the caller's org, injected by the trusted LB hop.
pub const ORG_HEADER: &str = "x-fiducia-org-id";

/// Max bytes for an org id (matches the `orgs.slug`/id column bounds).
const MAX_ORG_BYTES: usize = 128;

/// SOH byte — a control char no valid key/name may contain, used to fence the
/// org prefix so a caller can never craft a key that lands in another org's space.
const DELIM: char = '\u{1}';

/// A validated org scope, attached to every state-touching `/v1` request.
#[derive(Debug, Clone)]
pub struct OrgScope(pub String);

impl OrgScope {
    /// Namespace a caller-supplied key into this org's private keyspace.
    pub fn scope(&self, key: &str) -> String {
        format!("{DELIM}{}{DELIM}{key}", self.0)
    }

    /// Recover the caller-facing key from a namespaced one, if it belongs to this
    /// org (used to un-prefix keys in list/prefix responses). `None` for keys
    /// outside this org's space — which is also the isolation filter for list reads.
    /// `scope("")` yields this org's list-prefix, so a prefix scan is just
    /// `scope(&caller_prefix)`.
    pub fn unscope<'a>(&self, scoped: &'a str) -> Option<&'a str> {
        scoped.strip_prefix(&format!("{DELIM}{}{DELIM}", self.0))
    }
}

/// Reject `x-fiducia-org-id` values that are empty, oversized, or contain control
/// characters (which would collide with the namespace delimiter).
fn valid_org(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ORG_BYTES
        && !value.chars().any(|c| c.is_control() || c.is_whitespace())
}

fn reject(code: &str, detail: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": code, "detail": detail })),
    )
        .into_response()
}

/// Paths under `/v1` that are node introspection, not tenant data, and so do not
/// require an org. Matched against the full request path.
fn is_exempt(path: &str) -> bool {
    path == "/v1/status" || path.starts_with("/v1/observe")
}

/// Middleware: require a valid org on every state-touching `/v1` request and make
/// it available to handlers as [`OrgScope`]. Mounted after `internal_auth::guard`
/// so the org header is already known to come from the trusted hop.
pub async fn require_org(mut request: Request, next: Next) -> Response {
    if is_exempt(request.uri().path()) {
        return next.run(request).await;
    }
    let org = request
        .headers()
        .get(ORG_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    match org {
        Some(org) if valid_org(&org) => {
            request.extensions_mut().insert(OrgScope(org));
            next.run(request).await
        }
        Some(_) => reject(
            "invalid_org",
            "x-fiducia-org-id must be non-empty, <=128 bytes, no control/whitespace",
        ),
        None => reject(
            "missing_org",
            "x-fiducia-org-id is required on coordination requests",
        ),
    }
}

/// Extractor: pull the validated [`OrgScope`] a handler runs under. Infallible in
/// practice because [`require_org`] runs first on every non-exempt `/v1` route;
/// a missing scope (a route mounted without the middleware) is a 500 config bug.
#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for OrgScope {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<OrgScope>().cloned().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "org_scope_missing", "detail": "route not org-guarded" })),
            )
                .into_response()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn org() -> OrgScope {
        OrgScope("org_9".to_string())
    }

    #[test]
    fn scope_roundtrips_and_isolates() {
        let a = OrgScope("org_a".into());
        let b = OrgScope("org_b".into());
        let scoped = a.scope("flags/checkout");
        assert_eq!(a.unscope(&scoped), Some("flags/checkout"));
        // Another org can't read A's namespaced key.
        assert_eq!(b.unscope(&scoped), None);
        // And two orgs' identical keys never collide.
        assert_ne!(a.scope("k"), b.scope("k"));
    }

    #[test]
    fn scoped_key_cannot_be_forged_by_a_crafted_key() {
        // A caller key that *looks* like it embeds another org still lands in the
        // caller's own fenced space (the leading delimiter belongs to the scope).
        let a = org();
        let evil = a.scope("\u{1}org_victim\u{1}secret");
        assert!(a.unscope(&evil).is_some());
        let victim = OrgScope("org_victim".into());
        assert_eq!(victim.unscope(&evil), None, "no cross-org escape");
    }

    #[test]
    fn org_validation() {
        assert!(valid_org("org_9"));
        assert!(valid_org("acme-prod-01"));
        assert!(!valid_org(""));
        assert!(!valid_org("has space"));
        assert!(!valid_org("ctrl\u{1}char"));
        assert!(!valid_org(&"x".repeat(200)));
    }

    #[test]
    fn introspection_paths_are_exempt() {
        assert!(is_exempt("/v1/status"));
        assert!(is_exempt("/v1/observe/metrics"));
        assert!(!is_exempt("/v1/kv"));
        assert!(!is_exempt("/v1/locks/acquire"));
    }
}
