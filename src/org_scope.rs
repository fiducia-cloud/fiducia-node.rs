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
//!      control byte. Even if a caller key contains SOH, the trusted prefix is
//!      prepended first, so it still cannot escape into another org.
//!
//! Only read-only node introspection without tenant state (`/v1/status`,
//! `/v1/observe/shards`, and `/v1/observe/metrics`) is exempt. Tenant
//! inventories require an org and are filtered before serialization.

use axum::{
    extract::{FromRequestParts, Request},
    http::{request::Parts, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::consensus::{propose_response, ProposeError, ProposeOutcome};

/// Header carrying the caller's org, injected by the trusted LB hop.
pub const ORG_HEADER: &str = "x-fiducia-org-id";

/// Max bytes for an org id (matches the `orgs.slug`/id column bounds).
const MAX_ORG_BYTES: usize = 128;

/// A validated org scope, attached to every state-touching `/v1` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgScope(pub String);

impl OrgScope {
    /// Namespace a caller-supplied key into this org's private keyspace.
    ///
    /// The format (`\x01<org>\x01<key>`) lives in the shared `fiducia-routing`
    /// crate because the scoped key is what the state machine routes on: the LB
    /// must hash the same scoped key to send a request to the right shard, so
    /// node and LB must share one definition, not two copies. Caller input is
    /// always appended after the complete trusted prefix, so even an input
    /// containing the SOH delimiter cannot escape into another org.
    pub fn scope(&self, key: &str) -> String {
        fiducia_routing::org_scoped_key(&self.0, key)
    }

    /// Recover the caller-facing key from a namespaced one, if it belongs to this
    /// org (used to un-prefix keys in list/prefix responses). `None` for keys
    /// outside this org's space — which is also the isolation filter for list reads.
    /// `scope("")` yields this org's list-prefix, so a prefix scan is just
    /// `scope(&caller_prefix)`.
    pub fn unscope<'a>(&self, scoped: &'a str) -> Option<&'a str> {
        scoped.strip_prefix(&fiducia_routing::org_scope_prefix(&self.0))
    }

    pub fn scope_all(&self, keys: impl IntoIterator<Item = String>) -> Vec<String> {
        keys.into_iter().map(|key| self.scope(&key)).collect()
    }

    pub fn unscope_value(&self, value: &mut Value) -> bool {
        unscope_value(self, value, false)
    }

    pub fn response(&self, mut value: Value) -> Response {
        if self.unscope_value(&mut value) {
            Json(value).into_response()
        } else {
            scope_violation()
        }
    }

    pub fn propose_response(
        &self,
        result: Result<ProposeOutcome, ProposeError>,
        uri: &axum::http::Uri,
    ) -> Response {
        match result {
            Ok(mut outcome) => {
                if !self.unscope_value(&mut outcome.output) {
                    return scope_violation();
                }
                propose_response(Ok(outcome), uri)
            }
            Err(err) => propose_response(Err(err), uri),
        }
    }
}

const IDENTITY_FIELDS: &[&str] = &[
    "key",
    "keys",
    "held_keys",
    "conflicts",
    "name",
    "tenant",
    "service",
    "holder",
    "superseded_by",
    "idempotency_key",
];

const OPAQUE_FIELDS: &[&str] = &[
    "value",
    "payload",
    "result",
    "context",
    "checkpoint",
    "metadata",
    "evidence",
    "target",
];

fn unscope_value(org: &OrgScope, value: &mut Value, identity: bool) -> bool {
    match value {
        Value::String(text) if identity => {
            if text.starts_with(fiducia_routing::ORG_SCOPE_DELIM) {
                let Some(caller_value) = org.unscope(text) else {
                    return false;
                };
                *text = caller_value.to_string();
            }
            true
        }
        Value::Array(items) => items
            .iter_mut()
            .all(|item| unscope_value(org, item, identity)),
        Value::Object(fields) => fields.iter_mut().all(|(field, child)| {
            if OPAQUE_FIELDS.contains(&field.as_str()) {
                true
            } else {
                unscope_value(org, child, IDENTITY_FIELDS.contains(&field.as_str()))
            }
        }),
        _ => true,
    }
}

fn scope_violation() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "org_scope_violation",
            "detail": "a response contained an identity outside the caller org"
        })),
    )
        .into_response()
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
/// require an org. The middleware is layered on the router that gets **nested**
/// under `/v1`, so at match time the request path has the `/v1` prefix already
/// stripped ("/status", not "/v1/status") — match both forms so the exemption
/// can't silently die if the mounting ever changes.
fn is_exempt(path: &str) -> bool {
    matches!(
        path,
        "/status"
            | "/observe/shards"
            | "/observe/metrics"
            | "/v1/status"
            | "/v1/observe/shards"
            | "/v1/observe/metrics"
    )
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
        assert!(is_exempt("/v1/observe/shards"));
        assert!(!is_exempt("/v1/observe/locks"));
        assert!(!is_exempt("/v1/observe/semaphores"));
        assert!(!is_exempt("/v1/observe/elections"));
        assert!(!is_exempt("/v1/kv"));
        assert!(!is_exempt("/v1/locks/acquire"));
    }

    #[test]
    fn response_identity_fields_are_unscoped_but_opaque_payloads_are_untouched() {
        let org = org();
        let mut value = json!({
            "key": org.scope("visible"),
            "lock": { "keys": [org.scope("a"), org.scope("b")] },
            "idempotency_key": org.scope("effect-once"),
            "payload": { "key": org.scope("application-data") },
        });

        assert!(org.unscope_value(&mut value));
        assert_eq!(value["key"], "visible");
        assert_eq!(value["lock"]["keys"], json!(["a", "b"]));
        assert_eq!(value["idempotency_key"], "effect-once");
        assert_eq!(
            value["payload"]["key"],
            org.scope("application-data"),
            "opaque application payloads are never rewritten"
        );
    }

    #[test]
    fn response_rejects_a_different_orgs_scoped_identity() {
        let mut value = json!({ "key": OrgScope("other".into()).scope("secret") });
        assert!(!org().unscope_value(&mut value));
    }
}
