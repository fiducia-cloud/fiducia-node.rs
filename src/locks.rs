//! Mutual-exclusion locks — single-key **and** multi-key **union** locks.
//!
//! A lock can cover a *set* of keys: acquiring `{a, b, c}` succeeds only when
//! every member is free, and conflicts with anyone holding *any* of them. The
//! grant is atomic (all-or-nothing) and the wait queue is FIFO and deadlock-free
//! (see [`crate::state`]). This is Fiducia's flagship primitive — the
//! live-mutex "lock on a combination of keys" model, made linearizable by Raft.
//!
//! Keys never live in the URL path — mutations carry them in the JSON body
//! (a union may be many keys), inspect takes `?key=` — so they may contain
//! slashes (`orders/42`).
//!
//! Routes (mounted under `/v1/locks`):
//!   * `POST /v1/locks/acquire`     — union acquire: `{ keys:[..]|key, holder, request_id?, ttl_ms?, wait?, wait_timeout_ms? }`
//!   * `POST /v1/locks/renew`       — token-bound lease renewal
//!   * `POST /v1/locks/release`     — release by `{ holder, fencing_token }`
//!   * `POST /v1/locks/cancel`      — remove one queued holder/key-set identity
//!   * `GET  /v1/locks?key=K`       — inspect a member key: holder, the held union, queue

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::Command;

/// Acquire body. Supply `keys` for a union lock, or `key` for a single-key lock.
#[derive(Debug, Default, Deserialize)]
pub struct AcquireBody {
    pub keys: Option<Vec<String>>,
    pub key: Option<String>,
    pub holder: Option<String>,
    /// Unique identity for this logical attempt, reused across retries and its
    /// cancellation. Omission keeps legacy wire compatibility but cannot close
    /// the cancel-before-late-acquire race.
    pub request_id: Option<String>,
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
    /// How long a `wait:true` request may remain queued, independently of the
    /// lease TTL it will receive if promoted.
    pub wait_timeout_ms: Option<u64>,
}

/// Token-bound renew body. The exact canonical key set must match the grant.
#[derive(Debug, Deserialize)]
pub struct RenewBody {
    pub keys: Option<Vec<String>>,
    pub key: Option<String>,
    pub holder: String,
    pub fencing_token: u64,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub holder: String,
    pub fencing_token: u64,
}

/// Queue identity to remove. Cancellation never releases an active grant.
#[derive(Debug, Deserialize)]
pub struct CancelBody {
    pub keys: Option<Vec<String>>,
    pub key: Option<String>,
    pub holder: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    // Keys never live in the URL path: mutations carry them in the JSON body
    // (a union may be many keys), and inspect takes `?key=` — both slash-safe.
    Router::new()
        .route("/", get(get_lock))
        .route("/acquire", post(acquire_union))
        .route("/renew", post(renew_token))
        .route("/release", post(release_token))
        .route("/cancel", post(cancel_waiter))
}

/// `GET /v1/locks?key=K` — inspect lock state for one member key.
#[tracing::instrument(name = "http.lock.get", skip(node, uri), fields(key = %q.key))]
async fn get_lock(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KeyParam>,
) -> Response {
    match node
        .query(ReadRequest::Lock {
            key: org.scope(&q.key),
        })
        .await
    {
        Ok(ReadResponse::Lock(lock)) => org.response(json!({ "key": q.key, "lock": lock })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/locks/acquire` — acquire the union of `keys` (or a single `key`).
#[tracing::instrument(
    name = "http.lock.acquire",
    skip(node, uri, body),
    fields(holder = ?body.holder, request_id = ?body.request_id, keys = ?body.keys, key = ?body.key, ttl_ms = ?body.ttl_ms, wait = ?body.wait, wait_timeout_ms = ?body.wait_timeout_ms)
)]
async fn acquire_union(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<AcquireBody>,
) -> Response {
    let keys = body
        .keys
        .clone()
        .or_else(|| body.key.clone().map(|k| vec![k]))
        .unwrap_or_default();
    if keys.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "no_keys", "detail": "provide `keys` or `key`" })),
        )
            .into_response();
    }
    if let Err(rejection) =
        crate::validate::lock_acquire(&keys, &body.holder, body.ttl_ms, body.wait_timeout_ms)
    {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::acquisition_request_id(&body.request_id) {
        return rejection.into_response();
    }
    acquire(node, org, uri, keys, body).await
}

async fn acquire(
    node: Arc<Node>,
    org: OrgScope,
    uri: Uri,
    keys: Vec<String>,
    body: AcquireBody,
) -> Response {
    let holder = body
        .holder
        .expect("holder was required by lock_acquire validation");
    let scoped_keys = org.scope_all(keys);
    // Release has no key, so fence the holder too: another org cannot release a
    // grant even if it learns the globally unique token.
    let scoped_holder = org.scope(&holder);
    let command = if let Some(request_id) = body.request_id {
        Command::LockAcquireAttempt {
            keys: scoped_keys,
            holder: scoped_holder,
            request_id,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
            wait_timeout_ms: body.wait_timeout_ms,
        }
    } else {
        Command::LockAcquireV2 {
            keys: scoped_keys,
            holder: scoped_holder,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
            wait_timeout_ms: body.wait_timeout_ms,
        }
    };
    let result = node.propose(command).await;
    org.propose_response(result, &uri)
}

/// `POST /v1/locks/renew` — extend an active union grant without minting a token.
#[tracing::instrument(
    name = "http.lock.renew",
    skip(node, uri, body),
    fields(holder = %body.holder, fencing_token = body.fencing_token, keys = ?body.keys, key = ?body.key, ttl_ms = ?body.ttl_ms)
)]
async fn renew_token(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<RenewBody>,
) -> Response {
    let keys = body
        .keys
        .clone()
        .or_else(|| body.key.clone().map(|key| vec![key]))
        .unwrap_or_default();
    if let Err(rejection) =
        crate::validate::lock_renew(&keys, &body.holder, body.fencing_token, body.ttl_ms)
    {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::LockRenew {
            keys: org.scope_all(keys),
            holder: org.scope(&body.holder),
            fencing_token: body.fencing_token,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/locks/release` — release a (possibly multi-key) grant by token.
#[tracing::instrument(
    name = "http.lock.release",
    skip(node, uri, body),
    fields(holder = %body.holder, fencing_token = body.fencing_token)
)]
async fn release_token(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ReleaseBody>,
) -> Response {
    if let Err(rejection) = crate::validate::lock_release(&body.holder) {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::validate_fencing_token(body.fencing_token) {
        return rejection.into_response();
    }
    release(node, org, uri, body).await
}

async fn release(node: Arc<Node>, org: OrgScope, uri: Uri, body: ReleaseBody) -> Response {
    let result = node
        .propose(Command::LockRelease {
            holder: org.scope(&body.holder),
            fencing_token: body.fencing_token,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/locks/cancel` — idempotently remove one exact queued request.
#[tracing::instrument(
    name = "http.lock.cancel",
    skip(node, uri, body),
    fields(holder = %body.holder, request_id = ?body.request_id, keys = ?body.keys, key = ?body.key)
)]
async fn cancel_waiter(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<CancelBody>,
) -> Response {
    let keys = body
        .keys
        .clone()
        .or_else(|| body.key.clone().map(|key| vec![key]))
        .unwrap_or_default();
    if let Err(rejection) = crate::validate::lock_cancel(&keys, &body.holder) {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::acquisition_request_id(&body.request_id) {
        return rejection.into_response();
    }
    let scoped_keys = org.scope_all(keys);
    let scoped_holder = org.scope(&body.holder);
    let command = if let Some(request_id) = body.request_id {
        Command::LockCancelAttempt {
            keys: scoped_keys,
            holder: scoped_holder,
            request_id,
        }
    } else {
        Command::LockCancel {
            keys: scoped_keys,
            holder: scoped_holder,
        }
    };
    let result = node.propose(command).await;
    org.propose_response(result, &uri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn router_builds_with_union_and_single_key_routes() {
        let _ = router();
    }

    #[test]
    fn acquire_body_accepts_union_keys_with_slashes() {
        let body: AcquireBody = serde_json::from_value(json!({
            "keys": ["orders/42", "inventory/sku-7"],
            "holder": "worker-a",
            "request_id": "attempt-01",
            "ttl_ms": 15_000,
            "wait": true
        }))
        .unwrap();

        assert_eq!(
            body.keys.unwrap(),
            vec!["orders/42".to_string(), "inventory/sku-7".to_string()]
        );
        assert_eq!(body.key, None);
        assert_eq!(body.holder.as_deref(), Some("worker-a"));
        assert_eq!(body.request_id.as_deref(), Some("attempt-01"));
        assert_eq!(body.ttl_ms, Some(15_000));
        assert_eq!(body.wait, Some(true));
        assert_eq!(body.wait_timeout_ms, None);
    }

    #[tokio::test]
    async fn acquire_rejects_missing_or_empty_holder_and_zero_ttl() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());

        let missing = acquire_union(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/locks/acquire"),
            Json(AcquireBody {
                keys: Some(vec!["k".to_string()]),
                key: None,
                holder: None,
                request_id: None,
                ttl_ms: Some(30_000),
                wait: Some(false),
                wait_timeout_ms: None,
            }),
        )
        .await;
        assert_eq!(missing.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(missing).await["error"],
            "missing_holder"
        );

        let empty = acquire_union(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/locks/acquire"),
            Json(AcquireBody {
                keys: Some(vec!["k".to_string()]),
                key: None,
                holder: Some(String::new()),
                request_id: None,
                ttl_ms: Some(30_000),
                wait: Some(false),
                wait_timeout_ms: None,
            }),
        )
        .await;
        assert_eq!(empty.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(empty).await["error"],
            "empty_field"
        );

        let zero_ttl = acquire_union(
            State(node),
            org,
            Uri::from_static("/v1/locks/acquire"),
            Json(AcquireBody {
                keys: Some(vec!["k".to_string()]),
                key: None,
                holder: Some("worker".to_string()),
                request_id: None,
                ttl_ms: Some(0),
                wait: Some(false),
                wait_timeout_ms: None,
            }),
        )
        .await;
        assert_eq!(zero_ttl.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(zero_ttl).await["error"],
            "invalid_ttl"
        );
    }

    #[tokio::test]
    async fn http_renew_cancel_and_promotion_race_preserve_authority() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        let acquire_body = |holder: &str, wait: bool| AcquireBody {
            keys: Some(vec!["resource".to_string()]),
            key: None,
            holder: Some(holder.to_string()),
            request_id: None,
            ttl_ms: Some(30_000),
            wait: Some(wait),
            wait_timeout_ms: Some(30_000),
        };

        let owner = crate::test_support::json(
            acquire_union(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/locks/acquire"),
                Json(acquire_body("owner", false)),
            )
            .await,
        )
        .await;
        let owner_token = owner["result"]["output"]["fencing_token"].as_u64().unwrap();
        let renewed = crate::test_support::json(
            renew_token(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/locks/renew"),
                Json(RenewBody {
                    keys: Some(vec!["resource".to_string()]),
                    key: None,
                    holder: "owner".to_string(),
                    fencing_token: owner_token,
                    ttl_ms: Some(60_000),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(renewed["result"]["output"]["renewed"], true);
        assert_eq!(renewed["result"]["output"]["fencing_token"], owner_token);

        let queued = crate::test_support::json(
            acquire_union(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/locks/acquire"),
                Json(acquire_body("cancelled-waiter", true)),
            )
            .await,
        )
        .await;
        assert!(queued["result"]["output"]["wait_expires_ms"].is_number());
        let cancelled = crate::test_support::json(
            cancel_waiter(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/locks/cancel"),
                Json(CancelBody {
                    keys: Some(vec!["resource".to_string()]),
                    key: None,
                    holder: "cancelled-waiter".to_string(),
                    request_id: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(cancelled["result"]["output"]["cancelled"], true);

        acquire_union(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/locks/acquire"),
            Json(acquire_body("promoted-waiter", true)),
        )
        .await;
        let released = crate::test_support::json(
            release_token(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/locks/release"),
                Json(ReleaseBody {
                    holder: "owner".to_string(),
                    fencing_token: owner_token,
                }),
            )
            .await,
        )
        .await;
        let promoted_token = released["result"]["output"]["promoted"][0]["fencing_token"]
            .as_u64()
            .unwrap();
        let raced = crate::test_support::json(
            cancel_waiter(
                State(node),
                org,
                Uri::from_static("/v1/locks/cancel"),
                Json(CancelBody {
                    keys: Some(vec!["resource".to_string()]),
                    key: None,
                    holder: "promoted-waiter".to_string(),
                    request_id: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(raced["result"]["output"]["cancelled"], false);
        assert_eq!(raced["result"]["output"]["acquired"], true);
        assert_eq!(raced["result"]["output"]["fencing_token"], promoted_token);
    }

    #[tokio::test]
    async fn identical_lock_keys_are_isolated_and_tokens_cannot_cross_orgs() {
        let node = crate::test_support::node(4);
        let org_a = OrgScope("org-a".into());
        let org_b = OrgScope("org-b".into());
        let uri = Uri::from_static("/v1/locks/acquire");

        let acquire_for = |holder: &str| AcquireBody {
            keys: Some(vec!["orders/42".to_string()]),
            key: None,
            holder: Some(holder.to_string()),
            request_id: None,
            ttl_ms: Some(30_000),
            wait: Some(false),
            wait_timeout_ms: None,
        };
        let a = crate::test_support::json(
            acquire_union(
                State(node.clone()),
                org_a.clone(),
                uri.clone(),
                Json(acquire_for("worker")),
            )
            .await,
        )
        .await;
        let b = crate::test_support::json(
            acquire_union(
                State(node.clone()),
                org_b.clone(),
                uri,
                Json(acquire_for("worker")),
            )
            .await,
        )
        .await;

        assert_eq!(a["result"]["output"]["acquired"], true);
        assert_eq!(b["result"]["output"]["acquired"], true);
        assert_eq!(a["result"]["output"]["keys"], json!(["orders/42"]));
        assert_eq!(b["result"]["output"]["keys"], json!(["orders/42"]));
        assert_eq!(a["result"]["output"]["holder"], "worker");
        assert_eq!(b["result"]["output"]["holder"], "worker");

        let token_a = a["result"]["output"]["fencing_token"]
            .as_u64()
            .expect("org A fencing token");
        let cross_org_release = crate::test_support::json(
            release_token(
                State(node.clone()),
                org_b,
                Uri::from_static("/v1/locks/release"),
                Json(ReleaseBody {
                    holder: "worker".to_string(),
                    fencing_token: token_a,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(
            cross_org_release["result"]["output"]["released"], false,
            "org B must not release org A's grant even with its token"
        );

        let inspected = crate::test_support::json(
            get_lock(
                State(node),
                org_a,
                Uri::from_static("/v1/locks"),
                Query(KeyParam {
                    key: "orders/42".to_string(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(inspected["key"], "orders/42");
        assert_eq!(inspected["lock"]["holder"], "worker");
        assert_eq!(inspected["lock"]["held_keys"], json!(["orders/42"]));
        assert!(
            !inspected.to_string().contains("\u{1}"),
            "internal org prefixes never reach the response"
        );
    }
}
