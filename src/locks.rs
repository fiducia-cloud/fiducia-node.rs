//! Mutual-exclusion locks — single-key **and** multi-key **union** locks.
//!
//! A lock can cover a *set* of keys: acquiring `{a, b, c}` succeeds only when
//! every member is free, and conflicts with anyone holding *any* of them. The
//! grant is atomic (all-or-nothing) and the wait queue is FIFO and deadlock-free
//! (see [`crate::state`]). This is Fiducia's flagship primitive — the
//! live-mutex "lock on a combination of keys" model, made linearizable by Raft.
//!
//! Keys never live in the URL path — acquire/release carry them in the JSON body
//! (a union may be many keys), inspect takes `?key=` — so they may contain
//! slashes (`orders/42`).
//!
//! Routes (mounted under `/v1/locks`):
//!   * `POST /v1/locks/acquire`     — union acquire: `{ keys:[..]|key, holder, ttl_ms?, wait? }`
//!   * `POST /v1/locks/release`     — release by `{ holder, fencing_token }`
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
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub holder: String,
    pub fencing_token: u64,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    // Keys never live in the URL path: acquire/release carry them in the JSON body
    // (a union may be many keys), and inspect takes `?key=` — both slash-safe.
    Router::new()
        .route("/", get(get_lock))
        .route("/acquire", post(acquire_union))
        .route("/release", post(release_token))
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
    fields(holder = ?body.holder, keys = ?body.keys, key = ?body.key, ttl_ms = ?body.ttl_ms, wait = ?body.wait)
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
    if let Err(rejection) = crate::validate::lock_acquire(&keys, &body.holder, body.ttl_ms) {
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
    let holder = body.holder.unwrap_or_else(|| "anonymous".to_string());
    let result = node
        .propose(Command::LockAcquire {
            keys: org.scope_all(keys),
            // Release has no key, so fence the holder too: another org cannot
            // release a grant even if it learns the globally unique token.
            holder: org.scope(&holder),
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
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
        assert_eq!(body.ttl_ms, Some(15_000));
        assert_eq!(body.wait, Some(true));
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
            ttl_ms: Some(30_000),
            wait: Some(false),
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
