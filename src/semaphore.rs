//! Counting semaphores — like a lock, but up to `limit` holders at once.
//!
//! A semaphore is the natural generalization of a mutex (`limit = 1`): N permits,
//! handed out FIFO, each carrying a fencing token and a TTL lease. Use it to cap
//! concurrency — a connection-pool size, a worker fan-out, a quota of in-flight
//! jobs — where a plain lock (one holder) is too strict.
//!
//! Keys never live in the URL path — in mutation JSON bodies, and
//! `?key=` for inspect — so they may contain slashes (`pools/db/primary`).
//!
//! Routes (mounted under `/v1/semaphores`):
//!   * `POST /v1/semaphores/acquire`  — `{ key, holder, request_id?, limit, ttl_ms?, wait?, wait_timeout_ms? }`
//!   * `POST /v1/semaphores/renew`    — token-bound permit renewal
//!   * `POST /v1/semaphores/release`  — `{ key, holder, fencing_token }`
//!   * `POST /v1/semaphores/cancel`   — remove one queued holder
//!   * `GET  /v1/semaphores?key=K`    — limit, current holders, free permits, queue

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

#[derive(Debug, Deserialize)]
pub struct AcquireBody {
    pub key: String,
    pub holder: Option<String>,
    pub request_id: Option<String>,
    /// Maximum concurrent holders. Set on first use and immutable thereafter.
    pub limit: u32,
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
    pub wait_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RenewBody {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
}

#[derive(Debug, Deserialize)]
pub struct CancelBody {
    pub key: String,
    pub holder: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    // Keys never live in the URL path: in mutation JSON bodies, and
    // `?key=` for inspect — both slash-safe.
    Router::new()
        .route("/", get(get_semaphore))
        .route("/acquire", post(acquire))
        .route("/renew", post(renew))
        .route("/release", post(release))
        .route("/cancel", post(cancel))
}

/// `GET /v1/semaphores?key=K` — inspect permits, holders, and the wait queue.
async fn get_semaphore(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KeyParam>,
) -> Response {
    match node
        .query(ReadRequest::Semaphore {
            key: org.scope(&q.key),
        })
        .await
    {
        Ok(ReadResponse::Semaphore(sem)) => org.response(json!({ "key": q.key, "semaphore": sem })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/semaphores/acquire` — take a permit of `key` or join the FIFO queue.
async fn acquire(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<AcquireBody>,
) -> Response {
    if let Err(rejection) = crate::validate::semaphore_acquire(
        &body.key,
        &body.holder,
        body.limit,
        body.ttl_ms,
        body.wait_timeout_ms,
    ) {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::acquisition_request_id(&body.request_id) {
        return rejection.into_response();
    }
    let holder = body
        .holder
        .expect("holder was required by semaphore_acquire validation");
    let key = org.scope(&body.key);
    let holder = org.scope(&holder);
    let command = if let Some(request_id) = body.request_id {
        Command::SemaphoreAcquireAttempt {
            key,
            holder,
            request_id,
            limit: body.limit,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
            wait_timeout_ms: body.wait_timeout_ms,
        }
    } else {
        Command::SemaphoreAcquireV2 {
            key,
            holder,
            limit: body.limit,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
            wait_timeout_ms: body.wait_timeout_ms,
        }
    };
    let result = node.propose(command).await;
    org.propose_response(result, &uri)
}

/// `POST /v1/semaphores/renew` — extend an active permit without minting a token.
async fn renew(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<RenewBody>,
) -> Response {
    if let Err(rejection) =
        crate::validate::semaphore_renew(&body.key, &body.holder, body.fencing_token, body.ttl_ms)
    {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::SemaphoreRenew {
            key: org.scope(&body.key),
            holder: org.scope(&body.holder),
            fencing_token: body.fencing_token,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/semaphores/release` — return one permit of `key` (admits the next waiter).
async fn release(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ReleaseBody>,
) -> Response {
    if let Err(rejection) = crate::validate::semaphore_holder(&body.key, &body.holder) {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::validate_fencing_token(body.fencing_token) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::SemaphoreRelease {
            key: org.scope(&body.key),
            holder: org.scope(&body.holder),
            fencing_token: body.fencing_token,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/semaphores/cancel` — idempotently remove one queued holder.
async fn cancel(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<CancelBody>,
) -> Response {
    if let Err(rejection) = crate::validate::semaphore_holder(&body.key, &body.holder) {
        return rejection.into_response();
    }
    if let Err(rejection) = crate::validate::acquisition_request_id(&body.request_id) {
        return rejection.into_response();
    }
    let key = org.scope(&body.key);
    let holder = org.scope(&body.holder);
    let command = if let Some(request_id) = body.request_id {
        Command::SemaphoreCancelAttempt {
            key,
            holder,
            request_id,
        }
    } else {
        Command::SemaphoreCancel { key, holder }
    };
    let result = node.propose(command).await;
    org.propose_response(result, &uri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn router_builds() {
        let _ = router();
    }

    #[test]
    fn acquire_body_accepts_slash_safe_key_and_limit() {
        let body: AcquireBody = serde_json::from_value(json!({
            "key": "pools/db/primary",
            "holder": "worker-a",
            "request_id": "attempt-01",
            "limit": 3,
            "ttl_ms": 45_000,
            "wait": true
        }))
        .unwrap();

        assert_eq!(body.key, "pools/db/primary");
        assert_eq!(body.holder.as_deref(), Some("worker-a"));
        assert_eq!(body.request_id.as_deref(), Some("attempt-01"));
        assert_eq!(body.limit, 3);
        assert_eq!(body.ttl_ms, Some(45_000));
        assert_eq!(body.wait, Some(true));
        assert_eq!(body.wait_timeout_ms, None);
    }

    #[tokio::test]
    async fn acquire_rejects_missing_or_empty_holder_and_zero_ttl() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        let body = |holder: Option<&str>, ttl_ms| AcquireBody {
            key: "pool".to_string(),
            holder: holder.map(str::to_string),
            request_id: None,
            limit: 1,
            ttl_ms,
            wait: Some(false),
            wait_timeout_ms: None,
        };

        let missing = acquire(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/semaphores/acquire"),
            Json(body(None, Some(30_000))),
        )
        .await;
        assert_eq!(missing.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(missing).await["error"],
            "missing_holder"
        );

        let empty = acquire(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/semaphores/acquire"),
            Json(body(Some(""), Some(30_000))),
        )
        .await;
        assert_eq!(empty.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(empty).await["error"],
            "empty_field"
        );

        let zero = acquire(
            State(node),
            org,
            Uri::from_static("/v1/semaphores/acquire"),
            Json(body(Some("worker"), Some(0))),
        )
        .await;
        assert_eq!(zero.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            crate::test_support::json(zero).await["error"],
            "invalid_ttl"
        );
    }

    #[tokio::test]
    async fn http_renew_cancel_and_promotion_race_preserve_authority() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        let acquire_body = |holder: &str, wait: bool| AcquireBody {
            key: "pool".to_string(),
            holder: Some(holder.to_string()),
            request_id: None,
            limit: 1,
            ttl_ms: Some(30_000),
            wait: Some(wait),
            wait_timeout_ms: Some(30_000),
        };

        let owner = crate::test_support::json(
            acquire(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/semaphores/acquire"),
                Json(acquire_body("owner", false)),
            )
            .await,
        )
        .await;
        let owner_token = owner["result"]["output"]["fencing_token"].as_u64().unwrap();
        let renewed = crate::test_support::json(
            renew(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/semaphores/renew"),
                Json(RenewBody {
                    key: "pool".to_string(),
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
            acquire(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/semaphores/acquire"),
                Json(acquire_body("cancelled-waiter", true)),
            )
            .await,
        )
        .await;
        assert!(queued["result"]["output"]["wait_expires_ms"].is_number());
        let cancelled = crate::test_support::json(
            cancel(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/semaphores/cancel"),
                Json(CancelBody {
                    key: "pool".to_string(),
                    holder: "cancelled-waiter".to_string(),
                    request_id: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(cancelled["result"]["output"]["cancelled"], true);

        acquire(
            State(node.clone()),
            org.clone(),
            Uri::from_static("/v1/semaphores/acquire"),
            Json(acquire_body("promoted-waiter", true)),
        )
        .await;
        let released = crate::test_support::json(
            release(
                State(node.clone()),
                org.clone(),
                Uri::from_static("/v1/semaphores/release"),
                Json(ReleaseBody {
                    key: "pool".to_string(),
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
            cancel(
                State(node),
                org,
                Uri::from_static("/v1/semaphores/cancel"),
                Json(CancelBody {
                    key: "pool".to_string(),
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
}
