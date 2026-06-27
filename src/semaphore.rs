//! Counting semaphores — like a lock, but up to `limit` holders at once.
//!
//! A semaphore is the natural generalization of a mutex (`limit = 1`): N permits,
//! handed out FIFO, each carrying a fencing token and a TTL lease. Use it to cap
//! concurrency — a connection-pool size, a worker fan-out, a quota of in-flight
//! jobs — where a plain lock (one holder) is too strict.
//!
//! Keys never live in the URL path — in the JSON body for acquire/release, and
//! `?key=` for inspect — so they may contain slashes (`pools/db/primary`).
//!
//! Routes (mounted under `/v1/semaphores`):
//!   * `POST /v1/semaphores/acquire`  — `{ key, holder, limit, ttl_ms?, wait? }`
//!   * `POST /v1/semaphores/release`  — `{ key, holder, fencing_token }`
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

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct AcquireBody {
    pub key: String,
    pub holder: Option<String>,
    /// Maximum concurrent holders. Set on first use; may be re-tuned later.
    pub limit: u32,
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    // Keys never live in the URL path: in the JSON body for acquire/release, and
    // `?key=` for inspect — both slash-safe.
    Router::new()
        .route("/", get(get_semaphore))
        .route("/acquire", post(acquire))
        .route("/release", post(release))
}

/// `GET /v1/semaphores?key=K` — inspect permits, holders, and the wait queue.
async fn get_semaphore(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KeyParam>,
) -> Response {
    match node.query(ReadRequest::Semaphore { key: q.key.clone() }).await {
        Ok(ReadResponse::Semaphore(sem)) => {
            Json(json!({ "key": q.key, "semaphore": sem })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/semaphores/acquire` — take a permit of `key` or join the FIFO queue.
async fn acquire(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<AcquireBody>,
) -> Response {
    let result = node
        .propose(Command::SemaphoreAcquire {
            key: body.key,
            holder: body.holder.unwrap_or_else(|| "anonymous".to_string()),
            limit: body.limit,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
            wait: body.wait.unwrap_or(false),
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/semaphores/release` — return one permit of `key` (admits the next waiter).
async fn release(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<ReleaseBody>,
) -> Response {
    let result = node
        .propose(Command::SemaphoreRelease {
            key: body.key,
            holder: body.holder,
            fencing_token: body.fencing_token,
        })
        .await;
    propose_response(result, &uri)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds() {
        let _ = router();
    }
}
