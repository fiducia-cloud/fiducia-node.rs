//! Mutual-exclusion locks.
//!
//! Routes (mounted under `/v1/locks`):
//!   * `POST /v1/locks/acquire`       — advertised body-key acquire endpoint
//!   * `GET  /v1/locks/{key}`         — inspect holder, fencing token, lease, queue
//!   * `POST /v1/locks/{key}/acquire` — try or queue for the lock
//!   * `POST /v1/locks/{key}/release` — release with holder + fencing token
//!   * `GET  /v1/locks/{key}/watch`   — SSE placeholder for lock changes

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct AcquireBody {
    pub holder: Option<String>,
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct AcquireWithKeyBody {
    pub key: String,
    pub holder: Option<String>,
    pub ttl_ms: Option<u64>,
    pub wait: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub holder: String,
    pub fencing_token: u64,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/acquire", post(acquire_with_body_key))
        .route("/:key", get(get_lock))
        .route("/:key/acquire", post(acquire))
        .route("/:key/release", post(release))
        .route("/:key/watch", get(watch))
}

/// `GET /v1/locks/{key}` — inspect lock state and FIFO wait queue.
async fn get_lock(State(node): State<Arc<Node>>, uri: Uri, Path(key): Path<String>) -> Response {
    match node.query(ReadRequest::Lock { key: key.clone() }).await {
        Ok(ReadResponse::Lock(lock)) => Json(json!({ "key": key, "lock": lock })).into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/locks/{key}/acquire` — acquire immediately or join FIFO queue.
async fn acquire(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(key): Path<String>,
    Json(body): Json<AcquireBody>,
) -> Response {
    acquire_key(
        node,
        uri,
        key,
        body.holder,
        body.ttl_ms,
        body.wait.unwrap_or(false),
    )
    .await
}

/// `POST /v1/locks/acquire` — compatibility route with key in JSON.
async fn acquire_with_body_key(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<AcquireWithKeyBody>,
) -> Response {
    acquire_key(
        node,
        uri,
        body.key,
        body.holder,
        body.ttl_ms,
        body.wait.unwrap_or(false),
    )
    .await
}

async fn acquire_key(
    node: Arc<Node>,
    uri: Uri,
    key: String,
    holder: Option<String>,
    ttl_ms: Option<u64>,
    wait: bool,
) -> Response {
    let result = node
        .propose(Command::LockAcquire {
            key,
            holder: holder.unwrap_or_else(|| "anonymous".to_string()),
            ttl_ms: ttl_ms.unwrap_or(30_000),
            wait,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/locks/{key}/release` — release only with the current fencing token.
async fn release(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(key): Path<String>,
    Json(body): Json<ReleaseBody>,
) -> Response {
    let result = node
        .propose(Command::LockRelease {
            key,
            holder: body.holder,
            fencing_token: body.fencing_token,
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/locks/{key}/watch` — SSE stream of lock changes.
async fn watch(State(_node): State<Arc<Node>>, Path(_key): Path<String>) -> Json<Value> {
    Json(json!({ "error": "not_implemented", "op": "locks.watch" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds_with_advertised_alias_and_keyed_routes() {
        let _ = router();
    }
}
