//! Locks, semaphores, and reader-writer locks (skeleton handlers).
//!
//! The flagship coordination primitive. Writes are proposed to the owning shard's
//! Raft group via [`Node::propose`] (so a follower redirects to the leader via the
//! 421 + `x-fiducia-leader` contract). `max == 1` is a mutex; `max > 1` a counting
//! semaphore. `wait` selects blocking (long-poll until granted) vs try-lock.
//!
//! Routes:
//!   * `POST /v1/locks/{key}/acquire`  — `{ ttl_ms?, wait?, max? }` (mutex/semaphore)
//!   * `POST /v1/locks/{key}/release`  — `{ lock_id }`
//!   * `GET  /v1/locks/{key}`          — current holders + queue depth (info)
//!   * `POST /v1/rw/{key}/read|write`  — reader-writer acquire (+ `/end`)

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, Node};
use crate::state::Command;

const DEFAULT_TTL_MS: u64 = 30_000;

#[derive(Debug, Deserialize)]
pub struct AcquireBody {
    #[serde(default)]
    pub holder: String,
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub wait: bool,
    pub max: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub lock_id: String,
}

#[derive(Debug, Deserialize)]
pub struct RwBody {
    #[serde(default)]
    pub holder: String,
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub wait: bool,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:key", get(info))
        .route("/:key/acquire", post(acquire))
        .route("/:key/release", post(release))
}

pub fn rw_router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:key/read", post(acquire_read))
        .route("/:key/read/end", post(end_read))
        .route("/:key/write", post(acquire_write))
        .route("/:key/write/end", post(end_write))
}

/// `POST /v1/locks/{key}/acquire` — mutex (max=1) or semaphore (max>1).
async fn acquire(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(body): Json<AcquireBody>) -> Response {
    // TODO: honor `wait` — block (long-poll) until granted vs try-lock now.
    let _ = body.wait;
    let result = node
        .propose(Command::LockAcquire {
            key,
            holder: body.holder,
            ttl_ms: body.ttl_ms.unwrap_or(DEFAULT_TTL_MS),
            max: body.max.unwrap_or(1),
        })
        .await;
    propose_response(result)
}

/// `POST /v1/locks/{key}/release` — release by the lock id from acquire.
async fn release(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(body): Json<ReleaseBody>) -> Response {
    propose_response(node.propose(Command::LockRelease { key, lock_id: body.lock_id }).await)
}

/// `GET /v1/locks/{key}` — holders + queue depth.
async fn info(State(_node): State<Arc<Node>>, Path(_key): Path<String>) -> Json<Value> {
    // TODO: query the shard for holders/queue (needs a Lock ReadRequest variant).
    Json(json!({ "error": "not_implemented", "op": "lock.info" }))
}

async fn acquire_read(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(b): Json<RwBody>) -> Response {
    let _ = b.wait;
    propose_response(node.propose(Command::RwAcquireRead { key, holder: b.holder, ttl_ms: b.ttl_ms.unwrap_or(DEFAULT_TTL_MS) }).await)
}
async fn end_read(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(b): Json<ReleaseBody>) -> Response {
    propose_response(node.propose(Command::RwEndRead { key, lock_id: b.lock_id }).await)
}
async fn acquire_write(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(b): Json<RwBody>) -> Response {
    let _ = b.wait;
    propose_response(node.propose(Command::RwAcquireWrite { key, holder: b.holder, ttl_ms: b.ttl_ms.unwrap_or(DEFAULT_TTL_MS) }).await)
}
async fn end_write(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(b): Json<ReleaseBody>) -> Response {
    propose_response(node.propose(Command::RwEndWrite { key, lock_id: b.lock_id }).await)
}
