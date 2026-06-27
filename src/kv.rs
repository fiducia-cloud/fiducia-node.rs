//! Config KV with watches (skeleton handlers).
//!
//! A linearizable, versioned key/value store for configuration and feature
//! flags — the etcd/ZooKeeper-znode primitive. Writes are proposed to the owning
//! shard's Raft group via [`Node::propose`]; single-key reads go through
//! [`Node::query`]. `watch` streams change events so clients get live config
//! push instead of polling.
//!
//! Routes (mounted under `/v1/kv`):
//!   * `GET    /v1/kv/{key}`        — read a key (+ its revision)
//!   * `PUT    /v1/kv/{key}`        — upsert `{ "value", "ttl_ms"? }`, optional CAS
//!   * `DELETE /v1/kv/{key}`        — delete a key
//!   * `GET    /v1/kv?prefix=...`   — list keys under a prefix
//!   * `GET    /v1/kv/{key}/watch`  — SSE stream of changes (key or prefix)

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct PutBody {
    pub value: String,
    pub ttl_ms: Option<u64>,
    /// Optional compare-and-swap guard: only write if the current revision
    /// equals this. `0` means "must not exist".
    pub prev_revision: Option<u64>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(list))
        .route("/:key", get(get_key).put(put_key).delete(delete_key))
        .route("/:key/watch", get(watch))
}

/// `GET /v1/kv/{key}` — read one key.
async fn get_key(State(node): State<Arc<Node>>, uri: Uri, Path(key): Path<String>) -> Response {
    match node.query(ReadRequest::Kv { key: key.clone() }).await {
        Ok(ReadResponse::Kv(Some(entry))) => {
            Json(json!({ "key": key, "found": true, "entry": entry })).into_response()
        }
        Ok(ReadResponse::Kv(None)) => Json(json!({ "key": key, "found": false })).into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `PUT /v1/kv/{key}` — upsert (optionally compare-and-swap).
async fn put_key(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Response {
    let result = node
        .propose(Command::KvPut {
            key,
            value: body.value,
            ttl_ms: body.ttl_ms,
            prev_revision: body.prev_revision,
        })
        .await;
    propose_response(result, &uri)
}

/// `DELETE /v1/kv/{key}` — remove a key.
async fn delete_key(State(node): State<Arc<Node>>, uri: Uri, Path(key): Path<String>) -> Response {
    let result = node.propose(Command::KvDelete { key }).await;
    propose_response(result, &uri)
}

/// `GET /v1/kv?prefix=...` — list keys under a prefix.
async fn list(State(_node): State<Arc<Node>>) -> Json<Value> {
    // TODO: a prefix can span shards, so this fans out across the shards it
    // touches (a per-shard Query each) and merges the results.
    Json(json!({ "error": "not_implemented", "op": "kv.list" }))
}

/// `GET /v1/kv/{key}/watch` — SSE stream of change events for a key or prefix.
async fn watch(State(_node): State<Arc<Node>>, Path(_key): Path<String>) -> Json<Value> {
    // TODO: return axum::response::sse::Sse subscribed to this shard's change
    // broadcast, filtered to the key/prefix, replaying from `?start_revision`.
    Json(json!({ "error": "not_implemented", "op": "kv.watch" }))
}
