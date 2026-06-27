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

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::Uri,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

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

#[derive(Debug, Deserialize)]
pub struct WatchQuery {
    /// When `true`, stream changes for every key that *starts with* `{key}`
    /// (best-effort: only keys on the same shard as the prefix are observed).
    pub prefix: Option<bool>,
}

/// `GET /v1/kv/{key}/watch` — SSE stream of change events for a key (or prefix).
///
/// Subscribes to the owning shard's change broadcast and pushes one SSE event per
/// committed put/delete that matches. The connection is long-lived (no request
/// timeout layer) with periodic keep-alive comments.
async fn watch(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    Query(q): Query<WatchQuery>,
) -> Response {
    let Some(rx) = node.watch(&key).await else {
        return Json(json!({ "error": "unavailable", "op": "kv.watch", "key": key }))
            .into_response();
    };
    let prefix = q.prefix.unwrap_or(false);
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        let matches = if prefix {
            event.key.starts_with(&key)
        } else {
            event.key == key
        };
        if !matches {
            return None;
        }
        Some(Ok::<Event, Infallible>(
            Event::default()
                .event(event.kind)
                .json_data(&event)
                .unwrap_or_else(|_| Event::default().comment("serialize-error")),
        ))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
