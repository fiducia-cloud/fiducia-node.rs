//! Config KV with watches.
//!
//! A linearizable, versioned key/value store for configuration and feature
//! flags — the etcd/ZooKeeper-znode primitive. Writes are proposed to the owning
//! shard's Raft group via [`Node::propose`]; single-key reads go through
//! [`Node::query`]. `watch` streams change events so clients get live config
//! push instead of polling.
//!
//! **The key is a `?key=` query parameter, never a path segment.** That keeps it
//! free of any path grammar (it may contain slashes, dots, be empty, etc.) and
//! gives the load balancer one uniform place to find the routing key on every
//! request — the same reason etcd carries keys in the request, not the URL.
//!
//! Routes (mounted under `/v1/kv`):
//!   * `GET    /v1/kv?key=K`              — read a key (+ its revision)
//!   * `GET    /v1/kv?key=K&watch=true`   — SSE stream of changes for that key
//!   * `GET    /v1/kv?prefix=P&watch=true`— SSE stream for every key under prefix `P`
//!   * `GET    /v1/kv?prefix=P`           — list keys under a prefix
//!   * `PUT    /v1/kv?key=K`              — upsert `{ "value", "ttl_ms"? }`, optional CAS
//!   * `DELETE /v1/kv?key=K`              — delete a key

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};

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

/// Query parameters shared by the KV verbs. `key` selects a single key;
/// `prefix` selects a range (for list / prefix-watch); `watch` switches a read
/// into an SSE stream.
#[derive(Debug, Default, Deserialize)]
pub struct KvParams {
    pub key: Option<String>,
    pub prefix: Option<String>,
    pub watch: Option<bool>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new().route("/", get(get_or_list).put(put_key).delete(delete_key))
}

/// `GET /v1/kv` — read a key, watch a key/prefix, or list a prefix, by query.
async fn get_or_list(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    if q.watch.unwrap_or(false) {
        return match (q.key, q.prefix) {
            (Some(key), _) => watch(node, key, false).await,
            (None, Some(prefix)) => watch(node, prefix, true).await,
            (None, None) => bad_request("watch requires `key` or `prefix`"),
        };
    }
    match q.key {
        Some(key) => match node.query(ReadRequest::Kv { key: key.clone() }).await {
            Ok(ReadResponse::Kv(Some(entry))) => {
                Json(json!({ "key": key, "found": true, "entry": entry })).into_response()
            }
            Ok(ReadResponse::Kv(None)) => {
                Json(json!({ "key": key, "found": false })).into_response()
            }
            Err(err) => read_error_response(err, &uri),
            _ => Json(json!({ "error": "unavailable" })).into_response(),
        },
        None => list(node, uri, q.prefix).await,
    }
}

/// `PUT /v1/kv?key=K` — upsert (optionally compare-and-swap). Value in the body.
async fn put_key(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KvParams>,
    Json(body): Json<PutBody>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
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

/// `DELETE /v1/kv?key=K` — remove a key.
async fn delete_key(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    let result = node.propose(Command::KvDelete { key }).await;
    propose_response(result, &uri)
}

/// `GET /v1/kv?prefix=...` — list keys under a prefix.
async fn list(node: Arc<Node>, uri: Uri, prefix: Option<String>) -> Response {
    let prefix = prefix.unwrap_or_default();
    match node.query_kv_prefix(prefix.clone()).await {
        Ok(entries) => {
            let entries: Vec<_> = entries
                .into_iter()
                .map(|(key, entry)| json!({ "key": key, "entry": entry }))
                .collect();
            Json(json!({ "prefix": prefix, "entries": entries })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
    }
}

/// SSE stream of change events for a key (or, when `prefix`, every key under it).
///
/// Subscribes to the owning shard's change broadcast and pushes one SSE event per
/// committed put/delete that matches. The connection is long-lived (no request
/// timeout layer) with periodic keep-alive comments.
async fn watch(node: Arc<Node>, key: String, prefix: bool) -> Response {
    if prefix {
        return watch_prefix(node, key).await;
    }
    let Some(rx) = node.watch(&key).await else {
        return Json(json!({ "error": "unavailable", "op": "kv.watch", "key": key }))
            .into_response();
    };
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        if !is_kv_change(event.kind) || event.key != key {
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

async fn watch_prefix(node: Arc<Node>, prefix: String) -> Response {
    let receivers = node.watch_all().await;
    if receivers.is_empty() {
        return Json(json!({ "error": "unavailable", "op": "kv.watch", "prefix": prefix }))
            .into_response();
    }

    let mut streams = StreamMap::new();
    for (idx, receiver) in receivers.into_iter().enumerate() {
        streams.insert(idx, BroadcastStream::new(receiver));
    }
    let stream = streams.filter_map(move |(_, item)| {
        let event = item.ok()?;
        if !is_kv_change(event.kind) || !event.key.starts_with(&prefix) {
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

fn is_kv_change(kind: &str) -> bool {
    matches!(kind, "put" | "delete")
}

fn bad_request(detail: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "bad_request", "detail": detail })),
    )
        .into_response()
}
