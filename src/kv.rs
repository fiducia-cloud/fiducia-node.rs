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

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
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
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::Command;

/// Marker prefix on a sealed KV value. Self-describing so a value can be
/// recognised as ciphertext on read without a schema flag, which is what lets
/// encrypted and plaintext-declared values coexist in the same keyspace.
const KV_ENVELOPE_PREFIX: &str = "fcenc:v1:";

/// KV value encryption at rest (AES-256-GCM).
///
/// **Default posture:** when `FIDUCIA_KV_ENCRYPTION_KEY` is set (base64 of a
/// 32-byte key, the same on every replica), values are sealed *before* they
/// enter the Raft log — so the on-disk log, the snapshots, and the in-memory
/// state machine all hold ciphertext only, closing the plaintext-at-rest hole.
/// A client may opt a specific write out with `"plaintext": true` (a hot key
/// it would rather not pay decrypt-on-read for); that value is stored verbatim.
///
/// Sealing happens once, on the node that receives the PUT, and the resulting
/// envelope string is what gets replicated — so every replica stores identical
/// bytes and the state machine stays deterministic despite the random nonce.
pub struct KvCipher {
    cipher: Aes256Gcm,
}

impl KvCipher {
    /// Load the cluster KV key from `FIDUCIA_KV_ENCRYPTION_KEY`. `None` (env
    /// unset/empty/malformed) means encryption is disabled and values are
    /// stored as-is — the pre-existing behaviour.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("FIDUCIA_KV_ENCRYPTION_KEY").ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let bytes = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;
        let key: [u8; 32] = bytes.try_into().ok()?;
        Some(Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key)),
        })
    }

    /// Seal plaintext into `PREFIX + base64(nonce ‖ ciphertext ‖ tag)`.
    pub fn seal(&self, plaintext: &str) -> String {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .expect("AES-256-GCM encryption is infallible for in-memory inputs");
        let mut blob = nonce.to_vec();
        blob.extend_from_slice(&ciphertext);
        format!(
            "{KV_ENVELOPE_PREFIX}{}",
            base64::engine::general_purpose::STANDARD.encode(blob)
        )
    }

    /// Return the plaintext when `stored` is one of our envelopes and
    /// authenticates; otherwise return `stored` unchanged. A plaintext-declared
    /// value (no prefix) or a value written when encryption was off simply
    /// passes through — so reads never fail because of a representation
    /// mismatch.
    pub fn unseal(&self, stored: &str) -> String {
        let Some(b64) = stored.strip_prefix(KV_ENVELOPE_PREFIX) else {
            return stored.to_string();
        };
        let Ok(blob) = base64::engine::general_purpose::STANDARD.decode(b64) else {
            return stored.to_string();
        };
        if blob.len() < 12 + 16 {
            return stored.to_string(); // need a nonce and a GCM tag
        }
        let (nonce, ciphertext) = blob.split_at(12);
        match self.cipher.decrypt(Nonce::from_slice(nonce), ciphertext) {
            Ok(plaintext) => String::from_utf8(plaintext).unwrap_or_else(|_| stored.to_string()),
            Err(_) => stored.to_string(),
        }
    }
}

/// Seal a to-be-written value with the node's cipher unless the caller opted
/// out or encryption is disabled.
fn seal_for_write(node: &Node, value: String, plaintext_opt_out: bool) -> String {
    match node.kv_cipher() {
        Some(cipher) if !plaintext_opt_out => cipher.seal(&value),
        _ => value,
    }
}

/// Unseal a read value with the node's cipher if configured.
fn unseal_for_read(node: &Node, value: &str) -> String {
    match node.kv_cipher() {
        Some(cipher) => cipher.unseal(value),
        None => value.to_string(),
    }
}

#[derive(Debug, Deserialize)]
pub struct PutBody {
    pub value: String,
    pub ttl_ms: Option<u64>,
    /// Optional compare-and-swap guard: only write if the current revision
    /// equals this. `0` means "must not exist".
    pub prev_revision: Option<u64>,
    /// Opt this write out of at-rest encryption (stored verbatim). Defaults to
    /// encrypted whenever the cluster has a KV key configured.
    #[serde(default)]
    pub plaintext: bool,
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
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    if q.watch.unwrap_or(false) {
        return match (q.key, q.prefix) {
            (Some(key), _) => watch(node, org.scope(&key), false, org).await,
            (None, Some(prefix)) => watch(node, org.scope(&prefix), true, org).await,
            (None, None) => bad_request("watch requires `key` or `prefix`"),
        };
    }
    match q.key {
        // The caller's key is namespaced into their org before it reaches the
        // state machine; the response echoes the caller-facing key, not the scoped
        // one, so the isolation is invisible to the client.
        Some(key) => match node
            .query(ReadRequest::Kv {
                key: org.scope(&key),
            })
            .await
        {
            Ok(ReadResponse::Kv(Some(mut entry))) => {
                entry.value = unseal_for_read(&node, &entry.value);
                Json(json!({ "key": key, "found": true, "entry": entry })).into_response()
            }
            Ok(ReadResponse::Kv(None)) => {
                Json(json!({ "key": key, "found": false })).into_response()
            }
            Err(err) => read_error_response(err, &uri),
            _ => Json(json!({ "error": "unavailable" })).into_response(),
        },
        None => list(node, org, q.prefix.unwrap_or_default()).await,
    }
}

/// `PUT /v1/kv?key=K` — upsert (optionally compare-and-swap). Value in the body.
async fn put_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KvParams>,
    Json(body): Json<PutBody>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    // Seal before the value enters the log, so ciphertext is what gets
    // replicated and persisted (log + snapshot). Sealing here (once) keeps the
    // replicated command byte-identical across replicas.
    let value = seal_for_write(&node, body.value, body.plaintext);
    let result = node
        .propose(Command::KvPut {
            key: org.scope(&key),
            value,
            ttl_ms: body.ttl_ms,
            prev_revision: body.prev_revision,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `DELETE /v1/kv?key=K` — remove a key.
async fn delete_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    let result = node
        .propose(Command::KvDelete {
            key: org.scope(&key),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/kv?prefix=...` — list live keys under a prefix, scoped to the caller's
/// org. The caller's prefix is namespaced, and every returned key is un-namespaced
/// back to the caller-facing form; keys outside the org's space are filtered out
/// (they never share the prefix), which is what makes the list read tenant-safe.
async fn list(node: Arc<Node>, org: OrgScope, prefix: String) -> Response {
    let scoped_prefix = org.scope(&prefix);
    let keys: Vec<_> = node
        .list_kv(&scoped_prefix)
        .await
        .into_iter()
        .filter_map(|mut item| {
            let unscoped = org.unscope(&item.key)?.to_string();
            item.key = unscoped;
            Some(item)
        })
        .collect();
    Json(json!({ "prefix": prefix, "count": keys.len(), "keys": keys })).into_response()
}

/// SSE stream of change events for a key (or, when `prefix`, every key under it).
///
/// Subscribes to the owning shard's change broadcast and pushes one SSE event per
/// committed put/delete that matches. The connection is long-lived (no request
/// timeout layer) with periodic keep-alive comments.
async fn watch(node: Arc<Node>, key: String, prefix: bool, org: OrgScope) -> Response {
    if prefix {
        return watch_prefix(node, key, org).await;
    }
    let Some(rx) = node.watch(&key).await else {
        return Json(json!({ "error": "unavailable", "op": "kv.watch" })).into_response();
    };
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        if event.scope != "kv" {
            return None; // ignore election/service changes on the shared shard stream
        }
        if event.key != key {
            return None;
        }
        Some(Ok::<Event, Infallible>(unscoped_change_event(&event, &org)))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

async fn watch_prefix(node: Arc<Node>, prefix: String, org: OrgScope) -> Response {
    let receivers = node.watch_all().await;
    if receivers.is_empty() {
        return Json(json!({ "error": "unavailable", "op": "kv.watch" })).into_response();
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
        Some(Ok::<Event, Infallible>(unscoped_change_event(&event, &org)))
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// Emit a KV change event with its key un-namespaced back to the caller-facing
/// form, so a watcher never sees another org's key or the internal prefix.
fn unscoped_change_event<T: serde::Serialize>(event: &T, org: &OrgScope) -> Event {
    let mut value = match serde_json::to_value(event) {
        Ok(v) => v,
        Err(_) => return Event::default().comment("serialize-error"),
    };
    if let Some(scoped) = value.get("key").and_then(|k| k.as_str()) {
        match org.unscope(scoped) {
            Some(caller_key) => value["key"] = json!(caller_key),
            None => return Event::default().comment("out-of-scope"),
        }
    }
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("change")
        .to_string();
    Event::default().event(kind).data(value.to_string())
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
