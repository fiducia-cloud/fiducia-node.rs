//! Leader-election API (skeleton handlers).
//!
//! Lets *clients* run their own leader elections on top of Fiducia: campaign for
//! a named leadership, hold it via a TTL lease, and observe who currently holds
//! it. The winner receives a monotonic fencing token to defeat stale leaders.
//! Mutations are proposed to the shard owning the election name; reads go through
//! [`Node::query`].
//!
//! This is distinct from the node's *internal* per-shard Raft leadership; a
//! client election named `name` is just state replicated by the shard that owns
//! `name`.
//!
//! Routes (mounted under `/v1/elections`):
//!   * `POST /v1/elections/{name}/campaign` — `{ "candidate", "ttl_ms" }`
//!   * `POST /v1/elections/{name}/renew`    — `{ "candidate", "fencing_token" }`
//!   * `POST /v1/elections/{name}/resign`   — `{ "candidate", "fencing_token" }`
//!   * `GET  /v1/elections/{name}`          — observe the current leader
//!   * `GET  /v1/elections/{name}/watch`    — SSE stream of leadership changes

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_json, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct CampaignBody {
    pub candidate: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct HoldBody {
    pub candidate: String,
    pub fencing_token: u64,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:name", get(observe))
        .route("/:name/campaign", post(campaign))
        .route("/:name/renew", post(renew))
        .route("/:name/resign", post(resign))
        .route("/:name/watch", get(watch))
}

/// `POST /v1/elections/{name}/campaign` — try to become leader.
async fn campaign(
    State(node): State<Arc<Node>>,
    Path(name): Path<String>,
    Json(body): Json<CampaignBody>,
) -> Json<Value> {
    let result = node
        .propose(Command::ElectionCampaign {
            name,
            candidate: body.candidate,
            ttl_ms: body.ttl_ms,
        })
        .await;
    Json(propose_json(result))
}

/// `POST /v1/elections/{name}/renew` — extend the lease (must hold the token).
async fn renew(
    State(node): State<Arc<Node>>,
    Path(name): Path<String>,
    Json(body): Json<HoldBody>,
) -> Json<Value> {
    let result = node
        .propose(Command::ElectionRenew {
            name,
            candidate: body.candidate,
            fencing_token: body.fencing_token,
        })
        .await;
    Json(propose_json(result))
}

/// `POST /v1/elections/{name}/resign` — give up leadership.
async fn resign(
    State(node): State<Arc<Node>>,
    Path(name): Path<String>,
    Json(body): Json<HoldBody>,
) -> Json<Value> {
    let result = node
        .propose(Command::ElectionResign {
            name,
            candidate: body.candidate,
            fencing_token: body.fencing_token,
        })
        .await;
    Json(propose_json(result))
}

/// `GET /v1/elections/{name}` — observe the current leader.
async fn observe(State(node): State<Arc<Node>>, Path(name): Path<String>) -> Json<Value> {
    match node.query(ReadRequest::Election { name: name.clone() }).await {
        Some(ReadResponse::Election(Some(l))) => Json(json!({ "name": name, "held": true, "leadership": l })),
        Some(ReadResponse::Election(None)) => Json(json!({ "name": name, "held": false })),
        _ => Json(json!({ "error": "unavailable" })),
    }
}

/// `GET /v1/elections/{name}/watch` — SSE stream of leadership changes.
async fn watch(State(_node): State<Arc<Node>>, Path(_name): Path<String>) -> Json<Value> {
    // TODO: SSE subscribed to leadership-change events for `name`.
    Json(json!({ "error": "not_implemented", "op": "election.watch" }))
}
