//! Rate limiting (skeleton handlers).
//!
//! Distributed token-bucket / sliding-window limiters with one source of truth:
//! a consume is a Raft-committed atomic check-and-decrement, so the quota is
//! enforced consistently no matter which replica answers. Per-key / per-tenant by
//! using the key (e.g. `tenant:42/api`).
//!
//! Routes:
//!   * `POST /v1/ratelimit/{key}/consume` — `{ cost? }` → allowed + remaining
//!   * `GET  /v1/ratelimit/{key}`         — peek the current budget
//!
//! NOTE: a real consume returns allowed/denied + remaining; that needs the apply
//! path to return a typed result (today `propose` returns only a commit outcome).
//! Tracked as a follow-up; the command + commit path are in place.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::Response,
    routing::post,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, Node};
use crate::state::Command;

fn default_cost() -> u64 {
    1
}

#[derive(Debug, Deserialize)]
pub struct ConsumeBody {
    #[serde(default = "default_cost")]
    pub cost: u64,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:key", axum::routing::get(peek))
        .route("/:key/consume", post(consume))
}

/// `POST /v1/ratelimit/{key}/consume` — atomic check-and-decrement.
async fn consume(State(node): State<Arc<Node>>, Path(key): Path<String>, Json(body): Json<ConsumeBody>) -> Response {
    propose_response(node.propose(Command::RateLimitConsume { key, cost: body.cost }).await)
}

/// `GET /v1/ratelimit/{key}` — current budget (peek).
async fn peek(State(_node): State<Arc<Node>>, Path(_key): Path<String>) -> Json<Value> {
    // TODO: query the shard for the current bucket/window state.
    Json(json!({ "error": "not_implemented", "op": "ratelimit.peek" }))
}
