//! Cron & scheduling (skeleton handlers).
//!
//! A replicated scheduler that survives leader failure: schedules are committed to
//! the owning shard's Raft group, the shard **leader** fires each job exactly once
//! (no duplicate fires across replicas/failover), and runs are recorded durably.
//!
//! Routes:
//!   * `PUT    /v1/cron/{name}`  — create/replace `{ schedule, target }`
//!   * `DELETE /v1/cron/{name}`  — remove a schedule
//!   * `GET    /v1/cron/{name}`  — schedule + recent run history
//!   * `GET    /v1/cron`         — list schedules

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::Response,
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, Node};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    /// Standard cron expression, or a one-shot timestamp.
    pub schedule: String,
    /// Where to deliver the fire (webhook URL, queue subject, or gRPC target).
    pub target: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(list))
        .route("/:name", put(create).get(get_one).delete(delete_one))
}

/// `PUT /v1/cron/{name}` — create or replace a schedule.
async fn create(State(node): State<Arc<Node>>, Path(name): Path<String>, Json(body): Json<CreateBody>) -> Response {
    propose_response(node.propose(Command::CronCreate { name, schedule: body.schedule, target: body.target }).await)
}

/// `DELETE /v1/cron/{name}` — remove a schedule.
async fn delete_one(State(node): State<Arc<Node>>, Path(name): Path<String>) -> Response {
    propose_response(node.propose(Command::CronDelete { name }).await)
}

/// `GET /v1/cron/{name}` — schedule + recent run history.
async fn get_one(State(_node): State<Arc<Node>>, Path(_name): Path<String>) -> Json<Value> {
    Json(json!({ "error": "not_implemented", "op": "cron.get" }))
}

/// `GET /v1/cron` — list schedules (spans shards).
async fn list(State(_node): State<Arc<Node>>) -> Json<Value> {
    Json(json!({ "error": "not_implemented", "op": "cron.list" }))
}
