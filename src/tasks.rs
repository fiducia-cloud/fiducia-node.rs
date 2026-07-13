//! Durable tasks — claimable units of work whose exclusive owner holds a fencing
//! token.
//!
//! A task is created once, then claimed by a worker (which mints a fresh fencing
//! token and an ownership lease). Only the current token holder may report
//! progress, complete, or fail it — a stale worker that lost the claim (paused
//! past its lease, then resumed) is rejected. A retryable failure or an expired
//! lease returns the task to the claimable pool so abandoned work is reassigned.
//!
//! This is the coordination backbone under higher-level work items: the node
//! owns *who may act*; the application database owns the rich task detail.
//!
//! Names never live in the URL path — in the JSON body for writes, and `?name=`
//! for inspect — so they may contain slashes (`repo/acme/api/issue/482`).
//!
//! Routes (mounted under `/v1/tasks`):
//!   * `POST /v1/tasks/create`   — `{ name, task_type, payload?, deadline_ms? }`
//!   * `POST /v1/tasks/claim`    — `{ name, worker, ttl_ms? }`
//!   * `POST /v1/tasks/progress` — `{ name, worker, fencing_token, percent, checkpoint? }`
//!   * `POST /v1/tasks/complete` — `{ name, worker, fencing_token, result? }`
//!   * `POST /v1/tasks/fail`     — `{ name, worker, fencing_token, retryable? }`
//!   * `POST /v1/tasks/cancel`   — `{ name }`
//!   * `GET  /v1/tasks?name=N`   — current status, owner, and fencing token

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub name: String,
    pub task_type: String,
    #[serde(default)]
    pub payload: Value,
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ClaimBody {
    pub name: String,
    pub worker: String,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ProgressBody {
    pub name: String,
    pub worker: String,
    pub fencing_token: u64,
    #[serde(default)]
    pub percent: u32,
    #[serde(default)]
    pub checkpoint: Value,
}

#[derive(Debug, Deserialize)]
pub struct CompleteBody {
    pub name: String,
    pub worker: String,
    pub fencing_token: u64,
    #[serde(default)]
    pub result: Value,
}

#[derive(Debug, Deserialize)]
pub struct FailBody {
    pub name: String,
    pub worker: String,
    pub fencing_token: u64,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Deserialize)]
pub struct CancelBody {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_task))
        .route("/create", post(create))
        .route("/claim", post(claim))
        .route("/progress", post(progress))
        .route("/complete", post(complete))
        .route("/fail", post(fail))
        .route("/cancel", post(cancel))
}

async fn get_task(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Task {
            name: org.scope(&q.name),
        })
        .await
    {
        Ok(ReadResponse::Task(task)) => org.response(json!({
            "name": q.name,
            "found": task.is_some(),
            "task": task,
        })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn create(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<CreateBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskCreate {
            name: org.scope(&body.name),
            task_type: body.task_type,
            payload: body.payload,
            deadline_ms: body.deadline_ms,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn claim(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ClaimBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, Some(&body.worker)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskClaim {
            name: org.scope(&body.name),
            worker: body.worker,
            ttl_ms: body.ttl_ms.unwrap_or(60_000),
        })
        .await;
    org.propose_response(result, &uri)
}

async fn progress(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ProgressBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, Some(&body.worker)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskProgress {
            name: org.scope(&body.name),
            worker: body.worker,
            fencing_token: body.fencing_token,
            percent: body.percent,
            checkpoint: body.checkpoint,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn complete(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<CompleteBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, Some(&body.worker)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskComplete {
            name: org.scope(&body.name),
            worker: body.worker,
            fencing_token: body.fencing_token,
            result: body.result,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn fail(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<FailBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, Some(&body.worker)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskFail {
            name: org.scope(&body.name),
            worker: body.worker,
            fencing_token: body.fencing_token,
            retryable: body.retryable,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn cancel(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<CancelBody>,
) -> Response {
    if let Err(rejection) = crate::validate::task(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::TaskCancel {
            name: org.scope(&body.name),
        })
        .await;
    org.propose_response(result, &uri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn router_builds() {
        let _ = router();
    }

    #[test]
    fn claim_body_defaults_and_parses() {
        let body: ClaimBody = serde_json::from_value(json!({
            "name": "repo/acme/api/issue/482",
            "worker": "agent-a"
        }))
        .unwrap();
        assert_eq!(body.name, "repo/acme/api/issue/482");
        assert_eq!(body.ttl_ms, None);
    }
}
