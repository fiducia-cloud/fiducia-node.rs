//! Fan-in barriers — coordinate parallel participants and resolve by policy.
//!
//! A barrier gathers arrivals from named participants and resolves per its
//! policy: `all` (every expected participant), `quorum` (any N), `first_success`
//! (first non-veto arrival), `any_veto` (all arrive but any veto aborts),
//! `best_by_deadline` (whoever arrived by the deadline), or `weighted_quorum`
//! (arrival weights sum to a threshold). Repeat arrivals by the same participant
//! are idempotent, and a bare `arrive` on an unknown barrier auto-creates a
//! single-participant `all` barrier.
//!
//! Names never live in the URL path — in the JSON body for create/arrive, and
//! `?name=` for inspect — so they may contain slashes (`release-942/reviewers`).
//!
//! Routes (mounted under `/v1/barriers`):
//!   * `POST /v1/barriers/create` — `{ name, policy, expected?, deadline_ms? }`
//!   * `POST /v1/barriers/arrive` — `{ name, participant, weight?, veto? }`
//!   * `GET  /v1/barriers?name=N` — current arrivals + resolution status

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::{BarrierPolicy, Command};

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub name: String,
    pub policy: BarrierPolicy,
    /// Participant count for `all` / `any_veto`. Defaults to 1.
    #[serde(default = "one")]
    pub expected: u32,
    pub deadline_ms: Option<u64>,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
pub struct ArriveBody {
    pub name: String,
    pub participant: String,
    #[serde(default = "one_u64")]
    pub weight: u64,
    #[serde(default)]
    pub veto: bool,
}

fn one_u64() -> u64 {
    1
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_barrier))
        .route("/create", post(create))
        .route("/arrive", post(arrive))
}

/// `GET /v1/barriers?name=N` — inspect arrivals and the resolution status.
async fn get_barrier(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Barrier {
            name: q.name.clone(),
        })
        .await
    {
        Ok(ReadResponse::Barrier(barrier)) => Json(json!({
            "name": q.name,
            "found": barrier.is_some(),
            "barrier": barrier,
        }))
        .into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/barriers/create` — define a barrier with a resolution policy.
async fn create(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<CreateBody>) -> Response {
    if let Err(rejection) = crate::validate::barrier(&body.name) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BarrierCreate {
            name: body.name,
            policy: body.policy,
            expected: body.expected,
            deadline_ms: body.deadline_ms,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/barriers/arrive` — record a participant's arrival or veto.
async fn arrive(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<ArriveBody>) -> Response {
    if let Err(rejection) = crate::validate::barrier_arrive(&body.name, &body.participant) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BarrierArrive {
            name: body.name,
            participant: body.participant,
            weight: body.weight,
            veto: body.veto,
        })
        .await;
    propose_response(result, &uri)
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
    fn create_body_parses_quorum_policy() {
        let body: CreateBody = serde_json::from_value(json!({
            "name": "release-942/reviewers",
            "policy": { "kind": "quorum", "required": 3 },
            "expected": 5
        }))
        .unwrap();
        assert_eq!(body.name, "release-942/reviewers");
        assert_eq!(body.expected, 5);
        assert!(matches!(body.policy, BarrierPolicy::Quorum { required: 3 }));
    }

    #[test]
    fn arrive_body_defaults_weight_and_veto() {
        let body: ArriveBody = serde_json::from_value(json!({
            "name": "b",
            "participant": "agent-a"
        }))
        .unwrap();
        assert_eq!(body.weight, 1);
        assert!(!body.veto);
    }
}
