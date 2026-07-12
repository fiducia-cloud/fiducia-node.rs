//! Approval-escrow effects — separate *preparing* a dangerous action from
//! *authorizing* it and *executing* it.
//!
//! Agents should not directly perform irreversible side effects (payments,
//! deploys, deletes, public posts). Instead they `prepare` the effect, one or
//! more principals `approve` it, and only then may it be `committed` — exactly
//! once (a repeat commit replays the recorded result without re-executing). An
//! `abort` kills it permanently. The `idempotency_key` binds the effect to the
//! external operation so the executor stays effectively-once.
//!
//! Names never live in the URL path — in the JSON body for writes, and `?name=`
//! for inspect.
//!
//! Routes (mounted under `/v1/effects`):
//!   * `POST /v1/effects/prepare` — `{ name, effect_type, payload?, risk?, idempotency_key, required_approvals? }`
//!   * `POST /v1/effects/approve` — `{ name, principal }`
//!   * `POST /v1/effects/commit`  — `{ name, result? }`
//!   * `POST /v1/effects/abort`   — `{ name }`
//!   * `GET  /v1/effects?name=N`  — current status, approvals, and result

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

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct PrepareBody {
    pub name: String,
    pub effect_type: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default = "medium")]
    pub risk: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub required_approvals: u32,
}

fn medium() -> String {
    "medium".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ApproveBody {
    pub name: String,
    pub principal: String,
}

#[derive(Debug, Deserialize)]
pub struct CommitBody {
    pub name: String,
    #[serde(default)]
    pub result: Value,
}

#[derive(Debug, Deserialize)]
pub struct NameBody {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_effect))
        .route("/prepare", post(prepare))
        .route("/approve", post(approve))
        .route("/commit", post(commit))
        .route("/abort", post(abort))
}

async fn get_effect(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node.query(ReadRequest::Effect { name: q.name.clone() }).await {
        Ok(ReadResponse::Effect(effect)) => Json(json!({
            "name": q.name,
            "found": effect.is_some(),
            "effect": effect,
        }))
        .into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn prepare(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<PrepareBody>) -> Response {
    if let Err(rejection) = crate::validate::effect(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::EffectPrepare {
            name: body.name,
            effect_type: body.effect_type,
            payload: body.payload,
            risk: body.risk,
            idempotency_key: body.idempotency_key,
            required_approvals: body.required_approvals,
        })
        .await;
    propose_response(result, &uri)
}

async fn approve(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<ApproveBody>) -> Response {
    if let Err(rejection) = crate::validate::effect(&body.name, Some(&body.principal)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::EffectApprove {
            name: body.name,
            principal: body.principal,
        })
        .await;
    propose_response(result, &uri)
}

async fn commit(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<CommitBody>) -> Response {
    if let Err(rejection) = crate::validate::effect(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::EffectCommit {
            name: body.name,
            result: body.result,
        })
        .await;
    propose_response(result, &uri)
}

async fn abort(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<NameBody>) -> Response {
    if let Err(rejection) = crate::validate::effect(&body.name, None) {
        return rejection.into_response();
    }
    let result = node.propose(Command::EffectAbort { name: body.name }).await;
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
    fn prepare_body_defaults_risk_and_approvals() {
        let body: PrepareBody = serde_json::from_value(json!({
            "name": "invoice-882/payment",
            "effect_type": "send_payment",
            "idempotency_key": "invoice-882:payment"
        }))
        .unwrap();
        assert_eq!(body.risk, "medium");
        assert_eq!(body.required_approvals, 0);
    }
}
