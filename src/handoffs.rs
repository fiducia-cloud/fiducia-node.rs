//! Atomic ownership handoffs — transfer responsibility for a resource from one
//! holder to another with no moment of dual or zero ownership.
//!
//! The original owner (`from`, presenting its current `from_token`) offers the
//! resource to `to` with a context manifest and an accept deadline. Until the
//! offer is accepted, `from` keeps authority. On accept, `to` receives a
//! strictly higher fencing token, so the guarded resource's fencing rejects the
//! old owner — the transfer is atomic. Reject or timeout leaves ownership with
//! `from`.
//!
//! Names never live in the URL path — in the JSON body for writes, and `?name=`
//! for inspect.
//!
//! Routes (mounted under `/v1/handoffs`):
//!   * `POST /v1/handoffs/offer`  — `{ name, resource, from, to, from_token, context?, ttl_ms? }`
//!   * `POST /v1/handoffs/accept` — `{ name, to }`
//!   * `POST /v1/handoffs/reject` — `{ name, to }`
//!   * `GET  /v1/handoffs?name=N` — current status, counterparties, and tokens

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
pub struct OfferBody {
    pub name: String,
    pub resource: String,
    pub from: String,
    pub to: String,
    pub from_token: u64,
    #[serde(default)]
    pub context: Value,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct DecisionBody {
    pub name: String,
    pub to: String,
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_handoff))
        .route("/offer", post(offer))
        .route("/accept", post(accept))
        .route("/reject", post(reject))
}

async fn get_handoff(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Handoff {
            name: org.scope(&q.name),
        })
        .await
    {
        Ok(ReadResponse::Handoff(handoff)) => org.response(json!({
            "name": q.name,
            "found": handoff.is_some(),
            "handoff": handoff,
        })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn offer(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<OfferBody>,
) -> Response {
    if let Err(rejection) = crate::validate::handoff(&body.name, Some(&body.from), Some(&body.to)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::HandoffOffer {
            name: org.scope(&body.name),
            resource: body.resource,
            from: body.from,
            to: body.to,
            from_token: body.from_token,
            context: body.context,
            ttl_ms: body.ttl_ms.unwrap_or(30_000),
        })
        .await;
    org.propose_response(result, &uri)
}

async fn accept(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<DecisionBody>,
) -> Response {
    if let Err(rejection) = crate::validate::handoff(&body.name, None, Some(&body.to)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::HandoffAccept {
            name: org.scope(&body.name),
            to: body.to,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn reject(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<DecisionBody>,
) -> Response {
    if let Err(rejection) = crate::validate::handoff(&body.name, None, Some(&body.to)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::HandoffReject {
            name: org.scope(&body.name),
            to: body.to,
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
    fn offer_body_defaults_context_and_ttl() {
        let body: OfferBody = serde_json::from_value(json!({
            "name": "ticket-482/handoff",
            "resource": "task:ticket-482",
            "from": "research-agent",
            "to": "legal-agent",
            "from_token": 7
        }))
        .unwrap();
        assert_eq!(body.from_token, 7);
        assert_eq!(body.ttl_ms, None);
        assert!(body.context.is_null());
    }
}
