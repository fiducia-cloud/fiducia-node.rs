//! Claims — a contestable, versioned ledger of assertions.
//!
//! A claim is a `subject`/`predicate`/`value` an agent believes, with a
//! confidence and evidence. Other agents can `support` or `contest` it; an
//! authorized process `resolve`s it (accepted/rejected), or a newer claim
//! `supersede`s it. This is a blackboard of immutable, versioned beliefs — the
//! coordination-grade counterpart to dumping every agent's prose into a shared
//! vector store. The rule: semantic similarity may *surface* a claim, but only an
//! authorized resolution moves it to authoritative `accepted` state.
//!
//! Names never live in the URL path — in the JSON body for writes, `?name=` to
//! inspect.
//!
//! Routes (mounted under `/v1/claims`):
//!   * `POST /v1/claims/assert`    — `{ name, subject, predicate, value, confidence?, author, evidence?, valid_until_ms? }`
//!   * `POST /v1/claims/support`   — `{ name, agent }`
//!   * `POST /v1/claims/contest`   — `{ name, agent, reason }`
//!   * `POST /v1/claims/resolve`   — `{ name, accepted }`
//!   * `POST /v1/claims/supersede` — `{ name, superseded_by }`
//!   * `GET  /v1/claims?name=N`    — subject/predicate/value, status, support, contests

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
pub struct AssertBody {
    pub name: String,
    pub subject: String,
    pub predicate: String,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub confidence: f32,
    pub author: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub valid_until_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SupportBody {
    pub name: String,
    pub agent: String,
}

#[derive(Debug, Deserialize)]
pub struct ContestBody {
    pub name: String,
    pub agent: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct ResolveBody {
    pub name: String,
    pub accepted: bool,
}

#[derive(Debug, Deserialize)]
pub struct SupersedeBody {
    pub name: String,
    pub superseded_by: String,
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_claim))
        .route("/assert", post(assert))
        .route("/support", post(support))
        .route("/contest", post(contest))
        .route("/resolve", post(resolve))
        .route("/supersede", post(supersede))
}

async fn get_claim(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Claim {
            name: org.scope(&q.name),
        })
        .await
    {
        Ok(ReadResponse::Claim(claim)) => org.response(json!({
            "name": q.name,
            "found": claim.is_some(),
            "claim": claim,
        })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn assert(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<AssertBody>,
) -> Response {
    if let Err(rejection) = crate::validate::claim(&body.name, Some(&body.author)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ClaimAssert {
            name: org.scope(&body.name),
            subject: body.subject,
            predicate: body.predicate,
            value: body.value,
            confidence: body.confidence,
            author: body.author,
            evidence: body.evidence,
            valid_until_ms: body.valid_until_ms,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn support(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<SupportBody>,
) -> Response {
    if let Err(rejection) = crate::validate::claim(&body.name, Some(&body.agent)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ClaimSupport {
            name: org.scope(&body.name),
            agent: body.agent,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn contest(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ContestBody>,
) -> Response {
    if let Err(rejection) = crate::validate::claim(&body.name, Some(&body.agent)) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ClaimContest {
            name: org.scope(&body.name),
            agent: body.agent,
            reason: body.reason,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn resolve(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ResolveBody>,
) -> Response {
    if let Err(rejection) = crate::validate::claim(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ClaimResolve {
            name: org.scope(&body.name),
            accepted: body.accepted,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn supersede(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<SupersedeBody>,
) -> Response {
    if let Err(rejection) = crate::validate::claim(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ClaimSupersede {
            name: org.scope(&body.name),
            superseded_by: org.scope(&body.superseded_by),
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
    fn assert_body_defaults_confidence_and_evidence() {
        let body: AssertBody = serde_json::from_value(json!({
            "name": "customer/219/refund_eligible",
            "subject": "customer:219",
            "predicate": "eligible_for_refund",
            "value": true,
            "author": "billing-agent"
        }))
        .unwrap();
        assert_eq!(body.subject, "customer:219");
        assert_eq!(body.confidence, 0.0);
        assert!(body.evidence.is_empty());
    }
}
