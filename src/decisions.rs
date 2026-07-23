//! Decisions — typed, weighted, evidence-bearing votes with deterministic
//! resolution.
//!
//! A decision is richer than a raw vote: it declares typed `options` and a
//! resolution `policy` (plurality once N votes, weight threshold, or unanimity),
//! and each vote carries a chosen option (or abstain), a confidence, a weight, an
//! optional veto, and evidence references. Re-voting replaces a voter's prior
//! vote. A veto aborts; ties break to the lexicographically smallest option; a
//! passed deadline forces a plurality outcome.
//!
//! Names never live in the URL path — in the JSON body for writes, `?name=` to
//! inspect.
//!
//! Routes (mounted under `/v1/decisions`):
//!   * `POST /v1/decisions/propose` — `{ name, question, options, policy, deadline_ms? }`
//!   * `POST /v1/decisions/vote`    — `{ name, voter, option?, confidence?, weight?, veto?, evidence? }`
//!   * `GET  /v1/decisions?name=N`  — options, tallies, votes, and resolution

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

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::{Command, DecisionPolicy};

#[derive(Debug, Deserialize)]
pub struct ProposeBody {
    pub name: String,
    pub question: String,
    pub options: Vec<String>,
    pub policy: DecisionPolicy,
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct VoteBody {
    pub name: String,
    pub voter: String,
    #[serde(default)]
    pub option: Option<String>,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default = "one_u64")]
    pub weight: u64,
    #[serde(default)]
    pub veto: bool,
    #[serde(default)]
    pub evidence: Vec<String>,
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
        .route("/", get(get_decision))
        .route("/propose", post(propose))
        .route("/vote", post(vote))
}

async fn get_decision(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Decision {
            name: org.scope(&q.name),
        })
        .await
    {
        Ok(ReadResponse::Decision(decision)) => org.response(json!({
            "name": q.name,
            "found": decision.is_some(),
            "decision": decision,
        })),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn propose(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<ProposeBody>,
) -> Response {
    if let Err(rejection) = crate::validate::decision(&body.name, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::DecisionPropose {
            name: org.scope(&body.name),
            question: body.question,
            options: body.options,
            policy: body.policy,
            deadline_ms: body.deadline_ms,
        })
        .await;
    org.propose_response(result, &uri)
}

async fn vote(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Json(body): Json<VoteBody>,
) -> Response {
    if let Err(rejection) = crate::validate::decision_vote(&body.name, &body.voter, body.weight) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::DecisionVote {
            name: org.scope(&body.name),
            voter: body.voter,
            option: body.option,
            confidence: body.confidence,
            weight: body.weight,
            veto: body.veto,
            evidence: body.evidence,
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
    fn propose_body_parses_plurality_policy() {
        let body: ProposeBody = serde_json::from_value(json!({
            "name": "deploy/safe",
            "question": "Is this deploy safe?",
            "options": ["approve", "reject", "human_review"],
            "policy": { "kind": "plurality", "min_votes": 3 }
        }))
        .unwrap();
        assert_eq!(body.options.len(), 3);
        assert!(matches!(
            body.policy,
            DecisionPolicy::Plurality { min_votes: 3 }
        ));
    }

    #[test]
    fn vote_body_defaults_weight_and_abstain() {
        let body: VoteBody = serde_json::from_value(json!({
            "name": "d",
            "voter": "agent-a"
        }))
        .unwrap();
        assert_eq!(body.weight, 1);
        assert_eq!(body.option, None);
        assert!(!body.veto);
    }
}
