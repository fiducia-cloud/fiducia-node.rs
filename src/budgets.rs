//! Hierarchical budgets — reserve, commit, and release spend across three axes
//! (US dollars in micros, tokens, and tool calls).
//!
//! A budget has a per-axis ceiling; a `None` axis is unlimited. A worker
//! `reserve`s an amount before spending — the reservation is rejected if it would
//! exceed any limited axis, so ten agents cannot each independently believe they
//! can spend the same remaining dollar. On `commit`, the actual spend (≤ the
//! reservation) is recorded and the difference is freed; `release` returns a
//! still-held reservation's full headroom. Nest budgets by naming them by scope
//! (`org/acme`, `org/acme/workflow/42`, …).
//!
//! Names never live in the URL path — in the JSON body for writes, `?name=` to
//! inspect.
//!
//! Routes (mounted under `/v1/budgets`):
//!   * `POST /v1/budgets/set`     — `{ name, limit }`
//!   * `POST /v1/budgets/reserve` — `{ name, reservation_id, holder, amount }`
//!   * `POST /v1/budgets/commit`  — `{ name, reservation_id, actual }`
//!   * `POST /v1/budgets/release` — `{ name, reservation_id }`
//!   * `GET  /v1/budgets?name=N`  — limit, reserved, spent, available, reservations

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
use crate::state::{BudgetAmount, Command};

#[derive(Debug, Deserialize)]
pub struct SetBody {
    pub name: String,
    pub limit: BudgetAmount,
}

#[derive(Debug, Deserialize)]
pub struct ReserveBody {
    pub name: String,
    pub reservation_id: String,
    pub holder: String,
    pub amount: BudgetAmount,
}

#[derive(Debug, Deserialize)]
pub struct CommitBody {
    pub name: String,
    pub reservation_id: String,
    pub actual: BudgetAmount,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub name: String,
    pub reservation_id: String,
}

#[derive(Debug, Deserialize)]
pub struct NameParam {
    pub name: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_budget))
        .route("/set", post(set))
        .route("/reserve", post(reserve))
        .route("/commit", post(commit))
        .route("/release", post(release))
}

async fn get_budget(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<NameParam>,
) -> Response {
    match node
        .query(ReadRequest::Budget {
            name: q.name.clone(),
        })
        .await
    {
        Ok(ReadResponse::Budget(budget)) => Json(json!({
            "name": q.name,
            "found": budget.is_some(),
            "budget": budget,
        }))
        .into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

async fn set(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<SetBody>) -> Response {
    if let Err(rejection) = crate::validate::budget(&body.name, None, None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BudgetSet {
            name: body.name,
            limit: body.limit,
        })
        .await;
    propose_response(result, &uri)
}

async fn reserve(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<ReserveBody>,
) -> Response {
    if let Err(rejection) =
        crate::validate::budget(&body.name, Some(&body.reservation_id), Some(&body.holder))
    {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BudgetReserve {
            name: body.name,
            reservation_id: body.reservation_id,
            holder: body.holder,
            amount: body.amount,
        })
        .await;
    propose_response(result, &uri)
}

async fn commit(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<CommitBody>) -> Response {
    if let Err(rejection) = crate::validate::budget(&body.name, Some(&body.reservation_id), None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BudgetCommit {
            name: body.name,
            reservation_id: body.reservation_id,
            actual: body.actual,
        })
        .await;
    propose_response(result, &uri)
}

async fn release(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<ReleaseBody>,
) -> Response {
    if let Err(rejection) = crate::validate::budget(&body.name, Some(&body.reservation_id), None) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::BudgetRelease {
            name: body.name,
            reservation_id: body.reservation_id,
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
    fn reserve_body_parses_partial_amount() {
        let body: ReserveBody = serde_json::from_value(json!({
            "name": "org/acme/workflow/42",
            "reservation_id": "res-1",
            "holder": "research-agent",
            "amount": { "usd_micros": 500000, "tokens": 100000 }
        }))
        .unwrap();
        assert_eq!(body.amount.usd_micros, Some(500000));
        assert_eq!(body.amount.tool_calls, None);
    }
}
