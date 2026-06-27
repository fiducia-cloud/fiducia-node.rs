//! Distributed rate limiting.
//!
//! Routes (mounted under `/v1/rate-limit`):
//!   * `POST /v1/rate-limit/{tenant}/{key}/check` — atomic check-and-decrement
//!   * `GET  /v1/rate-limit/{tenant}/{key}`       — inspect last known quota state

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::{Command, RateLimitAlgorithm};

#[derive(Debug, Deserialize)]
pub struct CheckBody {
    pub algorithm: RateLimitAlgorithm,
    pub limit: u32,
    pub window_ms: u64,
    pub refill_per_second: Option<f64>,
    pub cost: Option<u32>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:tenant/:key", get(get_limit))
        .route("/:tenant/:key/check", post(check))
}

/// `POST /v1/rate-limit/{tenant}/{key}/check` — atomic quota decision.
async fn check(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path((tenant, key)): Path<(String, String)>,
    Json(body): Json<CheckBody>,
) -> Response {
    let result = node
        .propose(Command::RateLimitCheck {
            key,
            tenant,
            algorithm: body.algorithm,
            limit: body.limit,
            window_ms: body.window_ms,
            refill_per_second: body.refill_per_second,
            cost: body.cost.unwrap_or(1),
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/rate-limit/{tenant}/{key}` — inspect current limiter snapshot.
async fn get_limit(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path((tenant, key)): Path<(String, String)>,
) -> Response {
    match node
        .query(ReadRequest::RateLimit {
            tenant: tenant.clone(),
            key: key.clone(),
        })
        .await
    {
        Ok(ReadResponse::RateLimit(Some(snapshot))) => {
            Json(json!({ "found": true, "limit": snapshot })).into_response()
        }
        Ok(ReadResponse::RateLimit(None)) => {
            Json(json!({ "found": false, "tenant": tenant, "key": key })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}
