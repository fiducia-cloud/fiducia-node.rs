//! Idempotency keys for retry-safe jobs and webhook/API dedupe.
//!
//! First claim for a key wins a TTL-scoped record with a fencing token. Replays
//! of the same key return the existing active record as a duplicate. A holder can
//! later mark the key complete and attach a small JSON result for duplicate
//! callers to replay.
//!
//! Routes (mounted under `/v1/idempotency`):
//!   * `POST /v1/idempotency/claim`    - `{ key, owner?, ttl_ms?|ttl?, metadata? }`
//!   * `POST /v1/idempotency/complete` - `{ key, owner, fencing_token, result? }`
//!   * `GET  /v1/idempotency?key=K`    - inspect the active record

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

const DEFAULT_TTL_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Deserialize)]
pub struct ClaimBody {
    pub key: String,
    pub owner: Option<String>,
    pub ttl_ms: Option<u64>,
    /// Human-friendly TTL such as `60s`, `15m`, `24h`, or `7d`.
    pub ttl: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CompleteBody {
    pub key: String,
    pub owner: String,
    pub fencing_token: u64,
    pub result: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_record))
        .route("/claim", post(claim))
        .route("/complete", post(complete))
}

/// `GET /v1/idempotency?key=K` - inspect an active idempotency record.
async fn get_record(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KeyParam>,
) -> Response {
    match node
        .query(ReadRequest::Idempotency { key: q.key.clone() })
        .await
    {
        Ok(ReadResponse::Idempotency(Some(record))) => {
            Json(json!({ "key": q.key, "found": true, "record": record })).into_response()
        }
        Ok(ReadResponse::Idempotency(None)) => {
            Json(json!({ "key": q.key, "found": false })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/idempotency/claim` - first claim wins for the TTL window.
async fn claim(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<ClaimBody>) -> Response {
    let ttl_ms = match claim_ttl_ms(&body) {
        Ok(ttl_ms) => ttl_ms,
        Err(reason) => return bad_request(reason),
    };
    let result = node
        .propose(Command::IdempotencyClaim {
            key: body.key,
            owner: body.owner.unwrap_or_else(|| "anonymous".to_string()),
            ttl_ms,
            metadata: body.metadata.unwrap_or_default(),
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/idempotency/complete` - attach an optional result to the claim.
async fn complete(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Json(body): Json<CompleteBody>,
) -> Response {
    let result = node
        .propose(Command::IdempotencyComplete {
            key: body.key,
            owner: body.owner,
            fencing_token: body.fencing_token,
            result: body.result,
        })
        .await;
    propose_response(result, &uri)
}

fn claim_ttl_ms(body: &ClaimBody) -> Result<u64, &'static str> {
    match (body.ttl_ms, body.ttl.as_deref()) {
        (Some(_), Some(_)) => Err("set only one of ttl_ms or ttl"),
        (Some(ttl_ms), None) if ttl_ms > 0 => Ok(ttl_ms),
        (Some(_), None) => Err("ttl_ms must be greater than zero"),
        (None, Some(ttl)) => parse_ttl_ms(ttl),
        (None, None) => Ok(DEFAULT_TTL_MS),
    }
}

fn parse_ttl_ms(value: &str) -> Result<u64, &'static str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("ttl must not be empty");
    }
    let (number, multiplier) = if let Some(number) = trimmed.strip_suffix("ms") {
        (number, 1)
    } else if let Some(number) = trimmed.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = trimmed.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = trimmed.strip_suffix('h') {
        (number, 60 * 60 * 1000)
    } else if let Some(number) = trimmed.strip_suffix('d') {
        (number, 24 * 60 * 60 * 1000)
    } else {
        (trimmed, 1)
    };
    let amount: u64 = number
        .trim()
        .parse()
        .map_err(|_| "ttl must be an integer duration")?;
    amount
        .checked_mul(multiplier)
        .filter(|ttl| *ttl > 0)
        .ok_or("ttl is too large or zero")
}

fn bad_request(reason: &'static str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "bad_request", "reason": reason })),
    )
        .into_response()
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
    fn claim_body_accepts_slash_safe_key_and_duration_ttl() {
        let body: ClaimBody = serde_json::from_value(json!({
            "key": "stripe-webhook/event_123",
            "owner": "worker-a",
            "ttl": "24h",
            "metadata": { "source": "stripe" }
        }))
        .unwrap();

        assert_eq!(body.key, "stripe-webhook/event_123");
        assert_eq!(body.owner.as_deref(), Some("worker-a"));
        assert_eq!(claim_ttl_ms(&body).unwrap(), 24 * 60 * 60 * 1000);
        assert_eq!(
            body.metadata
                .as_ref()
                .and_then(|metadata| metadata.get("source"))
                .map(String::as_str),
            Some("stripe")
        );
    }

    #[test]
    fn ttl_parser_accepts_ms_seconds_minutes_hours_days_and_plain_ms() {
        assert_eq!(parse_ttl_ms("500ms").unwrap(), 500);
        assert_eq!(parse_ttl_ms("60s").unwrap(), 60_000);
        assert_eq!(parse_ttl_ms("15m").unwrap(), 15 * 60_000);
        assert_eq!(parse_ttl_ms("24h").unwrap(), 24 * 60 * 60 * 1000);
        assert_eq!(parse_ttl_ms("7d").unwrap(), 7 * 24 * 60 * 60 * 1000);
        assert_eq!(parse_ttl_ms("1234").unwrap(), 1234);
    }

    #[test]
    fn claim_body_rejects_ambiguous_or_zero_ttl() {
        let both = ClaimBody {
            key: "k".to_string(),
            owner: None,
            ttl_ms: Some(1),
            ttl: Some("1s".to_string()),
            metadata: None,
        };
        let zero = ClaimBody {
            key: "k".to_string(),
            owner: None,
            ttl_ms: Some(0),
            ttl: None,
            metadata: None,
        };

        assert!(claim_ttl_ms(&both).is_err());
        assert!(claim_ttl_ms(&zero).is_err());
    }
}
