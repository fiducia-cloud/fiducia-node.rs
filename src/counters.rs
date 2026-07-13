//! Distributed counters — a replicated signed integer per key, with a monotonic
//! revision for compare-and-set.
//!
//! A counter is the natural primitive for success/failure thresholds, quota
//! tallies, and barrier fan-in counts: `add` moves it by a (possibly negative)
//! delta, `set` writes an absolute value (e.g. reset to 0), and both accept an
//! optional `prev_revision` to make the mutation a compare-and-set. Reads are
//! linearizable off the owning shard's leader.
//!
//! Keys never live in the URL path — in the JSON body for add/set, and `?key=`
//! for inspect — so they may contain slashes (`rollout/v2.4.1/failures`).
//!
//! Routes (mounted under `/v1/counters`):
//!   * `POST /v1/counters/add`  — `{ key, delta, prev_revision? }`
//!   * `POST /v1/counters/set`  — `{ key, value, prev_revision? }`
//!   * `GET  /v1/counters?key=K` — current `{ value, mod_revision }`

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
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct AddBody {
    pub key: String,
    pub delta: i64,
    /// When set, the add applies only if the counter's current revision matches.
    pub prev_revision: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SetBody {
    pub key: String,
    pub value: i64,
    pub prev_revision: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    pub key: String,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(get_counter))
        .route("/add", post(add))
        .route("/set", post(set))
}

/// `GET /v1/counters?key=K` — read the current value and revision (absent = 0).
async fn get_counter(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Query(q): Query<KeyParam>,
) -> Response {
    match node
        .query(ReadRequest::Counter { key: q.key.clone() })
        .await
    {
        Ok(ReadResponse::Counter(entry)) => Json(json!({
            "key": q.key,
            "found": entry.is_some(),
            "counter": entry,
        }))
        .into_response(),
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/counters/add` — atomically add `delta` (creating the counter at 0).
async fn add(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<AddBody>) -> Response {
    if let Err(rejection) = crate::validate::counter(&body.key) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::CounterAdd {
            key: body.key,
            delta: body.delta,
            prev_revision: body.prev_revision,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/counters/set` — write an absolute value (e.g. reset to 0).
async fn set(State(node): State<Arc<Node>>, uri: Uri, Json(body): Json<SetBody>) -> Response {
    if let Err(rejection) = crate::validate::counter(&body.key) {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::CounterSet {
            key: body.key,
            value: body.value,
            prev_revision: body.prev_revision,
        })
        .await;
    propose_response(result, &uri)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds() {
        let _ = router();
    }

    #[test]
    fn add_body_accepts_slash_safe_key_and_negative_delta() {
        let body: AddBody = serde_json::from_value(json!({
            "key": "rollout/v2.4.1/failures",
            "delta": -1
        }))
        .unwrap();
        assert_eq!(body.key, "rollout/v2.4.1/failures");
        assert_eq!(body.delta, -1);
        assert_eq!(body.prev_revision, None);
    }
}
