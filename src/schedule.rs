//! Cron and one-shot scheduling.
//!
//! Routes (mounted under `/v1/cron`):
//!   * `PUT  /v1/cron/schedules/{name}`         — upsert cron or one-shot job
//!   * `GET  /v1/cron/schedules/{name}`         — read job definition
//!   * `POST /v1/cron/schedules/{name}/runs`    — record a fired delivery
//!   * `GET  /v1/cron/schedules/{name}/history` — read durable run history

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::{valid_cron_expression, Command, DeliverySemantics, ScheduleTarget};

#[derive(Debug, Deserialize)]
pub struct UpsertBody {
    pub cron: Option<String>,
    pub one_shot_at_ms: Option<u64>,
    pub target: ScheduleTarget,
    pub delivery: Option<DeliverySemantics>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct RecordRunBody {
    pub fire_id: String,
    pub fired_at_ms: Option<u64>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/schedules/:name", get(get_schedule).put(upsert))
        .route("/schedules/:name/runs", post(record_run))
        .route("/schedules/:name/history", get(history))
}

/// `PUT /v1/cron/schedules/{name}` — create or update a schedule.
async fn upsert(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Response {
    if body.cron.is_some() == body.one_shot_at_ms.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "exactly_one_schedule_mode_required" })),
        )
            .into_response();
    }
    if let Some(cron) = body.cron.as_deref() {
        if !valid_cron_expression(cron) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_cron_expression" })),
            )
                .into_response();
        }
    }

    let result = node
        .propose(Command::ScheduleUpsert {
            name,
            cron: body.cron,
            one_shot_at_ms: body.one_shot_at_ms,
            target: body.target,
            delivery: body.delivery.unwrap_or(DeliverySemantics::AtLeastOnce),
            max_retries: body.max_retries.unwrap_or(3),
            // Stamp the clock here (the proposer), so the state machine computes the
            // initial next-fire deterministically on every replica.
            now_ms: now_ms(),
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/cron/schedules/{name}` — read a schedule definition.
async fn get_schedule(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    match node
        .query(ReadRequest::Schedule { name: name.clone() })
        .await
    {
        Ok(ReadResponse::Schedule(Some(schedule))) => {
            Json(json!({ "found": true, "schedule": schedule })).into_response()
        }
        Ok(ReadResponse::Schedule(None)) => {
            Json(json!({ "found": false, "name": name })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `POST /v1/cron/schedules/{name}/runs` — record a fired delivery.
async fn record_run(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<RecordRunBody>,
) -> Response {
    let result = node
        .propose(Command::ScheduleRecordRun {
            name,
            fire_id: body.fire_id,
            fired_at_ms: body.fired_at_ms.unwrap_or_else(now_ms),
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/cron/schedules/{name}/history` — read durable run history.
async fn history(State(node): State<Arc<Node>>, uri: Uri, Path(name): Path<String>) -> Response {
    match node
        .query(ReadRequest::ScheduleHistory { name: name.clone() })
        .await
    {
        Ok(ReadResponse::ScheduleHistory(history)) => {
            Json(json!({ "name": name, "history": history })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
