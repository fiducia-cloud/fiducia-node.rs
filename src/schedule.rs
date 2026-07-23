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

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
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
    org: OrgScope,
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
    if let Err(error) = validate_target(&body.target) {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))).into_response();
    }

    let result = node
        .propose(Command::ScheduleUpsert {
            name: org.scope(&name),
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
    org.propose_response(result, &uri)
}

/// `GET /v1/cron/schedules/{name}` — read a schedule definition.
async fn get_schedule(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    match node
        .query(ReadRequest::Schedule {
            name: org.scope(&name),
        })
        .await
    {
        Ok(ReadResponse::Schedule(Some(schedule))) => {
            org.response(json!({ "found": true, "schedule": schedule }))
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
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<RecordRunBody>,
) -> Response {
    let result = node
        .propose(Command::ScheduleRecordRun {
            name: org.scope(&name),
            fire_id: body.fire_id,
            fired_at_ms: body.fired_at_ms.unwrap_or_else(now_ms),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/cron/schedules/{name}/history` — read durable run history.
async fn history(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    match node
        .query(ReadRequest::ScheduleHistory {
            name: org.scope(&name),
        })
        .await
    {
        Ok(ReadResponse::ScheduleHistory(history)) => {
            Json(json!({ "name": name, "history": history })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// Max bytes for a delivery target URL — it is replicated and kept forever.
const MAX_TARGET_BYTES: usize = 2048;

/// Bound where a fire may be delivered.
///
/// The firing loop POSTs to this URL *from inside the cluster*, so an unchecked
/// caller-supplied target turns the node into an SSRF proxy: cloud metadata
/// (`169.254.169.254`), a peer's admin port, or any in-namespace service. All
/// three target kinds are delivered over HTTP (see
/// [`crate::schedule_runner::target_url`]), so all three are checked the same
/// way: a parseable, credential-free `http`/`https` URL whose host is *outside*
/// the trust boundary — the same scheme/host allow-list Vault addresses use
/// ([`crate::kv::cleartext_internal_host_allowed`]), inverted, because here an
/// internal host is the attack rather than the permitted case.
fn validate_target(target: &ScheduleTarget) -> Result<(), &'static str> {
    let raw = match target {
        ScheduleTarget::Webhook { url } => url,
        ScheduleTarget::Queue { name } => name,
        ScheduleTarget::Grpc { endpoint } => endpoint,
    };
    if raw.len() > MAX_TARGET_BYTES {
        return Err("target_too_long");
    }
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid_target_url")?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("target_scheme_not_allowed");
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("target_must_not_carry_credentials");
    }
    let host = url.host_str().ok_or("target_must_include_a_host")?;
    if crate::kv::cleartext_internal_host_allowed(host) {
        return Err("target_host_not_allowed");
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
