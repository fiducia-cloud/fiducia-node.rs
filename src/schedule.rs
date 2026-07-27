//! Tenant-scoped cron control plane.
//!
//! Routes (mounted under `/v1/cron`):
//!   * `GET    /v1/cron/schedules`                 — list caller schedules
//!   * `PUT    /v1/cron/schedules/{name}`          — create/update a schedule
//!   * `GET    /v1/cron/schedules/{name}`          — read one definition
//!   * `DELETE /v1/cron/schedules/{name}`          — delete definition + history
//!   * `POST   /v1/cron/schedules/{name}/pause`    — stop scheduled claims
//!   * `POST   /v1/cron/schedules/{name}/resume`   — resume, normally skipping gaps
//!   * `POST   /v1/cron/schedules/{name}/trigger`  — enqueue a manual run
//!   * `GET    /v1/cron/schedules/{name}/history`  — newest-first run trail
//!
//! Run history is written only by the elected runner. The historical public
//! `POST .../runs` endpoint was removed because it let a customer forge an audit
//! trail that looked like a delivery result.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::{valid_cron_expression, Command, DeliverySemantics, ScheduleTarget};

const MAX_NAME_BYTES: usize = 128;
const MAX_TARGET_BYTES: usize = 2048;
const MAX_FUNCTION_ID_BYTES: usize = 128;
const MAX_RETRIES: u32 = 10;
const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;
const DEFAULT_HISTORY_LIMIT: usize = 50;
const MAX_HISTORY_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
pub struct UpsertBody {
    pub cron: Option<String>,
    pub one_shot_at_ms: Option<u64>,
    pub target: ScheduleTarget,
    pub delivery: Option<DeliverySemantics>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct ResumeQuery {
    /// Deliberately replay occurrences missed while paused. False by default to
    /// prevent an accidental resume from causing an unbounded traffic burst.
    #[serde(default)]
    catch_up: bool,
}

#[derive(Debug, Default, Deserialize)]
struct TriggerQuery {
    /// Optional caller idempotency token represented as epoch milliseconds. A
    /// repeated value is a no-op; omitting it mints the current timestamp.
    fire_id_ms: Option<u64>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/schedules", get(list_schedules))
        .route(
            "/schedules/:name",
            get(get_schedule).put(upsert).delete(delete_schedule),
        )
        .route("/schedules/:name/pause", axum::routing::post(pause))
        .route("/schedules/:name/resume", axum::routing::post(resume))
        .route("/schedules/:name/trigger", axum::routing::post(trigger))
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
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
    if body.cron.is_some() == body.one_shot_at_ms.is_some() {
        return bad_request("exactly_one_schedule_mode_required");
    }
    if let Some(cron) = body.cron.as_deref() {
        if !valid_cron_expression(cron) {
            return bad_request("invalid_cron_expression");
        }
    }
    if let Err(error) = validate_target(&body.target) {
        return bad_request(error);
    }
    let max_retries = body.max_retries.unwrap_or(3);
    if let Err(error) = validate_max_retries(max_retries) {
        return bad_request(error);
    }

    let result = node
        .propose(Command::ScheduleUpsert {
            name: org.scope(&name),
            cron: body.cron,
            one_shot_at_ms: body.one_shot_at_ms,
            target: body.target,
            delivery: body.delivery.unwrap_or(DeliverySemantics::AtLeastOnce),
            max_retries,
            // Stamp the clock at proposal time so every replica computes the
            // initial cursor deterministically.
            now_ms: now_ms(),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/cron/schedules` — deterministic, cursor-paginated caller inventory.
async fn list_schedules(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    Query(query): Query<ListQuery>,
) -> Response {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let mut schedules: Vec<_> = node
        .list_schedules()
        .await
        .into_iter()
        .filter_map(|mut schedule| {
            let name = org.unscope(&schedule.name)?.to_string();
            if query.cursor.as_ref().is_some_and(|cursor| name <= *cursor) {
                return None;
            }
            schedule.name = name;
            Some(schedule)
        })
        .collect();
    schedules.sort_by(|left, right| left.name.cmp(&right.name));
    let has_more = schedules.len() > limit;
    schedules.truncate(limit);
    let next_cursor = has_more
        .then(|| schedules.last().map(|s| s.name.clone()))
        .flatten();
    Json(json!({
        "count": schedules.len(),
        "schedules": schedules,
        "next_cursor": next_cursor,
    }))
    .into_response()
}

/// `GET /v1/cron/schedules/{name}` — read a schedule definition.
async fn get_schedule(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
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
        _ => unavailable(),
    }
}

/// `DELETE /v1/cron/schedules/{name}` — idempotently remove definition/history.
async fn delete_schedule(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
    let result = node
        .propose(Command::ScheduleDelete {
            name: org.scope(&name),
        })
        .await;
    org.propose_response(result, &uri)
}

async fn pause(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    set_enabled(node, org, uri, name, false, false).await
}

async fn resume(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
    Query(query): Query<ResumeQuery>,
) -> Response {
    set_enabled(node, org, uri, name, true, query.catch_up).await
}

async fn set_enabled(
    node: Arc<Node>,
    org: OrgScope,
    uri: Uri,
    name: String,
    enabled: bool,
    catch_up: bool,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
    let result = node
        .propose(Command::ScheduleSetEnabled {
            name: org.scope(&name),
            enabled,
            now_ms: now_ms(),
            catch_up,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/cron/schedules/{name}/trigger` — enqueue an idempotent manual run.
async fn trigger(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
    Query(query): Query<TriggerQuery>,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
    let requested_at_ms = now_ms();
    let fire_id_ms = query.fire_id_ms.unwrap_or(requested_at_ms);
    let result = node
        .propose(Command::ScheduleTrigger {
            name: org.scope(&name),
            fire_id_ms,
            requested_at_ms,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/cron/schedules/{name}/history` — newest-first bounded run trail.
async fn history(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(name): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return bad_request(error);
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .clamp(1, MAX_HISTORY_LIMIT);
    match node
        .query(ReadRequest::ScheduleHistory {
            name: org.scope(&name),
        })
        .await
    {
        Ok(ReadResponse::ScheduleHistory(history)) => {
            let runs: Vec<_> = history.into_iter().rev().take(limit).collect();
            Json(json!({
                "name": name,
                "count": runs.len(),
                "order": "newest_first",
                "history": runs,
            }))
            .into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => unavailable(),
    }
}

fn validate_max_retries(max_retries: u32) -> Result<(), &'static str> {
    if max_retries > MAX_RETRIES {
        Err("max_retries_exceeds_limit")
    } else {
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("schedule_name_required");
    }
    if name.len() > MAX_NAME_BYTES {
        return Err("schedule_name_too_long");
    }
    if name.chars().any(|c| c.is_control()) {
        return Err("schedule_name_contains_control_character");
    }
    Ok(())
}

/// Bound where a fire may be delivered. External HTTP targets are protected
/// against SSRF; function identifiers are resolved only against the
/// operator-configured lambda-service base URL in the runner.
fn validate_target(target: &ScheduleTarget) -> Result<(), &'static str> {
    let raw = match target {
        ScheduleTarget::Webhook { url } => url,
        ScheduleTarget::Queue { name } => name,
        ScheduleTarget::Grpc { endpoint } => endpoint,
        ScheduleTarget::Function { function_id } => {
            if function_id.is_empty() {
                return Err("function_id_required");
            }
            if function_id.len() > MAX_FUNCTION_ID_BYTES {
                return Err("function_id_too_long");
            }
            if matches!(function_id.as_str(), "." | "..")
                || !function_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err("invalid_function_id");
            }
            return Ok(());
        }
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

fn bad_request(error: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))).into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": "unavailable" })),
    )
        .into_response()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names_and_retry_bounds() {
        assert!(validate_name("billing-hourly").is_ok());
        assert_eq!(validate_name(""), Err("schedule_name_required"));
        assert_eq!(
            validate_name("bad\nname"),
            Err("schedule_name_contains_control_character")
        );
        assert_eq!(validate_max_retries(10), Ok(()));
        assert_eq!(validate_max_retries(11), Err("max_retries_exceeds_limit"));
    }

    #[test]
    fn function_targets_accept_only_opaque_safe_identifiers() {
        assert!(validate_target(&ScheduleTarget::Function {
            function_id: "fn_01H-hourly.billing".to_string(),
        })
        .is_ok());
        for invalid in ["../../admin", ".", ".."] {
            assert_eq!(
                validate_target(&ScheduleTarget::Function {
                    function_id: invalid.to_string(),
                }),
                Err("invalid_function_id")
            );
        }
    }

    #[test]
    fn external_targets_still_reject_internal_ssrf_and_credentials() {
        assert_eq!(
            validate_target(&ScheduleTarget::Webhook {
                url: "http://169.254.169.254/latest/meta-data".to_string(),
            }),
            Err("target_host_not_allowed")
        );
        assert_eq!(
            validate_target(&ScheduleTarget::Webhook {
                url: "https://user:secret@example.com/hook".to_string(),
            }),
            Err("target_must_not_carry_credentials")
        );
    }
}
