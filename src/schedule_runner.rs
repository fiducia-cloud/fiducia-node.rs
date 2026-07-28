//! Leader-elected cron firing and delivery.
//!
//! Every due fire is claimed through Raft before I/O. Delivery is bounded by a
//! process-wide semaphore, uses an idempotency key, retries only transient
//! failures, propagates W3C trace context, and commits a sanitized diagnostic
//! result to the run trail. JSON tracing events flow to Loki through the shared
//! telemetry pipeline; low-cardinality OpenTelemetry instruments flow to the
//! collector/Prometheus path.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry::propagation::Injector;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{global, KeyValue};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use reqwest::{StatusCode, Url};
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::consensus::{Node, ReadRequest, ReadResponse};
use crate::cron::CronSchedule;
use crate::state::{Command, RunStatus, RunTrigger, Schedule, ScheduleTarget};

const TICK: Duration = Duration::from_secs(5);
const MAX_CLAIMS_PER_TICK: usize = 16;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_IN_FLIGHT: usize = 64;
const MAX_MAX_IN_FLIGHT: usize = 1_024;
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Tracks fires currently being delivered by this node. The key never contains a
/// target URL or secret.
type InFlight = Arc<Mutex<HashSet<String>>>;

#[derive(Clone)]
struct RunnerConfig {
    lambda_base_url: Option<Url>,
    lambda_server_auth: Option<HeaderValue>,
    max_in_flight: usize,
}

impl RunnerConfig {
    fn from_env() -> Self {
        let max_in_flight = std::env::var("FIDUCIA_CRON_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT)
            .clamp(1, MAX_MAX_IN_FLIGHT);
        let lambda_base_url = std::env::var("FIDUCIA_LAMBDA_SERVICE_URL")
            .ok()
            .and_then(|raw| normalize_lambda_base(&raw));
        let lambda_server_auth = std::env::var("FIDUCIA_LAMBDA_SERVER_AUTH_SECRET")
            .ok()
            .and_then(|value| HeaderValue::from_str(&value).ok());
        Self {
            lambda_base_url,
            lambda_server_auth,
            max_in_flight,
        }
    }
}

struct CronMetrics {
    claims: Counter<u64>,
    deliveries: Counter<u64>,
    attempts: Counter<u64>,
    retries: Counter<u64>,
    deferred: Counter<u64>,
    duration_ms: Histogram<f64>,
    in_flight: UpDownCounter<i64>,
}

impl CronMetrics {
    fn new() -> Self {
        let meter = global::meter("fiducia-node.cron");
        Self {
            claims: meter
                .u64_counter("fiducia.cron.claims")
                .with_description("Raft-committed cron fire claims")
                .build(),
            deliveries: meter
                .u64_counter("fiducia.cron.deliveries")
                .with_description("Completed cron delivery outcomes")
                .build(),
            attempts: meter
                .u64_counter("fiducia.cron.delivery.attempts")
                .with_description("Outbound cron delivery attempts")
                .build(),
            retries: meter
                .u64_counter("fiducia.cron.delivery.retries")
                .with_description("Transient cron delivery retries")
                .build(),
            deferred: meter
                .u64_counter("fiducia.cron.delivery.deferred")
                .with_description("Pending cron fires deferred by concurrency limits")
                .build(),
            duration_ms: meter
                .f64_histogram("fiducia.cron.delivery.duration")
                .with_unit("ms")
                .with_description("End-to-end cron delivery latency")
                .build(),
            in_flight: meter
                .i64_up_down_counter("fiducia.cron.delivery.in_flight")
                .with_description("Cron deliveries currently executing")
                .build(),
        }
    }
}

fn cron_metrics() -> &'static CronMetrics {
    static METRICS: OnceLock<CronMetrics> = OnceLock::new();
    METRICS.get_or_init(CronMetrics::new)
}

#[derive(Debug)]
struct DeliveryOutcome {
    delivered: bool,
    attempts: u32,
    error: Option<String>,
    error_class: Option<String>,
    http_status: Option<u16>,
    duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseDisposition {
    Success,
    Retry,
    Permanent,
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}

/// Spawn the firing loop. Call once at process startup.
pub fn spawn(node: Arc<Node>) {
    let config = RunnerConfig::from_env();
    tracing::info!(
        cron.max_in_flight = config.max_in_flight,
        cron.lambda_configured = config.lambda_base_url.is_some(),
        "cron runner configured"
    );
    tokio::spawn(run(node, config));
}

async fn run(node: Arc<Node>, config: RunnerConfig) {
    let http = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build the schedule-delivery HTTP client");
    let in_flight: InFlight = Arc::new(Mutex::new(HashSet::new()));
    let gate = Arc::new(Semaphore::new(config.max_in_flight));
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        sweep(&node, &http, &in_flight, &gate, &config).await;
    }
}

async fn sweep(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
    gate: &Arc<Semaphore>,
    config: &RunnerConfig,
) {
    let span = tracing::debug_span!("fiducia.cron.sweep");
    async {
        let leading: HashSet<u32> = node.status().await.leading_shards.into_iter().collect();
        if leading.is_empty() {
            return;
        }
        let now = now_ms();
        for schedule in node.list_schedules().await {
            if !leading.contains(&node.shard_for(&schedule.name)) {
                continue;
            }
            // Pausing prevents new scheduled claims, but an explicit manual run
            // and any already-claimed interrupted delivery must still execute.
            redeliver_pending(node, http, in_flight, gate, config, &schedule).await;
            if schedule.enabled {
                claim_due(node, http, in_flight, gate, config, &schedule, now).await;
            }
        }
    }
    .instrument(span)
    .await;
}

async fn claim_due(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
    gate: &Arc<Semaphore>,
    config: &RunnerConfig,
    schedule: &Schedule,
    now: u64,
) {
    let cron = schedule
        .cron
        .as_deref()
        .and_then(|expr| CronSchedule::parse(expr).ok());
    let mut next = schedule.next_fire_ms;
    let mut claims = 0;
    while let Some(fire) = next {
        if fire > now || claims >= MAX_CLAIMS_PER_TICK {
            break;
        }
        let claimed = match node
            .propose(Command::ScheduleClaimFire {
                name: schedule.name.clone(),
                fire_id_ms: fire,
            })
            .await
        {
            Ok(outcome) => outcome.output.get("claimed").and_then(|v| v.as_bool()) == Some(true),
            Err(error) => {
                tracing::warn!(
                    cron.schedule = %unscoped_name(&schedule.name),
                    cron.fire_id = fire,
                    ?error,
                    "cron fire claim failed; this node will not deliver it"
                );
                false
            }
        };
        if !claimed {
            break;
        }
        cron_metrics().claims.add(
            1,
            &[
                KeyValue::new("trigger", "scheduled"),
                KeyValue::new("target.kind", target_kind(&schedule.target)),
            ],
        );
        claims += 1;
        if !deliver_in_background(
            DeliveryContext {
                node,
                http,
                in_flight,
                gate,
                config,
            },
            schedule.clone(),
            fire,
            RunTrigger::Scheduled,
        ) {
            // The committed Pending run is intentionally left for the next sweep.
            break;
        }
        next = if schedule.one_shot_at_ms.is_some() {
            None
        } else {
            cron.as_ref().and_then(|parsed| parsed.next_after(fire))
        };
    }
}

async fn redeliver_pending(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
    gate: &Arc<Semaphore>,
    config: &RunnerConfig,
    schedule: &Schedule,
) {
    let history = match node
        .query(ReadRequest::ScheduleHistory {
            name: schedule.name.clone(),
        })
        .await
    {
        Ok(ReadResponse::ScheduleHistory(history)) => history,
        Ok(other) => {
            tracing::warn!(cron.schedule = %unscoped_name(&schedule.name), response = ?other, "unexpected cron-history response");
            return;
        }
        Err(error) => {
            tracing::debug!(cron.schedule = %unscoped_name(&schedule.name), ?error, "cron history unavailable on this node");
            return;
        }
    };
    for run in history
        .iter()
        .filter(|run| run.status == RunStatus::Pending)
    {
        let Ok(fire) = run.fire_id.parse::<u64>() else {
            tracing::warn!(cron.schedule = %unscoped_name(&schedule.name), cron.fire_id = %run.fire_id, "invalid pending cron fire id");
            continue;
        };
        if !deliver_in_background(
            DeliveryContext {
                node,
                http,
                in_flight,
                gate,
                config,
            },
            schedule.clone(),
            fire,
            run.trigger,
        ) {
            break;
        }
    }
}

/// Returns false when the fire is already running or the concurrency budget is
/// exhausted. A committed Pending record remains durable and is retried later.
#[derive(Clone, Copy)]
struct DeliveryContext<'a> {
    node: &'a Arc<Node>,
    http: &'a reqwest::Client,
    in_flight: &'a InFlight,
    gate: &'a Arc<Semaphore>,
    config: &'a RunnerConfig,
}

fn deliver_in_background(
    context: DeliveryContext<'_>,
    schedule: Schedule,
    fire_id_ms: u64,
    trigger: RunTrigger,
) -> bool {
    let DeliveryContext {
        node,
        http,
        in_flight,
        gate,
        config,
    } = context;
    let key = format!("{}#{fire_id_ms}", schedule.name);
    {
        let mut active = in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(key.clone()) {
            return false;
        }
    }
    let permit = match gate.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
            cron_metrics().deferred.add(
                1,
                &[
                    KeyValue::new("reason", "concurrency_limit"),
                    KeyValue::new("target.kind", target_kind(&schedule.target)),
                    KeyValue::new("trigger", trigger_label(trigger)),
                ],
            );
            tracing::debug!(
                cron.schedule = %unscoped_name(&schedule.name),
                cron.fire_id = fire_id_ms,
                "cron delivery deferred by concurrency limit"
            );
            return false;
        }
    };

    let node = node.clone();
    let http = http.clone();
    let in_flight = in_flight.clone();
    let config = config.clone();
    let target_kind = target_kind(&schedule.target);
    let schedule_name = unscoped_name(&schedule.name).to_string();
    let span = tracing::info_span!(
        "fiducia.cron.delivery",
        otel.kind = "producer",
        cron.schedule = %schedule_name,
        cron.fire_id = fire_id_ms,
        cron.target.kind = target_kind,
        cron.trigger = trigger_label(trigger),
        cron.delivered = tracing::field::Empty,
        cron.attempts = tracing::field::Empty,
        cron.http_status = tracing::field::Empty,
    );
    tokio::spawn(
        async move {
            execute_delivery_task(
                node, http, in_flight, permit, config, schedule, fire_id_ms, trigger, key,
            )
            .await;
        }
        .instrument(span),
    );
    true
}

#[allow(clippy::too_many_arguments)]
async fn execute_delivery_task(
    node: Arc<Node>,
    http: reqwest::Client,
    in_flight: InFlight,
    _permit: OwnedSemaphorePermit,
    config: RunnerConfig,
    schedule: Schedule,
    fire_id_ms: u64,
    trigger: RunTrigger,
    key: String,
) {
    let current_span = tracing::Span::current();
    let context = current_span.context();
    let span_context = context.span().span_context().clone();
    let trace_id = span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string());
    let span_id = span_context
        .is_valid()
        .then(|| span_context.span_id().to_string());

    cron_metrics().in_flight.add(
        1,
        &[
            KeyValue::new("target.kind", target_kind(&schedule.target)),
            KeyValue::new("trigger", trigger_label(trigger)),
        ],
    );
    let outcome = deliver(&http, &config, &schedule, fire_id_ms, trigger).await;
    cron_metrics().in_flight.add(
        -1,
        &[
            KeyValue::new("target.kind", target_kind(&schedule.target)),
            KeyValue::new("trigger", trigger_label(trigger)),
        ],
    );

    current_span.record("cron.delivered", outcome.delivered);
    current_span.record("cron.attempts", outcome.attempts);
    if let Some(status) = outcome.http_status {
        current_span.record("cron.http_status", status);
    }

    let result_label = if outcome.delivered {
        "delivered"
    } else {
        "failed"
    };
    let status_class_label = outcome
        .http_status
        .map(status_class)
        .map(str::to_string)
        .or_else(|| outcome.error_class.clone())
        .unwrap_or_else(|| "none".to_string());
    cron_metrics().deliveries.add(
        1,
        &[
            KeyValue::new("result", result_label),
            KeyValue::new("target.kind", target_kind(&schedule.target)),
            KeyValue::new("trigger", trigger_label(trigger)),
            KeyValue::new("status.class", status_class_label),
        ],
    );
    cron_metrics().duration_ms.record(
        outcome.duration_ms as f64,
        &[
            KeyValue::new("result", result_label),
            KeyValue::new("target.kind", target_kind(&schedule.target)),
            KeyValue::new("trigger", trigger_label(trigger)),
        ],
    );

    let commit = node
        .propose(Command::ScheduleRecordResultV2 {
            name: schedule.name.clone(),
            fire_id_ms,
            delivered: outcome.delivered,
            attempts: outcome.attempts,
            error: outcome.error.clone(),
            error_class: outcome.error_class.clone(),
            http_status: outcome.http_status,
            duration_ms: outcome.duration_ms,
            completed_at_ms: now_ms(),
            trace_id,
            span_id,
        })
        .await;
    match commit {
        Ok(_) => tracing::info!(
            cron.schedule = %unscoped_name(&schedule.name),
            cron.fire_id = fire_id_ms,
            cron.delivered = outcome.delivered,
            cron.attempts = outcome.attempts,
            cron.duration_ms = outcome.duration_ms,
            cron.error_class = outcome.error_class.as_deref().unwrap_or("none"),
            "cron delivery completed"
        ),
        Err(error) => tracing::error!(
            cron.schedule = %unscoped_name(&schedule.name),
            cron.fire_id = fire_id_ms,
            cron.delivered = outcome.delivered,
            cron.attempts = outcome.attempts,
            ?error,
            "cron delivery result was not committed; idempotent redelivery is expected"
        ),
    }
    in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&key);
}

async fn deliver(
    http: &reqwest::Client,
    config: &RunnerConfig,
    schedule: &Schedule,
    fire_id_ms: u64,
    trigger: RunTrigger,
) -> DeliveryOutcome {
    let started = Instant::now();
    let (url, function_auth) = match resolved_target(&schedule.target, config) {
        Ok(target) => target,
        Err(error) => {
            return DeliveryOutcome {
                delivered: false,
                attempts: 0,
                error: Some(error.to_string()),
                error_class: Some("configuration".to_string()),
                http_status: None,
                duration_ms: elapsed_ms(started),
            };
        }
    };
    let function_org_id = if function_auth {
        match scoped_org(&schedule.name) {
            Some(org_id) => Some(org_id.to_string()),
            None => {
                return DeliveryOutcome {
                    delivered: false,
                    attempts: 0,
                    error: Some("function schedule is missing tenant scope".to_string()),
                    error_class: Some("configuration".to_string()),
                    http_status: None,
                    duration_ms: elapsed_ms(started),
                };
            }
        }
    } else {
        None
    };
    let pinned_client = if function_auth {
        None
    } else {
        match public_target_client(&url).await {
            Ok(client) => Some(client),
            Err(error) => {
                return DeliveryOutcome {
                    delivered: false,
                    attempts: 0,
                    error: Some(error.to_string()),
                    error_class: Some("target_policy".to_string()),
                    http_status: None,
                    duration_ms: elapsed_ms(started),
                };
            }
        }
    };
    let http = pinned_client.as_ref().unwrap_or(http);
    let name = unscoped_name(&schedule.name);
    let body = json!({
        "schedule": name,
        "fire_id": fire_id_ms.to_string(),
        "fired_at_ms": fire_id_ms,
        "target_kind": target_kind(&schedule.target),
    });
    let max_attempts = schedule.max_retries.saturating_add(1);
    let idempotency_key = delivery_idempotency_key(name, fire_id_ms);
    let mut attempts = 0u32;
    let mut last_error = None;
    let mut error_class = None;
    let mut last_status = None;

    while attempts < max_attempts {
        attempts += 1;
        cron_metrics().attempts.add(
            1,
            &[
                KeyValue::new("target.kind", target_kind(&schedule.target)),
                KeyValue::new("trigger", trigger_label(trigger)),
            ],
        );
        let attempt_span = tracing::info_span!(
            "fiducia.cron.delivery.attempt",
            otel.kind = "client",
            cron.attempt = attempts,
            cron.target.kind = target_kind(&schedule.target),
            cron.trigger = trigger_label(trigger),
        );
        let response = async {
            let mut headers = HeaderMap::new();
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(
                    &tracing::Span::current().context(),
                    &mut HeaderInjector(&mut headers),
                );
            });
            let mut request = http
                .post(url.clone())
                .headers(headers)
                .header("Idempotency-Key", &idempotency_key)
                .header("X-Fiducia-Schedule", name)
                .json(&body);
            if function_auth {
                if let Some(secret) = config.lambda_server_auth.clone() {
                    request = request.header("x-server-auth", secret);
                }
                if let Some(org_id) = function_org_id.as_deref() {
                    request = request.header("x-fiducia-org-id", org_id);
                }
            }
            request.send().await
        }
        .instrument(attempt_span)
        .await;

        match response {
            Ok(response) => {
                let status = response.status();
                last_status = Some(status.as_u16());
                match response_disposition(status) {
                    ResponseDisposition::Success => {
                        return DeliveryOutcome {
                            delivered: true,
                            attempts,
                            error: None,
                            error_class: None,
                            http_status: last_status,
                            duration_ms: elapsed_ms(started),
                        };
                    }
                    ResponseDisposition::Permanent => {
                        last_error = Some(format!("HTTP {}", status.as_u16()));
                        error_class = Some("http_permanent".to_string());
                        break;
                    }
                    ResponseDisposition::Retry => {
                        last_error = Some(format!("HTTP {}", status.as_u16()));
                        error_class = Some("http_transient".to_string());
                        if attempts < max_attempts {
                            let delay = retry_after_delay(response.headers())
                                .unwrap_or_else(|| retry_delay(fire_id_ms, attempts));
                            record_retry(&schedule.target, trigger, "http_transient");
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
            Err(error) => {
                let class = if error.is_timeout() {
                    "timeout"
                } else if error.is_connect() {
                    "connect"
                } else {
                    "request"
                };
                tracing::warn!(
                    cron.schedule = %name,
                    cron.fire_id = fire_id_ms,
                    cron.attempt = attempts,
                    cron.error_class = class,
                    "cron target request failed"
                );
                last_error = Some(match class {
                    "timeout" => "request timed out".to_string(),
                    "connect" => "connection failed".to_string(),
                    _ => "request failed".to_string(),
                });
                error_class = Some(class.to_string());
                if attempts < max_attempts {
                    record_retry(&schedule.target, trigger, class);
                    tokio::time::sleep(retry_delay(fire_id_ms, attempts)).await;
                }
            }
        }
    }

    DeliveryOutcome {
        delivered: false,
        attempts,
        error: last_error,
        error_class,
        http_status: last_status,
        duration_ms: elapsed_ms(started),
    }
}

fn record_retry(target: &ScheduleTarget, trigger: RunTrigger, reason: &'static str) {
    cron_metrics().retries.add(
        1,
        &[
            KeyValue::new("target.kind", target_kind(target)),
            KeyValue::new("trigger", trigger_label(trigger)),
            KeyValue::new("reason", reason),
        ],
    );
}

fn response_disposition(status: StatusCode) -> ResponseDisposition {
    if status.is_success() {
        ResponseDisposition::Success
    } else if status == StatusCode::REQUEST_TIMEOUT
        || status.as_u16() == 425
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        ResponseDisposition::Retry
    } else {
        ResponseDisposition::Permanent
    }
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

/// Exponential backoff (200ms, 400ms, ...) with deterministic 0-25% jitter. The
/// deterministic seed keeps tests stable while de-synchronizing different fires.
fn retry_delay(fire_id_ms: u64, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6);
    let base_ms = 200u64.saturating_mul(1u64 << exponent);
    let mixed = fire_id_ms
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(attempt % 64);
    let jitter_ms = mixed % (base_ms / 4 + 1);
    Duration::from_millis(base_ms.saturating_add(jitter_ms))
}

fn resolved_target(
    target: &ScheduleTarget,
    config: &RunnerConfig,
) -> Result<(Url, bool), &'static str> {
    match target {
        ScheduleTarget::Webhook { url } => parse_stored_url(url).map(|url| (url, false)),
        ScheduleTarget::Queue { name } => parse_stored_url(name).map(|url| (url, false)),
        ScheduleTarget::Grpc { endpoint } => parse_stored_url(endpoint).map(|url| (url, false)),
        ScheduleTarget::Function { function_id } => {
            let base = config
                .lambda_base_url
                .as_ref()
                .ok_or("function_runtime_unconfigured")?;
            if config.lambda_server_auth.is_none() {
                return Err("function_runtime_auth_unconfigured");
            }
            let mut url = base.clone();
            url.path_segments_mut()
                .map_err(|_| "function_target_invalid")?
                .pop_if_empty()
                .push("invoke")
                .push(function_id);
            Ok((url, true))
        }
    }
}

async fn public_target_client(url: &Url) -> Result<reqwest::Client, &'static str> {
    let host = url.host_str().ok_or("target_host_missing")?;
    if crate::kv::cleartext_internal_host_allowed(host) {
        return Err("target_host_not_allowed");
    }
    let port = url.port_or_known_default().ok_or("target_port_missing")?;
    let addresses: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| "target_dns_failed")?
        .collect();
    if addresses.is_empty() {
        return Err("target_dns_empty");
    }
    if addresses
        .iter()
        .any(|address| crate::kv::cleartext_internal_host_allowed(&address.ip().to_string()))
    {
        return Err("target_resolved_host_not_allowed");
    }
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| "target_client_build_failed")
}

fn parse_stored_url(raw: &str) -> Result<Url, &'static str> {
    let url = Url::parse(raw).map_err(|_| "stored target URL is invalid")?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("stored target scheme is invalid");
    }
    Ok(url)
}

fn normalize_lambda_base(raw: &str) -> Option<Url> {
    let mut url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let path = url.path().trim_end_matches('/').to_string();
    url.set_path(&format!("{path}/"));
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

fn trigger_label(trigger: RunTrigger) -> &'static str {
    match trigger {
        RunTrigger::Scheduled => "scheduled",
        RunTrigger::Manual => "manual",
        RunTrigger::Legacy => "legacy",
    }
}

fn target_kind(target: &ScheduleTarget) -> &'static str {
    match target {
        ScheduleTarget::Webhook { .. } => "webhook",
        ScheduleTarget::Queue { .. } => "queue",
        ScheduleTarget::Grpc { .. } => "grpc",
        ScheduleTarget::Function { .. } => "function",
    }
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

fn scoped_org(name: &str) -> Option<&str> {
    name.strip_prefix(fiducia_routing::ORG_SCOPE_DELIM)
        .and_then(|rest| rest.split_once(fiducia_routing::ORG_SCOPE_DELIM))
        .map(|(org_id, _)| org_id)
        .filter(|org_id| !org_id.is_empty())
}

fn unscoped_name(name: &str) -> &str {
    name.strip_prefix(fiducia_routing::ORG_SCOPE_DELIM)
        .and_then(|rest| rest.split_once(fiducia_routing::ORG_SCOPE_DELIM))
        .map(|(_, name)| name)
        .unwrap_or(name)
}

fn delivery_idempotency_key(schedule_name: &str, fire_id_ms: u64) -> String {
    format!("fiducia-schedule:{schedule_name}:{fire_id_ms}")
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
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
    fn delivery_idempotency_key_is_stable_and_schedule_specific() {
        let fire = 1_725_000_000_000;
        assert_eq!(
            delivery_idempotency_key("billing-hourly", fire),
            delivery_idempotency_key("billing-hourly", fire),
        );
        assert_ne!(
            delivery_idempotency_key("billing-hourly", fire),
            delivery_idempotency_key("email-hourly", fire),
        );
    }

    #[test]
    fn scoped_schedule_names_recover_tenant_and_public_name() {
        let delimiter = fiducia_routing::ORG_SCOPE_DELIM;
        let name = format!("{delimiter}acme{delimiter}billing-hourly");
        assert_eq!(scoped_org(&name), Some("acme"));
        assert_eq!(unscoped_name(&name), "billing-hourly");
        assert_eq!(scoped_org("billing-hourly"), None);
        let empty_org = format!("{delimiter}{delimiter}billing-hourly");
        assert_eq!(scoped_org(&empty_org), None);
    }

    #[test]
    fn retry_policy_does_not_retry_customer_errors() {
        assert_eq!(
            response_disposition(StatusCode::BAD_REQUEST),
            ResponseDisposition::Permanent
        );
        assert_eq!(
            response_disposition(StatusCode::UNAUTHORIZED),
            ResponseDisposition::Permanent
        );
        assert_eq!(
            response_disposition(StatusCode::TOO_MANY_REQUESTS),
            ResponseDisposition::Retry
        );
        assert_eq!(
            response_disposition(StatusCode::BAD_GATEWAY),
            ResponseDisposition::Retry
        );
    }

    #[test]
    fn retry_after_and_backoff_are_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("9999"));
        assert_eq!(retry_after_delay(&headers), Some(MAX_RETRY_AFTER));
        let delay = retry_delay(1234, 100);
        assert!(delay <= Duration::from_millis(16_000));
    }

    #[test]
    fn lambda_base_is_operator_controlled_and_normalized() {
        let base = normalize_lambda_base("http://fiducia-lambda:8080/api").unwrap();
        assert_eq!(base.as_str(), "http://fiducia-lambda:8080/api/");
        let config = RunnerConfig {
            lambda_base_url: Some(base),
            lambda_server_auth: Some(HeaderValue::from_static("secret")),
            max_in_flight: 1,
        };
        let (resolved, authenticated) = resolved_target(
            &ScheduleTarget::Function {
                function_id: "fn_1".to_string(),
            },
            &config,
        )
        .unwrap();
        assert!(authenticated);
        assert_eq!(
            resolved.as_str(),
            "http://fiducia-lambda:8080/api/invoke/fn_1"
        );
        assert!(normalize_lambda_base("file:///tmp/functions").is_none());
        assert!(normalize_lambda_base("https://user:secret@example.com").is_none());
    }
}
