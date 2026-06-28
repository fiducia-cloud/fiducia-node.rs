//! The cron firing loop — the leader-elected engine that turns stored schedules
//! into delivered fires.
//!
//! Runs on every node but **acts only for shards this node currently leads**, so
//! firing is leader-elected with no duplicates. Each tick it:
//!   1. claims every due fire via a Raft-committed [`Command::ScheduleClaimFire`]
//!      — the claim dedups by fire time, so exactly one fire happens even across a
//!      leader change;
//!   2. delivers the fire to its target over HTTP with the fire id as an
//!      `Idempotency-Key`, retrying with exponential backoff up to `max_retries`; and
//!   3. records the outcome (delivered/failed + attempts) to the durable history.
//!
//! **At-least-once**: a `Pending` run a leader left behind when it died
//! mid-delivery is re-delivered by the new leader. **Exactly-once** is the same
//! path plus the idempotency key, which lets the target dedup the re-delivery.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::consensus::{Node, ReadRequest, ReadResponse};
use crate::cron::CronSchedule;
use crate::state::{Command, RunStatus, Schedule, ScheduleTarget};

/// How often to scan for due fires. Cron granularity is one minute, so a few
/// seconds keeps fires prompt without busy-looping.
const TICK: Duration = Duration::from_secs(5);
/// Cap fires claimed per schedule per tick, so a long backlog (after downtime)
/// drains steadily instead of in one burst.
const MAX_CLAIMS_PER_TICK: usize = 16;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Tracks fires currently being delivered by this node, so the re-delivery path
/// doesn't double-fire something a freshly-claimed delivery is already handling.
type InFlight = Arc<Mutex<HashSet<String>>>;

/// Spawn the firing loop. Idempotent per process; call once at startup.
pub fn spawn(node: Arc<Node>) {
    tokio::spawn(run(node));
}

async fn run(node: Arc<Node>) {
    let http = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .unwrap_or_default();
    let in_flight: InFlight = Arc::new(Mutex::new(HashSet::new()));
    let mut tick = tokio::time::interval(TICK);
    loop {
        tick.tick().await;
        sweep(&node, &http, &in_flight).await;
    }
}

async fn sweep(node: &Arc<Node>, http: &reqwest::Client, in_flight: &InFlight) {
    let leading: HashSet<u32> = node.status().await.leading_shards.into_iter().collect();
    if leading.is_empty() {
        return;
    }
    let now = now_ms();
    for schedule in node.list_schedules().await {
        // Only this node, only for shards it leads (firing is leader-elected).
        if !schedule.enabled || !leading.contains(&node.shard_for(&schedule.name)) {
            continue;
        }
        // Re-deliver fires a dead leader left Pending (at-least-once), then claim
        // and deliver newly-due fires.
        redeliver_pending(node, http, in_flight, &schedule).await;
        claim_due(node, http, in_flight, &schedule, now).await;
    }
}

/// Claim and deliver every fire due at or before `now` (bounded per tick).
async fn claim_due(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
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
        // Claim through Raft: succeeds only on the leader, and only once per fire.
        let claimed = match node
            .propose(Command::ScheduleClaimFire {
                name: schedule.name.clone(),
                fire_id_ms: fire,
            })
            .await
        {
            Ok(outcome) => outcome.output.get("claimed").and_then(|v| v.as_bool()) == Some(true),
            Err(_) => false, // NotLeader (or transient) — stop firing this schedule
        };
        if !claimed {
            break;
        }
        claims += 1;
        deliver_in_background(node, http, in_flight, schedule.clone(), fire);
        // Advance locally to match the state machine's cursor advance.
        next = if schedule.one_shot_at_ms.is_some() {
            None
        } else {
            cron.as_ref().and_then(|c| c.next_after(fire))
        };
    }
}

/// Re-deliver any fire still `Pending` (claimed but not confirmed — its deliverer
/// died). The in-flight guard skips fires this node is already delivering.
async fn redeliver_pending(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
    schedule: &Schedule,
) {
    let history = match node
        .query(ReadRequest::ScheduleHistory {
            name: schedule.name.clone(),
        })
        .await
    {
        Ok(ReadResponse::ScheduleHistory(history)) => history,
        _ => return,
    };
    for run in history.iter().filter(|r| r.status == RunStatus::Pending) {
        if let Ok(fire) = run.fire_id.parse::<u64>() {
            deliver_in_background(node, http, in_flight, schedule.clone(), fire);
        }
    }
}

/// Deliver `fire_id_ms` for `schedule` on a background task, then record the
/// result. No-op if this node is already delivering that fire.
fn deliver_in_background(
    node: &Arc<Node>,
    http: &reqwest::Client,
    in_flight: &InFlight,
    schedule: Schedule,
    fire_id_ms: u64,
) {
    let key = format!("{}#{}", schedule.name, fire_id_ms);
    if !in_flight.lock().unwrap().insert(key.clone()) {
        return; // already delivering this exact fire
    }
    let node = node.clone();
    let http = http.clone();
    let in_flight = in_flight.clone();
    tokio::spawn(async move {
        let (delivered, attempts, error) = deliver(&http, &schedule, fire_id_ms).await;
        let _ = node
            .propose(Command::ScheduleRecordResult {
                name: schedule.name.clone(),
                fire_id_ms,
                delivered,
                attempts,
                error,
            })
            .await;
        in_flight.lock().unwrap().remove(&key);
    });
}

/// Deliver one fire to its target over HTTP, retrying with backoff. Returns
/// `(delivered, attempts, last_error)`.
async fn deliver(
    http: &reqwest::Client,
    schedule: &Schedule,
    fire_id_ms: u64,
) -> (bool, u32, Option<String>) {
    let url = target_url(&schedule.target);
    let body = json!({
        "schedule": schedule.name,
        "fire_id": fire_id_ms.to_string(),
        "fired_at_ms": fire_id_ms,
        "target": schedule.target,
    });
    let max_attempts = schedule.max_retries.saturating_add(1);
    let mut attempts = 0u32;
    let mut last_error = None;
    while attempts < max_attempts {
        attempts += 1;
        match http
            .post(&url)
            .header("Idempotency-Key", fire_id_ms.to_string())
            .header("X-Fiducia-Schedule", &schedule.name)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return (true, attempts, None),
            Ok(resp) => last_error = Some(format!("HTTP {}", resp.status())),
            Err(err) => last_error = Some(err.to_string()),
        }
        if attempts < max_attempts {
            // 200ms, 400ms, 800ms, … capped.
            let backoff = Duration::from_millis(200 * 2u64.pow(attempts.min(6)));
            tokio::time::sleep(backoff).await;
        }
    }
    (false, attempts, last_error)
}

/// The HTTP endpoint to POST a fire to. Per the "uniform HTTP" delivery model, all
/// three target kinds are delivered over HTTP for now (native gRPC/queue connectors
/// are a follow-up): the webhook URL, the queue's ingress URL, or the gRPC endpoint.
fn target_url(target: &ScheduleTarget) -> String {
    match target {
        ScheduleTarget::Webhook { url } => url.clone(),
        ScheduleTarget::Queue { name } => name.clone(),
        ScheduleTarget::Grpc { endpoint } => endpoint.clone(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
