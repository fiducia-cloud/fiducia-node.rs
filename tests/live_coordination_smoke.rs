// End-to-end smoke test: drives a running node's HTTP coordination API
// (locks, KV, etc.) to verify the live wire contract, not just unit behavior.
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::{Client, Method};
use serde_json::{json, Value};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn live_base_url() -> Option<String> {
    std::env::var("FIDUCIA_LIVE_BASE_URL")
        .ok()
        .map(|value| value.trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

fn unique_prefix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("fiducia-live-smoke-{}-{}", millis, std::process::id())
}

fn output(value: Value) -> Value {
    value
        .get("result")
        .and_then(|result| result.get("output"))
        .cloned()
        .or_else(|| value.get("output").cloned())
        .unwrap_or(value)
}

async fn call(
    client: &Client,
    base: &str,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> TestResult<Value> {
    let mut request = client.request(method, format!("{base}{path}"));
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|err| format!("non-JSON response from {path}: {status} {text}: {err}"))?;
    if !status.is_success() {
        return Err(format!("request to {path} failed: {status} {value}").into());
    }
    Ok(output(value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "set FIDUCIA_LIVE_BASE_URL to a deployed fiducia-load-balance or fiducia-node HTTP endpoint"]
async fn live_lock_semaphore_and_multikey_smoke() -> TestResult {
    let Some(base) = live_base_url() else {
        eprintln!("skipping live smoke: FIDUCIA_LIVE_BASE_URL is not set");
        return Ok(());
    };
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let prefix = unique_prefix();
    let a = format!("{prefix}-a");
    let b = format!("{prefix}-b");
    let c = format!("{prefix}-c");

    let health = call(&client, &base, Method::GET, "/healthz", None).await?;
    assert_eq!(health["status"], "ok");

    let first = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/acquire",
        Some(json!({
            "keys": [b, a, a],
            "holder": format!("{prefix}-lock-1"),
            "ttl_ms": 10_000,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(first["acquired"], true);
    assert_eq!(first["keys"], json!([a, b]));

    let no_wait = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/acquire",
        Some(json!({
            "key": a,
            "holder": format!("{prefix}-lock-2"),
            "ttl_ms": 10_000,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(no_wait["acquired"], false);
    assert_eq!(no_wait["queued"], false);

    let queued = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/acquire",
        Some(json!({
            "keys": [b, c],
            "holder": format!("{prefix}-lock-3"),
            "ttl_ms": 10_000,
            "wait": true
        })),
    )
    .await?;
    assert_eq!(queued["queued"], true);

    let inspected = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/locks?key={b}"),
        None,
    )
    .await?;
    assert_eq!(inspected["lock"]["holder"], format!("{prefix}-lock-1"));
    assert_eq!(
        inspected["lock"]["wait_queue"][0]["holder"],
        format!("{prefix}-lock-3")
    );

    let released = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/release",
        Some(json!({
            "holder": format!("{prefix}-lock-1"),
            "fencing_token": first["fencing_token"]
        })),
    )
    .await?;
    assert_eq!(released["released"], true);
    let promoted_token = released["promoted"][0]["fencing_token"].clone();

    let stale = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/release",
        Some(json!({
            "holder": format!("{prefix}-lock-1"),
            "fencing_token": first["fencing_token"]
        })),
    )
    .await?;
    assert_eq!(stale["released"], false);

    call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/release",
        Some(json!({
            "holder": format!("{prefix}-lock-3"),
            "fencing_token": promoted_token
        })),
    )
    .await?;

    let ttl_key = format!("{prefix}-ttl-lock");
    let ttl_first = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/acquire",
        Some(json!({
            "key": ttl_key,
            "holder": format!("{prefix}-ttl-lock-1"),
            "ttl_ms": 250,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(ttl_first["acquired"], true);
    let ttl_waiter = call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/acquire",
        Some(json!({
            "key": ttl_key,
            "holder": format!("{prefix}-ttl-lock-2"),
            "ttl_ms": 10_000,
            "wait": true
        })),
    )
    .await?;
    assert_eq!(ttl_waiter["queued"], true);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let ttl_state = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/locks?key={ttl_key}"),
        None,
    )
    .await?;
    assert_eq!(ttl_state["lock"]["holder"], format!("{prefix}-ttl-lock-2"));

    call(
        &client,
        &base,
        Method::POST,
        "/v1/locks/release",
        Some(json!({
            "holder": format!("{prefix}-ttl-lock-2"),
            "fencing_token": ttl_state["lock"]["fencing_token"]
        })),
    )
    .await?;

    let sem_key = format!("{prefix}-sem");
    let sem1 = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-1"),
            "limit": 2,
            "ttl_ms": 10_000,
            "wait": false
        })),
    )
    .await?;
    let sem2 = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-2"),
            "limit": 2,
            "ttl_ms": 10_000,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(sem1["acquired"], true);
    assert_eq!(sem2["acquired"], true);

    let sem_no_wait = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-no-wait"),
            "limit": 2,
            "ttl_ms": 10_000,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(sem_no_wait["acquired"], false);
    assert_eq!(sem_no_wait["queued"], false);

    let sem_waiter = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-3"),
            "limit": 2,
            "ttl_ms": 10_000,
            "wait": true
        })),
    )
    .await?;
    assert_eq!(sem_waiter["queued"], true);

    let sem_release = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/release",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-1"),
            "fencing_token": sem1["fencing_token"]
        })),
    )
    .await?;
    assert_eq!(sem_release["released"], true);
    let sem_promoted_token = sem_release["promoted"][0]["fencing_token"].clone();

    call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/release",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-2"),
            "fencing_token": sem2["fencing_token"]
        })),
    )
    .await?;
    call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/release",
        Some(json!({
            "key": sem_key,
            "holder": format!("{prefix}-sem-3"),
            "fencing_token": sem_promoted_token
        })),
    )
    .await?;

    let sem_ttl_key = format!("{prefix}-sem-ttl");
    let sem_ttl_first = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_ttl_key,
            "holder": format!("{prefix}-sem-ttl-1"),
            "limit": 1,
            "ttl_ms": 250,
            "wait": false
        })),
    )
    .await?;
    assert_eq!(sem_ttl_first["acquired"], true);
    let sem_ttl_waiter = call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/acquire",
        Some(json!({
            "key": sem_ttl_key,
            "holder": format!("{prefix}-sem-ttl-2"),
            "limit": 1,
            "ttl_ms": 10_000,
            "wait": true
        })),
    )
    .await?;
    assert_eq!(sem_ttl_waiter["queued"], true);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let sem_ttl_state = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/semaphores?key={sem_ttl_key}"),
        None,
    )
    .await?;
    assert_eq!(
        sem_ttl_state["semaphore"]["holders"][0]["holder"],
        format!("{prefix}-sem-ttl-2")
    );

    call(
        &client,
        &base,
        Method::POST,
        "/v1/semaphores/release",
        Some(json!({
            "key": sem_ttl_key,
            "holder": format!("{prefix}-sem-ttl-2"),
            "fencing_token": sem_ttl_state["semaphore"]["holders"][0]["fencing_token"]
        })),
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "set FIDUCIA_LIVE_BASE_URL to a deployed fiducia-load-balance or fiducia-node HTTP endpoint"]
async fn live_coordination_primitives_smoke() -> TestResult {
    let Some(base) = live_base_url() else {
        eprintln!("skipping live smoke: FIDUCIA_LIVE_BASE_URL is not set");
        return Ok(());
    };
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let prefix = unique_prefix();

    let kv_key = format!("{prefix}-config");
    let created = call(
        &client,
        &base,
        Method::PUT,
        &format!("/v1/kv?key={kv_key}"),
        Some(json!({ "value": "on", "prev_revision": 0 })),
    )
    .await?;
    assert_eq!(created["ok"], true);
    let created_revision = created["revision"]
        .as_u64()
        .ok_or("kv create response missed revision")?;

    let kv = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/kv?key={kv_key}"),
        None,
    )
    .await?;
    assert_eq!(kv["found"], true);
    assert_eq!(kv["entry"]["value"], "on");

    let stale = call(
        &client,
        &base,
        Method::PUT,
        &format!("/v1/kv?key={kv_key}"),
        Some(json!({ "value": "off", "prev_revision": 0 })),
    )
    .await?;
    assert_eq!(stale["ok"], false);
    assert_eq!(stale["reason"], "cas_mismatch");

    let updated = call(
        &client,
        &base,
        Method::PUT,
        &format!("/v1/kv?key={kv_key}"),
        Some(json!({ "value": "off", "prev_revision": created_revision })),
    )
    .await?;
    assert_eq!(updated["ok"], true);

    let tenant = format!("{prefix}-tenant");
    let limit_key = format!("{prefix}-checkout");
    let first_limit = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/rate-limit/{tenant}/{limit_key}/check"),
        Some(json!({
            "algorithm": "sliding_window",
            "limit": 1,
            "window_ms": 60_000,
            "cost": 1
        })),
    )
    .await?;
    let second_limit = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/rate-limit/{tenant}/{limit_key}/check"),
        Some(json!({
            "algorithm": "sliding_window",
            "limit": 1,
            "window_ms": 60_000,
            "cost": 1
        })),
    )
    .await?;
    assert_eq!(first_limit["allowed"], true);
    assert_eq!(second_limit["allowed"], false);
    let limit_state = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/rate-limit/{tenant}/{limit_key}"),
        None,
    )
    .await?;
    assert_eq!(limit_state["found"], true);
    assert_eq!(limit_state["limit"]["remaining"], 0);

    let schedule = format!("{prefix}-nightly");
    let scheduled = call(
        &client,
        &base,
        Method::PUT,
        &format!("/v1/cron/schedules/{schedule}"),
        Some(json!({
            "cron": "0 0 * * *",
            "target": { "kind": "webhook", "url": "https://example.test/hook" },
            "delivery": "exactly_once",
            "max_retries": 2
        })),
    )
    .await?;
    assert_eq!(scheduled["scheduled"], true);

    let schedule_state = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/cron/schedules/{schedule}"),
        None,
    )
    .await?;
    assert_eq!(schedule_state["found"], true);
    assert_eq!(schedule_state["schedule"]["delivery"], "exactly_once");

    let fire_id = format!("{prefix}-fire");
    let first_run = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/cron/schedules/{schedule}/runs"),
        Some(json!({ "fire_id": fire_id, "fired_at_ms": 1 })),
    )
    .await?;
    let duplicate_run = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/cron/schedules/{schedule}/runs"),
        Some(json!({ "fire_id": fire_id, "fired_at_ms": 2 })),
    )
    .await?;
    assert_eq!(first_run["recorded"], true);
    assert_eq!(duplicate_run["duplicate"], true);
    let history = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/cron/schedules/{schedule}/history"),
        None,
    )
    .await?;
    assert_eq!(history["history"].as_array().map(Vec::len), Some(1));

    let election = format!("{prefix}-leader");
    let won = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/elections/{election}/campaign"),
        Some(json!({ "candidate": "node-a", "ttl_ms": 30_000 })),
    )
    .await?;
    assert_eq!(won["won"], true);
    let token = won["leadership"]["fencing_token"]
        .as_u64()
        .ok_or("election campaign response missed fencing token")?;

    let blocked = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/elections/{election}/campaign"),
        Some(json!({ "candidate": "node-b", "ttl_ms": 30_000 })),
    )
    .await?;
    assert_eq!(blocked["won"], false);

    let observed = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/elections/{election}"),
        None,
    )
    .await?;
    assert_eq!(observed["held"], true);
    assert_eq!(observed["leadership"]["leader"], "node-a");

    let renewed = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/elections/{election}/renew"),
        Some(json!({ "candidate": "node-a", "fencing_token": token })),
    )
    .await?;
    assert_eq!(renewed["renewed"], true);

    let resigned = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/elections/{election}/resign"),
        Some(json!({ "candidate": "node-a", "fencing_token": token })),
    )
    .await?;
    assert_eq!(resigned["resigned"], true);

    let service = format!("{prefix}-api");
    let registered = call(
        &client,
        &base,
        Method::PUT,
        &format!("/v1/services/{service}/instances/i-1"),
        Some(json!({
            "address": "10.0.0.1:9000",
            "ttl_ms": 30_000,
            "metadata": { "az": "a" }
        })),
    )
    .await?;
    assert_eq!(registered["registered"], true);

    let instances = call(
        &client,
        &base,
        Method::GET,
        &format!("/v1/services/{service}"),
        None,
    )
    .await?;
    assert_eq!(instances["instances"][0]["instance_id"], "i-1");
    assert_eq!(instances["instances"][0]["metadata"]["az"], "a");

    let services = call(&client, &base, Method::GET, "/v1/services", None).await?;
    assert!(services["services"]
        .as_array()
        .map(|items| items.iter().any(|item| item == &json!(service)))
        .unwrap_or(false));

    let heartbeat = call(
        &client,
        &base,
        Method::POST,
        &format!("/v1/services/{service}/instances/i-1/heartbeat"),
        Some(json!({ "ttl_ms": 45_000 })),
    )
    .await?;
    assert_eq!(heartbeat["heartbeat"], true);

    let deregistered = call(
        &client,
        &base,
        Method::DELETE,
        &format!("/v1/services/{service}/instances/i-1"),
        None,
    )
    .await?;
    assert_eq!(deregistered["deregistered"], true);

    call(
        &client,
        &base,
        Method::DELETE,
        &format!("/v1/kv?key={kv_key}"),
        None,
    )
    .await?;

    Ok(())
}

/// End-to-end coverage of the higher-level coordination primitives — counters,
/// barriers, tasks, effects, handoffs, decisions, budgets, and claims — driving
/// each guarantee through the live HTTP → Raft → apply → read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "set FIDUCIA_LIVE_BASE_URL to a deployed fiducia-load-balance or fiducia-node HTTP endpoint"]
async fn live_higher_level_primitives_smoke() -> TestResult {
    let Some(base) = live_base_url() else {
        eprintln!("skipping live smoke: FIDUCIA_LIVE_BASE_URL is not set");
        return Ok(());
    };
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let p = unique_prefix();
    let post = Method::POST;
    let get = Method::GET;

    // --- counters: accumulate + compare-and-set + read-after-write ---
    let ckey = format!("{p}/failures");
    let r = call(&client, &base, post.clone(), "/v1/counters/add", Some(json!({"key": ckey, "delta": 3}))).await?;
    assert_eq!(r["value"], 3, "counter add");
    let r = call(&client, &base, post.clone(), "/v1/counters/add", Some(json!({"key": ckey, "delta": -1}))).await?;
    assert_eq!(r["value"], 2, "counter accumulates");
    let r = call(&client, &base, post.clone(), "/v1/counters/add", Some(json!({"key": ckey, "delta": 100, "prev_revision": 0}))).await?;
    assert_eq!(r["reason"], "cas_mismatch", "stale CAS rejected");
    let r = call(&client, &base, get.clone(), &format!("/v1/counters?key={ckey}"), None).await?;
    assert_eq!(r["counter"]["value"], 2, "counter read-after-write");

    // --- barriers: quorum resolves on distinct arrivals; duplicates idempotent ---
    let bname = format!("{p}/panel");
    call(&client, &base, post.clone(), "/v1/barriers/create", Some(json!({"name": bname, "policy": {"kind": "quorum", "required": 2}, "expected": 3}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/barriers/arrive", Some(json!({"name": bname, "participant": "a"}))).await?;
    assert_eq!(r["resolved"], false, "1/2 pending");
    call(&client, &base, post.clone(), "/v1/barriers/arrive", Some(json!({"name": bname, "participant": "a"}))).await?; // duplicate
    let r = call(&client, &base, post.clone(), "/v1/barriers/arrive", Some(json!({"name": bname, "participant": "b"}))).await?;
    assert_eq!(r["barrier"]["status"], "satisfied", "2nd distinct arrival resolves");

    // --- tasks: exclusive claim + stale-token fencing ---
    let tname = format!("{p}/issue-482");
    call(&client, &base, post.clone(), "/v1/tasks/create", Some(json!({"name": tname, "task_type": "impl"}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/tasks/claim", Some(json!({"name": tname, "worker": "agent-a", "ttl_ms": 60000}))).await?;
    let tok = r["fencing_token"].as_u64().expect("fencing token");
    let r = call(&client, &base, post.clone(), "/v1/tasks/claim", Some(json!({"name": tname, "worker": "agent-b"}))).await?;
    assert_eq!(r["reason"], "already_claimed", "second claim rejected");
    let r = call(&client, &base, post.clone(), "/v1/tasks/progress", Some(json!({"name": tname, "worker": "agent-a", "fencing_token": tok + 999, "percent": 50}))).await?;
    assert_eq!(r["reason"], "fenced", "stale token fenced");
    let r = call(&client, &base, post.clone(), "/v1/tasks/complete", Some(json!({"name": tname, "worker": "agent-a", "fencing_token": tok, "result": {"pr": 1}}))).await?;
    assert_eq!(r["task"]["status"], "completed", "current token completes");

    // --- effects: approval gate + exactly-once commit (replay) ---
    let ename = format!("{p}/pay");
    call(&client, &base, post.clone(), "/v1/effects/prepare", Some(json!({"name": ename, "effect_type": "send_payment", "idempotency_key": ename, "required_approvals": 2}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/effects/commit", Some(json!({"name": ename, "result": {}}))).await?;
    assert_eq!(r["reason"], "not_approved", "commit before approval rejected");
    call(&client, &base, post.clone(), "/v1/effects/approve", Some(json!({"name": ename, "principal": "finance-a"}))).await?;
    call(&client, &base, post.clone(), "/v1/effects/approve", Some(json!({"name": ename, "principal": "finance-a"}))).await?; // dup
    call(&client, &base, post.clone(), "/v1/effects/approve", Some(json!({"name": ename, "principal": "finance-b"}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/effects/commit", Some(json!({"name": ename, "result": {"c": "ok1"}}))).await?;
    assert_eq!(r["committed"], true, "first commit executes");
    let r = call(&client, &base, post.clone(), "/v1/effects/commit", Some(json!({"name": ename, "result": {"c": "ok2"}}))).await?;
    assert_eq!(r["committed"], false, "duplicate commit replays");
    let r = call(&client, &base, get.clone(), &format!("/v1/effects?name={ename}"), None).await?;
    assert_eq!(r["effect"]["result"]["c"], "ok1", "original result preserved");

    // --- handoffs: atomic transfer, strictly higher to_token ---
    let hres = format!("{p}/ticket");
    call(&client, &base, post.clone(), "/v1/tasks/create", Some(json!({"name": hres, "task_type": "research"}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/tasks/claim", Some(json!({"name": hres, "worker": "research-agent"}))).await?;
    let ftok = r["fencing_token"].as_u64().expect("from token");
    let hname = format!("{p}/ho");
    call(&client, &base, post.clone(), "/v1/handoffs/offer", Some(json!({"name": hname, "resource": format!("task:{hres}"), "from": "research-agent", "to": "legal-agent", "from_token": ftok}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/handoffs/accept", Some(json!({"name": hname, "to": "wrong"}))).await?;
    assert_eq!(r["reason"], "not_recipient", "only recipient accepts");
    let r = call(&client, &base, post.clone(), "/v1/handoffs/accept", Some(json!({"name": hname, "to": "legal-agent"}))).await?;
    assert!(r["to_token"].as_u64().unwrap() > ftok, "accept mints strictly higher token");

    // --- decisions: weighted plurality + unknown-option rejection ---
    let dname = format!("{p}/deploy");
    call(&client, &base, post.clone(), "/v1/decisions/propose", Some(json!({"name": dname, "question": "safe?", "options": ["approve", "reject"], "policy": {"kind": "plurality", "min_votes": 3}}))).await?;
    call(&client, &base, post.clone(), "/v1/decisions/vote", Some(json!({"name": dname, "voter": "a", "option": "approve", "weight": 1}))).await?;
    call(&client, &base, post.clone(), "/v1/decisions/vote", Some(json!({"name": dname, "voter": "b", "option": "reject", "weight": 1}))).await?;
    let r = call(&client, &base, get.clone(), &format!("/v1/decisions?name={dname}"), None).await?;
    assert_eq!(r["decision"]["status"], "open", "open below min_votes");
    call(&client, &base, post.clone(), "/v1/decisions/vote", Some(json!({"name": dname, "voter": "c", "option": "approve", "weight": 5}))).await?;
    let r = call(&client, &base, get.clone(), &format!("/v1/decisions?name={dname}"), None).await?;
    assert_eq!(r["decision"]["winner"], "approve", "resolves to heaviest option");
    let r = call(&client, &base, post.clone(), "/v1/decisions/vote", Some(json!({"name": dname, "voter": "d", "option": "maybe"}))).await?;
    assert_eq!(r["reason"], "unknown_option", "unknown option rejected");

    // --- budgets: no oversubscribe; commit frees the difference ---
    let gname = format!("{p}/wf");
    call(&client, &base, post.clone(), "/v1/budgets/set", Some(json!({"name": gname, "limit": {"usd_micros": 1000000}}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/budgets/reserve", Some(json!({"name": gname, "reservation_id": "a", "holder": "agent-a", "amount": {"usd_micros": 600000}}))).await?;
    assert_eq!(r["reserved"], true, "first reservation ok");
    let r = call(&client, &base, post.clone(), "/v1/budgets/reserve", Some(json!({"name": gname, "reservation_id": "b", "holder": "agent-b", "amount": {"usd_micros": 600000}}))).await?;
    assert_eq!(r["reason"], "insufficient_budget", "cannot oversubscribe");
    call(&client, &base, post.clone(), "/v1/budgets/commit", Some(json!({"name": gname, "reservation_id": "a", "actual": {"usd_micros": 200000}}))).await?;
    let r = call(&client, &base, get.clone(), &format!("/v1/budgets?name={gname}"), None).await?;
    assert_eq!(r["budget"]["available"]["usd_micros"], 800000, "commit frees the difference");

    // --- claims: contest → re-assert version bump → authoritative resolve ---
    let clname = format!("{p}/refund");
    call(&client, &base, post.clone(), "/v1/claims/assert", Some(json!({"name": clname, "subject": "customer:219", "predicate": "refund_eligible", "value": true, "author": "billing"}))).await?;
    call(&client, &base, post.clone(), "/v1/claims/support", Some(json!({"name": clname, "agent": "audit"}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/claims/contest", Some(json!({"name": clname, "agent": "fraud", "reason": "chargeback"}))).await?;
    assert_eq!(r["claim"]["status"], "contested", "contest → contested");
    let r = call(&client, &base, post.clone(), "/v1/claims/assert", Some(json!({"name": clname, "subject": "customer:219", "predicate": "refund_eligible", "value": false, "author": "fraud"}))).await?;
    assert_eq!(r["claim"]["version"], 2, "re-assert bumps version");
    call(&client, &base, post.clone(), "/v1/claims/resolve", Some(json!({"name": clname, "accepted": true}))).await?;
    let r = call(&client, &base, post.clone(), "/v1/claims/support", Some(json!({"name": clname, "agent": "late"}))).await?;
    assert_eq!(r["reason"], "terminal", "resolved claim is terminal");

    Ok(())
}
