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
