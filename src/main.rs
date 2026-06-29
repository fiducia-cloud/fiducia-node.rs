//! fiducia-node — the Raft-replicated coordination engine.
//!
//! A node hosts replicas of many shards (each an independent Raft group),
//! leading some and following others, and exposes the coordination API over
//! HTTP: locks, idempotency keys, rate limits, cron schedules, config KV,
//! leader election, and service discovery.
//!
//! The routing, consensus, state-machine primitives, replication, watches, and
//! TTL expiry are implemented in the respective modules.

mod consensus;
mod cron;
mod discovery;
mod election;
mod idempotency;
mod indexed_queue;
mod internal_auth;
mod kv;
mod locks;
mod metrics;
mod observe;
mod persist;
mod raft_api;
mod rate_limit;
mod schedule;
mod schedule_runner;
mod semaphore;
mod state;
mod transport;
mod validate;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};

use consensus::{Node, NodeConfig};

const SERVICE: &str = "fiducia-node";

/// Cap request bodies (KV values). Generous for coordination data; rejects
/// memory-exhaustion payloads. NOTE: deliberately **no request timeout** — KV
/// `watch` streams and blocking lock acquires are long-lived by design.
const MAX_BODY_BYTES: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fiducia_telemetry::init(SERVICE);

    // Bootstrap this node. Single-node by default; FIDUCIA_PEERS / shard count
    // come from the environment (see consensus::NodeConfig).
    let config = NodeConfig::default();
    tracing::info!(
        "{SERVICE} bootstrapping node_id={} shards={} peers={:?}",
        config.node_id,
        config.shard_count,
        config.peers
    );
    let node = Arc::new(Node::bootstrap_http(config));

    // The cron firing loop: fires due schedules on the shards this node leads.
    schedule_runner::spawn(node.clone());

    // Log whether the internal trust boundary (LB→node, node→node) is enforced.
    internal_auth::init_and_log();

    let v1 = Router::new()
        .route("/status", get(status))
        .nest("/kv", kv::router())
        .nest("/idempotency", idempotency::router())
        .nest("/locks", locks::router())
        .nest("/semaphores", semaphore::router())
        .nest("/rate-limit", rate_limit::router())
        .nest("/ratelimit", rate_limit::router())
        .nest("/cron", schedule::router())
        .nest("/elections", election::router())
        .nest("/services", discovery::router())
        .nest("/observe", observe::router());

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        // `/v1` (client data plane, reached via the LB) and `/raft` (peer RPC) are
        // cluster-internal: when FIDUCIA_INTERNAL_SECRET is set, both require the
        // trusted-hop header. Health probes stay open for k8s. The guard is a
        // no-op when the secret is unset (dev / single-node), so this is additive.
        .nest("/v1", v1.layer(middleware::from_fn(internal_auth::guard)))
        // Internal node↔node Raft RPC (peer transport server side); not under /v1.
        .nest(
            "/raft",
            raft_api::router().layer(middleware::from_fn(internal_auth::guard)),
        )
        .with_state(node)
        // Hardening (outermost last): catch handler panics → 500 and cap body
        // size. No TimeoutLayer — watches/long-poll are intentionally long-lived.
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!("{SERVICE} listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

/// `GET /v1/status` — per-shard consensus status for this node.
async fn status(axum::extract::State(node): axum::extract::State<Arc<Node>>) -> Json<Value> {
    Json(json!({
        "service": SERVICE,
        "version": env!("CARGO_PKG_VERSION"),
        "consensus": node.status().await,
    }))
}

#[cfg(test)]
mod interface_contract_tests {
    use fiducia_interfaces::{LockAcquireManyRequest, ProposeErrorReason};

    #[test]
    fn generated_interfaces_are_importable() {
        let request = LockAcquireManyRequest {
            keys: vec!["orders/42".to_string(), "inventory/sku-7".to_string()],
            holder: Some("worker-a".to_string()),
            ttl_ms: Some(30_000),
            wait: Some(false),
        };

        assert_eq!(request.keys.len(), 2);
        assert!(matches!(
            ProposeErrorReason::NotLeader,
            ProposeErrorReason::NotLeader
        ));
    }
}
