//! fiducia-node — the Raft-replicated coordination engine.
//!
//! A node hosts replicas of many shards (each an independent Raft group),
//! leading some and following others, and exposes the coordination API over
//! HTTP: locks, rate limits, cron schedules, config KV, leader election, and
//! service discovery.
//!
//! This is a **skeleton**: the routing, consensus, and state-machine shapes are
//! in place; the per-command logic, replication, watches, and TTL expiry are
//! marked with `TODO`s in the respective modules.

mod consensus;
mod discovery;
mod election;
mod kv;
mod locks;
mod raft_api;
mod rate_limit;
mod schedule;
mod semaphore;
mod state;
mod transport;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::get, Json, Router};
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

    let v1 = Router::new()
        .route("/status", get(status))
        .nest("/kv", kv::router())
        .nest("/locks", locks::router())
        .nest("/semaphores", semaphore::router())
        .nest("/rate-limit", rate_limit::router())
        .nest("/ratelimit", rate_limit::router())
        .nest("/cron", schedule::router())
        .nest("/elections", election::router())
        .nest("/services", discovery::router());

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .nest("/v1", v1)
        // Internal node↔node Raft RPC (peer transport server side); not under /v1.
        .nest("/raft", raft_api::router())
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
