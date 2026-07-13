//! fiducia-node — the Raft-replicated coordination engine.
//!
//! A node hosts replicas of many shards (each an independent Raft group),
//! leading some and following others, and exposes the coordination API over
//! HTTP: locks, idempotency keys, rate limits, cron schedules, config KV,
//! leader election, and service discovery.
//!
//! The routing, consensus, state-machine primitives, replication, watches, and
//! TTL expiry are implemented in the respective modules.

mod barriers;
mod budgets;
mod claims;
mod consensus;
mod counters;
mod cron;
mod decisions;
mod discovery;
mod effects;
mod election;
mod handoffs;
mod idempotency;
mod indexed_queue;
mod internal_auth;
mod kv;
mod locks;
mod metrics;
mod observe;
mod org_scope;
mod persist;
mod raft_api;
mod rate_limit;
mod schedule;
mod schedule_runner;
mod semaphore;
mod state;
mod tasks;
mod transport;
mod validate;

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::{json, Value};
use tower_http::{catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};

use consensus::{Node, NodeConfig};

const SERVICE: &str = "fiducia-node";

/// Cap request bodies (KV values). Generous for coordination data; rejects
/// memory-exhaustion payloads. NOTE: deliberately **no request timeout** — KV
/// `watch` streams and blocking lock acquires are long-lived by design.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Body cap for the **peer** plane (`/raft`). Unlike a client write, an
/// `InstallSnapshot` carries an entire shard's state-machine image and an
/// `AppendEntries` can carry the whole retained log suffix (up to the
/// compaction threshold of entries, each holding a near-`MAX_BODY_BYTES`
/// value). Capping peers at `MAX_BODY_BYTES` would make a follower that fell
/// behind unrecoverable the moment a shard's state outgrew 1 MiB: every
/// snapshot install would 413 and replication would stall forever. Peers are
/// authenticated by the internal-secret guard (which rejects before reading
/// the body), so the larger cap is not exposed to untrusted callers; it still
/// bounds a misbehaving peer.
const MAX_PEER_BODY_BYTES: usize = 256 * 1024 * 1024;

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
        .nest("/counters", counters::router())
        .nest("/barriers", barriers::router())
        .nest("/tasks", tasks::router())
        .nest("/effects", effects::router())
        .nest("/handoffs", handoffs::router())
        .nest("/decisions", decisions::router())
        .nest("/budgets", budgets::router())
        .nest("/claims", claims::router())
        .nest("/idempotency", idempotency::router())
        .nest("/locks", locks::router())
        .nest("/semaphores", semaphore::router())
        .nest("/rate-limit", rate_limit::router())
        .nest("/ratelimit", rate_limit::router())
        .nest("/cron", schedule::router())
        .nest("/elections", election::router())
        .nest("/services", discovery::router())
        .nest("/observe", observe::router());

    // Two listeners on two ports, so the client and peer planes are separable at
    // L4 (a NetworkPolicy can lock the client port to in-namespace while leaving
    // the peer port reachable cross-cluster — see fiducia-infra). Both still
    // require the trusted-hop secret at L7 when FIDUCIA_INTERNAL_SECRET is set.
    //
    //   :PORT (8090)            — client/data plane: /healthz, /readyz, /v1/*
    //   :FIDUCIA_PEER_PORT(9090)— peer plane: node↔node Raft RPC (/raft/*)
    //
    // Peers address each other at host:9090 (FIDUCIA_PEERS / topology.toml
    // node_peer_endpoint), so /raft MUST live on the peer port — previously it
    // was only on :8090, so cross-cluster AppendEntries to :9090 hit nothing.
    let client_app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        // Missing auth fails closed unless local dev explicitly opts out.
        // Order (axum layers apply bottom-up): internal_auth runs first to prove
        // the trusted hop, THEN require_org enforces + attaches a validated org
        // scope so every coordination handler runs under a tenant namespace.
        .nest(
            "/v1",
            v1.layer(middleware::from_fn(org_scope::require_org))
                .layer(middleware::from_fn(internal_auth::guard)),
        )
        .with_state(node.clone())
        // Hardening (outermost last): catch handler panics → 500 and cap body
        // size. No TimeoutLayer — watches/long-poll are intentionally long-lived.
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new());

    let peer_app = Router::new()
        // A health route on the peer port too, so the peer listener can be probed.
        .route("/healthz", get(health))
        .nest(
            "/raft",
            raft_api::router().layer(middleware::from_fn(internal_auth::guard)),
        )
        .with_state(node)
        .layer(TraceLayer::new_for_http())
        // Both caps must be raised: tower-http's layer AND axum's built-in
        // `DefaultBodyLimit` (2 MiB), which `Json` extraction enforces
        // independently and which would otherwise 413 large snapshots.
        .layer(RequestBodyLimitLayer::new(MAX_PEER_BODY_BYTES))
        .layer(DefaultBodyLimit::max(MAX_PEER_BODY_BYTES))
        .layer(CatchPanicLayer::new());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);
    let peer_port: u16 = std::env::var("FIDUCIA_PEER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9090);
    let client_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let peer_addr = SocketAddr::from(([0, 0, 0, 0], peer_port));

    tracing::info!("{SERVICE} client plane on http://{client_addr} (/v1), peer plane on http://{peer_addr} (/raft)");
    let client_listener = tokio::net::TcpListener::bind(client_addr).await?;
    let peer_listener = tokio::net::TcpListener::bind(peer_addr).await?;
    // Serve both concurrently; if either listener exits, the node exits.
    tokio::try_join!(
        axum::serve(client_listener, client_app).into_future(),
        axum::serve(peer_listener, peer_app).into_future(),
    )?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

async fn readiness(State(node): State<Arc<Node>>) -> Response {
    let status = node.status().await;
    let storage_faulted_shards: Vec<_> = status
        .shards
        .iter()
        .filter(|shard| !shard.storage_healthy)
        .map(|shard| shard.shard_id)
        .collect();
    let all_shards_running = status.shards.len() == status.shard_count as usize;
    let ready = all_shards_running && storage_faulted_shards.is_empty();
    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status_code,
        Json(json!({
            "status": if ready { "ok" } else { "unavailable" },
            "service": SERVICE,
            "all_shards_running": all_shards_running,
            "storage_faulted_shards": storage_faulted_shards,
        })),
    )
        .into_response()
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

#[cfg(test)]
mod test_support {
    use std::sync::Arc;

    use axum::{body::to_bytes, response::Response};

    use crate::consensus::{Node, NodeConfig};
    use crate::transport::{LoopbackRegistry, Transport};

    pub fn node(shard_count: u32) -> Arc<Node> {
        let registry = LoopbackRegistry::new();
        Arc::new(Node::bootstrap(
            NodeConfig {
                node_id: "org-isolation-test".to_string(),
                peers: Vec::new(),
                shard_count,
                data_dir: None,
            },
            Transport::loopback(registry),
        ))
    }

    pub async fn json(response: Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&body).expect("JSON response")
    }
}

#[cfg(test)]
mod peer_plane_limit_tests {
    use std::future::IntoFuture;

    use axum::Router;
    use tower_http::limit::RequestBodyLimitLayer;

    use crate::state::{Command, StateMachine};
    use crate::transport::{InstallSnapshotReq, InstallSnapshotResp};

    /// A snapshot install whose body exceeds the client-plane cap must still be
    /// accepted on the peer plane. With the peer plane capped at
    /// `MAX_BODY_BYTES`, any shard whose state outgrew 1 MiB could never again
    /// bring a lagging follower up to date (every install would 413), so this
    /// pins the regression with a real HTTP round trip through the same layer
    /// stack `main` builds for the peer listener.
    #[tokio::test]
    async fn snapshot_install_larger_than_client_cap_is_accepted_on_peer_plane() {
        let node = crate::test_support::node(1);
        let app = Router::new()
            .nest("/raft", crate::raft_api::router())
            .with_state(node)
            .layer(RequestBodyLimitLayer::new(super::MAX_PEER_BODY_BYTES))
            .layer(axum::extract::DefaultBodyLimit::max(
                super::MAX_PEER_BODY_BYTES,
            ));

        // Build a genuine >1 MiB state-machine image (a committed 2 MiB value).
        let machine = StateMachine::new();
        machine.apply(Command::KvPut {
            key: "big".to_string(),
            value: "v".repeat(2 * 1024 * 1024),
            ttl_ms: None,
            prev_revision: None,
        });
        let request = InstallSnapshotReq {
            term: 1_000_000, // above any term the fresh test shard could reach
            leader_id: "peer-test-leader".to_string(),
            last_included_index: 5,
            last_included_term: 1_000_000,
            state: machine.snapshot().expect("snapshot serializes"),
        };
        let body = serde_json::to_vec(&request).expect("request serializes");
        assert!(
            body.len() > super::MAX_BODY_BYTES,
            "test must exceed the client-plane cap to prove the peer cap differs"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(axum::serve(listener, app).into_future());

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/raft/0/snapshot"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .expect("peer-plane request");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "oversized-for-client snapshot must not be rejected on the peer plane"
        );
        let resp: InstallSnapshotResp = response.json().await.expect("snapshot response");
        assert!(resp.success, "follower must accept and ack the snapshot");
        assert_eq!(resp.match_index, 5);

        server.abort();
    }
}
