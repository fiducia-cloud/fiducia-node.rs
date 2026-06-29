//! Read-only observability surface for operators and the admin UI.
//!
//! These endpoints expose the *internal* coordination state that the primitive
//! APIs deliberately don't: who holds every lock and who is queued behind it,
//! semaphore lease state, the full election leadership table, per-shard Raft
//! health + quorum, and aggregated call latency. Every route is a read; none
//! mutate state.
//!
//! Lock/semaphore inventory is a leader-gated read of the single lock-coordinator
//! shard, so a node that does not lead that shard returns a `NotLeader` redirect
//! (the load balancer / admin tier follows it). Elections, shards, and metrics
//! are answered from local applied state on whatever node is asked.
//!
//! Routes (mounted under `/v1/observe`):
//!   * `GET /v1/observe/locks`      — every lock grant + the FIFO wait queue
//!   * `GET /v1/observe/semaphores` — every semaphore: holders, free permits, queue
//!   * `GET /v1/observe/elections`  — every named election's current leader
//!   * `GET /v1/observe/shards`     — per-shard Raft role/term/quorum + a rollup
//!   * `GET /v1/observe/metrics`    — per-op call counts, error rate, latency

use std::sync::Arc;

use axum::{
    extract::State,
    http::Uri,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;

use crate::consensus::{read_error_response, Node, Role};

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/locks", get(locks))
        .route("/semaphores", get(semaphores))
        .route("/elections", get(elections))
        .route("/shards", get(shards))
        .route("/metrics", get(metrics))
}

/// `GET /v1/observe/locks` — every active lock grant and the single FIFO wait
/// queue behind them (leader-gated read of the lock-coordinator shard).
async fn locks(State(node): State<Arc<Node>>, uri: Uri) -> Response {
    match node.lock_inventory().await {
        Ok(inv) => Json(json!({
            "held": inv.held.len(),
            "waiting": inv.wait_queue.len(),
            "inventory": inv,
        }))
        .into_response(),
        Err(err) => read_error_response(err, &uri),
    }
}

/// `GET /v1/observe/semaphores` — every counting semaphore with its holders,
/// free permits, and wait queue.
async fn semaphores(State(node): State<Arc<Node>>, uri: Uri) -> Response {
    match node.semaphore_inventory().await {
        Ok(list) => Json(json!({ "count": list.len(), "semaphores": list })).into_response(),
        Err(err) => read_error_response(err, &uri),
    }
}

/// `GET /v1/observe/elections` — the current leader of every named election,
/// merged across all shards this node hosts.
async fn elections(State(node): State<Arc<Node>>) -> Response {
    let elections = node.list_elections().await;
    Json(json!({ "count": elections.len(), "elections": elections })).into_response()
}

/// `GET /v1/observe/shards` — per-shard Raft health (role, term, leader, commit /
/// applied / log indices, per-peer replication, quorum) plus a node-level rollup
/// highlighting shards that are leaderless or one failure from losing quorum.
async fn shards(State(node): State<Arc<Node>>) -> Response {
    let status = node.status().await;

    let leaderless: Vec<_> = status
        .shards
        .iter()
        .filter(|s| s.leader_id.is_none())
        .map(|s| s.shard_id)
        .collect();
    // Among shards this node leads, the ones that have lost their safety margin:
    // a majority is *not* caught up to the commit index, so one more failure
    // would stall the shard. Only the leader can judge this.
    let at_risk: Vec<_> = status
        .shards
        .iter()
        .filter(|s| s.role == Role::Leader && !s.has_quorum)
        .map(|s| s.shard_id)
        .collect();

    Json(json!({
        "node_id": status.node_id,
        "shard_count": status.shard_count,
        "leader_count": status.leader_count,
        "follower_count": status.follower_count,
        "quorum": {
            "leaderless_shards": leaderless,
            "at_risk_led_shards": at_risk,
            "all_led_shards_have_quorum": at_risk.is_empty(),
        },
        "shards": status.shards,
    }))
    .into_response()
}

/// `GET /v1/observe/metrics` — aggregated per-operation call counts, error
/// counts, and a cumulative latency histogram.
async fn metrics(State(node): State<Arc<Node>>) -> Response {
    let ops = node.metrics().snapshot();
    Json(json!({ "operations": ops })).into_response()
}
