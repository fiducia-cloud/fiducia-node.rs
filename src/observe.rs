//! Read-only observability surface for operators and the admin UI.
//!
//! These endpoints expose coordination state that the primitive APIs deliberately
//! do not. Tenant inventories (locks, semaphores, elections) are filtered to the
//! validated caller org; node-wide shard health and aggregate metrics contain no
//! tenant identities. Every route is a read; none mutate state.
//!
//! Lock/semaphore inventory is a leader-gated read of the single lock-coordinator
//! shard, so a node that does not lead that shard returns a `NotLeader` redirect
//! (the load balancer / admin tier follows it). Elections, shards, and metrics
//! are answered from local applied state on whatever node is asked.
//!
//! Routes (mounted under `/v1/observe`):
//!   * `GET /v1/observe/locks`      — caller-org grants + FIFO waiters
//!   * `GET /v1/observe/semaphores` — caller-org permits and waiters
//!   * `GET /v1/observe/elections`  — caller-org election leaders
//!   * `GET /v1/observe/shards`     — per-shard Raft role/term/quorum + a rollup
//!   * `GET /v1/observe/metrics`    — per-op call counts, error rate, latency

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;

use crate::consensus::{read_error_response, Node, Role};
use crate::org_scope::OrgScope;

/// Header the load balancer forwards carrying the caller's granted scopes
/// (space-separated), derived from the verified API key / JWT.
const SCOPES_HEADER: &str = "x-fiducia-scopes";

/// Scopes that may observe a tenant's coordination *inventory* (every holder +
/// fencing token in the org). This is an operator surface, not a per-key read.
const ADMIN_OBSERVE_SCOPES: &[&str] = &["*", "admin:*", "admin:read", "admin:write"];

/// Defense-in-depth admin gate for the tenant-inventory observe reads
/// (`/v1/observe/{locks,semaphores,elections}`), which return *every* holder and
/// fencing token in the caller org — a fencing token is a capability, so a plain
/// `locks:read` key must not be able to enumerate them.
///
/// The load balancer already gates `/v1/observe/*` behind an admin scope (see
/// `required_scopes_for_route` in fiducia-load-balance). This is the node-side
/// backstop: if a request still arrives carrying a forwarded scope set that lacks
/// an admin scope (an LB misconfiguration, a future direct-admin caller, or a
/// narrower key than the LB gate assumed), the node refuses it too.
///
/// When **no** scope header is present at all — auth disabled for local/dev, or
/// direct in-cluster node introspection inside the trusted-hop boundary — the
/// check is skipped so those paths keep working. Reaching the node already
/// requires the internal-auth secret, so "no scopes" is not an untrusted caller.
fn require_admin_scope(headers: &HeaderMap) -> Result<(), Response> {
    let Some(raw) = headers.get(SCOPES_HEADER).and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };
    let has_admin = raw
        .split_whitespace()
        .any(|granted| ADMIN_OBSERVE_SCOPES.contains(&granted));
    if has_admin {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "insufficient_scope",
            "detail": "observing the tenant coordination inventory requires an admin scope",
            "required_scopes": ["admin:read", "admin:write"],
        })),
    )
        .into_response())
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/locks", get(locks))
        .route("/semaphores", get(semaphores))
        .route("/elections", get(elections))
        .route("/shards", get(shards))
        .route("/metrics", get(metrics))
}

/// `GET /v1/observe/locks` — active lock grants and FIFO waiters for the caller
/// org (leader-gated read of the lock-coordinator shard).
async fn locks(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Err(denied) = require_admin_scope(&headers) {
        return denied;
    }
    match node.lock_inventory().await {
        Ok(mut inv) => {
            inv.held.retain(|holding| {
                !holding.keys.is_empty()
                    && holding.keys.iter().all(|key| org.unscope(key).is_some())
            });
            inv.wait_queue.retain(|waiter| {
                !waiter.keys.is_empty() && waiter.keys.iter().all(|key| org.unscope(key).is_some())
            });
            org.response(json!({
                "held": inv.held.len(),
                "waiting": inv.wait_queue.len(),
                "inventory": inv,
            }))
        }
        Err(err) => read_error_response(err, &uri),
    }
}

/// `GET /v1/observe/semaphores` — caller-org counting semaphores with holders,
/// free permits, and waiters.
async fn semaphores(State(node): State<Arc<Node>>, org: OrgScope, uri: Uri) -> Response {
    match node.semaphore_inventory().await {
        Ok(mut list) => {
            list.retain(|semaphore| org.unscope(&semaphore.key).is_some());
            org.response(json!({ "count": list.len(), "semaphores": list }))
        }
        Err(err) => read_error_response(err, &uri),
    }
}

/// `GET /v1/observe/elections` — current leaders for caller-org elections,
/// merged across all shards this node hosts.
async fn elections(State(node): State<Arc<Node>>, org: OrgScope) -> Response {
    let elections: Vec<_> = node
        .list_elections()
        .await
        .into_iter()
        .filter(|entry| org.unscope(&entry.name).is_some())
        .collect();
    org.response(json!({ "count": elections.len(), "elections": elections }))
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
    let storage_faulted: Vec<_> = status
        .shards
        .iter()
        .filter(|s| !s.storage_healthy)
        .map(|s| s.shard_id)
        .collect();
    let status_complete = status.hosted_shards.len() == status.shard_count as usize
        && status.unresponsive_shards.is_empty()
        && status.shards.len() == status.hosted_shards.len();

    Json(json!({
        "node_id": status.node_id,
        "shard_count": status.shard_count,
        "leader_count": status.leader_count,
        "follower_count": status.follower_count,
        "quorum": {
            "leaderless_shards": leaderless,
            "at_risk_led_shards": at_risk,
            "all_led_shards_have_quorum": status_complete && at_risk.is_empty(),
            "storage_faulted_shards": storage_faulted,
            "unresponsive_shards": status.unresponsive_shards,
            "status_complete": status_complete,
            "all_shard_storage_healthy": status_complete && storage_faulted.is_empty(),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::state::Command;

    #[tokio::test]
    async fn tenant_observability_inventory_never_leaks_other_org_entries() {
        let node = crate::test_support::node(4);
        let org_a = OrgScope("org-a".into());
        let org_b = OrgScope("org-b".into());

        for org in [&org_a, &org_b] {
            node.propose(Command::LockAcquire {
                keys: vec![org.scope("shared-lock")],
                holder: org.scope("worker"),
                ttl_ms: 30_000,
                wait: false,
            })
            .await
            .expect("lock commit");
            node.propose(Command::SemaphoreAcquire {
                key: org.scope("shared-pool"),
                holder: org.scope("worker"),
                limit: 2,
                ttl_ms: 30_000,
                wait: false,
            })
            .await
            .expect("semaphore commit");
            node.propose(Command::ElectionCampaign {
                name: org.scope("scheduler"),
                candidate: "candidate".to_string(),
                ttl_ms: 30_000,
                metadata: HashMap::new(),
            })
            .await
            .expect("election commit");
        }

        for org in [org_a, org_b] {
            let lock_inventory = crate::test_support::json(
                locks(
                    State(node.clone()),
                    org.clone(),
                    Uri::from_static("/v1/observe/locks"),
                )
                .await,
            )
            .await;
            assert_eq!(lock_inventory["held"], 1);
            assert_eq!(
                lock_inventory["inventory"]["held"][0]["keys"],
                json!(["shared-lock"])
            );
            assert_eq!(lock_inventory["inventory"]["held"][0]["holder"], "worker");

            let semaphore_inventory = crate::test_support::json(
                semaphores(
                    State(node.clone()),
                    org.clone(),
                    Uri::from_static("/v1/observe/semaphores"),
                )
                .await,
            )
            .await;
            assert_eq!(semaphore_inventory["count"], 1);
            assert_eq!(semaphore_inventory["semaphores"][0]["key"], "shared-pool");

            let election_inventory =
                crate::test_support::json(elections(State(node.clone()), org).await).await;
            assert_eq!(election_inventory["count"], 1);
            assert_eq!(election_inventory["elections"][0]["name"], "scheduler");
        }
    }
}
