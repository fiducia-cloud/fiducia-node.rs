//! Service discovery (skeleton handlers).
//!
//! A registry of live service instances with TTL-based health: instances
//! register an address, heartbeat to stay listed, and silently drop out when
//! their lease expires (crash-safe — no stale endpoints). Mutations are proposed
//! to the shard owning the service name; reads go through [`Node::query`]. A
//! service's instances all route to one shard, so listing a service is a
//! single-shard read.
//!
//! Routes (mounted under `/v1/services`):
//!   * `GET    /v1/services`                                  — list services
//!   * `GET    /v1/services/{service}`                        — list live instances
//!   * `PUT    /v1/services/{service}/instances/{id}`         — register `{ "address", "ttl_ms" }`
//!   * `POST   /v1/services/{service}/instances/{id}/heartbeat` — renew lease
//!   * `DELETE /v1/services/{service}/instances/{id}`         — deregister
//!   * `GET    /v1/services/{service}/watch`                  — SSE of instance changes

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::Uri,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct RegisterBody {
    pub address: String,
    pub ttl_ms: u64,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatBody {
    pub ttl_ms: Option<u64>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/", get(list_services))
        .route("/:service", get(list_instances))
        .route("/:service/watch", get(watch))
        .route("/:service/instances/:id", put(register).delete(deregister))
        .route(
            "/:service/instances/:id/heartbeat",
            axum::routing::post(heartbeat),
        )
}

/// `GET /v1/services` — list known service names with their live-instance counts.
///
/// Services span shards, so this fans a serializable read out across every shard
/// and merges the per-shard summaries.
async fn list_services(State(node): State<Arc<Node>>) -> Response {
    let services = node.list_services().await;
    Json(json!({ "count": services.len(), "services": services })).into_response()
}

/// `GET /v1/services/{service}` — list live (unexpired) instances.
async fn list_instances(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(service): Path<String>,
) -> Response {
    match node
        .query(ReadRequest::Service {
            service: service.clone(),
        })
        .await
    {
        Ok(ReadResponse::Service(instances)) => {
            Json(json!({ "service": service, "instances": instances })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `PUT /v1/services/{service}/instances/{id}` — register/refresh an instance.
async fn register(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path((service, id)): Path<(String, String)>,
    Json(body): Json<RegisterBody>,
) -> Response {
    let metadata = body.metadata.unwrap_or_default();
    if let Err(rejection) =
        crate::validate::service_register(&service, &id, &body.address, body.ttl_ms, &metadata)
    {
        return rejection.into_response();
    }
    let result = node
        .propose(Command::ServiceRegister {
            service,
            instance_id: id,
            address: body.address,
            ttl_ms: body.ttl_ms,
            metadata,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/services/{service}/instances/{id}/heartbeat` — renew the lease.
async fn heartbeat(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path((service, id)): Path<(String, String)>,
    Json(body): Json<HeartbeatBody>,
) -> Response {
    let result = node
        .propose(Command::ServiceHeartbeat {
            service,
            instance_id: id,
            ttl_ms: body.ttl_ms,
        })
        .await;
    propose_response(result, &uri)
}

/// `DELETE /v1/services/{service}/instances/{id}` — deregister an instance.
async fn deregister(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path((service, id)): Path<(String, String)>,
) -> Response {
    let result = node
        .propose(Command::ServiceDeregister {
            service,
            instance_id: id,
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/services/{service}/watch` — SSE stream of instance add/remove events.
///
/// Subscribes to the owning shard's change broadcast and emits one SSE event per
/// committed `register`/`heartbeat`/`deregister` for this service, so clients can
/// keep a live view of the instance set instead of polling. (TTL-expiry removals
/// surface on the next read/registration rather than as a push event.)
async fn watch(State(node): State<Arc<Node>>, Path(service): Path<String>) -> Response {
    let Some(rx) = node.watch(&service).await else {
        return Json(json!({ "error": "unavailable", "op": "discovery.watch", "service": service }))
            .into_response();
    };
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        if event.scope != "service" || event.key != service {
            return None;
        }
        Some(Ok::<Event, Infallible>(
            Event::default()
                .event(event.kind)
                .json_data(&event)
                .unwrap_or_else(|_| Event::default().comment("serialize-error")),
        ))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
