//! Service discovery handlers.
//!
//! A registry of live service instances with TTL-based health: instances
//! register an address, heartbeat to stay listed, and silently drop out when
//! their lease expires (crash-safe — no stale endpoints). Mutations are proposed
//! to the service-discovery coordinator shard; reads go through [`Node::query`].
//! That keeps the service-name registry linearizable while still allowing each
//! service lookup to return only that service's live instances.
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
    extract::{Path, Query, State},
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

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::{Command, ServiceInstance};

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
async fn list_services(State(node): State<Arc<Node>>, org: OrgScope) -> Response {
    let services: Vec<_> = node
        .list_services()
        .await
        .into_iter()
        .filter_map(|mut summary| {
            summary.service = org.unscope(&summary.service)?.to_string();
            Some(summary)
        })
        .collect();
    Json(json!({ "count": services.len(), "services": services })).into_response()
}

/// `GET /v1/services/{service}` — list live instances, optionally filtered by
/// exact metadata matches such as `?metadata.region=us-east`.
async fn list_instances(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(service): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let filters = metadata_filters(&query);
    match node
        .query(ReadRequest::Service {
            service: org.scope(&service),
        })
        .await
    {
        Ok(ReadResponse::Service(instances)) => {
            let instances = filter_instances(instances, &filters);
            Json(json!({ "service": service, "instances": instances })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

fn metadata_filters(query: &HashMap<String, String>) -> HashMap<String, String> {
    query
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("metadata.")
                .filter(|metadata_key| !metadata_key.trim().is_empty())
                .map(|metadata_key| (metadata_key.to_string(), value.to_string()))
        })
        .collect()
}

fn filter_instances(
    instances: Vec<ServiceInstance>,
    filters: &HashMap<String, String>,
) -> Vec<ServiceInstance> {
    if filters.is_empty() {
        return instances;
    }
    instances
        .into_iter()
        .filter(|instance| {
            filters
                .iter()
                .all(|(key, value)| instance.metadata.get(key) == Some(value))
        })
        .collect()
}

/// `PUT /v1/services/{service}/instances/{id}` — register/refresh an instance.
async fn register(
    State(node): State<Arc<Node>>,
    org: OrgScope,
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
            service: org.scope(&service),
            instance_id: id,
            address: body.address,
            ttl_ms: body.ttl_ms,
            metadata,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `POST /v1/services/{service}/instances/{id}/heartbeat` — renew the lease.
async fn heartbeat(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path((service, id)): Path<(String, String)>,
    Json(body): Json<HeartbeatBody>,
) -> Response {
    let result = node
        .propose(Command::ServiceHeartbeat {
            service: org.scope(&service),
            instance_id: id,
            ttl_ms: body.ttl_ms,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `DELETE /v1/services/{service}/instances/{id}` — deregister an instance.
async fn deregister(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path((service, id)): Path<(String, String)>,
) -> Response {
    let result = node
        .propose(Command::ServiceDeregister {
            service: org.scope(&service),
            instance_id: id,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/services/{service}/watch` — SSE stream of instance add/remove events.
///
/// Subscribes to the owning shard's change broadcast and emits one SSE event per
/// committed `register`/`heartbeat`/`deregister` for this service, so clients can
/// keep a live view of the instance set instead of polling. (TTL-expiry removals
/// surface on the next read/registration rather than as a push event.)
async fn watch(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    Path(service): Path<String>,
) -> Response {
    // Service events are committed on the single SERVICE_DOMAIN shard (writes all
    // route there), so subscribe to that shard's broadcast — not shard_for(service)
    // — then filter to this service below. Watching the name-hashed shard would miss
    // every event for services whose name doesn't hash to SERVICE_DOMAIN.
    let Some(rx) = node.watch(crate::state::SERVICE_DOMAIN).await else {
        return Json(
            json!({ "error": "unavailable", "op": "discovery.watch", "service": service }),
        )
        .into_response();
    };
    let scoped_service = org.scope(&service);
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        if event.scope != "service" || event.key != scoped_service {
            return None;
        }
        let mut value = serde_json::to_value(&event).ok()?;
        if !org.unscope_value(&mut value) {
            return None;
        }
        Some(Ok::<Event, Infallible>(
            Event::default()
                .event(event.kind)
                .json_data(value)
                .unwrap_or_else(|_| Event::default().comment("serialize-error")),
        ))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_filters_only_accept_prefixed_nonempty_keys() {
        let query = HashMap::from([
            ("metadata.region".to_string(), "us-east".to_string()),
            ("metadata.".to_string(), "ignored".to_string()),
            ("limit".to_string(), "10".to_string()),
        ]);

        assert_eq!(
            metadata_filters(&query),
            HashMap::from([("region".to_string(), "us-east".to_string())])
        );
    }

    #[test]
    fn filter_instances_requires_all_metadata_matches() {
        let instances = vec![
            instance(
                "a",
                [
                    ("region".to_string(), "us-east".to_string()),
                    ("version".to_string(), "blue".to_string()),
                ],
            ),
            instance(
                "b",
                [
                    ("region".to_string(), "us-east".to_string()),
                    ("version".to_string(), "green".to_string()),
                ],
            ),
            instance(
                "c",
                [
                    ("region".to_string(), "eu-west".to_string()),
                    ("version".to_string(), "blue".to_string()),
                ],
            ),
        ];
        let filters = HashMap::from([
            ("region".to_string(), "us-east".to_string()),
            ("version".to_string(), "blue".to_string()),
        ]);

        let filtered = filter_instances(instances, &filters);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].instance_id, "a");
    }

    fn instance<const N: usize>(id: &str, metadata: [(String, String); N]) -> ServiceInstance {
        ServiceInstance {
            instance_id: id.to_string(),
            address: format!("http://{id}.internal"),
            lease_expires_ms: u64::MAX,
            metadata: HashMap::from(metadata),
        }
    }

    #[tokio::test]
    async fn service_inventory_and_instances_are_filtered_per_org() {
        let node = crate::test_support::node(4);
        let org_a = OrgScope("org-a".into());
        let org_b = OrgScope("org-b".into());

        for (org, id) in [(org_a.clone(), "a-1"), (org_b.clone(), "b-1")] {
            let response = register(
                State(node.clone()),
                org,
                Uri::from_static("/v1/services/api/instances/test"),
                Path(("api".to_string(), id.to_string())),
                Json(RegisterBody {
                    address: format!("http://{id}.internal"),
                    ttl_ms: 30_000,
                    metadata: None,
                }),
            )
            .await;
            let body = crate::test_support::json(response).await;
            assert_eq!(body["result"]["output"]["registered"], true);
            assert_eq!(body["result"]["output"]["service"], "api");
        }

        for (org, own_id) in [(org_a, "a-1"), (org_b, "b-1")] {
            let inventory =
                crate::test_support::json(list_services(State(node.clone()), org.clone()).await)
                    .await;
            assert_eq!(inventory["count"], 1);
            assert_eq!(inventory["services"][0]["service"], "api");
            assert_eq!(inventory["services"][0]["instances"], 1);

            let instances = crate::test_support::json(
                list_instances(
                    State(node.clone()),
                    org,
                    Uri::from_static("/v1/services/api"),
                    Path("api".to_string()),
                    Query(HashMap::new()),
                )
                .await,
            )
            .await;
            assert_eq!(instances["service"], "api");
            assert_eq!(instances["instances"].as_array().unwrap().len(), 1);
            assert_eq!(instances["instances"][0]["instance_id"], own_id);
        }
    }
}
