//! Service discovery handlers.
//!
//! A registry of live service instances with TTL-based health: instances
//! register an address, heartbeat to stay listed, and silently drop out when
//! their lease expires (crash-safe — no stale endpoints). Mutations are proposed
//! to the service-discovery coordinator shard; reads go through [`Node::query`].
//! That keeps the service-name registry linearizable while still allowing each
//! service lookup to return only that service's live instances.
//!
//! Discovery is intentionally a typed Raft-backed registry, not a second copy of
//! the same data encoded into generic KV keys. It does share the shard's committed
//! change broadcast with KV/election watches. The broadcast is an acceleration
//! channel, not durable truth: a watcher starts with an authoritative snapshot,
//! receives committed deltas, refreshes after every matching delta, resynchronizes
//! after receiver lag, and refreshes at lease expiry. A reconnect therefore always
//! reconstructs current state even when an earlier process missed notifications.
//!
//! Routes (mounted under `/v1/services`):
//!   * `GET    /v1/services`                                  — list services
//!   * `GET    /v1/services/{service}`                        — list live instances
//!   * `PUT    /v1/services/{service}/instances/{id}`         — register `{ "address", "ttl_ms" }`
//!   * `POST   /v1/services/{service}/instances/{id}/heartbeat` — renew lease
//!   * `DELETE /v1/services/{service}/instances/{id}`         — deregister
//!   * `GET    /v1/services/{service}/watch`                  — SSE snapshot + deltas

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    http::{header::CACHE_CONTROL, HeaderValue, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::consensus::{
    read_error_response, ChangeEvent, Node, ProposeError, ReadRequest, ReadResponse,
};
use crate::org_scope::OrgScope;
use crate::state::{Command, ServiceInstance};

/// Bound each client independently. A slow client can make this queue fill, but
/// cannot make the process allocate without limit. Once the queue drains, the
/// shard broadcast reports receiver lag and the watcher sends a fresh snapshot.
const WATCH_QUEUE_CAPACITY: usize = 32;
/// Recheck long leases periodically so a wall-clock correction cannot postpone
/// expiry reconciliation indefinitely. Short leases schedule their exact expiry.
const WATCH_MAX_RECONCILE_MS: u64 = 60_000;
/// Read just after the encoded millisecond expiry rather than racing the same tick.
const WATCH_EXPIRY_SAFETY_MARGIN_MS: u64 = 2;
const WATCH_RETRY_AFTER: Duration = Duration::from_secs(1);

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
    // A heartbeat re-stamps the registration's expiry, so it is bounded exactly
    // like the register path is.
    if let Some(ttl_ms) = body.ttl_ms {
        if let Err(rejection) = crate::validate::ttl(ttl_ms) {
            return rejection.into_response();
        }
    }
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

#[derive(Debug)]
enum SnapshotReadError {
    Consensus(ProposeError),
    UnexpectedResponse,
}

async fn read_service_instances(
    node: &Node,
    scoped_service: &str,
) -> Result<Vec<ServiceInstance>, SnapshotReadError> {
    match node
        .query(ReadRequest::Service {
            service: scoped_service.to_string(),
        })
        .await
    {
        Ok(ReadResponse::Service(instances)) => Ok(instances),
        Ok(_) => Err(SnapshotReadError::UnexpectedResponse),
        Err(err) => Err(SnapshotReadError::Consensus(err)),
    }
}

#[derive(Debug)]
struct WatchMessage {
    event: &'static str,
    id: Option<u64>,
    data: Value,
}

impl WatchMessage {
    fn into_sse(self) -> Event {
        let mut event = Event::default()
            .event(self.event)
            .retry(WATCH_RETRY_AFTER)
            .data(self.data.to_string());
        if let Some(id) = self.id {
            event = event.id(id.to_string());
        }
        event
    }
}

fn change_message(event: &ChangeEvent, org: &OrgScope) -> Result<WatchMessage, &'static str> {
    let mut value = serde_json::to_value(event).map_err(|_| "serialize_change")?;
    if !org.unscope_value(&mut value) {
        return Err("org_scope_violation");
    }
    Ok(WatchMessage {
        event: event.kind,
        id: Some(event.revision),
        data: value,
    })
}

fn snapshot_message(
    service: &str,
    reason: &'static str,
    trigger_revision: Option<u64>,
    skipped_events: Option<u64>,
    instances: Vec<ServiceInstance>,
) -> WatchMessage {
    let mut data = json!({
        "scope": "service",
        "kind": "snapshot",
        "service": service,
        "trigger_revision": trigger_revision,
        "reason": reason,
        "authoritative": true,
        "instances": instances,
    });
    if let Some(skipped_events) = skipped_events {
        data["skipped_events"] = json!(skipped_events);
    }
    WatchMessage {
        event: "snapshot",
        id: trigger_revision,
        data,
    }
}

fn unavailable_message(service: &str, reason: &'static str) -> WatchMessage {
    WatchMessage {
        event: "unavailable",
        id: None,
        data: json!({
            "scope": "service",
            "kind": "unavailable",
            "service": service,
            "reason": reason,
            "retryable": true,
        }),
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn reconcile_delay_at(instances: &[ServiceInstance], now_ms: u64) -> Duration {
    let delay_ms = instances
        .iter()
        .map(|instance| instance.lease_expires_ms)
        .min()
        .map(|expires_ms| {
            expires_ms
                .saturating_sub(now_ms)
                .saturating_add(WATCH_EXPIRY_SAFETY_MARGIN_MS)
                .clamp(1, WATCH_MAX_RECONCILE_MS)
        })
        .unwrap_or(WATCH_MAX_RECONCILE_MS);
    Duration::from_millis(delay_ms)
}

fn reconcile_delay(instances: &[ServiceInstance]) -> Duration {
    reconcile_delay_at(instances, unix_now_ms())
}

async fn publish_snapshot(
    node: &Arc<Node>,
    tx: &mpsc::Sender<WatchMessage>,
    service: &str,
    scoped_service: &str,
    reason: &'static str,
    revision: Option<u64>,
    skipped_events: Option<u64>,
) -> Option<Duration> {
    let instances = match read_service_instances(node, scoped_service).await {
        Ok(instances) => instances,
        Err(SnapshotReadError::Consensus(_)) => {
            let _ = tx
                .send(unavailable_message(service, "consensus_unavailable"))
                .await;
            return None;
        }
        Err(SnapshotReadError::UnexpectedResponse) => {
            let _ = tx
                .send(unavailable_message(service, "unexpected_read_response"))
                .await;
            return None;
        }
    };
    let next_reconcile = reconcile_delay(&instances);
    if tx
        .send(snapshot_message(
            service,
            reason,
            revision,
            skipped_events,
            instances,
        ))
        .await
        .is_err()
    {
        return None;
    }
    Some(next_reconcile)
}

async fn run_watch(
    node: Arc<Node>,
    org: OrgScope,
    service: String,
    scoped_service: String,
    mut changes: broadcast::Receiver<ChangeEvent>,
    tx: mpsc::Sender<WatchMessage>,
    initial_instances: Vec<ServiceInstance>,
) {
    let initial_delay = reconcile_delay(&initial_instances);
    if tx
        .send(snapshot_message(
            &service,
            "initial",
            None,
            None,
            initial_instances,
        ))
        .await
        .is_err()
    {
        return;
    }

    let reconcile = tokio::time::sleep(initial_delay);
    tokio::pin!(reconcile);

    loop {
        tokio::select! {
            _ = tx.closed() => return,
            received = changes.recv() => match received {
                Ok(event) => {
                    if event.scope != "service" || event.key != scoped_service {
                        continue;
                    }
                    let revision = event.revision;
                    let message = match change_message(&event, &org) {
                        Ok(message) => message,
                        Err(reason) => {
                            let _ = tx.send(unavailable_message(&service, reason)).await;
                            return;
                        }
                    };
                    if tx.send(message).await.is_err() {
                        return;
                    }
                    let Some(next_delay) = publish_snapshot(
                        &node,
                        &tx,
                        &service,
                        &scoped_service,
                        "change",
                        Some(revision),
                        None,
                    )
                    .await
                    else {
                        return;
                    };
                    reconcile
                        .as_mut()
                        .reset(tokio::time::Instant::now() + next_delay);
                }
                Err(broadcast::error::RecvError::Lagged(skipped_events)) => {
                    let Some(next_delay) = publish_snapshot(
                        &node,
                        &tx,
                        &service,
                        &scoped_service,
                        "lagged",
                        None,
                        Some(skipped_events),
                    )
                    .await
                    else {
                        return;
                    };
                    reconcile
                        .as_mut()
                        .reset(tokio::time::Instant::now() + next_delay);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    let _ = tx
                        .send(unavailable_message(&service, "change_bus_closed"))
                        .await;
                    return;
                }
            },
            _ = &mut reconcile => {
                let Some(next_delay) = publish_snapshot(
                    &node,
                    &tx,
                    &service,
                    &scoped_service,
                    "lease_reconcile",
                    None,
                    None,
                )
                .await
                else {
                    return;
                };
                reconcile
                    .as_mut()
                    .reset(tokio::time::Instant::now() + next_delay);
            }
        }
    }
}

/// `GET /v1/services/{service}/watch` — SSE snapshot plus committed changes.
///
/// The watch subscribes before its initial linearizable read, so a concurrent
/// mutation is either present in that snapshot or queued on the broadcast (and
/// may safely appear twice). Each matching delta is followed by an authoritative
/// snapshot. Receiver lag therefore becomes an explicit resynchronization rather
/// than a silent gap, and a lease-expiry timer publishes removals even when no
/// later write happens. Consumers should replace their local instance set on every
/// `snapshot` event and treat `register`/`heartbeat`/`deregister` as low-latency
/// hints; the broadcast itself is not a retained cross-restart event log.
async fn watch(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Path(service): Path<String>,
) -> Response {
    // Service events are committed on the single SERVICE_DOMAIN shard (writes all
    // route there), so subscribe to that shard's broadcast — not shard_for(service)
    // — then filter to this service below. Watching the name-hashed shard would miss
    // every event for services whose name doesn't hash to SERVICE_DOMAIN.
    let Some(changes) = node.watch(crate::state::SERVICE_DOMAIN).await else {
        return Json(
            json!({ "error": "unavailable", "op": "discovery.watch", "service": service }),
        )
        .into_response();
    };
    let scoped_service = org.scope(&service);
    // Subscribe first, then read. A mutation racing this establishment is either
    // reflected by the read or queued in `changes`; duplicate refreshes are safe.
    let initial_instances = match read_service_instances(&node, &scoped_service).await {
        Ok(instances) => instances,
        Err(SnapshotReadError::Consensus(err)) => return read_error_response(err, &uri),
        Err(SnapshotReadError::UnexpectedResponse) => {
            return Json(json!({
                "error": "unavailable",
                "op": "discovery.watch.snapshot",
                "service": service,
            }))
            .into_response();
        }
    };

    let (tx, outgoing) = mpsc::channel(WATCH_QUEUE_CAPACITY);
    tokio::spawn(run_watch(
        node,
        org,
        service,
        scoped_service,
        changes,
        tx,
        initial_instances,
    ));
    let stream = ReceiverStream::new(outgoing)
        .map(|message| Ok::<Event, Infallible>(message.into_sse()));
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response
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

    #[test]
    fn reconcile_delay_targets_earliest_expiry_and_is_bounded() {
        let instances = vec![
            ServiceInstance {
                instance_id: "later".into(),
                address: "http://later.internal".into(),
                lease_expires_ms: 5_000,
                metadata: HashMap::new(),
            },
            ServiceInstance {
                instance_id: "first".into(),
                address: "http://first.internal".into(),
                lease_expires_ms: 1_200,
                metadata: HashMap::new(),
            },
        ];
        assert_eq!(
            reconcile_delay_at(&instances, 1_000),
            Duration::from_millis(202)
        );
        assert_eq!(
            reconcile_delay_at(&instances, 10_000),
            Duration::from_millis(WATCH_EXPIRY_SAFETY_MARGIN_MS)
        );
        assert_eq!(
            reconcile_delay_at(&[], 1_000),
            Duration::from_millis(WATCH_MAX_RECONCILE_MS)
        );

        let far_future = vec![ServiceInstance {
            instance_id: "far".into(),
            address: "http://far.internal".into(),
            lease_expires_ms: u64::MAX,
            metadata: HashMap::new(),
        }];
        assert_eq!(
            reconcile_delay_at(&far_future, 1_000),
            Duration::from_millis(WATCH_MAX_RECONCILE_MS)
        );
    }

    #[test]
    fn change_messages_are_revisioned_and_unscoped() {
        let org = OrgScope("org-a".into());
        let event = ChangeEvent {
            scope: "service",
            kind: "register",
            key: org.scope("api"),
            revision: 42,
            detail: Some(json!({ "instance": instance("a-1", []) })),
        };

        let message = change_message(&event, &org).expect("change message");
        assert_eq!(message.event, "register");
        assert_eq!(message.id, Some(42));
        assert_eq!(message.data["key"], "api");
        assert_eq!(message.data["revision"], 42);
        assert_eq!(message.data["detail"]["instance"]["instance_id"], "a-1");
    }

    #[test]
    fn snapshot_messages_are_explicitly_authoritative() {
        let message = snapshot_message(
            "api",
            "lagged",
            None,
            Some(17),
            vec![instance("a-1", [])],
        );

        assert_eq!(message.event, "snapshot");
        assert_eq!(message.id, None);
        assert_eq!(message.data["kind"], "snapshot");
        assert_eq!(message.data["service"], "api");
        assert_eq!(message.data["reason"], "lagged");
        assert_eq!(message.data["authoritative"], true);
        assert_eq!(message.data["skipped_events"], 17);
        assert_eq!(message.data["instances"][0]["instance_id"], "a-1");
    }

    fn instance<const N: usize>(id: &str, metadata: [(String, String); N]) -> ServiceInstance {
        ServiceInstance {
            instance_id: id.to_string(),
            address: format!("http://{id}.internal"),
            lease_expires_ms: u64::MAX,
            metadata: HashMap::from(metadata),
        }
    }

    async fn propose_register(
        node: &Arc<Node>,
        org: &OrgScope,
        service: &str,
        instance_id: &str,
        ttl_ms: u64,
    ) {
        node.propose(Command::ServiceRegister {
            service: org.scope(service),
            instance_id: instance_id.to_string(),
            address: format!("http://{instance_id}.internal"),
            ttl_ms,
            metadata: HashMap::new(),
        })
        .await
        .expect("register proposal");
    }

    async fn start_test_watch(
        node: Arc<Node>,
        org: OrgScope,
        service: &str,
    ) -> mpsc::Receiver<WatchMessage> {
        let scoped_service = org.scope(service);
        let changes = node
            .watch(crate::state::SERVICE_DOMAIN)
            .await
            .expect("service watch receiver");
        let initial_instances = read_service_instances(&node, &scoped_service)
            .await
            .expect("initial service snapshot");
        let (tx, outgoing) = mpsc::channel(WATCH_QUEUE_CAPACITY);
        tokio::spawn(run_watch(
            node,
            org,
            service.to_string(),
            scoped_service,
            changes,
            tx,
            initial_instances,
        ));
        outgoing
    }

    async fn next_message(outgoing: &mut mpsc::Receiver<WatchMessage>) -> WatchMessage {
        tokio::time::timeout(Duration::from_secs(2), outgoing.recv())
            .await
            .expect("watch message timeout")
            .expect("watch stream closed")
    }

    #[tokio::test]
    async fn watch_starts_with_snapshot_and_tracks_committed_mutations() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        let mut outgoing = start_test_watch(node.clone(), org.clone(), "api").await;

        let initial = next_message(&mut outgoing).await;
        assert_eq!(initial.event, "snapshot");
        assert_eq!(initial.data["reason"], "initial");
        assert!(initial.data["instances"].as_array().unwrap().is_empty());

        propose_register(&node, &org, "api", "a-1", 30_000).await;
        let register = next_message(&mut outgoing).await;
        let register_revision = register.id.expect("register revision");
        assert_eq!(register.event, "register");
        assert_eq!(register.data["key"], "api");
        let register_snapshot = next_message(&mut outgoing).await;
        assert_eq!(register_snapshot.event, "snapshot");
        assert_eq!(register_snapshot.id, Some(register_revision));
        assert_eq!(register_snapshot.data["reason"], "change");
        assert_eq!(
            register_snapshot.data["instances"][0]["instance_id"],
            "a-1"
        );

        node.propose(Command::ServiceHeartbeat {
            service: org.scope("api"),
            instance_id: "a-1".into(),
            ttl_ms: Some(60_000),
        })
        .await
        .expect("heartbeat proposal");
        let heartbeat = next_message(&mut outgoing).await;
        let heartbeat_revision = heartbeat.id.expect("heartbeat revision");
        assert_eq!(heartbeat.event, "heartbeat");
        assert!(heartbeat_revision > register_revision);
        let heartbeat_snapshot = next_message(&mut outgoing).await;
        assert_eq!(heartbeat_snapshot.event, "snapshot");
        assert_eq!(heartbeat_snapshot.id, Some(heartbeat_revision));
        assert_eq!(heartbeat_snapshot.data["instances"].as_array().unwrap().len(), 1);

        node.propose(Command::ServiceDeregister {
            service: org.scope("api"),
            instance_id: "a-1".into(),
        })
        .await
        .expect("deregister proposal");
        let deregister = next_message(&mut outgoing).await;
        let deregister_revision = deregister.id.expect("deregister revision");
        assert_eq!(deregister.event, "deregister");
        assert!(deregister_revision > heartbeat_revision);
        let deregister_snapshot = next_message(&mut outgoing).await;
        assert_eq!(deregister_snapshot.event, "snapshot");
        assert_eq!(deregister_snapshot.id, Some(deregister_revision));
        assert!(
            deregister_snapshot.data["instances"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn watch_filters_other_services_and_organizations() {
        let node = crate::test_support::node(4);
        let org_a = OrgScope("org-a".into());
        let org_b = OrgScope("org-b".into());
        let mut outgoing = start_test_watch(node.clone(), org_a.clone(), "api").await;
        let _initial = next_message(&mut outgoing).await;

        propose_register(&node, &org_b, "api", "b-1", 30_000).await;
        propose_register(&node, &org_a, "worker", "worker-1", 30_000).await;
        propose_register(&node, &org_a, "api", "a-1", 30_000).await;

        let matching = next_message(&mut outgoing).await;
        assert_eq!(matching.event, "register");
        assert_eq!(matching.data["key"], "api");
        assert_eq!(
            matching.data["detail"]["instance"]["instance_id"],
            "a-1"
        );
        let snapshot = next_message(&mut outgoing).await;
        assert_eq!(snapshot.data["instances"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot.data["instances"][0]["instance_id"], "a-1");
    }

    #[tokio::test]
    async fn lagged_watch_resynchronizes_from_authoritative_state() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        propose_register(&node, &org, "api", "a-1", 30_000).await;

        let scoped_service = org.scope("api");
        let (changes_tx, changes) = broadcast::channel(1);
        for revision in 1..=3 {
            changes_tx
                .send(ChangeEvent {
                    scope: "service",
                    kind: "heartbeat",
                    key: scoped_service.clone(),
                    revision,
                    detail: None,
                })
                .expect("queued change");
        }
        let (tx, mut outgoing) = mpsc::channel(WATCH_QUEUE_CAPACITY);
        tokio::spawn(run_watch(
            node,
            org,
            "api".into(),
            scoped_service,
            changes,
            tx,
            Vec::new(),
        ));

        let intentionally_stale_initial = next_message(&mut outgoing).await;
        assert!(
            intentionally_stale_initial.data["instances"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let resync = next_message(&mut outgoing).await;
        assert_eq!(resync.event, "snapshot");
        assert_eq!(resync.data["reason"], "lagged");
        assert_eq!(resync.data["skipped_events"], 2);
        assert_eq!(resync.data["instances"][0]["instance_id"], "a-1");
    }

    #[tokio::test]
    async fn lease_expiry_emits_snapshot_without_another_mutation() {
        let node = crate::test_support::node(4);
        let org = OrgScope("org-a".into());
        let mut outgoing = start_test_watch(node.clone(), org.clone(), "api").await;
        let initial = next_message(&mut outgoing).await;
        assert!(initial.data["instances"].as_array().unwrap().is_empty());

        propose_register(&node, &org, "api", "short-lived", 500).await;
        let registered = next_message(&mut outgoing).await;
        assert_eq!(registered.event, "register");
        let live = next_message(&mut outgoing).await;
        assert_eq!(live.event, "snapshot");
        assert_eq!(live.data["instances"].as_array().unwrap().len(), 1);

        let expired = next_message(&mut outgoing).await;
        assert_eq!(expired.event, "snapshot");
        assert_eq!(expired.data["reason"], "lease_reconcile");
        assert!(expired.data["instances"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn watch_response_disables_intermediary_caching() {
        let node = crate::test_support::node(1);
        let response = watch(
            State(node),
            OrgScope("org-a".into()),
            Uri::from_static("/v1/services/api/watch"),
            Path("api".to_string()),
        )
        .await;

        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-cache, no-transform")
        );
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .expect("SSE content type");
        assert!(content_type.starts_with("text/event-stream"));
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
