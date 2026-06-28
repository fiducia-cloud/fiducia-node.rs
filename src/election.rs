//! Leader-election API.
//!
//! Lets *clients* run their own leader elections on top of Fiducia: campaign for
//! a named leadership, hold it via a TTL lease, and observe who currently holds
//! it. The winner receives a monotonic fencing token to defeat stale leaders.
//! Mutations are proposed to the shard owning the election name; reads go through
//! [`Node::query`].
//!
//! This is distinct from the node's *internal* per-shard Raft leadership; a
//! client election named `name` is just state replicated by the shard that owns
//! `name`.
//!
//! Routes (mounted under `/v1/elections`):
//!   * `POST /v1/elections/{name}/campaign` — `{ "candidate", "ttl_ms" }`
//!   * `POST /v1/elections/{name}/renew`    — `{ "candidate", "fencing_token" }`
//!   * `POST /v1/elections/{name}/resign`   — `{ "candidate", "fencing_token" }`
//!   * `GET  /v1/elections/{name}`          — observe the current leader
//!   * `GET  /v1/elections/{name}/watch`    — SSE stream of leadership changes

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
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::consensus::{propose_response, read_error_response, Node, ReadRequest, ReadResponse};
use crate::state::Command;

#[derive(Debug, Deserialize)]
pub struct CampaignBody {
    pub candidate: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct HoldBody {
    pub candidate: String,
    pub fencing_token: u64,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:name", get(observe))
        .route("/:name/campaign", post(campaign))
        .route("/:name/renew", post(renew))
        .route("/:name/resign", post(resign))
        .route("/:name/watch", get(watch))
}

/// `POST /v1/elections/{name}/campaign` — try to become leader.
async fn campaign(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<CampaignBody>,
) -> Response {
    let result = node
        .propose(Command::ElectionCampaign {
            name,
            candidate: body.candidate,
            ttl_ms: body.ttl_ms,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/elections/{name}/renew` — extend the lease (must hold the token).
async fn renew(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<HoldBody>,
) -> Response {
    let result = node
        .propose(Command::ElectionRenew {
            name,
            candidate: body.candidate,
            fencing_token: body.fencing_token,
        })
        .await;
    propose_response(result, &uri)
}

/// `POST /v1/elections/{name}/resign` — give up leadership.
async fn resign(
    State(node): State<Arc<Node>>,
    uri: Uri,
    Path(name): Path<String>,
    Json(body): Json<HoldBody>,
) -> Response {
    let result = node
        .propose(Command::ElectionResign {
            name,
            candidate: body.candidate,
            fencing_token: body.fencing_token,
        })
        .await;
    propose_response(result, &uri)
}

/// `GET /v1/elections/{name}` — observe the current leader.
async fn observe(State(node): State<Arc<Node>>, uri: Uri, Path(name): Path<String>) -> Response {
    match node
        .query(ReadRequest::Election { name: name.clone() })
        .await
    {
        Ok(ReadResponse::Election(Some(l))) => {
            Json(json!({ "name": name, "held": true, "leadership": l })).into_response()
        }
        Ok(ReadResponse::Election(None)) => {
            Json(json!({ "name": name, "held": false })).into_response()
        }
        Err(err) => read_error_response(err, &uri),
        _ => Json(json!({ "error": "unavailable" })).into_response(),
    }
}

/// `GET /v1/elections/{name}/watch` — SSE stream of leadership changes.
async fn watch(State(node): State<Arc<Node>>, Path(name): Path<String>) -> Response {
    let Some(rx) = node.watch(&name).await else {
        return Json(json!({ "error": "unavailable", "op": "election.watch", "name": name }))
            .into_response();
    };
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?;
        if event.key != name {
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
