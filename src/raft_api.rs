//! Inbound peer Raft RPC endpoints (the server side of [`crate::transport`]).
//!
//! A leader/candidate on another node posts `AppendEntries` / `RequestVote` here
//! for a specific shard; we demux to that shard's actor and return its reply.
//! These are **internal** (node↔node) and mounted at the top level (`/raft/…`),
//! not under `/v1`, so they're distinct from the client coordination API.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};

use crate::consensus::{Node, ShardId};
use crate::transport::{AppendEntriesReq, RequestVoteReq};

pub fn router() -> Router<Arc<Node>> {
    Router::new()
        .route("/:shard/append", post(append))
        .route("/:shard/vote", post(vote))
}

/// `POST /raft/{shard}/append` — replicate log entries / heartbeat.
async fn append(
    State(node): State<Arc<Node>>,
    Path(shard): Path<ShardId>,
    Json(req): Json<AppendEntriesReq>,
) -> Response {
    match node.append_entries(shard, req).await {
        Some(resp) => Json(resp).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// `POST /raft/{shard}/vote` — solicit a vote in an election.
async fn vote(
    State(node): State<Arc<Node>>,
    Path(shard): Path<ShardId>,
    Json(req): Json<RequestVoteReq>,
) -> Response {
    match node.request_vote(shard, req).await {
        Some(resp) => Json(resp).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
