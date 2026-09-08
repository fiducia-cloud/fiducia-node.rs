//! Exercise the real HTTP and loopback transport, not only the pure predicate.

use super::*;

const TERM: u64 = 3;
const INDEX: u64 = 7;

fn append_request() -> AppendEntriesReq {
    AppendEntriesReq {
        term: TERM,
        leader_id: "leader".to_string(),
        prev_log_index: INDEX,
        prev_log_term: TERM,
        entries: Vec::new(),
        leader_commit: INDEX,
        command_protocol: crate::state::CURRENT_COMMAND_PROTOCOL,
    }
}

fn snapshot_request() -> InstallSnapshotReq {
    InstallSnapshotReq {
        term: TERM,
        leader_id: "leader".to_string(),
        last_included_index: INDEX,
        last_included_term: TERM,
        state: Vec::new(),
    }
}

fn cases() -> [(u64, bool, u64, bool); 7] {
    [
        (TERM, true, INDEX - 1, false),
        (TERM, true, INDEX, true),
        (TERM, true, INDEX + 2, true),
        (TERM, false, 0, true),
        (TERM - 1, true, INDEX, false),
        (TERM + 1, true, 0, true),
        (TERM + 1, false, 0, true),
    ]
}

fn assert_append(
    response: Option<AppendEntriesResp>,
    term: u64,
    success: bool,
    index: u64,
    delivered: bool,
) {
    assert_eq!(response.is_some(), delivered);
    if let Some(response) = response {
        assert_eq!(response.term, term, "higher terms must reach the actor");
        assert_eq!(response.match_index, index);
        assert_eq!(response.success, success && term == TERM);
        let expected_protocol = if term > TERM {
            crate::state::LEGACY_COMMAND_PROTOCOL
        } else {
            crate::state::CURRENT_COMMAND_PROTOCOL
        };
        assert_eq!(response.command_protocol, expected_protocol);
    }
}

fn assert_snapshot(
    response: Option<InstallSnapshotResp>,
    term: u64,
    success: bool,
    index: u64,
    delivered: bool,
) {
    assert_eq!(response.is_some(), delivered);
    if let Some(response) = response {
        assert_eq!(response.term, term, "higher terms must reach the actor");
        assert_eq!(response.match_index, index);
        assert_eq!(response.success, success && term == TERM);
    }
}

#[tokio::test]
async fn loopback_replication_replies_require_request_evidence() {
    for (term, success, index, delivered) in cases() {
        let registry = LoopbackRegistry::new();
        let (tx, mut rx) = mpsc::channel(2);
        registry.register("follower", 0, tx);
        let peer = tokio::spawn(async move {
            match rx.recv().await.expect("append request") {
                ShardMsg::AppendEntries { req, resp } => {
                    assert_eq!(req.term, TERM);
                    assert_eq!(req.prev_log_index, INDEX);
                    resp.send(AppendEntriesResp {
                        term,
                        success,
                        match_index: index,
                        command_protocol: crate::state::CURRENT_COMMAND_PROTOCOL,
                    })
                    .unwrap();
                }
                _ => panic!("wrong RPC"),
            }
            match rx.recv().await.expect("snapshot request") {
                ShardMsg::InstallSnapshot { req, resp } => {
                    assert_eq!(req.term, TERM);
                    assert_eq!(req.last_included_index, INDEX);
                    resp.send(InstallSnapshotResp {
                        term,
                        success,
                        match_index: index,
                    })
                    .unwrap();
                }
                _ => panic!("wrong RPC"),
            }
        });
        let transport = Transport::loopback(registry);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            assert_append(
                transport
                    .append_entries("follower", 0, append_request())
                    .await,
                term,
                success,
                index,
                delivered,
            );
            assert_snapshot(
                transport
                    .install_snapshot("follower", 0, snapshot_request())
                    .await,
                term,
                success,
                index,
                delivered,
            );
            peer.await.unwrap();
        })
        .await
        .expect("loopback reply test timed out");
    }
}

#[tokio::test]
async fn http_replication_replies_use_the_same_admission_guard() {
    for (term, success, index, delivered) in cases() {
        let reply = serde_json::json!({
            "term": term,
            "success": success,
            "match_index": index,
            "command_protocol": crate::state::CURRENT_COMMAND_PROTOCOL,
        });
        let app = axum::Router::new().route(
            "/raft/:shard/:rpc",
            axum::routing::post(move || {
                let reply = reply.clone();
                async move { axum::Json(reply) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let transport = Transport::Http(reqwest::Client::new());
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            assert_append(
                transport
                    .append_entries(&address, 0, append_request())
                    .await,
                term,
                success,
                index,
                delivered,
            );
            assert_snapshot(
                transport
                    .install_snapshot(&address, 0, snapshot_request())
                    .await,
                term,
                success,
                index,
                delivered,
            );
        })
        .await;
        server.abort();
        let _ = server.await;
        result.expect("HTTP reply test timed out");
    }
}

#[tokio::test]
async fn append_index_overflow_is_rejected_before_sending() {
    let registry = LoopbackRegistry::new();
    let (tx, mut rx) = mpsc::channel(1);
    registry.register("follower", 0, tx);
    let mut req = append_request();
    req.prev_log_index = u64::MAX;
    req.entries.push(LogEntry {
        term: TERM,
        index: u64::MAX,
        proposed_at_ms: 0,
        command: None,
    });
    let transport = Transport::loopback(registry);
    assert!(transport.append_entries("follower", 0, req).await.is_none());
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
