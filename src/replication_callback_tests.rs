//! Actor-facing regression tests: these drive ShardMsg dispatch, not just the
//! pure helper. No live endpoint or wall-clock sleeps are needed.
use super::*;

fn fixture() -> ShardActor {
    let (tx, _) = mpsc::channel(32);
    let mut actor = ShardActor::new(
        0,
        "a".into(),
        vec!["b".into(), "c".into()],
        Arc::new(Transport::Loopback(LoopbackRegistry::new())),
        tx,
        RaftTiming {
            election_min_ms: 60_000,
            ..RaftTiming::default()
        },
        None,
        Recovered::default(),
    )
    .unwrap();
    actor.role = Role::Leader;
    actor.current_term = 3;
    actor.leader_id = Some("a".into());
    actor.leader = Some(LeaderState::default());
    actor
}

fn stamp(actor: &ShardActor, sequence: u64, expired: bool) -> RequestStamp<Instant> {
    let sent_at = if expired {
        Instant::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap()
    } else {
        Instant::now()
    };
    RequestStamp {
        term: actor.current_term,
        sequence,
        sent_at,
    }
}

fn arm(actor: &mut ShardActor, request: RequestStamp<Instant>) {
    let leader = actor.leader.as_mut().unwrap();
    leader.pending_requests.insert("b".into(), request);
    leader.request_sequence = leader.request_sequence.max(request.sequence);
    leader.in_flight.insert("b".into(), true);
}

fn append(actor: &mut ShardActor, request: RequestStamp<Instant>, response_term: u64) {
    actor.handle(ShardMsg::AppendReply {
        from: "b".into(),
        request,
        up_to: 0,
        rtt_ms: Some(1),
        resp: Some(AppendEntriesResp {
            term: response_term,
            success: true,
            match_index: 0,
            command_protocol: LEGACY_COMMAND_PROTOCOL,
        }),
    });
}

#[tokio::test]
async fn delayed_append_cannot_resurrect_lease() {
    let mut actor = fixture();
    let request = stamp(&actor, 1, true);
    arm(&mut actor, request);
    append(&mut actor, request, 3);
    assert!(!actor.leader_lease_held());
    assert_eq!(
        actor.leader.as_ref().unwrap().last_contact["b"],
        request.sent_at
    );
    assert!(actor
        .handle_query(ReadRequest::Kv { key: "x".into() })
        .is_err());
}

#[tokio::test]
async fn current_append_supplies_real_quorum_contact() {
    let mut actor = fixture();
    let request = stamp(&actor, 1, false);
    arm(&mut actor, request);
    append(&mut actor, request, 3);
    assert!(actor.leader_lease_held());
    assert_eq!(
        actor.leader.as_ref().unwrap().last_contact["b"],
        request.sent_at
    );
}

#[tokio::test]
async fn duplicate_callback_cannot_clear_newer_in_flight_request() {
    let mut actor = fixture();
    let old = stamp(&actor, 1, false);
    arm(&mut actor, old);
    append(&mut actor, old, 3);
    let active = stamp(&actor, 2, false);
    arm(&mut actor, active);
    append(&mut actor, old, 3);
    let leader = actor.leader.as_ref().unwrap();
    assert!(leader.in_flight["b"]);
    assert_eq!(leader.pending_requests["b"], active);
    assert_eq!(leader.last_contact["b"], old.sent_at);
}

#[tokio::test]
async fn old_term_timeout_does_not_consume_new_leadership_request() {
    let mut actor = fixture();
    let active = stamp(&actor, 1, false);
    arm(&mut actor, active);
    let old = RequestStamp { term: 2, ..active };
    actor.handle(ShardMsg::AppendReply {
        from: "b".into(),
        request: old,
        up_to: 0,
        rtt_ms: Some(999),
        resp: None,
    });
    let leader = actor.leader.as_ref().unwrap();
    assert!(leader.in_flight["b"]);
    assert_eq!(leader.pending_requests["b"], active);
    assert!(leader.last_contact.is_empty());
    assert!(leader.peer_command_protocol.is_empty());
    assert_eq!(actor.metrics.append_rtt_ms_last, None);
}

#[tokio::test]
async fn matching_timeout_releases_slot_without_authority() {
    let mut actor = fixture();
    let active = stamp(&actor, 1, false);
    arm(&mut actor, active);
    actor.handle(ShardMsg::AppendReply {
        from: "b".into(),
        request: active,
        up_to: 0,
        rtt_ms: Some(1),
        resp: None,
    });
    let leader = actor.leader.as_ref().unwrap();
    assert!(!leader.in_flight["b"]);
    assert!(!leader.pending_requests.contains_key("b"));
    assert!(leader.last_contact.is_empty());
}

#[tokio::test]
async fn higher_term_from_obsolete_callback_still_steps_down() {
    let mut actor = fixture();
    let active = stamp(&actor, 2, false);
    arm(&mut actor, active);
    let old = RequestStamp {
        term: 2,
        sequence: 1,
        ..active
    };
    append(&mut actor, old, 4);
    assert_eq!(actor.role, Role::Follower);
    assert_eq!(actor.current_term, 4);
    assert!(actor.leader.is_none());
}

#[tokio::test]
async fn unconfigured_peer_cannot_supply_term_or_quorum_authority() {
    let mut actor = fixture();
    let request = stamp(&actor, 1, false);
    actor.handle(ShardMsg::SnapshotReply {
        from: "unknown".into(),
        request,
        up_to: 0,
        resp: Some(InstallSnapshotResp {
            term: 4,
            success: true,
            match_index: 0,
        }),
    });
    assert_eq!(actor.role, Role::Leader);
    assert_eq!(actor.current_term, 3);
    assert!(!actor.leader_lease_held());
}

#[tokio::test]
async fn delayed_snapshot_preserves_progress_without_renewing_lease() {
    let mut actor = fixture();
    actor.snapshot_index = 9;
    actor.snapshot_term = 3;
    let request = stamp(&actor, 1, true);
    arm(&mut actor, request);
    actor
        .leader
        .as_mut()
        .unwrap()
        .match_index
        .insert("b".into(), 9);
    actor
        .leader
        .as_mut()
        .unwrap()
        .next_index
        .insert("b".into(), 10);
    actor.handle(ShardMsg::SnapshotReply {
        from: "b".into(),
        request,
        up_to: 5,
        resp: Some(InstallSnapshotResp {
            term: 3,
            success: true,
            match_index: 9,
        }),
    });
    let leader = actor.leader.as_ref().unwrap();
    assert_eq!(leader.match_index["b"], 9);
    assert_eq!(leader.next_index["b"], 10);
    assert_eq!(leader.last_contact["b"], request.sent_at);
    assert!(!actor.leader_lease_held());
}

#[tokio::test]
async fn snapshot_under_acknowledgement_cannot_credit_progress_or_contact() {
    let mut actor = fixture();
    let request = stamp(&actor, 1, false);
    arm(&mut actor, request);
    actor.handle(ShardMsg::SnapshotReply {
        from: "b".into(),
        request,
        up_to: 5,
        resp: Some(InstallSnapshotResp {
            term: 3,
            success: true,
            match_index: 4,
        }),
    });
    let leader = actor.leader.as_ref().unwrap();
    assert!(leader.match_index.is_empty());
    assert!(leader.last_contact.is_empty());
    assert!(!leader.in_flight["b"]);
}

#[tokio::test]
async fn absent_peers_never_form_quorum_even_with_large_timeout() {
    let mut actor = fixture();
    actor.timing.election_min_ms = u64::MAX;
    assert!(!actor.leader_lease_held());
}

#[tokio::test]
async fn delayed_election_votes_do_not_start_a_new_lease_at_delivery() {
    let mut actor = fixture();
    actor.role = Role::Candidate;
    actor.campaign_started_at = stamp(&actor, 1, true).sent_at;
    actor.votes.insert("a".into());
    actor.votes.insert("b".into());
    actor.become_leader();
    assert_eq!(
        actor.leader.as_ref().unwrap().last_contact["b"],
        actor.campaign_started_at
    );
    assert!(!actor.leader_lease_held());
}

#[tokio::test]
async fn request_sequence_exhaustion_steps_down_without_wrapping() {
    let mut actor = fixture();
    actor.leader.as_mut().unwrap().request_sequence = u64::MAX;
    actor.send_append_to("b");
    assert_eq!(actor.role, Role::Follower);
    assert!(actor.leader.is_none());
}

#[tokio::test]
async fn negative_append_does_not_rewind_below_known_prefix() {
    let mut actor = fixture();
    let request = stamp(&actor, 1, false);
    arm(&mut actor, request);
    actor
        .leader
        .as_mut()
        .unwrap()
        .match_index
        .insert("b".into(), 8);
    actor
        .leader
        .as_mut()
        .unwrap()
        .next_index
        .insert("b".into(), 10);
    actor.handle(ShardMsg::AppendReply {
        from: "b".into(),
        request,
        up_to: 9,
        rtt_ms: Some(1),
        resp: Some(AppendEntriesResp {
            term: 3,
            success: false,
            match_index: 1,
            command_protocol: LEGACY_COMMAND_PROTOCOL,
        }),
    });
    assert_eq!(actor.leader.as_ref().unwrap().next_index["b"], 9);
    assert_eq!(actor.leader.as_ref().unwrap().match_index["b"], 8);
}
