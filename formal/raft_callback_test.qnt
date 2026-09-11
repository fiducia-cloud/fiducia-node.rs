module raft_callback_test {
  import raft_callback.* from "./raft_callback"

  run duplicateCannotClearNewerRequestTest =
    init
      .then(dispatch(1))
      .then(deliver_success(mk_request(1, 1, 0, 1), 1))
      .then(dispatch(2))
      .then(stale_callback(mk_request(1, 1, 0, 1), 1))
      .expect(and {
        s.active.contains(mk_request(1, 2, 0, 2)),
        s.last_event == EVENT_STALE_IGNORED,
        raft_callback_safety,
      })

  run delayedReplyDoesNotRenewAtArrivalTest =
    init
      .then(dispatch(1))
      .then(tick)
      .then(tick)
      .then(deliver_success(mk_request(1, 1, 0, 1), 1))
      .expect(and {
        s.last_contact == 0,
        s.now == 2,
        not(contact_is_fresh(s)),
        raft_callback_safety,
      })
      .then(serve_read)
      .fail()

  run obsoleteRequestHigherTermForcesStepDownTest =
    init
      .then(dispatch(1))
      .then(deliver_success(mk_request(1, 1, 0, 1), 1))
      .then(dispatch(2))
      .then(higher_term_callback(mk_request(1, 1, 0, 1), 2))
      .expect(and {
        s.current_term == 2,
        not(s.leader),
        s.active.size() == 0,
        s.last_contact == -1,
        s.last_event == EVENT_HIGHER_TERM,
        raft_callback_safety,
      })

  run leadershipTransferRequiresFreshTermEvidenceTest =
    init
      .then(dispatch(1))
      .then(deliver_success(mk_request(1, 1, 0, 1), 1))
      .then(higher_term_callback(mk_request(1, 1, 0, 1), 2))
      .then(become_leader)
      .expect(and {
        s.current_term == 3,
        s.leader,
        s.last_contact == -1,
        not(contact_is_fresh(s)),
        raft_callback_safety,
      })
      .then(serve_read)
      .fail()

  run sameSequenceMayRecurOnlyInNewTermTest =
    init
      .then(dispatch(1))
      .then(higher_term_callback(mk_request(1, 1, 0, 1), 2))
      .then(become_leader)
      .then(dispatch(1))
      .expect(and {
        s.used_requests.contains(mk_request(1, 1, 0, 1)),
        s.used_requests.contains(mk_request(3, 1, 0, 1)),
        request_generation_is_unique,
        raft_callback_safety,
      })

  run peerTailCannotInflateAcknowledgedPrefixTest =
    init
      .then(dispatch(1))
      .then(deliver_success(mk_request(1, 1, 0, 1), 2))
      .expect(and {
        s.matched_index == 1,
        raft_callback_safety,
      })

  run oldRequestedPrefixCannotRegressProgressTest =
    init
      .then(dispatch(2))
      .then(deliver_success(mk_request(1, 1, 0, 2), 2))
      .then(dispatch(1))
      .then(deliver_success(mk_request(1, 2, 0, 1), 1))
      .expect(and {
        s.matched_index == 2,
        raft_callback_safety,
      })

  run freshDispatchTimeEvidenceCanServeReadTest =
    init
      .then(dispatch(1))
      .then(tick)
      .then(deliver_negative(mk_request(1, 1, 0, 1)))
      .then(serve_read)
      .expect(and {
        s.last_event == EVENT_READ,
        contact_is_fresh(s),
        raft_callback_safety,
      })
}
