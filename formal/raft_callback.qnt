// Bounded reachable-state model for Fiducia's Raft callback ownership,
// higher-term step-down, dispatch-time contact evidence, and prefix credit.
//
// This complements the production-function exhaustive Rust tables in
// tests/formal_raft_callback_refinement.rs. It deliberately models multiple
// successive requests and leadership terms so Apalache can check stale and
// duplicate callbacks against a reachable actor state rather than isolated
// function inputs.
module raft_callback {
  type Request = {
    term: int,
    sequence: int,
    sent_at: int,
    requested_index: int,
  }

  type State = {
    now: int,
    current_term: int,
    leader: bool,
    next_sequence: int,
    used_requests: Set[Request],
    active: Set[Request],
    last_contact: int,
    contact_term: int,
    matched_index: int,
    last_event: int,
  }

  pure val MAX_TERM = 3
  pure val MAX_SEQUENCE = 2
  pure val MAX_TIME = 4
  pure val MAX_INDEX = 2
  pure val LEASE_WINDOW = 2

  pure val TERMS: Set[int] = Set(1, 2, 3)
  pure val SEQUENCES: Set[int] = Set(1, 2)
  pure val TIMES: Set[int] = Set(0, 1, 2, 3, 4)
  pure val INDEXES: Set[int] = Set(0, 1, 2)

  // Event codes are model-local evidence only.
  pure val EVENT_NONE = 0
  pure val EVENT_DISPATCH = 1
  pure val EVENT_DELIVER = 2
  pure val EVENT_STALE_IGNORED = 3
  pure val EVENT_HIGHER_TERM = 4
  pure val EVENT_TIMEOUT = 5
  pure val EVENT_READ = 6
  pure val EVENT_LEADER = 7

  var s: State

  pure def mk_request(
    term: int,
    sequence: int,
    sent_at: int,
    requested_index: int
  ): Request = {
    term: term,
    sequence: sequence,
    sent_at: sent_at,
    requested_index: requested_index,
  }

  pure def valid_request(request: Request): bool = and {
    TERMS.contains(request.term),
    SEQUENCES.contains(request.sequence),
    TIMES.contains(request.sent_at),
    INDEXES.contains(request.requested_index),
  }

  pure def max_int(left: int, right: int): int =
    if (left > right) left else right

  pure def contact_is_fresh(state: State): bool = and {
    state.leader,
    state.last_contact >= 0,
    state.contact_term == state.current_term,
    state.now >= state.last_contact,
    state.now - state.last_contact < LEASE_WINDOW,
  }

  action init: bool = {
    s' = {
      now: 0,
      current_term: 1,
      leader: true,
      next_sequence: 1,
      used_requests: Set(),
      active: Set(),
      last_contact: -1,
      contact_term: 0,
      matched_index: 0,
      last_event: EVENT_NONE,
    }
  }

  // Dispatch ownership is minted by the actor, not by transport. Sequence
  // identity is term-scoped and dispatch time is the only time a callback may
  // later contribute as contact evidence.
  action dispatch(requested_index: int): bool = {
    val request = mk_request(
      s.current_term,
      s.next_sequence,
      s.now,
      requested_index
    )
    all {
      s.leader,
      s.active.size() == 0,
      s.next_sequence <= MAX_SEQUENCE,
      INDEXES.contains(requested_index),
      s' = {
        ...s,
        next_sequence: s.next_sequence + 1,
        used_requests: s.used_requests.union(Set(request)),
        active: Set(request),
        last_event: EVENT_DISPATCH,
      },
    }
  }

  // Same-term success may consume only the exact outstanding request. Contact
  // advances to request dispatch time, never callback arrival. Prefix credit is
  // bounded to the requested prefix even when a peer reports a larger suffix.
  action deliver_success(request: Request, reported_index: int): bool = all {
    valid_request(request),
    s.leader,
    request.term == s.current_term,
    s.active.contains(request),
    request.sent_at <= s.now,
    INDEXES.contains(reported_index),
    reported_index >= request.requested_index,
    s' = {
      ...s,
      active: Set(),
      last_contact:
        if (s.contact_term == s.current_term and s.last_contact >= 0)
          max_int(s.last_contact, request.sent_at)
        else
          request.sent_at,
      contact_term: s.current_term,
      matched_index: max_int(s.matched_index, request.requested_index),
      last_event: EVENT_DELIVER,
    },
  }

  // A same-term negative reply still proves peer contact for the exact request,
  // but cannot advance replication progress.
  action deliver_negative(request: Request): bool = all {
    valid_request(request),
    s.leader,
    request.term == s.current_term,
    s.active.contains(request),
    request.sent_at <= s.now,
    s' = {
      ...s,
      active: Set(),
      last_contact:
        if (s.contact_term == s.current_term and s.last_contact >= 0)
          max_int(s.last_contact, request.sent_at)
        else
          request.sent_at,
      contact_term: s.current_term,
      last_event: EVENT_DELIVER,
    },
  }

  // Duplicate, delayed, or old-request callbacks at or below the current term
  // are inert. In particular they cannot clear a newer active slot, advance the
  // prefix, or renew contact evidence.
  action stale_callback(request: Request, response_term: int): bool = all {
    valid_request(request),
    TERMS.contains(response_term),
    response_term <= s.current_term,
    not(s.active.contains(request)),
    s' = {
      ...s,
      last_event: EVENT_STALE_IGNORED,
    },
  }

  // Newer-term evidence wins even when it arrives for an obsolete request.
  // Step-down clears leadership-scoped callback/contact authority.
  action higher_term_callback(request: Request, response_term: int): bool = all {
    valid_request(request),
    TERMS.contains(response_term),
    response_term > s.current_term,
    s' = {
      ...s,
      current_term: response_term,
      leader: false,
      next_sequence: 1,
      active: Set(),
      last_contact: -1,
      contact_term: 0,
      last_event: EVENT_HIGHER_TERM,
    },
  }

  // Timeout owns only its exact outstanding slot and never mints contact or
  // replication evidence.
  action timeout(request: Request): bool = all {
    valid_request(request),
    s.active.contains(request),
    s' = {
      ...s,
      active: Set(),
      last_event: EVENT_TIMEOUT,
    },
  }

  // A later election creates a fresh leadership term. Sequence values may be
  // reused only because request identity includes the term.
  action become_leader: bool = all {
    not(s.leader),
    s.current_term < MAX_TERM,
    s' = {
      ...s,
      current_term: s.current_term + 1,
      leader: true,
      next_sequence: 1,
      active: Set(),
      last_contact: -1,
      contact_term: 0,
      last_event: EVENT_LEADER,
    },
  }

  // This is an abstract read-admission event, not a read implementation. It is
  // enabled only while dispatch-time evidence remains fresh in this term.
  action serve_read: bool = all {
    contact_is_fresh(s),
    s' = {
      ...s,
      last_event: EVENT_READ,
    },
  }

  action tick: bool = all {
    s.now < MAX_TIME,
    s' = {
      ...s,
      now: s.now + 1,
      last_event: EVENT_NONE,
    },
  }

  action idle: bool = s' = {
    ...s,
    last_event: EVENT_NONE,
  }

  action step: bool = {
    nondet request_term = TERMS.oneOf()
    nondet request_sequence = SEQUENCES.oneOf()
    nondet sent_at = TIMES.oneOf()
    nondet requested_index = INDEXES.oneOf()
    nondet reported_index = INDEXES.oneOf()
    nondet response_term = TERMS.oneOf()
    val request = mk_request(
      request_term,
      request_sequence,
      sent_at,
      requested_index
    )
    any {
      dispatch(requested_index),
      deliver_success(request, reported_index),
      deliver_negative(request),
      stale_callback(request, response_term),
      higher_term_callback(request, response_term),
      timeout(request),
      become_leader,
      serve_read,
      tick,
      idle,
    }
  }

  val domain_bounds = and {
    TIMES.contains(s.now),
    TERMS.contains(s.current_term),
    s.next_sequence >= 1,
    s.next_sequence <= MAX_SEQUENCE + 1,
    s.active.size() <= 1,
    INDEXES.contains(s.matched_index),
    s.used_requests.forall(request => valid_request(request)),
    s.active.forall(request => and {
      valid_request(request),
      s.used_requests.contains(request),
      request.term == s.current_term,
      request.sent_at <= s.now,
      s.leader,
    }),
  }

  // A term-scoped sequence identifies at most one dispatched request. This is
  // the bounded analogue of production sequence non-reuse before exhaustion.
  val request_generation_is_unique =
    s.used_requests.forall(left => s.used_requests.forall(right =>
      left == right or left.term != right.term or left.sequence != right.sequence))

  val current_term_sequence_frontier =
    s.used_requests.forall(request =>
      request.term != s.current_term or request.sequence < s.next_sequence)

  // Contact must be traceable to an actual actor dispatch in the current term;
  // arrival time can therefore never invent fresh evidence.
  val contact_has_dispatch_provenance = or {
    and {
      s.last_contact == -1,
      s.contact_term == 0,
    },
    and {
      s.last_contact >= 0,
      s.last_contact <= s.now,
      s.contact_term == s.current_term,
      s.used_requests.exists(request => and {
        request.term == s.contact_term,
        request.sent_at == s.last_contact,
      }),
    },
  }

  // Leadership loss invalidates all leadership-scoped callback and read-lease
  // evidence. A later campaign must establish new-term evidence from scratch.
  val follower_has_no_leader_authority =
    s.leader or and {
      s.active.size() == 0,
      s.last_contact == -1,
      s.contact_term == 0,
    }

  val read_admission_is_fresh =
    s.last_event != EVENT_READ or contact_is_fresh(s)

  val higher_term_event_is_fail_closed =
    s.last_event != EVENT_HIGHER_TERM or and {
      not(s.leader),
      s.active.size() == 0,
      s.last_contact == -1,
      s.contact_term == 0,
    }

  val raft_callback_safety = and {
    domain_bounds,
    request_generation_is_unique,
    current_term_sequence_frontier,
    contact_has_dispatch_provenance,
    follower_has_no_leader_authority,
    read_admission_is_fresh,
    higher_term_event_is_fail_closed,
  }

  val active_request_reached = s.active.size() == 1
  val stale_callback_ignored_reached = s.last_event == EVENT_STALE_IGNORED
  val higher_term_step_down_reached = s.last_event == EVENT_HIGHER_TERM
  val fresh_read_reached = s.last_event == EVENT_READ
  val leadership_transfer_reached = s.leader and s.current_term > 1
  val expired_contact_reached = and {
    s.leader,
    s.last_contact >= 0,
    s.now - s.last_contact >= LEASE_WINDOW,
  }
}
