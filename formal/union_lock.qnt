// Bounded executable model of fiducia-node's protocol-v2 union-lock manager.
//
// The finite domain is deliberate: three holders, two keys, two request ids,
// at most two queued attempts, bounded logical time, and four fencing tokens.
// It retains the correctness surface that matters: atomic union acquisition,
// overlapping FIFO reservations, disjoint progress, idempotent retries,
// cancellation races, logical expiry, renewal authority, release, promotion,
// and fencing-token exhaustion.
module union_lock {
  type Attempt = {
    holder: int,
    keys: Set[int],
    request_id: int,
  }

  type Grant = {
    attempt: Attempt,
    token: int,
    expires_at: int,
  }

  type Waiter = {
    attempt: Attempt,
    ttl: int,
    wait_expires_at: int,
    seq: int,
  }

  type Cancellation = {
    attempt: Attempt,
    expires_at: int,
  }

  type State = {
    now: int,
    next_token: int,
    minted_tokens: Set[int],
    next_queue_seq: int,
    grants: Set[Grant],
    queue: Set[Waiter],
    cancellations: Set[Cancellation],
  }

  pure val HOLDERS: Set[int] = Set(1, 2, 3)
  pure val KEY_SETS: Set[Set[int]] = Set(Set(1), Set(2), Set(1, 2))
  pure val REQUEST_IDS: Set[int] = Set(1, 2)
  pure val TTLS: Set[int] = Set(2, 3)
  pure val TOKENS: Set[int] = Set(1, 2, 3, 4)
  pure val MAX_QUEUE = 2
  pure val MAX_QUEUE_SEQ = 6
  pure val MAX_TIME = 8
  pure val MAX_TOKEN = 4
  pure val CANCELLATION_TTL = 3

  var s: State

  pure def mk_attempt(holder: int, lock_keys: Set[int], request_id: int): Attempt = {
    holder: holder,
    keys: lock_keys,
    request_id: request_id,
  }

  pure def valid_attempt(a: Attempt): bool = and {
    HOLDERS.contains(a.holder),
    KEY_SETS.contains(a.keys),
    REQUEST_IDS.contains(a.request_id),
  }

  pure def overlaps(left: Set[int], right: Set[int]): bool =
    left.intersect(right).size() > 0

  pure def max_int(left: int, right: int): int =
    if (left > right) left else right

  pure def has_grant(grants: Set[Grant], a: Attempt): bool =
    grants.exists(g => g.attempt == a)

  pure def has_queued_attempt(queue: Set[Waiter], a: Attempt): bool =
    queue.exists(w => w.attempt == a)

  pure def has_queue_identity(queue: Set[Waiter], a: Attempt): bool =
    queue.exists(w => and {
      w.attempt.holder == a.holder,
      w.attempt.keys == a.keys,
    })

  pure def has_active_cancellation(
    cancellations: Set[Cancellation],
    now: int,
    a: Attempt
  ): bool =
    cancellations.exists(c => c.attempt == a and c.expires_at > now)

  pure def conflicts_with_grants(
    grants: Set[Grant],
    lock_keys: Set[int]
  ): bool =
    grants.exists(g => overlaps(g.attempt.keys, lock_keys))

  pure def conflicts_with_queue(
    queue: Set[Waiter],
    lock_keys: Set[int]
  ): bool =
    queue.exists(w => overlaps(w.attempt.keys, lock_keys))

  pure def is_before(left: Waiter, right: Waiter): bool =
    left.seq < right.seq

  pure def is_head(queue: Set[Waiter], waiter: Waiter): bool =
    queue.forall(other => not(is_before(other, waiter)))

  action init: bool = {
    s' = {
      now: 0,
      next_token: 1,
      minted_tokens: Set(),
      next_queue_seq: 1,
      grants: Set(),
      queue: Set(),
      cancellations: Set(),
    }
  }

  // All member keys are installed in one grant record or none are installed.
  action acquire_now(
    holder: int,
    lock_keys: Set[int],
    request_id: int,
    ttl: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      valid_attempt(a),
      TTLS.contains(ttl),
      not(has_grant(s.grants, a)),
      not(has_queued_attempt(s.queue, a)),
      not(has_active_cancellation(s.cancellations, s.now, a)),
      not(conflicts_with_grants(s.grants, lock_keys)),
      not(conflicts_with_queue(s.queue, lock_keys)),
      s.next_token <= MAX_TOKEN,
      s' = {
        ...s,
        next_token: s.next_token + 1,
        minted_tokens: s.minted_tokens.union(Set(s.next_token)),
        grants: s.grants.union(Set({
          attempt: a,
          token: s.next_token,
          expires_at: s.now + ttl,
        })),
      },
    }
  }

  // The implementation indexes queue identity by holder + canonical key set.
  // A different request id cannot occupy that identity at the same time.
  action enqueue_wait(
    holder: int,
    lock_keys: Set[int],
    request_id: int,
    ttl: int,
    wait_ttl: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      valid_attempt(a),
      TTLS.contains(ttl),
      TTLS.contains(wait_ttl),
      not(has_grant(s.grants, a)),
      not(has_queued_attempt(s.queue, a)),
      not(has_queue_identity(s.queue, a)),
      not(has_active_cancellation(s.cancellations, s.now, a)),
      or {
        conflicts_with_grants(s.grants, lock_keys),
        conflicts_with_queue(s.queue, lock_keys),
        s.next_token > MAX_TOKEN,
      },
      s.queue.size() < MAX_QUEUE,
      s.next_queue_seq <= MAX_QUEUE_SEQ,
      s' = {
        ...s,
        next_queue_seq: s.next_queue_seq + 1,
        queue: s.queue.union(Set({
          attempt: a,
          ttl: ttl,
          wait_expires_at: s.now + wait_ttl,
          seq: s.next_queue_seq,
        })),
      },
    }
  }

  // Exact retries do not mint a token and do not extend a lease or wait limit.
  action retry_attempt(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      or {
        has_grant(s.grants, a),
        has_queued_attempt(s.queue, a),
      },
      s' = s,
    }
  }

  // Renewal requires the exact holder, canonical key set, request id, and token.
  action renew(
    holder: int,
    lock_keys: Set[int],
    request_id: int,
    token: int,
    ttl: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      TTLS.contains(ttl),
      s.grants.exists(g => g.attempt == a and g.token == token),
      s' = {
        ...s,
        grants: s.grants.map(g =>
          if (g.attempt == a and g.token == token)
            {
              attempt: g.attempt,
              token: g.token,
              expires_at: max_int(g.expires_at, s.now + ttl),
            }
          else
            g),
      },
    }
  }

  action release(holder: int, token: int): bool = all {
    HOLDERS.contains(holder),
    TOKENS.contains(token),
    s.grants.exists(g => g.attempt.holder == holder and g.token == token),
    s' = {
      ...s,
      grants: s.grants.filter(g =>
        not(g.attempt.holder == holder and g.token == token)),
    },
  }

  // A cancellation racing after promotion returns the live grant; it never
  // silently releases that newly authoritative lease.
  action cancel_active_retry(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      has_grant(s.grants, a),
      s' = s,
    }
  }

  action cancel_queued(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      not(has_grant(s.grants, a)),
      has_queued_attempt(s.queue, a),
      s' = {
        ...s,
        queue: s.queue.filter(w => w.attempt != a),
        cancellations: s.cancellations.union(Set({
          attempt: a,
          expires_at: s.now + CANCELLATION_TTL,
        })),
      },
    }
  }

  // A cancel that arrives before a delayed acquire installs a bounded tombstone.
  action cancel_absent(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      valid_attempt(a),
      not(has_grant(s.grants, a)),
      not(has_queued_attempt(s.queue, a)),
      not(has_active_cancellation(s.cancellations, s.now, a)),
      s' = {
        ...s,
        cancellations: s.cancellations.union(Set({
          attempt: a,
          expires_at: s.now + CANCELLATION_TTL,
        })),
      },
    }
  }

  // Time advances only through the replicated logical-clock command. Expiry is
  // applied in the same transition, so expired authority is never observable.
  action tick: bool = all {
    s.now < MAX_TIME,
    s' = {
      ...s,
      now: s.now + 1,
      grants: s.grants.filter(g => g.expires_at > s.now + 1),
      queue: s.queue.filter(w => w.wait_expires_at > s.now + 1),
      cancellations: s.cancellations.filter(c => c.expires_at > s.now + 1),
    },
  }

  // Promote the earliest queued attempt when all of its keys are free.
  action promote_head(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      s.queue.exists(w => w.attempt == a and is_head(s.queue, w)),
      not(conflicts_with_grants(s.grants, lock_keys)),
      not(has_active_cancellation(s.cancellations, s.now, a)),
      s.next_token <= MAX_TOKEN,
      s' = {
        ...s,
        next_token: s.next_token + 1,
        minted_tokens: s.minted_tokens.union(Set(s.next_token)),
        grants: s.grants.union(
          s.queue
            .filter(w => w.attempt == a)
            .map(w => {
              attempt: w.attempt,
              token: s.next_token,
              expires_at: s.now + w.ttl,
            })),
        queue: s.queue.filter(w => w.attempt != a),
      },
    }
  }

  // With MAX_QUEUE = 2, the non-head waiter may progress only if the head is
  // still held-blocked and the candidate is disjoint from the head reservation.
  action promote_after_blocked_head(
    holder: int,
    lock_keys: Set[int],
    request_id: int
  ): bool = {
    val a = mk_attempt(holder, lock_keys, request_id)
    all {
      s.queue.size() == 2,
      s.queue.exists(candidate => and {
        candidate.attempt == a,
        s.queue.exists(first => and {
          is_head(s.queue, first),
          is_before(first, candidate),
          conflicts_with_grants(s.grants, first.attempt.keys),
          not(conflicts_with_grants(s.grants, candidate.attempt.keys)),
          not(overlaps(first.attempt.keys, candidate.attempt.keys)),
        }),
      }),
      not(has_active_cancellation(s.cancellations, s.now, a)),
      s.next_token <= MAX_TOKEN,
      s' = {
        ...s,
        next_token: s.next_token + 1,
        minted_tokens: s.minted_tokens.union(Set(s.next_token)),
        grants: s.grants.union(
          s.queue
            .filter(w => w.attempt == a)
            .map(w => {
              attempt: w.attempt,
              token: s.next_token,
              expires_at: s.now + w.ttl,
            })),
        queue: s.queue.filter(w => w.attempt != a),
      },
    }
  }

  // Serialization details are outside this abstract model. Observable snapshot
  // equivalence is represented as an explicit stuttering transition.
  action snapshot_round_trip: bool = s' = s
  action idle: bool = s' = s

  action step: bool = {
    nondet holder = HOLDERS.oneOf()
    nondet lock_keys = KEY_SETS.oneOf()
    nondet request_id = REQUEST_IDS.oneOf()
    nondet ttl = TTLS.oneOf()
    nondet wait_ttl = TTLS.oneOf()
    nondet token = TOKENS.oneOf()
    any {
      acquire_now(holder, lock_keys, request_id, ttl),
      enqueue_wait(holder, lock_keys, request_id, ttl, wait_ttl),
      retry_attempt(holder, lock_keys, request_id),
      renew(holder, lock_keys, request_id, token, ttl),
      release(holder, token),
      cancel_active_retry(holder, lock_keys, request_id),
      cancel_queued(holder, lock_keys, request_id),
      cancel_absent(holder, lock_keys, request_id),
      tick,
      promote_head(holder, lock_keys, request_id),
      promote_after_blocked_head(holder, lock_keys, request_id),
      snapshot_round_trip,
      idle,
    }
  }

  val domain_bounds = and {
    s.now >= 0,
    s.now <= MAX_TIME,
    s.next_token >= 1,
    s.next_token <= MAX_TOKEN + 1,
    s.next_queue_seq >= 1,
    s.next_queue_seq <= MAX_QUEUE_SEQ + 1,
    s.queue.size() <= MAX_QUEUE,
    s.grants.forall(g => and {
      valid_attempt(g.attempt),
      TOKENS.contains(g.token),
      g.expires_at > s.now,
    }),
    s.queue.forall(w => and {
      valid_attempt(w.attempt),
      TTLS.contains(w.ttl),
      w.wait_expires_at > s.now,
      w.seq >= 1,
      w.seq < s.next_queue_seq,
    }),
    s.cancellations.forall(c => and {
      valid_attempt(c.attempt),
      c.expires_at > s.now,
    }),
  }

  // minted_tokens is ghost history. Its size and bounds establish that tokens
  // are the exact contiguous prefix [1, next_token), hence never reused.
  val fencing_is_monotonic = and {
    s.minted_tokens.size() == s.next_token - 1,
    s.minted_tokens.forall(token => token >= 1 and token < s.next_token),
    s.grants.forall(g => s.minted_tokens.contains(g.token)),
    s.grants.forall(left => s.grants.forall(right =>
      left == right or left.token != right.token)),
  }

  val live_grants_are_disjoint =
    s.grants.forall(left => s.grants.forall(right =>
      left == right or not(overlaps(left.attempt.keys, right.attempt.keys))))

  val queue_order_is_unique =
    s.queue.forall(left => s.queue.forall(right =>
      left == right or left.seq != right.seq))

  val queue_identities_are_unique =
    s.queue.forall(left => s.queue.forall(right =>
      left == right or not(and {
        left.attempt.holder == right.attempt.holder,
        left.attempt.keys == right.attempt.keys,
      })))

  val attempts_do_not_straddle_grant_and_queue =
    s.grants.forall(g => not(s.queue.exists(w => w.attempt == g.attempt)))

  val cancellations_fail_closed =
    s.cancellations.forall(c => and {
      not(s.grants.exists(g => g.attempt == c.attempt)),
      not(s.queue.exists(w => w.attempt == c.attempt)),
    })

  val union_lock_safety = and {
    domain_bounds,
    fencing_is_monotonic,
    live_grants_are_disjoint,
    queue_order_is_unique,
    queue_identities_are_unique,
    attempts_do_not_straddle_grant_and_queue,
    cancellations_fail_closed,
  }

  val queued_work_reached = s.queue.size() > 0
  val concurrent_disjoint_grants_reached = s.grants.size() == 2
  val cancellation_tombstone_reached = s.cancellations.size() > 0
  val token_exhaustion_reached = s.next_token == MAX_TOKEN + 1
}
