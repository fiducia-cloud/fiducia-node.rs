module union_lock_test {
  import union_lock.* from "./union_lock"

  run immediateAcquireTest =
    init
      .then(acquire_now(1, Set(1, 2), 1, 3))
      .expect(and {
        s.grants.size() == 1,
        s.grants.exists(g => and {
          g.attempt == mk_attempt(1, Set(1, 2), 1),
          g.token == 1,
          g.expires_at == 3,
        }),
        s.next_token == 2,
      })

  run retryDoesNotExtendLeaseTest =
    init
      .then(acquire_now(1, Set(1), 1, 2))
      .then(retry_attempt(1, Set(1), 1))
      .expect(and {
        s.grants.exists(g => and {
          g.attempt == mk_attempt(1, Set(1), 1),
          g.expires_at == 2,
        }),
        s.next_token == 2,
      })

  run queueReservationBlocksOverlappingBargeTest =
    init
      .then(acquire_now(1, Set(1), 1, 3))
      .then(enqueue_wait(2, Set(1, 2), 1, 2, 3))
      .then(acquire_now(3, Set(2), 1, 2).fail())
      .expect(s.queue.size() == 1)

  run disjointRequestMayBypassQueueTest =
    init
      .then(acquire_now(1, Set(1), 1, 3))
      .then(enqueue_wait(2, Set(1), 1, 2, 3))
      .then(acquire_now(3, Set(2), 1, 2))
      .expect(and {
        s.grants.size() == 2,
        s.queue.size() == 1,
        union_lock_safety,
      })

  run cancelPreventsDelayedAcquireTest =
    init
      .then(cancel_absent(1, Set(1), 1))
      .then(acquire_now(1, Set(1), 1, 2).fail())
      .then(enqueue_wait(1, Set(1), 1, 2, 2).fail())
      .expect(and {
        s.cancellations.size() == 1,
        union_lock_safety,
      })

  run releaseThenPromoteUnionTest =
    init
      .then(acquire_now(1, Set(1), 1, 3))
      .then(enqueue_wait(2, Set(1, 2), 1, 2, 3))
      .then(release(1, 1))
      .then(promote_head(2, Set(1, 2), 1))
      .expect(and {
        s.queue.size() == 0,
        s.grants.size() == 1,
        s.grants.exists(g => and {
          g.attempt == mk_attempt(2, Set(1, 2), 1),
          g.token == 2,
        }),
        union_lock_safety,
      })

  run blockedHeadAllowsDisjointSecondPromotionTest =
    init
      .then(acquire_now(1, Set(1), 1, 3))
      .then(acquire_now(2, Set(2), 1, 3))
      .then(enqueue_wait(3, Set(1), 1, 2, 3))
      .then(enqueue_wait(3, Set(2), 2, 2, 3))
      .then(release(2, 2))
      .then(promote_after_blocked_head(3, Set(2), 2))
      .expect(and {
        s.queue.size() == 1,
        s.queue.exists(w => w.attempt == mk_attempt(3, Set(1), 1)),
        s.grants.exists(g => g.attempt == mk_attempt(3, Set(2), 2)),
        union_lock_safety,
      })

  run cancelPromotionRaceKeepsLiveGrantTest =
    init
      .then(acquire_now(1, Set(1), 1, 3))
      .then(cancel_active_retry(1, Set(1), 1))
      .expect(and {
        s.grants.exists(g => g.attempt == mk_attempt(1, Set(1), 1)),
        s.cancellations.size() == 0,
        union_lock_safety,
      })

  run expiryRemovesAuthorityTest =
    init
      .then(acquire_now(1, Set(1), 1, 2))
      .then(enqueue_wait(2, Set(1), 1, 2, 2))
      .then(tick)
      .then(tick)
      .expect(and {
        s.grants.size() == 0,
        s.queue.size() == 0,
        union_lock_safety,
      })
}
