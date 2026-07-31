#!/usr/bin/env python3
"""Bounded safety model for Fiducia lease grants and fencing tokens.

This is deliberately smaller than the production Raft implementation.  It
exhaustively explores the lease/fencing contract that the implementation must
refine: one effective holder, monotonically increasing grants, stale-operation
rejection, and downstream fencing.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

NONE = -1
ACTORS = (0, 1)
MAX_TIME = 4
MAX_TOKEN = 4
MAX_DEPTH = 10
LEASE_TICKS = 2


@dataclass(frozen=True, slots=True)
class State:
    now: int = 0
    next_token: int = 0
    holder: int = NONE
    token: int = 0
    deadline: int = 0
    downstream_max: int = 0

    @property
    def active(self) -> bool:
        return self.holder != NONE


def cleared(state: State, *, now: int | None = None) -> State:
    return State(
        now=state.now if now is None else now,
        next_token=state.next_token,
        holder=NONE,
        token=0,
        deadline=0,
        downstream_max=state.downstream_max,
    )


def accepts_downstream_write(fencing_token: int, max_seen: int) -> bool:
    """A target may repeat the current token but must reject every older one."""
    return fencing_token >= max_seen


def successors(state: State):
    if state.now < MAX_TIME:
        new_now = state.now + 1
        if state.active and state.deadline <= new_now:
            yield "tick+expire", cleared(state, now=new_now)
        else:
            yield "tick", State(
                now=new_now,
                next_token=state.next_token,
                holder=state.holder,
                token=state.token,
                deadline=state.deadline,
                downstream_max=state.downstream_max,
            )

    if not state.active and state.next_token < MAX_TOKEN:
        for actor in ACTORS:
            fresh = state.next_token + 1
            yield f"acquire({actor})", State(
                now=state.now,
                next_token=fresh,
                holder=actor,
                token=fresh,
                deadline=state.now + LEASE_TICKS,
                downstream_max=state.downstream_max,
            )

    if state.active:
        # Only the exact holder/token pair can renew or release.  Invalid and
        # stale requests are represented by the absence of a state transition.
        yield f"renew({state.holder},{state.token})", State(
            now=state.now,
            next_token=state.next_token,
            holder=state.holder,
            token=state.token,
            deadline=state.now + LEASE_TICKS,
            downstream_max=state.downstream_max,
        )
        yield f"release({state.holder},{state.token})", cleared(state)

        if accepts_downstream_write(state.token, state.downstream_max):
            yield f"write({state.holder},{state.token})", State(
                now=state.now,
                next_token=state.next_token,
                holder=state.holder,
                token=state.token,
                deadline=state.deadline,
                downstream_max=max(state.downstream_max, state.token),
            )


def assert_invariants(state: State) -> None:
    assert 0 <= state.now <= MAX_TIME
    assert 0 <= state.downstream_max <= state.next_token <= MAX_TOKEN
    if state.active:
        assert state.holder in ACTORS
        assert 1 <= state.token <= state.next_token
        assert state.deadline > state.now, "an expired lease remained effective"
    else:
        assert state.token == 0
        assert state.deadline == 0


def main() -> None:
    initial = State()
    queue = deque([(initial, 0)])
    seen = {initial}
    transitions = 0

    while queue:
        state, depth = queue.popleft()
        assert_invariants(state)
        if depth == MAX_DEPTH:
            continue

        for action, target in successors(state):
            transitions += 1
            assert_invariants(target)
            if action.startswith("acquire"):
                assert target.token == state.next_token + 1
                assert target.next_token > state.next_token
            if action.startswith("write"):
                assert target.downstream_max >= state.downstream_max
            if target not in seen:
                seen.add(target)
                queue.append((target, depth + 1))

    # Exhaust the downstream fence predicate independently of holder state.
    for max_seen in range(MAX_TOKEN + 1):
        for candidate in range(MAX_TOKEN + 1):
            accepted = accepts_downstream_write(candidate, max_seen)
            assert accepted == (candidate >= max_seen)
            if candidate < max_seen:
                assert not accepted, "a stale fencing token was accepted"

    print(
        f"fiducia lease model: {len(seen)} states, "
        f"{transitions} transitions; all invariants hold"
    )


if __name__ == "__main__":
    main()
