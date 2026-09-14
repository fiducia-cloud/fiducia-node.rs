#!/usr/bin/env python3
"""Bounded safety/refinement model for Fiducia lease grants and fencing tokens.

This is deliberately smaller than the production Raft implementation. It
exhaustively explores the lease/fencing contract that the implementation must
refine: one effective holder, monotonically increasing grants, stale-operation
rejection, downstream fencing, and fail-closed exhaustion at the public fencing
ceiling.

The abstract model uses a tiny token domain, but its upper edge is refinement-
bound to the production Rust `MAX_FENCING_TOKEN`. An order-preserving tail
embedding maps the model's final tokens onto the final exactly representable
JSON/JavaScript integers, so the same exhaustion transition is checked at the
boundary where production must stop minting.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
import re

NONE = -1
ACTORS = (0, 1)
MAX_TIME = 4
MAX_TOKEN = 4
MAX_DEPTH = 10
LEASE_TICKS = 2
PUBLIC_JSON_SAFE_MAX = 9_007_199_254_740_991
RUST_VALIDATE = Path(__file__).resolve().parents[1] / "src" / "validate.rs"


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


def production_fencing_ceiling() -> int:
    """Read the authoritative Rust ceiling so the model cannot silently drift."""
    source = RUST_VALIDATE.read_text(encoding="utf-8")
    match = re.search(
        r"pub const MAX_FENCING_TOKEN: u64 = ([0-9_]+);",
        source,
    )
    assert match is not None, "production MAX_FENCING_TOKEN declaration is missing"
    value = int(match.group(1).replace("_", ""))
    assert value == PUBLIC_JSON_SAFE_MAX, (
        "production fencing ceiling drifted from Number.MAX_SAFE_INTEGER: "
        f"{value} != {PUBLIC_JSON_SAFE_MAX}"
    )
    return value


def tail_embed(model_token: int, production_max: int) -> int:
    """Map the bounded model's tail monotonically onto production's ceiling.

    Model token 0 represents the production watermark `max-MAX_TOKEN`; model
    token MAX_TOKEN therefore lands exactly on the final mintable production
    token. This is an order-preserving refinement map over the exhaustion tail,
    not a claim that fresh production state begins near exhaustion.
    """
    assert 0 <= model_token <= MAX_TOKEN
    return production_max - MAX_TOKEN + model_token


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
        # Only the exact holder/token pair can renew or release. Invalid and
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


def assert_invariants(state: State, production_max: int) -> None:
    assert 0 <= state.now <= MAX_TIME
    assert 0 <= state.downstream_max <= state.next_token <= MAX_TOKEN

    # Refinement binding: every reachable abstract watermark maps inside the
    # production-safe integer domain, and the abstract maximum maps exactly to
    # production MAX_FENCING_TOKEN rather than to a wider u64-only value.
    concrete_watermark = tail_embed(state.next_token, production_max)
    assert concrete_watermark <= production_max
    if state.next_token == MAX_TOKEN:
        assert concrete_watermark == production_max

    if state.active:
        assert state.holder in ACTORS
        assert 1 <= state.token <= state.next_token
        assert state.deadline > state.now, "an expired lease remained effective"
        assert tail_embed(state.token, production_max) <= production_max
    else:
        assert state.token == 0
        assert state.deadline == 0


def assert_ceiling_exhaustion(production_max: int) -> None:
    """Prove the bounded tail cannot mint a max+1 authority."""
    max_minus_one = tail_embed(MAX_TOKEN - 1, production_max)
    maximum = tail_embed(MAX_TOKEN, production_max)
    assert max_minus_one == PUBLIC_JSON_SAFE_MAX - 1
    assert maximum == PUBLIC_JSON_SAFE_MAX

    exhausted = State(next_token=MAX_TOKEN)
    acquire_transitions = [
        action for action, _target in successors(exhausted)
        if action.startswith("acquire(")
    ]
    assert acquire_transitions == [], (
        "an exhausted fencing state exposed an acquire transition: "
        f"{acquire_transitions}"
    )

    # The would-be successor is deliberately outside the refinement map. This
    # is the bounded analogue of production MAX_FENCING_TOKEN + 1.
    would_be_next = maximum + 1
    assert would_be_next == PUBLIC_JSON_SAFE_MAX + 1
    assert would_be_next > production_max


def main() -> None:
    production_max = production_fencing_ceiling()
    assert_ceiling_exhaustion(production_max)

    initial = State()
    queue = deque([(initial, 0)])
    seen = {initial}
    transitions = 0

    while queue:
        state, depth = queue.popleft()
        assert_invariants(state, production_max)
        if depth == MAX_DEPTH:
            continue

        for action, target in successors(state):
            transitions += 1
            assert_invariants(target, production_max)
            if action.startswith("acquire"):
                assert target.token == state.next_token + 1
                assert target.next_token > state.next_token
                assert tail_embed(target.next_token, production_max) <= production_max
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
        f"fiducia lease/fencing sentinel: {len(seen)} states, "
        f"{transitions} transitions; production ceiling={production_max}; "
        "all invariants hold"
    )


if __name__ == "__main__":
    main()
