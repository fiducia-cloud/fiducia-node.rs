#!/usr/bin/env python3
"""Fail CI if replicated-state read APIs regain mutation side effects.

This is intentionally a source-level guard in addition to Rust tests. The original
P0 regression was a direct call from inventory methods into `expire_due`, which
can promote waiters and mint fencing tokens. Keeping the forbidden call graph
visible here makes that safety boundary reviewable even when time-dependent tests
would otherwise miss it.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE = ROOT / "src" / "state.rs"
CONSENSUS = ROOT / "src" / "consensus.rs"

READ_METHODS = (
    "lock_inventory",
    "semaphore_inventory",
    "election_inventory",
)

FORBIDDEN_CALLS = (
    "expire_due(",
    "lock_promote(",
    "semaphore_promote(",
    "election_promote(",
    "next_token(",
)

EXPECTED_DELEGATES = {
    "lock_inventory": "lock_inventory_live_at(now_ms())",
    "semaphore_inventory": "semaphore_inventory_live_at(now_ms())",
    "election_inventory": "election_inventory_live_at(now_ms())",
}


def extract_function(source: str, name: str) -> str:
    pattern = re.compile(rf"\b(?:pub\s+)?fn\s+{re.escape(name)}\s*\(")
    match = pattern.search(source)
    if match is None:
        raise AssertionError(f"missing function: {name}")

    brace = source.find("{", match.end())
    if brace < 0:
        raise AssertionError(f"missing body for function: {name}")

    depth = 0
    for index in range(brace, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[match.start() : index + 1]
    raise AssertionError(f"unterminated body for function: {name}")


def main() -> int:
    state = STATE.read_text(encoding="utf-8")
    consensus = CONSENSUS.read_text(encoding="utf-8")
    failures: list[str] = []

    for method in READ_METHODS:
        body = extract_function(state, method)
        delegate = EXPECTED_DELEGATES[method]
        if delegate not in body:
            failures.append(f"{method} must delegate to {delegate}")
        for forbidden in FORBIDDEN_CALLS:
            if forbidden in body:
                failures.append(f"{method} contains forbidden mutating call {forbidden}")

    query_handler = extract_function(consensus, "handle_query_local")
    for method in READ_METHODS:
        if f".{method}()" not in query_handler:
            failures.append(f"handle_query_local no longer routes through {method}()")

    if failures:
        print("pure read-path guard failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("pure read-path guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
