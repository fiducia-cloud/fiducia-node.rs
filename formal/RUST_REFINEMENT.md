# Rust implementation refinement checks

`tests/formal_union_lock_refinement.rs` closes the first implementation-facing
verification gap in the protocol-v2 union-lock work. The Quint specification is
still the design model; this test compiles the production `StateMachine` and
compares it after every explored transition with a small independent Rust
reference transition system.

## What is checked

The harness explores a finite domain containing:

- three holders;
- two lock keys and the key sets `{k1}`, `{k2}`, and `{k1,k2}`;
- two request identities, including two attempts that deliberately share the
  same `(holder, canonical key-set)` queue identity;
- blocking and no-wait acquisition;
- exact retries with changed TTL/wait arguments;
- token-bound renewal and holder-bound release, including rejected stale/wrong
  authority;
- cancel-before-acquire, queued cancellation, duplicate cancellation, and
  cancel-after-promotion behavior;
- lease, waiter, and cancellation-tombstone deadlines;
- overlapping FIFO reservations with progress for disjoint work;
- snapshot/restore after arbitrary reachable states;
- fail-closed fencing-token exhaustion, including preservation of an already
  queued waiter.

For each transition, the test compares the production result and a canonical
projection of the complete authority-bearing state:

- last minted fencing token;
- every grant's attempt identity, canonical key set, token, and deadline;
- FIFO waiter order, immutable request identity, held TTL, request time, and wait
  deadline;
- the held-key secondary index;
- exact cancellation digests and expiration times.

The state is invariant-checked after every step: live grants are disjoint,
fencing tokens are unique and monotonic, queue identities are unique, an attempt
cannot be both queued and granted, and cancellation tombstones exclude matching
grants and waiters.

## Deliberate bounds and claim strength

The breadth-first exploration is capped at depth 5, 25,000 unique abstract
states, and 400,000 checked transitions. The test fails rather than silently
truncating if either resource cap is reached. Coverage assertions also fail if a
critical behavior listed above becomes unreachable.

This is a bounded implementation-refinement check for the replicated lock state
machine. It is not an unbounded liveness proof and does not replace Raft transport,
partition, stale-leader, disk-fault, or multi-process fault injection. Those are
separate models and integration layers under DEN-80.

## Run locally

```bash
nix develop -c agent-check formal-refinement
```

Ordinary Rust CI runs this exact targeted command with `--nocapture`, so the
explored-state summary is visible in the required check, and then runs the full
`cargo test --all-targets --all-features --locked` suite. The separate
formal-methods workflow typechecks, simulates, generates ITF traces, and performs
bounded Apalache checking for the Quint specification.
