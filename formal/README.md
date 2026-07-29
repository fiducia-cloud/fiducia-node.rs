# Formal verification

This directory contains executable specifications for correctness-critical
`fiducia-node` state machines. Product models remain next to the implementation;
the planned shared Rust runner in DEN-565 will discover and execute `fm.toml`
without moving product semantics into a central repository.

## First model: protocol-v2 union locks

`union_lock.qnt` models the modern attempt-scoped multi-key lock lifecycle from
`src/state.rs`:

- immediate all-or-nothing union acquisition;
- holder/key-set/request-id retry identity;
- FIFO queue reservations with progress for disjoint work;
- holder/token renewal and release authority;
- logical-clock lease and waiter expiry;
- cancellation tombstones and the cancel-versus-promotion race;
- fresh, monotonic fencing tokens and bounded token exhaustion;
- abstract snapshot round-trip equivalence.

`union_lock_test.qnt` contains deterministic scenario traces for the most
important boundaries, including overlapping anti-barging, disjoint queue
progress, retry non-extension, cancellation, expiry, and union promotion.

### Source correspondence

| Model concept | Implementation |
|---|---|
| commands and protocol-v2 attempt identity | `src/state.rs`: `LockAcquireV2`, `LockAcquireAttempt`, `LockRenew`, `LockRelease`, `LockCancelAttempt` |
| active grants, held-key index, and indexed FIFO queue | `LockManager`, `LockGrant`, `LockWaiter` |
| transition semantics | `apply_lock_acquire`, `apply_lock_renew`, `apply_lock_release`, `apply_lock_cancel` |
| reservation and promotion | `lock_first_grantable`, `lock_promote` |
| logical expiry and persistence | `expire_due`, state-machine snapshot/restore validation |

## Checked safety properties

The aggregate invariant `union_lock_safety` checks:

1. every key set and identity remains in the finite model domain;
2. live grants are pairwise disjoint, so a key has at most one owner;
3. every grant is atomic over its full union of keys;
4. fencing tokens form a contiguous, never-reused monotonic history;
5. queue sequence numbers and holder/key-set identities are unique;
6. an attempt cannot be both queued and granted;
7. a live cancellation tombstone excludes both queued and granted authority;
8. grants, waiters, and tombstones cannot remain observable after expiry.

The deterministic tests also check behavior that is easier to understand as a
trace than as a state invariant: exact retries do not extend authority, a
blocked queue head reserves its overlapping keys, disjoint work can progress,
and cancellation racing after promotion preserves the live grant.

## Rust implementation refinement

`tests/formal_union_lock_refinement.rs` compiles the production `StateMachine`
and compares its output and complete authority-bearing state after every
explored transition with an independent Rust reference transition system. The
canonical projection includes grants, the held-key index, FIFO waiter order,
immutable attempt identity, logical deadlines, cancellation digests, and the
last minted fencing token.

The breadth-first implementation check is capped at depth 5, 25,000 unique
abstract states, and 400,000 transitions. It fails rather than silently
truncating at a resource cap, and separate coverage assertions require every
load-bearing behavior to remain reachable. A targeted regression also verifies
that `u64` fencing-token exhaustion fails closed without dropping an existing
waiter. See `RUST_REFINEMENT.md` for the exact domain and claim boundary.

## Exact bounds and claim strength

This is a **finite abstraction plus bounded implementation refinement**, not a
proof of the unbounded production system. CI records the model and adapter
bounds in `fm.toml`: three holders, two keys, two request ids, two live waiters,
six queue sequence values, logical time through eight, four abstract fencing
tokens, and the Rust exploration caps above.

- `quint test` validates named deterministic traces.
- `quint run` explores 10,000 traces through 35 transitions and requires the
  critical-state witnesses to be reached.
- `quint verify` uses Apalache for exhaustive checking through the configured
  bounded transition depth on pull requests, pushes to `main`, scheduled runs,
  and manual dispatch.
- `cargo test --test formal_union_lock_refinement` checks the real Rust state
  machine against the independent reference model and runs directly in the
  formal-methods workflow as well as ordinary Rust CI.
- Generated ITF traces are retained as artifacts, but the Rust refinement
  harness does not yet consume those exact generated files. It independently
  explores the same transition contract.
- Liveness is not yet claimed. A later model must state fairness, logical-clock,
  eventual-release, and non-exhaustion assumptions explicitly.
- Raft transport, partitions, stale leaders, disk faults, and multi-process
  recovery remain separate DEN-80 verification and fault-injection layers.

## Local commands

```bash
QUINT_PACKAGE='@informalsystems/quint@0.32.0'

npx --yes --package="$QUINT_PACKAGE" quint typecheck formal/union_lock.qnt
npx --yes --package="$QUINT_PACKAGE" quint typecheck formal/union_lock_test.qnt
npx --yes --package="$QUINT_PACKAGE" quint test \
  formal/union_lock_test.qnt --main=union_lock_test --match='.*Test$'
npx --yes --package="$QUINT_PACKAGE" quint run \
  formal/union_lock.qnt \
  --main=union_lock \
  --max-samples=10000 \
  --max-steps=35 \
  --invariant=union_lock_safety \
  --witnesses \
    queued_work_reached \
    concurrent_disjoint_grants_reached \
    cancellation_tombstone_reached \
    token_exhaustion_reached
npx --yes --package="$QUINT_PACKAGE" quint verify \
  formal/union_lock.qnt \
  --main=union_lock \
  --max-steps=10 \
  --invariant=union_lock_safety
cargo test --test formal_union_lock_refinement --locked -- --nocapture
```

Java 17 or newer is required for the bounded `quint verify` job.

## Next implementation slice

The next adapter slice is to feed the generated ITF corpus through the existing
canonical Rust projection so one counterexample can move unchanged from Quint
to a checked-in implementation regression. The next distributed-system slice
is a separate Raft model and fault harness for partitions, stale leaders,
leadership transfer, crash/restart, durable log recovery, and duplicate delivery.
TypeScript, Dart, Go, and Gleam adapters should use the same versioned
JSON-lines/ITF contract from DEN-565.
