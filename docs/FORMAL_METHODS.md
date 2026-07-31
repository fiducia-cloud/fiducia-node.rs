# Formal-methods procedure: coordination safety

This repository implements coordination primitives whose failures are
qualitatively different from ordinary request failures: a split-brain grant,
stale fencing token, or double-committed effect can cause two workers to perform
an irreversible action. Changes to the Raft state machine and the replicated
coordination APIs therefore require an explicit model/evidence step.

## Verification boundary

The executable model in `formal/check_model.py` is a deliberately small
abstraction of the lease and fencing contract. Production concepts refine into
it as follows:

| Production concept | Abstract model |
| --- | --- |
| committed Raft grant | `acquire(actor)` |
| lease term / monotonically allocated fence | `next_token` / `token` |
| current effective owner | `holder` with `deadline > now` |
| expiry or committed release | `tick+expire` / `release` |
| downstream stale-writer defense | `downstream_max` |

The model does **not** prove the full Raft implementation, network transport,
storage engine, clock source, or every higher-level primitive. It makes the
safety contract precise and creates a review gate for the implementation tests
that refine it.

## Required invariants

A change touching consensus, leases, ownership, or effects must preserve all of
the following:

1. **Single effective holder.** At most one holder is effective for a key or
   atomic key-union at a logical instant.
2. **Monotonic fencing.** Every new committed grant has a token strictly greater
   than every earlier grant in that namespace.
3. **Stale-operation rejection.** Renew, release, commit, and completion require
   the exact current owner and fencing token.
4. **Fail-closed expiry.** A lease at or beyond its deadline is not effective,
   even if cleanup has not yet removed its record.
5. **Quorum before visibility.** A client-visible success is derived from a
   committed entry, never merely from leader-local acceptance.
6. **Leader-term commit rule.** A leader does not use an earlier-term entry to
   infer commitment of its current term without the Raft commitment rule.
7. **Exactly-once effects are fenced, not assumed.** Retries may occur; an
   idempotency/effect record plus the current fence prevents duplicate external
   effects.
8. **Snapshot refinement.** Install/restore preserves committed state,
   allocation counters, tombstones, and the minimum fencing token that a future
   grant must exceed.

Liveness claims must be stated separately. For example, FIFO queue progress
requires an eventual-leader/eventual-delivery assumption and must not be
presented as an unconditional safety proof.

## Change procedure

### 1. Classify the change

A formal review is required when a PR changes any of these surfaces:

- Raft election, append, commit, apply, snapshot, or recovery logic;
- lock, semaphore, election, task, handoff, barrier, effect, budget, decision,
  idempotency, cron-claim, or multi-key ownership semantics;
- fencing-token allocation, comparison, persistence, or API serialization;
- expiration, cancellation, retry, deduplication, or ambiguous-result handling;
- readiness behavior that can admit a node with unsafe or unavailable state.

Pure telemetry, spelling, generated documentation, and request presentation do
not require a model change unless they alter a value used in a safety decision.

### 2. Update the abstract transition first

Before changing production code, add or modify the smallest transition in
`formal/check_model.py` that captures the intended semantic change. Record:

- the state variables added or removed;
- the invariant affected;
- the finite bound used by the checker;
- assumptions intentionally left outside the model; and
- the production function/module expected to refine the transition.

A bounded model is not accepted merely because it finds no counterexample. The
bound must exercise at least two actors, expiry, reacquisition, and a stale
fencing attempt.

### 3. Run the bounded checker

```sh
python3 formal/check_model.py
```

The checker performs exhaustive breadth-first exploration within its declared
bounds and independently exhausts the downstream fence predicate. A failure
must print or be reduced to a reproducible state/action sequence before the PR
is approved.

### 4. Add refinement tests in Rust

For every changed abstract transition, add a deterministic production test at
the narrowest layer that can prove the refinement. At minimum, relevant changes
need tests for:

- grant → commit → apply ordering;
- lease expiry followed by reacquisition with a greater token;
- delayed stale renew/release/write after reacquisition;
- leader failover between acceptance and response;
- snapshot/restart with no token regression;
- atomic multi-key all-or-nothing behavior; and
- ambiguous client retry with idempotent result recovery.

Use controlled logical time and deterministic message scheduling where
possible. A wall-clock sleep or a probabilistic chaos run may supplement, but
must not replace, a deterministic counterexample test.

### 5. Record evidence in the PR

The PR description must include a **Formal evidence** section containing:

```text
Model transition(s):
Invariant(s):
Bound / assumptions:
Production refinement tests:
Commands and results:
Known unproved surface:
```

If the model is intentionally unchanged, explain why the implementation change
is a refinement-preserving refactor and name the tests that demonstrate this.

## Reviewer checklist

A reviewer should block the PR when any answer is unclear:

- Can two histories produce two effective owners for the same namespace?
- Can a restored or newly elected node allocate a lower/equal fencing token?
- Can a stale token mutate state after expiry, cancellation, or reassignment?
- Does an API success correspond to committed/applied state?
- Is wall-clock time being confused with Raft/logical ordering?
- Are retry and timeout outcomes modeled as unknown rather than as failure?
- Do snapshots and migrations preserve every safety-relevant monotonic field?
- Is a liveness statement relying on an unstated fairness or synchrony
  assumption?

## Escalation path

The bounded Python model is the mandatory baseline. Introduce or update a
TLA+/PlusCal, Alloy, Apalache, Kani, Loom, Shuttle, or model-based property test
when the change adds concurrency or state dimensions that cannot be represented
without collapsing a safety distinction. Keep the smaller model as a fast CI
sentinel and link the stronger artifact from this document.
