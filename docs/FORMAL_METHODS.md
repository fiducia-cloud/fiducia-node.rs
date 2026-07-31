# Formal change and evidence procedure

Fiducia's coordination guarantees are safety contracts, not implementation
intentions. A change to consensus, locking, leases, fencing, snapshots, or an
exactly-once workflow must identify the affected invariant and provide evidence
at the model, trace, and production-refinement layers.

This procedure complements the repository's existing formal verification stack;
it does not replace it.

## Verification stack

| Layer | Repository mechanism | Purpose |
| --- | --- | --- |
| protocol model | Quint specifications under `formal/` | precise state transitions and invariants |
| bounded checking | Apalache through `agent-check formal-verify*` | exhaustive counterexample search within reviewed bounds |
| deterministic traces | `agent-check formal-test` / `formal-simulate` | named critical histories and invariant reachability |
| model-based traces | `agent-check formal-mbt` | generate implementation-facing transition histories |
| production refinement | `tests/formal_union_lock_refinement.rs` and ITF replay | show Rust behavior refines model transitions |
| independent sentinel | `formal/check_lease_fencing_sentinel.py` | dependency-free cross-check of lease/fence fundamentals |
| provenance | `agent-check formal-provenance` | record tools, inputs, bounds, and artifacts used by CI |

The Python sentinel is intentionally smaller and independent. It is useful as a
fast disagreement detector; passing it is not a substitute for Quint typechecking,
Apalache, or Rust refinement tests.

## Safety obligations

A relevant change must preserve every applicable obligation:

1. **Single effective owner:** one key or atomic key-union cannot have two
   effective holders at the same logical instant.
2. **Monotonic fencing:** every newly committed grant receives a token strictly
   greater than all earlier grants in the namespace, including after restart or
   snapshot installation.
3. **Exact stale-operation rejection:** renew, release, complete, cancel, and
   downstream mutation reject a stale holder/token pair.
4. **Commit before visibility:** client-visible success derives from committed
   and applied replicated state, not leader-local acceptance.
5. **Atomic union semantics:** a multi-key request acquires all keys or none;
   cancellation and timeout cannot leave a hidden partial grant.
6. **Ambiguous retry safety:** timeout and transport loss are modeled as unknown
   outcomes; request identity, cancellation, and idempotency make retry safe.
7. **Snapshot refinement:** restore preserves allocation counters, tombstones,
   queue order where promised, and all other safety-relevant monotonic state.
8. **Exactly-once effects are fenced:** external effects may be retried, but a
   committed effect/idempotency record and fencing token prevent duplication.
9. **Fail-closed readiness:** a node with unavailable, corrupt, or unapplied
   durable state cannot advertise readiness for authoritative traffic.

Liveness is reviewed separately. FIFO progress, eventual grant, and leader
availability require explicit fairness, delivery, and eventual-synchrony
assumptions; they must not be presented as unconditional safety theorems.

## When this procedure is required

Use the full procedure for changes to:

- election, append, commit, apply, recovery, snapshot, or membership behavior;
- locks, semaphores, elections, tasks, handoffs, barriers, effects, budgets,
  decisions, idempotency, or cron-claim semantics;
- fencing-token allocation, persistence, comparison, or serialization;
- expiry, queueing, cancellation, retry, deduplication, and ambiguous responses;
- storage or readiness paths that can affect authoritative state.

A presentation-only or telemetry-only change may state "no formal transition
change" only when it names the untouched safety boundary and provides the
ordinary regression tests that support that classification.

## Change procedure

### 1. State the semantic delta first

Before editing production code, write down:

- the old and new transition;
- state variables and guards affected;
- invariant(s) potentially weakened or strengthened;
- assumptions and finite bounds;
- the Rust function/module expected to refine the transition.

If the intended behavior cannot be stated as a transition and postcondition, the
change is not ready for implementation review.

### 2. Update the strongest applicable model

Change the Quint model and named deterministic traces whenever the abstract
behavior changes. Add a minimal counterexample trace before the fix when
practical. Keep symmetry reductions and bounds reviewable; do not increase a
bound merely to make a failing run disappear.

Update `formal/check_lease_fencing_sentinel.py` when lease/fence fundamentals
change. A disagreement between the sentinel and the Quint model is a review
blocker until the abstraction mismatch is explained.

### 3. Add production refinement evidence

For each changed abstract transition, add a deterministic Rust test at the
narrowest layer that exercises the real state machine. Relevant changes should
cover, as applicable:

- grant → commit → apply ordering;
- expiry and reacquisition with a greater fence;
- delayed stale renew/release/write after reassignment;
- leader failover between acceptance and response;
- snapshot/restart without token regression;
- multi-key all-or-nothing behavior;
- ambiguous retry and cancellation ordering;
- ITF/model-generated trace replay.

Wall-clock sleeps and probabilistic chaos runs may supplement but cannot replace
a deterministic trace.

### 4. Run the locked verification commands

```sh
python3 formal/check_lease_fencing_sentinel.py
nix develop -c agent-check formal-typecheck
nix develop -c agent-check formal-test
nix develop -c agent-check formal-simulate
nix develop -c agent-check formal-mbt
nix develop -c agent-check formal-verify
nix develop -c agent-check formal-refinement
nix develop -c agent-check formal-provenance
```

Use `formal-verify-deep` for scheduled/manual deep bounds and before merging a
change that expands protocol state or concurrency.

### 5. Record evidence in the PR

Every applicable PR should contain:

```text
Formal surface:
Old → new transition:
Safety/liveness obligation(s):
Model/spec files:
Bounds and assumptions:
Deterministic/model-generated traces:
Production refinement tests:
Commands and results:
Known unproved surface:
Artifact/provenance location:
```

## Reviewer stop conditions

Block approval when any of these is unresolved:

- a success response can precede commit/apply;
- two histories can produce two effective owners;
- a restored node can reuse or lower a fence;
- timeout is treated as proof of failure;
- a stale token can mutate replicated or downstream state;
- a snapshot/migration omits a monotonic field or tombstone;
- a liveness claim relies on unstated fairness/synchrony;
- the model changed without a production refinement trace, or vice versa;
- tool versions, bounds, or verification inputs are not reproducible.
