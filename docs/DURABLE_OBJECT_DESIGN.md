# Durable-Object-inspired coordination design

Fiducia is not a reimplementation of Cloudflare Durable Objects. Its replicated
coordination state is Raft-backed and it must preserve union-lock atomicity,
quorum semantics, deterministic replay, and fencing across replicas. Durable
Objects are nevertheless the primary design reference for the *shape* of an
authority: one serialized correctness domain, durable state, bounded work,
idempotent retries, and explicit recovery from process restarts.

## 1. Atom of coordination

A Durable Object works best when one logical unit owns one correctness decision.
Fiducia applies the same rule, but the atom is the **conflict domain**, not always
one key.

A simple single-key lock can be reasoned about as one authority. A union lock
cannot be split independently by key: `{a,b}` and `{b,c}` overlap on `b`, so two
independent authorities could both grant unless the routing/transaction boundary
contains the overlap. Fiducia therefore keeps union-lock conflict detection in a
single replicated authority domain unless and until a formally verified routing
scheme can prove equivalent atomicity.

This is the key difference from blindly mapping every Fiducia key to one Durable
Object.

## 2. Persistent state is authority; memory is cache

Durable Objects may hibernate, restart, or move and must reconstruct correctness
from durable storage. Fiducia follows the same rule at a different layer:

- committed Raft log entries and snapshots are authoritative;
- leader-local timers, maps, and caches are accelerators only;
- restart/re-election must recover the same grant, queue, tombstone, fencing, and
  expiry decisions from replicated state;
- no safety property may depend on a shutdown callback running.

Snapshot restore must reject impossible or internally inconsistent authority
rather than repairing it heuristically.

## 3. Timers are wake-ups, never the lease clock

Cloudflare alarms are at-least-once and can be delayed/retried. Fiducia timers
have the same safety treatment:

- expiry is derived from replicated timestamps/lease state;
- a timer may propose deterministic cleanup after a deadline;
- acquire/renew/release/read paths still evaluate authoritative expiry;
- duplicate or late cleanup must be idempotent;
- missing a timer must not extend authority.

In other words, the stored deadline is the contract. A timer only causes work to
be revisited.

## 4. Retry and transport ambiguity

A network failure after a commit does not tell the caller whether the command was
committed. Correct clients therefore reuse the same logical request identity for
retries.

For acquisition:

- same request identity replays the same logical outcome;
- reissuing the same holder/resource acquire must not extend authority;
- only the token-bound renew operation extends a live lease;
- cancel uses the same request identity and must survive acquire/cancel races;
- if cancellation races promotion, the caller receives enough fenced grant data
  to release the raced grant explicitly.

Transport failure remains distinct from contention. "I do not know" is a safety
state, not proof that somebody else owns the resource.

## 5. Fencing domain

The public JSON contract uses positive fencing tokens from `1` through
`9_007_199_254_740_991`, JavaScript's largest exactly representable integer.
No authority may wrap or reuse a token.

The `/v1` response boundary fails closed if an unsafe token reaches serialization
(see `org_scope.rs`). The state-machine mint point, handoff floor advancement,
validation, snapshot restore, and formal model must enforce the same ceiling;
that remaining mint-point work is tracked by DEN-1154 / issue #61.

Fail-closed exhaustion is intentional. Availability is never recovered by
rounding, wrapping, resetting, or reusing fencing authority.

## 6. Backpressure is replicated semantics

A single Durable Object can overload if too much work queues behind one object.
Fiducia has the analogous risk at a Raft conflict domain. Admission limits cannot
be based on one leader's transient memory pressure because followers must replay
the same state-machine decision.

Queue/tombstone/capacity limits therefore need protocol-visible deterministic
rules: per-key, per-tenant, per-shard, and global bounds with explicit rejection
reasons. DEN-1600 tracks this work.

Clients should apply bounded retries with jitter for retryable overload while
preserving request identity. They must not convert overload/transport failures
into contention or silently switch to an unfenced fallback.

## 7. Stronger than a Durable Object where needed

Fiducia intentionally keeps properties that are outside a single Durable
Object's scope:

- Raft quorum replication across nodes/failure domains;
- deterministic replicated command application;
- multi-key union-lock atomicity;
- fencing tokens shared with external datastores;
- formal/refinement tests over queue, expiry, cancellation, recovery, and
  snapshots;
- explicit linearizable-read/leader-lease rules.

Cloudflare Durable Objects are therefore inspiration for the authority boundary
and lifecycle model, not a reason to replace Raft with eventually convergent
service-registry behavior.

## Review checklist

For every new coordination primitive or backend, verify:

1. What is the smallest correctness atom?
2. Which persisted state reconstructs authority after restart?
3. Is every retry tied to a stable logical request identity?
4. Can a timer run late, twice, or not at all without extending authority?
5. Are all queues/ledgers/body fields deterministically bounded?
6. Does transport ambiguity remain distinct from contention?
7. Can every authoritative write be fenced in the protected datastore?
8. Are public fencing tokens exact in every supported runtime?
9. Do snapshots/replay reject invalid authority rather than normalize it?
10. Are overload and capacity decisions replayable by every replica?
