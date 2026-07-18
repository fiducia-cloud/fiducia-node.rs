# fiducia-node — next steps (2026-07-18 deep-dive)

Concrete follow-ups found by the platform deep-dive. Full context + cross-repo
sequencing: `fiducia-monorepo/docs/discovery-backlog-2026-07.md` and
`.../hardening-roadmap-2026-07.md`. This repo's `persist.rs` is the reference-quality
implementation the brain should be ported toward; the gaps here are on the read path,
GC, and misconfig edges.

## P0 — highest correctness value (test-first)
- **C2 (CRITICAL, ~M) — read paths mutate replicated state + mint fencing tokens off the log.**
  `state.rs:2291,2301` (`semaphore_inventory`/`election_inventory` call `expire_due(now_ms())`),
  `state.rs:2409` (`expire_due` promotes waiters), `state.rs:3354` (`lock_promote` → `next_token()`),
  served ungated on any replica via `consensus.rs:2342 handle_query_local`.
  A `LockInventory`/`SemaphoreInventory`/`ElectionList` read landing on a **follower** promotes
  a queued waiter and **mints a fencing token on that follower alone** — not in the log →
  (a) replica divergence from deterministic replay, (b) two nodes mint the same token for
  different holders → fencing-token reuse / split-brain.
  **Fix:** add pure `*_live_at(now)` variants (compute live-at-now without mutating `self`,
  mirroring the read-only `record.view(name, now)` used by `barrier_get`/`task_get`); route
  `handle_query_local` through them. Keep `expire_due`/`*_promote`/`next_token` reachable ONLY
  from `apply_at` under `proposed_at_ms`.
  **Test first:** (a) an inventory read on a follower leaves `last_applied` and the fencing
  counter unchanged; (b) fencing-token monotonicity across snapshot→restore (P3-2).

## P2 — robustness / misconfig
- **M6 (~S) — `peers` never de-duplicated / self-excluded** (`consensus.rs:296-304`): a duplicate
  in `FIDUCIA_PEERS` double-counts a follower's `match_index` → commit-without-quorum. Dedup +
  drop self at parse; derive quorum/commit-count from the deduped set.
- **M11 (~M) — unbounded map growth**: tasks/effects/handoffs/decisions/barriers/rate_limits are
  never GC'd (`state.rs:2409` only prunes kv/locks/semaphores/elections/idempotency/services) →
  heap + snapshot bloat → slow InstallSnapshot/recovery, eventual OOM. Add a deterministic
  committed retention sweep (driven by `proposed_at_ms`) for terminal records; evict idle
  rate-limit buckets.
- **M27 (~S) — `FIDUCIA_KV_ENCRYPTION_KEY` (a secret) missing from `[env].ignore`** in
  `.cli-flags.toml` (`kv.rs:73` reads it). Add it.

## Lower priority (confirmed)
- `apply_task_claim` mints a fencing token before validation (`state.rs:2646`) — move `next_token()`
  below the not_found/terminal/!claimable guards (matches `apply_handoff_accept`).
- `proposed_at_ms` not clamped monotonic across leader change (`consensus.rs:1155,2028,2093`) — a
  lagging new leader's backward "now" can honor a lease past its intended expiry; clamp
  `now = proposed_at_ms.max(last_applied_ts)`.
- `CounterAdd` saturates silently at `i64::MAX` (`state.rs:2508`) — a counter used as a monotonic
  ID generator would hand out duplicates with no error; return `{ok:false, reason:"overflow"}`.
- Rate-limit bucket uses `f64` in the apply path (`state.rs:3546`) — deterministic on a fixed
  arch, but a heterogeneous cluster could diverge persisted `tokens`; convert to integer
  milli-token fixed-point.

## Test-coverage gaps to backfill (guardrails)
- P3-1: in-flight proposal drain on leadership loss (`fail_pending` resolves exactly once as
  `NotLeader`) — `consensus.rs:1267`.
- P3-2: fencing-token monotonicity after snapshot→restore (existing test asserts structure only).
- P3-3: InstallSnapshot / boot **rejection** of an invariant-violating snapshot (unit validator
  is tested; the integration wiring — `success==false`, live state untouched — is not) — `consensus.rs:1645`.

Persistence (`persist.rs`) is already reference-quality; the one open item is adding a
length+checksum header to persisted files (M5, shared with brain).

Toolchain: pins 1.95.0 — build/test with `RUSTC`+`RUSTDOC` set to the rustup 1.95.0 binaries
(a Homebrew 1.96.1 on PATH shadows rustup and breaks doctests). See the hardening-roadmap doc.
