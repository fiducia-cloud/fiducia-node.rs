# RFC 0001: Reservations — long-TTL, inspectable leases

**Repo:** `fiducia-cloud/fiducia-node.rs` (engine) + `fiducia-cloud/fiducia-interfaces` (schema/codegen)
**Branch:** `feat/reservations-primitive` → `main` · **Status:** Draft

## Motivation

Fiducia's locks are built for critical sections: seconds-long TTLs, heartbeat renewal, fencing tokens, one anonymous holder. Users keep asking a different question: "can I hold this *thing* for this *customer* for 90 minutes?" — shopping carts, ticket seats, appointment slots, GPU bookings. Today they either misuse a lock (90-minute TTL on a mutex: no capacity semantics, no listing, holder death releases a hold the customer still believes they have) or build reservation tables in their own database and only come to Fiducia for the sweeper election.

Reservations are a distinct primitive: **capacity-aware, long-lived, queryable, owned by an external principal rather than a process.** Crash of the *claiming process* must NOT release the reservation — the customer still holds it. That single difference (no liveness coupling) is why this cannot be a mode on locks.

## Non-goals

- Not a replacement for the app's database as system of record for the *domain object* (the seat, the order). Fiducia holds the reservation state; the app maps reservation keys to domain rows. For single-database apps a `held_until` column remains the right answer; reservations target multi-service and multi-region inventory where no single DB owns availability.
- No payment/ordering semantics. `convert` is a terminal state transition, nothing more.

## API sketch

```
POST /v1/reservations
  { "pool": "event-421/sec-A", "capacity_ref": true,
    "owner": "cart:7f3a...", "count": 2, "ttl": "90m" }
→ { "reservation_id": "rsv_01H...", "expires": "...",
    "fencing_token": "00000000-2B11", "pool_remaining": 141 }

POST /v1/reservations/{id}/extend     { "ttl": "15m" }   (bounded by pool max_ttl)
POST /v1/reservations/{id}/convert    { "token": "..." } (terminal: held → converted)
POST /v1/reservations/{id}/release
GET  /v1/reservations/{id}
GET  /v1/pools/{pool}                  → capacity, held, converted, remaining
GET  /v1/pools/{pool}/reservations?owner=cart:7f3a&state=held   (listing!)
WATCH /v1/pools/{pool}                 → events: reserved | extended | expiring(T-60s) | expired | converted | released
```

Pools declare capacity (`capacity: 143`) or are unbounded (pure timed holds). `expiring` watch events give users the "cart expires in 10 minutes" hook for free.

## Semantics

- **No heartbeat liveness.** TTL is the only expiry mechanism. `extend` is explicit and policy-bounded (`max_ttl`, `max_extensions` per pool).
- **Fencing tokens on convert/release** defeat stale retried workers, same guarantee as locks.
- **Capacity is linearizable.** Reserve/expire/convert are committed log entries; `pool_remaining` can never oversell within a pool (a pool never spans shards — see below).
- **Expiry is exactly-once.** The shard leader owns a timer index over its reservations; expiry is proposed as a log entry, so followers replay the same expiry on failover and watchers see exactly one `expired` event.

## Engine changes (`fiducia-node.rs`)

1. New state-machine entries: `ReservationCreate/Extend/Convert/Release/Expire`.
2. Per-shard timer wheel keyed by expiry; only the leader proposes `Expire` entries. Coarse (1s) resolution is fine at 90-minute scales.
3. **Shard routing:** reservations route by `pool`, not by reservation id, so a pool's capacity accounting is single-shard linearizable (uses existing `fnv1a + shard_for` from `fiducia-routing.rs`). Hot-pool mitigation (sub-pools) deferred.
4. Snapshot format bump: reservations + timer state included; log-compaction safe because expiry entries are idempotent on replay.
5. Watch fan-out reuses the KV watch plumbing with a new event enum.

## Interfaces (`fiducia-interfaces`)

JSON Schema for the six endpoints + watch events; codegen to Rust/TS/Python/Go clients. `state` enum: `held | converted | released | expired`.

## Migration/compat

Purely additive: new keyspace prefix `rsv/`, new log-entry tags, snapshot version gate. Old nodes reject the new entries → require cluster min-version before enabling (existing feature-gate mechanism).

## Open questions

1. Should `convert` optionally return a signed receipt (JWT) the app can store as proof, for audit trails?
2. Per-owner reservation limits (anti-scalping: max N held per owner per pool) — v1 or follow-up?
3. Regional pools with global capacity — out of scope here, but the pool abstraction shouldn't preclude it.
