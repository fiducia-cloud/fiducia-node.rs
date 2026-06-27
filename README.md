# fiducia-node

The Raft-replicated **coordination engine** behind [fiducia.cloud](https://fiducia.cloud).
A node is a data-plane process that hosts shard replicas and serves the
coordination API over HTTP.

The consensus core is **real**: each shard runs a faithful Raft (randomized
leader election, log replication with the `AppendEntries` consistency check,
quorum commit gated to the leader's term, step-down on a higher term, and
leader-only linearizable reads). Client writes block until their entry commits on
a quorum. A node hosts every shard and leads some / follows others. What's still
the control plane's job (and not done here) is dynamic shard **placement** —
moving replicas/leadership between nodes — which lives in
[`fiducia-brain`](https://github.com/fiducia-cloud/fiducia-brain.rs).

## What a node serves

All over HTTP (`/v1`):

| Primitive            | Routes              | What it is                                                      |
|----------------------|---------------------|----------------------------------------------------------------|
| **Locks (multi-key)** | `/v1/locks/*`      | Mutual exclusion over a **union** of keys — the flagship. Atomic all-or-nothing, FIFO, deadlock-free, fencing tokens, TTL leases. |
| **Semaphores**        | `/v1/semaphores/*` | Counting locks: up to `limit` concurrent holders, FIFO queue beyond the cap. |
| **Config KV + watches** | `/v1/kv/*`       | Linearizable, versioned key/value with live SSE `watch` streams (etcd/znode). |
| **Rate limiting**     | `/v1/rate-limit/*` | Atomic token-bucket / sliding-window checks per tenant+key.     |
| **Cron / schedules**  | `/v1/cron/*`       | Durable schedules with at-least-once / exactly-once run records. |
| **Leader election**   | `/v1/elections/*`  | Clients campaign for a named leadership with TTL leases + fencing tokens. |
| **Service discovery** | `/v1/services/*`   | TTL-health registry of live service instances (Consul/etcd).   |

Plus `/healthz`, `/readyz`, `/v1/status` (per-shard consensus status), and the
internal `/raft/{shard}/{append,vote}` peer endpoints.

## The flagship: multi-key UNION locks + semaphores

Fiducia's most valuable primitive is the distributed lock — and not just one key
at a time. You can lock the **union** of a key *set*:

```bash
# Acquire {orders/42, inventory/sku-9} atomically — all or nothing.
curl -XPOST localhost:8090/v1/locks/acquire \
  -d '{"keys":["orders/42","inventory/sku-9"],"holder":"worker-a","ttl_ms":30000,"wait":true}'
# → { "committed": true, "result": { "output": {
#       "acquired": true, "keys": ["inventory/sku-9","orders/42"],
#       "fencing_token": 7, "lease_expires_ms": ... } } }

# Release the whole set by its fencing token.
curl -XPOST localhost:8090/v1/locks/release -d '{"holder":"worker-a","fencing_token":7}'
```

Semantics (this is the [live-mutex](https://github.com/ORESoftware/live-mutex)
model, made linearizable by Raft):

- **Union, not intersection.** Holding `{a,b}` conflicts with anyone wanting
  `a` *or* `b`. A request for `{b,c}` waits; a disjoint `{d,e}` is granted
  immediately.
- **Atomic & deadlock-free.** A set is granted all-at-once or not at all — never
  half-held — so there's no hold-and-wait, hence no deadlock.
- **FIFO-fair, no starvation.** A queued multi-key request *reserves* its keys, so
  a later overlapping request can't barge ahead of it.
- **Fencing tokens.** Every grant carries a strictly-increasing token; pass it to
  the resource you're protecting to fence off a slow previous holder.
- **TTL leases.** A holder that dies has its grant auto-expire; the freed keys
  promote the next grantable waiter.

**Semaphores** generalize a lock to *N* holders (a mutex is `limit = 1`):

```bash
curl -XPOST localhost:8090/v1/semaphores/acquire \
  -d '{"key":"db-pool","holder":"conn-1","limit":10,"ttl_ms":30000,"wait":true}'
```

**Keys are never in the URL path** — they go in `?key=` (or, for the multi-key
lock acquire/release, the JSON body). So they're free of any path grammar and may
contain slashes, dots, or be empty (`flags/checkout`, `orders/42`,
`pools/db/primary`, even a key named `acquire`):

```bash
curl     'localhost:8090/v1/kv?key=flags/checkout'              # read
curl -XPUT 'localhost:8090/v1/kv?key=flags/checkout' -d '{"value":"on"}'
curl -N   'localhost:8090/v1/kv?key=flags/checkout&watch=true'  # SSE watch
curl     'localhost:8090/v1/locks?key=orders/42'               # inspect
```

This is also why the load balancer can read the routing key the same way on every
by-key request — it's always `?key=`, never a per-endpoint path shape.

> **Why locks route to one coordinator.** Granting `{a,b,c}` atomically and
> detecting it conflicts with a holder of `{b}` requires one state machine to see
> every member key together. So **all** lock/semaphore state lives in a single
> Raft group (the `LOCK_DOMAIN` routing key) — the single-broker model live-mutex
> uses. KV / rate-limit / discovery stay sharded by their own key. Sharding the
> lock space itself (cross-shard 2PC for sets that span coordinators) is the
> documented scaling path.

## Architecture: sharded multi-Raft

Fiducia does **not** run one Raft group for the whole keyspace. The keyspace is
partitioned into **shards**, and **each shard is its own independent Raft group**
with its own log, term, and elected leader.

```
                keyspace
   ┌──────┬──────┬──────┬──────┬─── … ──┐
   │shard0│shard1│shard2│shard3│  shardN │   (key → shard via stable hash)
   └──┬───┴──┬───┴──┬───┴──┬───┴─────────┘
      │      │      │      │
   ┌──▼──────▼──────▼──────▼───────────────┐
   │  node-a   node-b   node-c   …          │   each node hosts many shard
   │  L s0     L s1     L s2                │   replicas; Leader (L) for some,
   │  F s1     F s0     F s0                │   Follower (F) for others
   └───────────────────────────────────────┘
```

A physical node is **leader for some shards and follower for others**, so
leadership — and write throughput — spreads across the cluster instead of
funneling through one global leader. Writes to keys in different shards never
serialize against each other (CockroachDB ranges / TiKV regions).

### Concurrency model: one actor per shard

Each shard is an independent async task ([`ShardActor`](src/consensus.rs)) that
*owns* its Raft state and state-machine partition — no locks on the hot path.
HTTP handlers and the peer transport reach a shard only by sending it a message
and awaiting a reply. Outbound RPCs are **never awaited inside the actor**: it
spawns the send, and the reply (`VoteReply`/`AppendReply`) comes back as another
inbox message, so a slow peer can't stall the shard.

### Peer transport (testable in-process)

[`Transport`](src/transport.rs) has two backings: **HTTP** (`reqwest` → a peer's
`/raft/{shard}/…`) for production, and an in-process **loopback** registry for
tests — so a whole multi-node cluster (election + replication + failover) runs
deterministically in one process with no sockets. See the `consensus` tests.

## Durability — what "backs" the store?

**There is no external database.** Like etcd, Consul, and TiKV, Fiducia *is* the
database: the **replicated log + the deterministic state machine** are the store.

- **The state machine** ([`state.rs`](src/state.rs)) is a pure fold over the
  committed log: every mutation is a `Command`, applied in commit order, producing
  KV entries, lock grants, semaphore permits, leases, etc. Reapplying the same log
  always yields the same state.
- **Durability = replication.** A write is durable once a **quorum** of the
  shard's Raft group has it in their logs. Losing a minority of replicas loses no
  committed data; a new leader is elected from the up-to-date majority.
- **Recovery = replay.** A restarted/replacement replica catches up by replaying
  the log (tail via `AppendEntries`, or a snapshot + tail once the leader has
  compacted).

Today the per-shard log and state machine are **in-memory** (durability comes
from replication across nodes). The seam to add on-disk durability is narrow and
deliberate:

| Piece | Status | On-disk path |
|-------|--------|--------------|
| Raft log + `commit_index`/`voted_for` | in-memory `Vec<LogEntry>` per shard | append-only WAL with periodic `fsync` (one shared engine batches fsync across shards) |
| State machine | in-memory maps | rebuilt from the log; bounded by **snapshots** |
| Log growth | unbounded | **snapshot + compaction**: persist a state-machine snapshot, truncate the log before it |

This is the standard Raft durability stack (WAL → snapshot → compaction); none of
it changes the API or the state-machine semantics above. A single embedded engine
(e.g. a segmented WAL, or `redb`/`sled`) plugs in behind that seam.

## Layout

| File               | Responsibility                                                       |
|--------------------|----------------------------------------------------------------------|
| `src/main.rs`      | axum wiring, router, health/status                                   |
| `src/consensus.rs` | **multi-Raft core**: per-shard election, replication, quorum commit  |
| `src/transport.rs` | peer transport (HTTP + in-process loopback) + Raft RPC wire types    |
| `src/raft_api.rs`  | inbound `/raft/{shard}/{append,vote}` peer endpoints                  |
| `src/state.rs`     | replicated state machine: `Command`s, **union locks**, semaphores, KV, … |
| `src/locks.rs`     | multi-key union lock handlers                                        |
| `src/semaphore.rs` | counting-semaphore handlers                                          |
| `src/kv.rs`        | config KV + SSE watch handlers                                       |
| `src/rate_limit.rs`, `src/schedule.rs`, `src/election.rs`, `src/discovery.rs` | the other primitives |

## Run locally

```bash
cargo run          # listens on :8090 (override PORT)
# Single node (default): leads every shard from t=0.
# A real group:
#   FIDUCIA_NODE_ID=node-a:8090 FIDUCIA_PEERS=node-b:8090,node-c:8090 cargo run
curl localhost:8090/v1/status        # per-shard role / term / commit index
```

## Related

- [`fiducia-brain.rs`](https://github.com/fiducia-cloud/fiducia-brain.rs) — control plane (placement, scaling, failure handling).
- [`fiducia-node-sidecar.rs`](https://github.com/fiducia-cloud/fiducia-node-sidecar.rs) — per-node bridge to the brain + observability.
- [`fiducia-backend.rs`](https://github.com/fiducia-cloud/fiducia-backend.rs) — the website/marketing webserver.
