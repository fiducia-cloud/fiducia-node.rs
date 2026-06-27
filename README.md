# fiducia-node

The Raft-replicated **coordination engine** behind [fiducia.cloud](https://fiducia.cloud).
A node is the data-plane process that runs on each VM or bare-metal machine,
hosts shard replicas, and serves the coordination API. This repository is
currently a **skeleton** — the architecture and HTTP surface are in place; the
clustered consensus and watch internals are still marked with `TODO`s.

## What a node serves

Six coordination primitives, all over HTTP (`/v1`):

| Primitive            | Routes                              | What it is                                                      |
|----------------------|-------------------------------------|----------------------------------------------------------------|
| **Config KV + watches** | `/v1/kv/*`                       | Linearizable, versioned key/value with live `watch` streams (etcd/znode). |
| **Mutual-exclusion locks** | `/v1/locks/*`                | TTL leases, fencing tokens, blocking/try acquire, FIFO waiters. |
| **Rate limiting**    | `/v1/rate-limit/*`                  | Token-bucket and sliding-window decisions committed per shard.  |
| **Cron / scheduling** | `/v1/cron/*`                       | Recurring and one-shot schedules with durable run history.      |
| **Leader election**     | `/v1/elections/*`                | Clients campaign for a named leadership with TTL leases + fencing tokens. |
| **Service discovery**   | `/v1/services/*`                 | TTL-health registry of live service instances (Consul/etcd).   |

Plus `/healthz`, `/readyz`, and `/v1/status` (per-shard consensus status).

## Architecture: sharded multi-Raft

Fiducia does **not** run one Raft group for the whole keyspace. The keyspace is
partitioned into **shards**, and **each shard is its own independent Raft
group** with its own log, term, and elected leader.

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
leadership — and write throughput — spreads across the whole cluster instead of
funneling through one global leader. The target placement invariant is that
every healthy node leads at least one shard and follows others; after failover,
a node may temporarily lead more shards until the control plane rebalances.
A command's *routing key* (KV key, lock key, rate-limit key, schedule name,
election name, or service name) is hashed to a shard; that shard's Raft group
orders and commits it. Writes to keys in different shards never serialize
against each other. This is the "multi-Raft" design used by CockroachDB
(ranges) and TiKV (regions).

### Skeleton vs. cluster

The current build is **single-node**: this process is the sole member — and thus
leader — of every shard, and "commit" means "append + apply locally". The shape
is the real multi-Raft shape; the cluster path slots in at the `TODO`s:

- per-shard peer RPC (`RequestVote` / `AppendEntries`) and elections
- per-shard log replication, quorum commit, snapshotting / compaction
- a change-event broadcast feeding the SSE `watch` endpoints
- TTL expiry sweeper for KV / leases / service instances

Shard **placement, rebalancing, scale up/down, node-failure handling, and
leader redistribution** are not a node's job — they belong to the control
plane, **`fiducia-brain`**, which tells nodes which shards to host and moves
leadership/replicas around.

### Storage backing

Config KV is **not** backed by Postgres, Supabase, Redis, or one central
database. The production backing store is the owning shard's replicated Raft log
plus local per-node snapshots. Each `PUT`/`DELETE` is a committed `Command` in
that shard's log; the applied state machine materializes the latest value,
revision, TTL, and watch events.

The current skeleton keeps that applied state in memory. The durable node store
is specified in [`docs/storage.md`](docs/storage.md): embedded RocksDB under
`FIDUCIA_NODE_DATA_DIR`, with column families for Raft log/meta, applied
coordination state, watch indexes, and snapshots. Postgres/Supabase remain the
business/control-plane database for orgs, projects, users, API keys, and audit.

## Layout

| File              | Responsibility                                                    |
|-------------------|-------------------------------------------------------------------|
| `src/main.rs`     | axum wiring, router, health/status                                |
| `src/consensus.rs`| multi-Raft core: shards, per-shard log + role, routing, `propose` |
| `src/state.rs`    | replicated state machine: `Command`s and coordination state       |
| `src/kv.rs`       | config KV + watch handlers                                        |
| `src/locks.rs`    | mutual-exclusion lock handlers                                    |
| `src/rate_limit.rs` | distributed rate-limit handlers                                 |
| `src/schedule.rs` | cron / one-shot schedule handlers                                 |
| `src/election.rs` | leader-election handlers                                          |
| `src/discovery.rs`| service-discovery handlers                                        |

## Run locally

```bash
cargo run          # listens on :8090 (override PORT)
# env: FIDUCIA_NODE_ID, FIDUCIA_PEERS=host1,host2, FIDUCIA_SHARD_COUNT=16
curl localhost:8090/v1/status
```

## Related

- [`fiducia-brain.rs`](https://github.com/fiducia-cloud/fiducia-brain.rs) — control plane (placement, scaling, failure handling).
- [`fiducia-load-balance.rs`](https://github.com/fiducia-cloud/fiducia-load-balance.rs) — key-aware router that sends requests to each shard leader.
- [`fiducia-backend.rs`](https://github.com/fiducia-cloud/fiducia-backend.rs) — the customer portal/webserver.
- [`fiducia-ui.web`](https://github.com/fiducia-cloud/fiducia-ui.web) — the website frontend.
