# fiducia-node

The Raft-replicated **coordination engine** behind [fiducia.cloud](https://fiducia.cloud).
A node is a data-plane process that hosts shard replicas and serves the
coordination API. This repository is currently a **skeleton** — the
architecture and HTTP surface are in place; the consensus, state-machine, and
watch internals are stubbed with `TODO`s.

## What a node serves

Three coordination primitives, all over HTTP (`/v1`):

| Primitive            | Routes                              | What it is                                                      |
|----------------------|-------------------------------------|----------------------------------------------------------------|
| **Config KV + watches** | `/v1/kv/*`                       | Linearizable, versioned key/value with live `watch` streams (etcd/znode). |
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
funneling through one global leader. A command's *routing key* (KV key, election
name, or service name) is hashed to a shard; that shard's Raft group orders and
commits it. Writes to keys in different shards never serialize against each
other. This is the "multi-Raft" design used by CockroachDB (ranges) and TiKV
(regions).

### Skeleton vs. cluster

The current build is **single-node**: this process is the sole member — and thus
leader — of every shard, and "commit" means "append + apply locally". The shape
is the real multi-Raft shape; the cluster path slots in at the `TODO`s:

- per-shard peer RPC (`RequestVote` / `AppendEntries`) and elections
- per-shard log replication, quorum commit, snapshotting / compaction
- a change-event broadcast feeding the SSE `watch` endpoints
- TTL expiry sweeper for KV / leases / service instances

Shard **placement, rebalancing, scale up/down, and node-failure handling** are
not a node's job — they belong to the control plane, **`fiducia-brain`**, which
tells nodes which shards to host and moves leadership/replicas around.

## Layout

| File              | Responsibility                                                    |
|-------------------|-------------------------------------------------------------------|
| `src/main.rs`     | axum wiring, router, health/status                                |
| `src/consensus.rs`| multi-Raft core: shards, per-shard log + role, routing, `propose` |
| `src/state.rs`    | replicated state machine: `Command`s, KV/election/registry state  |
| `src/kv.rs`       | config KV + watch handlers                                        |
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
- [`fiducia-backend.rs`](https://github.com/fiducia-cloud/fiducia-backend.rs) — the website/marketing webserver.
- [`fiducia-ui.web`](https://github.com/fiducia-cloud/fiducia-ui.web) — the website frontend.
