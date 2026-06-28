# Fiducia Node Storage

## Short Answer

The config KV store is backed by the same thing that backs every Fiducia
coordination primitive: the owning shard's replicated Raft log and its applied
state-machine snapshot.

It is not backed by Postgres, Supabase, Redis, or a single central database.
Postgres/Supabase are for the business/control plane: orgs, projects, users,
API keys, audit, billing, and dashboard metadata.

## Current State

Today `StateMachine` keeps the applied materialized state in memory while the
public contract follows the production path:

1. A request is routed by key to a shard.
2. The shard leader appends the mutation as a `Command`.
3. The command is acknowledged only after the shard's Raft quorum commits it.
4. The applied state machine materializes the current KV value, revision, TTL,
   wait queue, limiter bucket, schedule history, election holder, or service
   registry entry.

Reads come from leader-applied state. Followers return a `not_leader` response
with the leader address so the load balancer can reroute.

## Production Engine

Use an embedded RocksDB database on every `fiducia-node` process.

Default path:

```text
FIDUCIA_NODE_DATA_DIR=/var/lib/fiducia-node
```

RocksDB is the right default for this layer because Fiducia needs low-latency
append-heavy Raft logs, prefix scans, compaction, snapshots, and a mature crash
recovery story. The database is local to one node. The distributed source of
truth remains Raft quorum, not the local database by itself.

Recommended column-family layout:

| Column family | Contents |
|---------------|----------|
| `raft_log` | Per-shard log entries keyed by `{shard_id, index}`. |
| `raft_meta` | Per-shard hard state, conf state, current term, vote, commit index, and applied index. |
| `state_kv` | Applied config KV entries keyed by `{shard_id, key}` with revision and optional expiry. |
| `state_locks` | Applied mutex, semaphore, multi-key lock, election lease state, and fencing-token counters. |
| `state_limits` | Applied rate-limit buckets/windows keyed by `{shard_id, tenant, key}`. |
| `state_schedules` | Schedule definitions, run history, retry state, and exactly-once fire IDs. |
| `state_services` | Service-discovery registrations and heartbeat leases. |
| `watch_index` | Recent committed revisions and key/prefix fanout cursors for SSE/WebSocket watches. |
| `snapshots` | Compact point-in-time shard snapshots used for replay and learner catch-up. |

## Write Path

For each committed mutation:

1. Persist the Raft log entry before it can be considered durable.
2. Advance `commit_index` once a majority has stored the entry.
3. Apply the command exactly once to the shard state machine.
4. Persist the resulting applied-state update and `applied_index`.
5. Emit watch events after the applied index advances.

The response can be acknowledged only after the command is durably committed by
a quorum. The local RocksDB write makes one replica crash-safe; the Raft quorum
makes the operation fleet-safe.

Lock writes include single-key mutexes, capped semaphores, and bounded
multi-key union locks. A multi-key grant stores the same `lock_id` under every
member key and stores a distinct fencing token per key. Release by `lock_id`
removes the holder from all members in one committed command, so there is no
partial release window.

## Recovery

On restart, a node opens RocksDB, loads `raft_meta`, restores the newest
snapshot for each hosted shard, and replays log entries after the snapshot's
last included index. Expired TTL data may be discarded during replay, but only
according to the committed timestamps in the log/snapshot.

## Compaction And Retention

Snapshots allow old log entries to be compacted once every active replica has
either applied them or can receive a snapshot. Retention policy should be
per-primitive:

- KV values: keep the latest live value and revision; compact old revisions
  after watch retention expires.
- Watches: keep enough recent revisions for reconnect and resume.
- Locks/semaphores/elections/service discovery: keep live leases plus
  audit/diagnostic events according to operator retention. Composite lock
  snapshots must preserve every member key, the shared `lock_id`, and each
  per-key fencing token.
- Schedules: keep run history according to plan limits and customer retention.
- Rate limits: keep only live bucket/window state unless analytics export is
  enabled.

## Disaster Recovery

Disaster recovery exports are shard snapshots plus the matching Raft metadata,
not SQL dumps. Restores must preserve shard IDs, last included index/term, and
fencing-token monotonicity so stale holders cannot become valid after restore.

Business-plane restore remains separate: Supabase/Postgres restores orgs,
projects, API keys, audit, and dashboard metadata.
