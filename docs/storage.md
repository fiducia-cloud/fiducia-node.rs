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

Reads come from leader-applied state. Followers return a retryable
`503 not_leader` response with `Retry-After` and, when known, a trusted-hop
leader hint so the load balancer can reroute. They do not emit `Location`;
generic clients retry their configured Fiducia endpoint without forwarding
credentials to a server-selected authority.

## Durable Engine

The current engine uses a small fsync-backed store per shard under
`FIDUCIA_DATA_DIR` (default `/var/lib/fiducia`):

| File | Contents and update rule |
|------|--------------------------|
| `meta` | Current term, vote, and commit index. Written to a temporary file, fsynced, atomically renamed, then followed by a directory fsync. |
| `log` | Newline-delimited `LogEntry` records. Pure tails are appended and fsynced; conflict replacement uses an atomic full rewrite. |
| `snapshot` | State-machine image plus last included index/term. Atomically replaced and fsynced before the compacted log prefix is removed. |

The applied state machine remains in memory and is deterministically rebuilt
from snapshot plus committed log suffix. The local files make one replica
crash-safe; the Raft quorum remains the distributed source of truth. A future
embedded-engine migration may change the physical layout, but must preserve the
ordering and recovery invariants below.

## Write Path

For each committed mutation:

1. Persist the Raft log entry before it can be considered durable.
2. Advance `commit_index` once a majority has stored the entry.
3. Persist that replica's new `commit_index` before applying it locally.
4. Apply the command exactly once to the shard state machine.
5. Emit watch events and resolve the client waiter after the applied index
   advances.

The response can be acknowledged only after the command is durably committed by
a quorum and the local commit pointer is durable. If any term/vote/log/meta or
required snapshot write fails, that shard permanently enters a fail-closed state
for the lifetime of the process: it steps down, rejects votes and Raft success
acknowledgements, fails proposals as unavailable, and serves no linearizable
reads. `/readyz` then returns 503 and the fault is visible in shard status.

Lock writes include single-key mutexes, capped semaphores, and bounded
multi-key union locks. A multi-key grant stores the same `lock_id` under every
member key and stores a distinct fencing token per key. Release by `lock_id`
removes the holder from all members in one committed command, so there is no
partial release window.

## Recovery

On restart, a node opens each shard directory, restores the newest snapshot,
and replays the contiguous log suffix through the durable commit index. Expired
TTL data may be discarded during replay, but only according to the committed
timestamps in the log/snapshot.

Recovery is intentionally fail-closed. It rejects malformed complete records,
blank records, duplicate or gapped indices, invalid zero index/term values,
missing hard-state metadata beside non-empty durable data, snapshot/log terms
ahead of the persisted current term, snapshot/log term disagreement, and a
`commit_index` beyond the durable tail. Those conditions require operator
repair or restoration; silently shortening the log would discard an entry
already recorded as committed. The sole
repairable append artifact is a malformed final record without a terminating
newline, which proves the append was torn before it could be acknowledged; that
fragment is discarded and the validated log is canonicalized. A durable
snapshot may raise an older meta commit pointer to its included index after a
crash between the two atomic renames, but recovery never lowers a commit index.

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
