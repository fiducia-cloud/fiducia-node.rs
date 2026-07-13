# fiducia-node

The Raft-replicated **coordination engine** behind [fiducia.cloud](https://fiducia.cloud).
A node is the data-plane process that runs on each VM or bare-metal machine,
hosts shard replicas, and serves the coordination API over HTTP.

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
| **Idempotency keys**  | `/v1/idempotency/*` | Retry-safe first-claim / duplicate-replay records with TTLs, owner fencing, and optional result payloads. |
| **Config KV + watches** | `/v1/kv/*`       | Linearizable, versioned key/value with live SSE `watch` streams (etcd/znode). |
| **Rate limiting**     | `/v1/rate-limit/*` | Atomic token-bucket / sliding-window checks per tenant+key.     |
| **Cron / schedules**  | `/v1/cron/*`       | Durable schedules with at-least-once / exactly-once run records. |
| **Leader election**   | `/v1/elections/*`  | Clients campaign for a named leadership with TTL leases + fencing tokens. |
| **Service discovery** | `/v1/services/*`   | TTL-health registry of live service instances (Consul/etcd).   |

Plus `/healthz`, `/readyz`, `/v1/status` (per-shard consensus status), and the
internal `/raft/{shard}/{append,vote}` peer endpoints. `/healthz` is process
liveness; `/readyz` returns 503 if any shard actor is missing or has tripped its
durable-storage fail-closed state. Local shard-status collection is bounded, so
a wedged/full actor inbox makes readiness fail instead of hanging the probe.

## B2B coordination flows

A B2B customer uses fiducia.cloud when they have many replicas of their own
service, but some decisions must have exactly one authoritative owner at a time.
Common failures this prevents:

- two cron runners sending the same invoices or payouts;
- two workers claiming the same tenant, shard, job, or migration;
- clients routing to stale service instances after a pod or VM dies;
- manual failover where people update config during an incident;
- split-brain primaries where old and new leaders both keep writing.

The control plane handles orgs, projects, environments, API keys, billing, and
placement. The node API below is the data-plane contract those customer replicas
use after the control plane has issued credentials and an endpoint.

The direct-node examples below use this helper so they exercise the same trusted
hop and org-scoping contract as production. Start the node with
`FIDUCIA_INTERNAL_SECRET=dev-secret` first; a load balancer normally injects both
headers instead.

```bash
fiducia() {
  curl -H 'x-fiducia-internal-auth: dev-secret' \
    -H 'x-fiducia-org-id: demo-org' \
    -H 'content-type: application/json' "$@"
}
```

### Leader election

Use this when exactly one replica may perform a critical action: run a scheduler,
own a tenant shard, coordinate a migration, drain a queue partition, or act as a
regional primary.

1. Pick a stable election name, for example `prod/invoice-reconciler/leader`.
2. Every replica campaigns with its own candidate id, lease TTL, and metadata:

```bash
fiducia -XPOST localhost:8090/v1/elections/prod%2Finvoice-reconciler%2Fleader/campaign \
  -d '{"candidate":"pod-a","ttl_ms":30000,"metadata":{"region":"us-east","address":"https://pod-a.internal","version":"2026.06.27"}}'
```

3. The winner receives `won: true` plus a `leadership` object containing
   `leader`, `lease_expires_ms`, `metadata`, and a monotonic `fencing_token`.
   Losers receive the current leader.
4. Only the winner performs the exclusive work. It renews before the lease
   expires:

```bash
fiducia -XPOST localhost:8090/v1/elections/prod%2Finvoice-reconciler%2Fleader/renew \
  -d '{"candidate":"pod-a","fencing_token":41}'
```

5. If renew fails, times out beyond the customer's safety threshold, or returns
   `not_leader`, the replica must stop doing leader-only work.
6. The customer passes the `fencing_token` into any downstream stateful system
   that can enforce it. For example, a database row, storage object, or payment
   processor idempotency table should reject writes from an older token. That is
   what prevents a slow old leader from causing damage after failover.
7. Other processes can read or watch the leader:

```bash
fiducia localhost:8090/v1/elections/prod%2Finvoice-reconciler%2Fleader
fiducia -N localhost:8090/v1/elections/prod%2Finvoice-reconciler%2Fleader/watch
```

### Service discovery

Use this when clients need a live list of service instances instead of hardcoded
endpoints or stale DNS answers.

1. Each instance registers itself under a service name with an address, TTL, and
   metadata:

```bash
fiducia -XPUT localhost:8090/v1/services/payments-api/instances/pod-a \
  -d '{"address":"https://pod-a.internal:8443","ttl_ms":30000,"metadata":{"region":"us-east","cloud":"aws","version":"2026.06.27"}}'
```

2. The instance heartbeats before its TTL expires:

```bash
fiducia -XPOST localhost:8090/v1/services/payments-api/instances/pod-a/heartbeat \
  -d '{"ttl_ms":30000}'
```

3. Clients resolve only live instances:

```bash
fiducia localhost:8090/v1/services/payments-api
fiducia 'localhost:8090/v1/services/payments-api?metadata.region=us-east&metadata.cloud=aws'
fiducia -N localhost:8090/v1/services/payments-api/watch
```

4. Metadata filters are exact-match AND filters over live instances, so callers
   can resolve only endpoints in a region, cloud, version, shard, or customer
   cell without pulling the whole registry client-side.
5. For ordinary stateless traffic, the client or load balancer can pick any live
   healthy instance, usually preferring the same region.
6. For primary traffic, combine discovery with election metadata: instances
   register as live, campaign for a named role, and clients follow the current
   election leader. The leader's metadata can include its routable address,
   region, cloud provider, version, or shard ownership.

## The flagship: multi-key UNION locks + semaphores

Fiducia's most valuable primitive is the distributed lock — and not just one key
at a time. You can lock the **union** of a key *set*:

```bash
# Acquire {orders/42, inventory/sku-9} atomically — all or nothing.
fiducia -XPOST localhost:8090/v1/locks/acquire \
  -d '{"keys":["orders/42","inventory/sku-9"],"holder":"worker-a","ttl_ms":30000,"wait":true}'
# → { "committed": true, "result": { "output": {
#       "acquired": true, "keys": ["inventory/sku-9","orders/42"],
#       "fencing_token": 7, "lease_expires_ms": ... } } }

# Release the whole set by its fencing token.
fiducia -XPOST localhost:8090/v1/locks/release -d '{"holder":"worker-a","fencing_token":7}'
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
fiducia -XPOST localhost:8090/v1/semaphores/acquire \
  -d '{"key":"db-pool","holder":"conn-1","limit":10,"ttl_ms":30000,"wait":true}'
```

**Idempotency keys** dedupe retry-prone work such as webhook handling, order
fulfillment, and "run this job once" APIs:

```bash
fiducia -XPOST localhost:8090/v1/idempotency/claim \
  -d '{"key":"stripe-webhook/event_123","owner":"worker-a","ttl":"24h","metadata":{"source":"stripe"}}'
fiducia -XPOST localhost:8090/v1/idempotency/complete \
  -d '{"key":"stripe-webhook/event_123","owner":"worker-a","fencing_token":7,"result":{"status":"ok"}}'
fiducia 'localhost:8090/v1/idempotency?key=stripe-webhook/event_123'
```

The first active claim receives a fencing token and stores the owner/metadata
until the TTL expires. Duplicates return the retained record. Completion requires
the original owner and fencing token, and duplicate completions replay the stored
result.

**Keys are never in the URL path** — they go in `?key=` (or, for the multi-key
lock acquire/release, the JSON body). So they're free of any path grammar and may
contain slashes, dots, or be empty (`flags/checkout`, `orders/42`,
`pools/db/primary`, even a key named `acquire`):

```bash
fiducia       'localhost:8090/v1/kv?key=flags/checkout'              # read
fiducia -XPUT 'localhost:8090/v1/kv?key=flags/checkout' -d '{"value":"on"}'
fiducia -N    'localhost:8090/v1/kv?key=flags/checkout&watch=true'  # SSE watch
fiducia       'localhost:8090/v1/locks?key=orders/42'               # inspect
```

This is also why the load balancer can read the routing key the same way on every
by-key request — it's always `?key=`, never a per-endpoint path shape.

> **Why locks route to one coordinator.** Granting `{a,b,c}` atomically and
> detecting it conflicts with a holder of `{b}` requires one state machine to see
> every member key together. So **all** lock/semaphore state lives in a single
> Raft group (the `LOCK_DOMAIN` routing key) — the single-broker model live-mutex
> uses. KV / rate-limit stay sharded by their own key; service discovery uses a
> registry coordinator so service names can be listed linearizably. Sharding the
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

### Cross-cloud RF=3 timing

The intended multi-cloud baseline is **RF=3 voters per shard**: one node in each
Kubernetes cluster / cloud provider, for example AWS + GCP + Hetzner. A committed
write waits for the leader plus one follower, so normal customer writes pay the
fastest remote follower RTT. The third follower may lag; that is safe as long as
clients only treat writes as successful after quorum commit.

Correctness-sensitive reads stay leader-only in this node. Do not serve locks,
fencing tokens, cron claims, or authoritative KV state from a random follower:
a lagging follower can be missing a committed entry until it catches up.

That makes topology a product choice:

- use cross-cloud RF=3 for premium/global-critical coordination where surviving
  a provider outage matters more than low write latency;
- use same-region or same-metro RF=3 for latency-sensitive locks, semaphores,
  rate limits, and cron claims;
- use customer-region shards when you need both: place the shard leader near the
  customer's traffic and choose followers from nearby failure domains.

Do not blindly put every customer lock group across AWS + GCP + Hetzner. A
healthy leader still waits for the fastest remote quorum member on every
committed write, and p99s jump when that normally-fast follower has jitter.

Tune Raft timing from measured inter-cloud RTT:

```bash
FIDUCIA_RAFT_RTT_MS=95              # optional helper: election >= 10x RTT
FIDUCIA_RAFT_HEARTBEAT_MS=100
FIDUCIA_RAFT_ELECTION_MIN_MS=1000
FIDUCIA_RAFT_ELECTION_JITTER_MS=500
FIDUCIA_RAFT_COMMIT_WAIT_MS=10000
FIDUCIA_RAFT_SNAPSHOT_THRESHOLD=1024 # committed entries between snapshots; 0 disables
```

`/v1/status` reports the active timing and per-shard metrics including append
RTT, quorum commit RTT, max follower lag, and observed leadership transfers.

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
- **Recovery = snapshot + deterministic replay.** Every log entry carries the
  leader-stamped proposal time used by all replicas. Restart replay therefore
  cannot refresh a lock, idempotency record, election, service registration, or
  other TTL. Legacy entries without a timestamp are conservatively epoch-anchored
  and expire instead of resurrecting.
- **Compaction is automatic.** After
  `FIDUCIA_RAFT_SNAPSHOT_THRESHOLD` newly applied entries (default 1024), the
  complete state machine is atomically snapshotted and the committed log prefix
  is removed. A lagging follower whose required prefix was compacted receives
  `InstallSnapshot`, then resumes with the retained log suffix. Leaders send that
  suffix in bounded, response-driven batches rather than one unbounded JSON
  request. Snapshot bodies still have an explicit operational ceiling
  (`FIDUCIA_RAFT_PEER_MAX_BODY_BYTES`); a shard whose serialized snapshot exceeds
  it requires a larger consistently configured ceiling (chunked snapshot
  transfer is not yet implemented).
- **Each shard is persisted under `FIDUCIA_DATA_DIR`** (default
  `/var/lib/fiducia`): atomic `meta`, newline-delimited `log`, and atomic
  `snapshot` files. The node fsyncs before acknowledging durability. Kubernetes
  deployments must mount this directory on stable persistent storage.
- **A persistence error takes the shard out of service.** The actor immediately
  steps down, refuses votes and replication acknowledgements, fails pending and
  new proposals as unavailable, and refuses linearizable reads until restart.
  In particular, the commit pointer is fsynced *before* applying a command or
  resolving its client waiter; logging an fsync error and continuing is never
  treated as durability.
- **Recovery is strict.** Newline-terminated malformed records, non-contiguous
  log indices, missing hard-state metadata beside durable data, term rollback,
  snapshot/log term disagreement, and a persisted `commit_index` beyond the
  durable snapshot/log tail abort shard startup. Only a final
  unterminated JSON fragment (a torn append that was never acknowledged) may be
  discarded. Recovery never clamps a committed index downward.

`/v1/status` exposes `snapshot_index` and `retained_log_entries` per shard so
operators can verify compaction rather than inferring it from disk usage. It
also exposes `storage_healthy` and, only after a fault, `storage_error`.
`hosted_shards` remains the actual actor inventory even when one actor wedges;
`unresponsive_shards` identifies bounded status probes that timed out, and both
readiness and the observe rollups fail closed while that list is non-empty.
Postgres/Supabase remain the business/control-plane database for organizations,
projects, users, API keys, audit, and billing—not the coordination store.

## Layout

| File               | Responsibility                                                       |
|--------------------|----------------------------------------------------------------------|
| `src/main.rs`      | axum wiring, router, health/status                                   |
| `src/consensus.rs` | **multi-Raft core**: per-shard election, replication, quorum commit  |
| `src/transport.rs` | peer transport (HTTP + in-process loopback) + Raft RPC wire types    |
| `src/peer_config.rs` | shared peer body/batch bounds and request timeouts                  |
| `src/raft_api.rs`  | inbound `/raft/{shard}/{append,vote,snapshot}` peer endpoints         |
| `src/state.rs`     | replicated state machine: `Command`s, **union locks**, semaphores, KV, … |
| `src/locks.rs`     | multi-key union lock handlers                                        |
| `src/semaphore.rs` | counting-semaphore handlers                                          |
| `src/idempotency.rs` | idempotency claim / completion handlers                            |
| `src/kv.rs`        | config KV + SSE watch handlers                                       |
| `src/rate_limit.rs`, `src/schedule.rs`, `src/election.rs`, `src/discovery.rs` | the other primitives |

## Run locally

```bash
FIDUCIA_INTERNAL_SECRET=dev-secret cargo run  # listens on :8090 (override PORT)
# Single node (default): leads every shard from t=0.
# A real group:
#   FIDUCIA_INTERNAL_SECRET=shared-secret FIDUCIA_NODE_ID=node-a:9090 \
#     FIDUCIA_PEERS=node-b:9090,node-c:9090 cargo run
curl -H 'x-fiducia-internal-auth: dev-secret' localhost:8090/v1/status
```

## Configuration & environment

Every knob is an environment variable, read once at boot. The full surface:

| Variable | Type | Default | Secret? | Meaning |
|----------|------|---------|---------|---------|
| `PORT` | integer | `8090` | no | Client/data-plane port (`/healthz`, `/readyz`, `/v1/*`). |
| `FIDUCIA_PEER_PORT` | integer | `9090` | no | Peer-plane port for node↔node Raft RPC (`/raft/*`). |
| `FIDUCIA_NODE_ID` | string | `node-a` | no | Stable Raft member id / client redirect target for this node. |
| `FIDUCIA_PEERS` | string | *(empty)* | no | Comma-separated peer node addresses; empty ⇒ single-node mode. |
| `FIDUCIA_SHARD_COUNT` | integer | `16` | no | Number of shards the keyspace is partitioned into (min `1`). |
| `FIDUCIA_DATA_DIR` | string | `/var/lib/fiducia` | no | Directory for durable per-shard Raft state (log/meta/snapshot). Must be writable. |
| `FIDUCIA_RAFT_SNAPSHOT_THRESHOLD` | integer | `1024` | no | Committed entries between snapshots and compaction; `0` disables. |
| `FIDUCIA_RAFT_PEER_MAX_BODY_BYTES` | integer | `268435456` | no | Absolute serialized request-body ceiling shared by inbound and outbound Raft HTTP. Snapshot state above this bound cannot transfer until the value is raised consistently. |
| `FIDUCIA_RAFT_APPEND_MAX_BYTES` | integer | `8388608` | no | Target serialized size for one response-driven AppendEntries batch, clamped to the peer body ceiling. |
| `FIDUCIA_RAFT_APPEND_MAX_ENTRIES` | integer | `64` | no | Maximum log entries in one AppendEntries batch. |
| `FIDUCIA_RAFT_RPC_TIMEOUT_MS` | integer | `10000` | no | Total timeout for vote and bounded AppendEntries HTTP requests. |
| `FIDUCIA_RAFT_SNAPSHOT_TIMEOUT_MS` | integer | `120000` | no | Longer total timeout for InstallSnapshot HTTP requests. |
| `FIDUCIA_INTERNAL_SECRET` | string | *(unset ⇒ fail closed)* | **yes** | Shared cluster secret enforced on `/v1` and `/raft`. Share with the LB and peer nodes. |
| `FIDUCIA_ALLOW_INSECURE_INTERNAL` | bool | `false` | no | Debug-build-only local-dev opt-out. Release binaries compile the bypass out. |
| `FIDUCIA_RAFT_PREVOTE` | bool | `true` | no | Raft PreVote (avoids term inflation from a partitioned node). Disable with `0`/`false`/`off`. |
| `FIDUCIA_RAFT_CHECK_QUORUM` | bool | `true` | no | Leader steps down without a quorum of live followers. Disable with `0`/`false`/`off`. |
| `FIDUCIA_RAFT_TICK_MS` | integer | `20` | no | Timer granularity; clamped ≤ heartbeat. |
| `FIDUCIA_RAFT_HEARTBEAT_MS` | integer | `50` | no | Leader heartbeat interval. |
| `FIDUCIA_RAFT_ELECTION_MIN_MS` | integer | `150` | no | Election-timeout floor; clamped up to ≥ 2× heartbeat. |
| `FIDUCIA_RAFT_ELECTION_JITTER_MS` | integer | `150` | no | Random jitter added to the election timeout. |

Bool vars accept `1`/`true` (and, for the Raft toggles, `0`/`false`/`off`).

### Secure-by-default trust boundary

The node has no per-request user auth of its own: it trusts the
`x-fiducia-org-id` the load balancer injects, and trusts `AppendEntries` from
peers. That trust is only sound if nothing else can reach the ports. The
internal-auth guard ([`src/internal_auth.rs`](src/internal_auth.rs)) enforces it
with a shared cluster secret and **fails closed**:

- `FIDUCIA_INTERNAL_SECRET` **set** → every `/v1` and `/raft` request must carry
  a matching `x-fiducia-internal-auth` header (compared in **constant time**);
  the LB and peer transport attach it. This is the production posture.
- `FIDUCIA_INTERNAL_SECRET` **unset** and no opt-out → the guard **rejects every
  internal request** (HTTP 401), and logs a loud `warn`. A prod node that boots
  without its secret refuses forged `x-fiducia-*` headers instead of trusting them.
- `FIDUCIA_ALLOW_INSECURE_INTERNAL=1` with the secret unset in a **debug build**
  → the boundary is disabled and logged loudly. Release binaries ignore this
  escape hatch and continue rejecting every internal request.

### Run a single node safely (local dev)

```bash
# Insecure local mode — no cluster secret, boundary explicitly disabled:
FIDUCIA_ALLOW_INSECURE_INTERNAL=1 cargo run    # listens on :8090 (override PORT)

# Or exercise the real posture locally by setting a secret and sending it:
FIDUCIA_INTERNAL_SECRET=dev-secret cargo run
curl -H 'x-fiducia-internal-auth: dev-secret' localhost:8090/v1/status
```

Without either variable the node boots but rejects every `/v1` and `/raft`
request (fail-closed) — that is intended, not a bug.

### flags-2-env: flags → env

`.cli-flags.toml` maps non-secret operational flags to these environment variables via the pinned
[`flags-2-env`](https://github.com/ORESoftware/flags-2-env) submodule
(`vendor/flags-2-env`). `scripts/with-flags2env.sh` parses the flags against that
schema, exports the resulting env map, then execs the command:

```bash
# Build the pinned parser for this platform:
make -B -C vendor/flags-2-env all
# Derive FIDUCIA_* from flags, then run the node:
scripts/with-flags2env.sh --node-id node-a:9090 --peers node-b:9090,node-c:9090 \
  --shard-count 16 -- cargo run
# Audit the schema (also run in CI by .github/workflows/cli-flags.yml):
vendor/flags-2-env/build/flags2env audit .cli-flags.toml
```

`FIDUCIA_INTERNAL_SECRET` and the dangerous
`FIDUCIA_ALLOW_INSECURE_INTERNAL` escape hatch are deliberately excluded from
the CLI schema. Configure them only through the environment or deployment
configuration so neither can be enabled or exposed casually through argv.

## Security

Trust-boundary and hardening posture applied to this crate:

- **Fail-closed internal auth.** `FIDUCIA_INTERNAL_SECRET` guards both `/v1` and
  `/raft`; an unset secret rejects all internal traffic. Debug builds alone may
  opt out with `FIDUCIA_ALLOW_INSECURE_INTERNAL=1`; release binaries compile
  that bypass out. See [`src/internal_auth.rs`](src/internal_auth.rs).
- **Per-org coordination keyspaces.** Every stateful `/v1` key, name, service,
  tenant, list, and watch is namespaced by the validated
  `x-fiducia-org-id`. Responses remove the internal prefix, and global lock,
  semaphore, service, and election inventories filter out other orgs.
- **Constant-time secret comparison.** The shared secret is compared with a
  length-checked, non-short-circuiting byte compare, so it can't be recovered a
  byte at a time via response timing.
- **Split client/peer planes.** Client (`:8090`) and peer (`:9090`) listeners are
  separable at L4 so a NetworkPolicy can lock the client port in-namespace while
  the peer port stays cross-cluster reachable; both still require the secret at L7.
- **Request-body caps.** Client-plane bodies are capped at 1 MiB. Authenticated
  Raft peer bodies use a separately configurable 256 MiB default ceiling; both
  tower-http and axum's built-in JSON cap enforce the same value. AppendEntries
  is split into bounded 8 MiB/64-entry batches and immediately continues from
  each successful reply. Snapshots get a 120-second request timeout, but remain
  subject to the explicit peer-body ceiling (there is no chunked transfer yet).
  Watch/long-poll streams are intentionally exempt from any request timeout.
- **Panic containment.** `CatchPanicLayer` converts a handler panic into a 500
  instead of crashing the process.
- **Fail-closed Raft durability.** Votes, log/snapshot acknowledgements, commit
  application, and client success all depend on successful durable writes. A
  faulted shard remains unavailable and visible through `/readyz`, `/v1/status`,
  and `/v1/observe/shards` until it restarts and validates its on-disk state.

**Dependency advisories:** `cargo audit` is clean — 0 advisories across the
dependency tree (171 crates), reconfirmed at the latest scan. No known or
accepted (ignored) advisories.

## Related

- [`fiducia-brain.rs`](https://github.com/fiducia-cloud/fiducia-brain.rs) — control plane (placement, scaling, failure handling).
- [`fiducia-node-sidecar.rs`](https://github.com/fiducia-cloud/fiducia-node-sidecar.rs) — per-node bridge to the brain + observability.
- [`fiducia-load-balance.rs`](https://github.com/fiducia-cloud/fiducia-load-balance.rs) — key-aware router that sends each request to the owning shard's leader.
- [`fiducia-backend.rs`](https://github.com/fiducia-cloud/fiducia-backend.rs) — the customer portal/webserver.
