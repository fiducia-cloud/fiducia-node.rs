# src — the fiducia-node engine

All Rust source for the node process: the Raft-replicated coordination engine
behind fiducia.cloud. `main.rs` boots the node, wires the axum HTTP router, and
declares the modules below.

The modules fall into three layers:

- **Consensus & replication** — `consensus.rs` (sharded/multi-Raft core,
  actor-per-shard), `transport.rs` and `raft_api.rs` (peer-to-peer Raft RPC),
  `persist.rs` (crash-safe on-disk term/vote/log/snapshot with strict recovery),
  `state.rs` (the replicated state machine: every mutation is a `Command`
  applied in log order). Persistence errors are returned to the shard actor;
  the actor fails closed before granting a vote, acknowledging replication,
  applying a new commit, or resolving a client proposal.
- **Coordination primitives (client `/v1` API)** — one module per primitive:
  `locks`, `semaphore`, `idempotency`, `kv`, `rate_limit`, `counters`,
  `barriers`, `budgets`, `claims`, `decisions`, `effects`, `handoffs`, `tasks`,
  `election`, `discovery`, plus scheduling (`schedule`, `schedule_runner`,
  `cron`).
- **Cross-cutting** — `internal_auth` (trusted-hop secret on internal planes),
  `validate` (pre-Raft input bounds), `metrics` + `observe` (in-process metrics
  and the read-only operator surface), `indexed_queue` (O(1)-by-key FIFO wait
  queue used by locks/semaphores).

Every file carries a `//!` module doc explaining its intent in detail.
