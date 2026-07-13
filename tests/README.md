# tests — integration tests

Cargo integration tests (compiled as separate crates against the public
surface). `live_coordination_smoke.rs` drives a running node's HTTP `/v1`
coordination API end-to-end — exercising the real wire contract (locks, KV,
etc.), not just in-process unit behavior — to catch regressions the module
unit tests can't see.

Consensus durability and corrupt-recovery cases live beside the implementation
in `src/consensus.rs` and `src/persist.rs`, where deterministic write-failure
injection verifies that no vote, append, snapshot, commit application, or client
success escapes a failed durable write. The recovery tests distinguish a safe
final torn append from complete corruption or a committed-index gap, which must
abort startup.
