# tests — integration tests

Cargo integration tests (compiled as separate crates against the public
surface). `live_coordination_smoke.rs` drives a running node's HTTP `/v1`
coordination API end-to-end — exercising the real wire contract (locks, KV,
etc.), not just in-process unit behavior — to catch regressions the module
unit tests can't see.
