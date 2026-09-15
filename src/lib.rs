#![forbid(unsafe_code)]

//! Small library surface for operational integrations around the fiducia-node
//! binary. Raft/state-machine internals remain private to the binary; external
//! bootstrap/migration coordination is shared so deployment tooling can depend
//! on it without invoking Fiducia's own `/v1/locks` API.

pub mod external_locks;
