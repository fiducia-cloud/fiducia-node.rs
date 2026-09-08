# Raft callback provenance and lease-age refinement (DEN-80)

This extends, rather than replaces, `RAFT_REPLY_MODEL.md` and the existing
Quint/Apalache union-lock verification. It concerns Fiducia's actor-per-shard
Raft implementation, not Paxos.

## Defect and transition boundary

Previously an old callback could clear a new leadership/request's `in_flight`
slot before its term was checked. Reply arrival also renewed `last_contact`,
even when the request had already aged beyond the read-lease window. Election
votes were credited at result arrival. Missing peers were represented by a
one-day-old timestamp, which is not 'never' under all timing configurations.
Snapshot replies could overwrite a newer match index and overflow the next index.

Production now stamps each dispatched RPC inside the actor, before spawning
transport, with its term, a checked sequence, and a monotonic dispatch instant.
The callback must match the outstanding request before consuming a slot or
changing metrics, capabilities, replication or contact evidence. Configured-peer
higher-term replies still force step-down even when their request is obsolete.
Timeouts release only their own slot and provide no contact/replication evidence.

Accepted contact is the maximum of previous evidence and **request dispatch**,
never arrival. Election-vote evidence starts at the campaign dispatch boundary,
before persistence/network work. Quorum counting ignores missing/future evidence
and uses exclusive expiry. Snapshot credit is monotonic and bounded to the
requested prefix; `next_index` saturates rather than wrapping. Negative append
backoff cannot cross an already acknowledged prefix.

## Executable checks

`src/raft_callback.rs` contains the production decision functions.
`tests/formal_raft_callback_refinement.rs` imports those exact functions and runs
without Tokio or external model-checker dependencies:

```sh
rustc --edition=2021 --deny warnings --test tests/formal_raft_callback_refinement.rs -o /tmp/fiducia-callback-model
/tmp/fiducia-callback-model --nocapture
cargo test --locked replication_callback_tests -- --nocapture
```

The exhaustive callback transition table covers current terms 1..3, request
terms 1..3, sequences 0..2, send ticks 0..3, delivery ticks 0..4, absent/present
outstanding requests, and absent/0..4 response terms. It checks exact ownership,
nonconsumption on duplicate delivery, and preservation of higher terms. Contact
checks span send ticks 0..4, delivery ticks 0..6, absent/prior contact, and lease
windows 0..4. Prefix checks include 0, 1, 2, 3, `u64::MAX-1`, and `u64::MAX`.
Healthy progress/quorum is explicitly reachable. Negative controls demonstrate
that dropping the request-generation guard or renewing at arrival reproduces a
counterexample. These are bounded exhaustive transition checks, **not** an
unbounded distributed-system proof or a full reachable-state election model.

Actor tests enter through real `ShardMsg` dispatch for delayed, duplicate,
old-term, missing, higher-term and unknown-peer callbacks, snapshot progress,
missing-contact quorums, campaign-age evidence and sequence exhaustion. They use
explicit old timestamps and no network sleeps. Exact expiry is tested in the
pure helper, avoiding a scheduler-sensitive boundary assertion.

## Assumptions and remaining obligations

Request stamps are process-local trusted metadata, not peer authentication.
Configured peers still require independent transport identity/authentication.
Within a term the actor never reuses a request sequence; exhaustion steps down.
Raft's existing election rules establish leadership terms; arbitrary repeated
pre-vote rounds and election-generation hardening remain separate work.

This prevents **extra lease time created by transport/scheduler/inbox delay**.
It does not prove read linearizability for arbitrary clock drift, process
suspension, leader transfer, mixed timing configurations or CheckQuorum disabled.
No clock-drift safety margin or ReadIndex protocol is introduced. The existing
current-term commit/read gate, fixed membership, and persistent storage behavior
retain their separate proof obligations. Saturating a next index is not a full
policy for log/term exhaustion. No production deployment or real PVC restore is
certified by these tests.

DEN-80 remains open for the broader audit and release gates after this slice.
