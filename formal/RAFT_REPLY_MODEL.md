# Raft replication-response admission — DEN-80

Fiducia uses a custom, sharded **Raft** implementation. This is an executable
bounded model of one replication-evidence boundary, not a Paxos model and not a
replacement for the existing union-lock Quint/Apalache verification.

Tracking: [DEN-80](https://linear.app/denman/issue/DEN-80) and
[fiducia-node.rs#54](https://github.com/fiducia-cloud/fiducia-node.rs/issues/54).

## Semantic change

Before this change, `handle_snapshot_reply` accepted a same-term `success`
without checking that the reply acknowledged the snapshot offered by that RPC.
A reply with `success=true, match_index=0` to an offered index 1 could therefore
credit an unsupported replicated prefix. Append's actor callback already checked
its bound, but transport admitted malformed replies into its contact/capability
bookkeeping before that check.

Both production HTTP and loopback delivery now apply `src/raft_reply.rs`:

| Observation relative to the originating request | Admission |
| --- | --- |
| Response term is older | Ignore |
| Response term is newer | Preserve term for step-down; clear replication success |
| Same term, successful, reported index below offered prefix | Ignore |
| Same term, successful, reported index covers offered prefix | Deliver |
| Same term, unsuccessful | Deliver consistency/backoff information |

A snapshot receiver may already hold a newer committed prefix, so a greater
reported index is legal. The leader still credits only its request's `up_to`.
For append, computation of the offered prefix uses checked arithmetic; an
unrepresentable prefix is rejected before sending. PreVote/RequestVote are NOT
passed through this rule: pre-vote intentionally uses a prospective term.

Higher-term responses are never hidden by the index guard. Their success flag
is cleared, and append parser capability is downgraded to the legacy baseline,
so an old request cannot advertise replication success or parser capability in a
newer leadership term. Actor role/current-term checks remain necessary.

## Executable evidence

`tests/formal_raft_reply_refinement.rs` imports the exact production admission
function rather than reimplementing it in an adapter. It exhaustively explores
a finite state machine of durable prefix, credited prefix, majority commit,
ordered apply, delivery/duplicate/reordered replies, dropped replies, and
higher-term step-down. It asserts:

- credited prefix never exceeds durable prefix;
- a committed index has a durable majority;
- application never crosses the committed prefix;
- a fully committed/applied execution is reachable (non-vacuity).

The negative control intentionally removes the same-term under-acknowledgement
guard. The checker must produce a counterexample; success without finding that
counterexample fails the test. A separate 2,592-case truth table exercises all
combinations of terms and indices in `{0,1,2,3,u64::MAX-1,u64::MAX}` and both
success values, including preservation of newer-term evidence.

`src/transport_replication_tests.rs` exercises the actual production transport
through both a real loopback TCP HTTP responder and the in-process shard inbox.
Both paths cover under-acknowledgement, exact/newer prefixes, negative replies,
stale terms, and higher-term success/failure. An overflow regression checks that
an invalid append prefix is not sent at all. These run with the normal product
Rust tests; the standalone model is not a substitute for those integration tests.

## Bounds and assumptions

The finite model has exactly three fixed members, one initial leader, and two
log entries, both from the current term. The leader begins with a durable log.
Follower durability can grow but cannot roll back. A reported index does not
exceed actual durable state, although a malformed success flag may assert that
an insufficient prefix covers a request. This is **not Byzantine fault tolerance**:
a peer that lies about the reported durable index is outside this abstraction.

There is no election/re-election, dynamic membership, arbitrary request-generation
concurrency, wall clock, read lease, disk corruption, fsync crash cut, or command
state machine in this model. The 10,000-state exploration ceiling is fail-closed:
exceeding it fails rather than reporting a partially explored graph as proved.
Completion applies only to the explicitly finite graph, not arbitrary clusters.

In particular, this does NOT prove the current-term Raft commit rule for inherited
log entries, leader completeness, general linearizability, or unconditional
liveness. Those remain distinct protocol/refinement obligations under DEN-80.

## Reproduction and provenance

With the repository-pinned Rust 1.95.0:

```sh
rustc --edition=2021 --deny warnings --test tests/formal_raft_reply_refinement.rs \
  -o /tmp/fiducia-raft-reply-model
/tmp/fiducia-raft-reply-model --nocapture
cargo test --all-targets --all-features --locked
```

The `Raft replication evidence model` job checks out the exact PR head, records
its commit, Rust version and SHA-256 hashes of the relevant inputs, runs the
exhaustive checker and negative control, and uploads the logs. It uses read-only
permissions and the repository's existing immutable action pins. The existing
Quint/Apalache lane remains intact. Transport, helper, formal tests and the
state-machine dependencies are included in formal workflow path coverage.

## Open audit obligations

Arrival-time `last_contact` is not request-time quorum evidence. Delayed replies
and old request generations must be considered before asserting read-lease
linearizability. Snapshot callbacks also need their own monotonic bookkeeping
and index-exhaustion review. This admission guard does not claim to fix either.
Multi-process partitions, per-member authentication, lost-response retries
across restart, real PVC backup/restore and sustained-contention fairness remain
separate verification work, not implicitly completed by this bounded model.
