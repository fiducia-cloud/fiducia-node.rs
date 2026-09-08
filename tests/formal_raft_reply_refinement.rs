//! Bounded replication-evidence model using the PRODUCTION reply admission rule.
//! It models one fixed-membership, three-node shard and two current-term log
//! entries. It does not model elections, leases, storage hardware, membership
//! changes, command semantics, or arbitrary concurrent request generations.

#[path = "../src/raft_reply.rs"]
mod raft_reply;

use std::collections::{HashSet, VecDeque};

use raft_reply::{admit_replication_reply, ReplyAdmission};

const TERM: u64 = 2;
const TAIL: u8 = 2;
const MAX_STATES: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Model {
    durable: [u8; 3],
    matched: [u8; 3],
    commit: u8,
    applied: u8,
    leader: bool,
}

impl Model {
    fn initial() -> Self {
        Self {
            durable: [TAIL, 0, 0],
            matched: [TAIL, 0, 0],
            commit: 0,
            applied: 0,
            leader: true,
        }
    }

    fn safe(self) -> bool {
        self.matched
            .iter()
            .zip(self.durable)
            .all(|(matched, durable)| *matched <= durable)
            && self
                .durable
                .iter()
                .filter(|index| **index >= self.commit)
                .count()
                >= 2
            && self.applied <= self.commit
            && self.commit <= TAIL
    }
}

#[derive(Debug)]
struct Exploration {
    states: usize,
    transitions: usize,
    underack_rejections: usize,
    higher_term_observations: usize,
    complete_apply_reached: bool,
    counterexample: Option<Vec<String>>,
}

fn explore(omit_underack_guard: bool) -> Exploration {
    let initial = Model::initial();
    let mut seen = HashSet::from([initial]);
    let mut pending = VecDeque::from([(initial, Vec::<String>::new())]);
    let mut result = Exploration {
        states: 0,
        transitions: 0,
        underack_rejections: 0,
        higher_term_observations: 0,
        complete_apply_reached: false,
        counterexample: None,
    };
    while let Some((state, trace)) = pending.pop_front() {
        result.states += 1;
        result.complete_apply_reached |= state.applied == TAIL;
        let mut next_states = Vec::new();
        // Durable acknowledgements may be delayed, reordered, or duplicated.
        // The reported prefix cannot exceed actual durability. A malformed
        // success flag may nevertheless claim success for a larger request.
        for peer in 1..3 {
            for index in state.durable[peer]..=TAIL {
                let mut next = state;
                next.durable[peer] = index;
                next_states.push((next, format!("persist peer={peer} through={index}")));
            }
            for requested in 1..=TAIL {
                for response_term in 0..=TERM + 1 {
                    for reported in 0..=state.durable[peer] {
                        for success in [false, true] {
                            let admission = if omit_underack_guard && response_term == TERM {
                                ReplyAdmission::Deliver
                            } else {
                                admit_replication_reply(
                                    TERM,
                                    u64::from(requested),
                                    response_term,
                                    success,
                                    u64::from(reported),
                                )
                            };
                            let mut next = state;
                            match admission {
                                ReplyAdmission::Ignore => {
                                    if success && response_term == TERM && reported < requested {
                                        result.underack_rejections += 1;
                                    }
                                }
                                ReplyAdmission::ObserveHigherTerm => {
                                    next.leader = false;
                                    result.higher_term_observations += 1;
                                }
                                ReplyAdmission::Deliver if next.leader && success => {
                                    // Model the snapshot callback's assignment, not an
                                    // idealized max(): delayed replies may regress
                                    // bookkeeping, but must never invent durability.
                                    next.matched[peer] = requested;
                                }
                                ReplyAdmission::Deliver => {}
                            }
                            next_states.push((next, format!(
                                "reply peer={peer} requested={requested} term={response_term} success={success} reported={reported} admission={admission:?}"
                            )));
                        }
                    }
                }
            }
        }
        // Lost replies / idle ticks stutter and cannot create replication proof.
        next_states.push((state, "drop response".to_string()));
        if state.leader {
            let mut matched = state.matched;
            matched.sort_unstable();
            let mut next = state;
            next.commit = next.commit.max(matched[1]);
            next_states.push((
                next,
                "commit majority-backed current-term prefix".to_string(),
            ));
        }
        if state.applied < state.commit {
            let mut next = state;
            next.applied += 1;
            next_states.push((next, "apply next committed entry".to_string()));
        }
        for (next, action) in next_states {
            result.transitions += 1;
            if !next.safe() {
                let mut counterexample = trace.clone();
                counterexample.push(action);
                counterexample.push(format!("unsafe state: {next:?}"));
                result.counterexample = Some(counterexample);
                return result;
            }
            if seen.insert(next) {
                assert!(
                    seen.len() <= MAX_STATES,
                    "model bound exceeded; never report partial exploration as success"
                );
                let mut next_trace = trace.clone();
                next_trace.push(action);
                pending.push_back((next, next_trace));
            }
        }
    }
    result
}

#[test]
fn exhaustive_replication_evidence_preserves_commit_and_apply_safety() {
    let result = explore(false);
    assert!(result.counterexample.is_none(), "{result:#?}");
    assert!(result.states > 100, "vacuous exploration: {result:#?}");
    assert!(result.underack_rejections > 0);
    assert!(result.higher_term_observations > 0);
    assert!(
        result.complete_apply_reached,
        "model never committed/applied the log"
    );
    println!("Raft reply refinement: {result:#?}");
}

#[test]
fn negative_control_detects_success_without_snapshot_prefix_evidence() {
    let result = explore(true);
    let trace = result
        .counterexample
        .expect("missing-guard mutant escaped detection");
    assert!(trace
        .iter()
        .any(|step| step.contains("requested=1") && step.contains("reported=0")));
    println!(
        "Expected counterexample to the omitted guard: {}",
        trace.join(" -> ")
    );
}

#[test]
fn exhaustive_guard_truth_table_and_integer_extremes() {
    let values = [0, 1, 2, 3, u64::MAX - 1, u64::MAX];
    let mut checked = 0;
    for request_term in values {
        for response_term in values {
            for requested in values {
                for reported in values {
                    for success in [false, true] {
                        let expected = if response_term > request_term {
                            ReplyAdmission::ObserveHigherTerm
                        } else if response_term < request_term || (success && reported < requested) {
                            ReplyAdmission::Ignore
                        } else {
                            ReplyAdmission::Deliver
                        };
                        assert_eq!(
                            admit_replication_reply(
                                request_term,
                                requested,
                                response_term,
                                success,
                                reported
                            ),
                            expected,
                            "request_term={request_term}, requested={requested}, response_term={response_term}, success={success}, reported={reported}"
                        );
                        checked += 1;
                    }
                }
            }
        }
    }
    assert_eq!(checked, 2_592);
    println!("Raft reply guard: {checked} exhaustive boundary cases");
}
