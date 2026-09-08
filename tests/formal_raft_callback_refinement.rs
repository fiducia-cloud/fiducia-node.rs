//! Bounded, exhaustive transition checks of the production callback rules.
//! See formal/RAFT_CALLBACK_MODEL.md for bounds and excluded obligations.

#[path = "../src/raft_callback.rs"]
mod raft_callback;

use raft_callback::{
    acknowledged_prefix, advance_contact, classify_callback, contact_is_fresh, CallbackAdmission,
    RequestStamp,
};
use std::time::Duration;

#[test]
fn exhaustive_callback_ownership_and_higher_term_transitions() {
    let mut requests = Vec::new();
    for term in 1..=3 {
        for sequence in 0..=2 {
            for sent_at in 0u64..=3 {
                requests.push(RequestStamp {
                    term,
                    sequence,
                    sent_at,
                });
            }
        }
    }
    let active: Vec<_> = std::iter::once(None)
        .chain(requests.iter().copied().map(Some))
        .collect();
    let responses = [None, Some(0), Some(1), Some(2), Some(3), Some(4)];
    let mut transitions = 0;
    let mut admitted = 0;
    let mut higher_terms = 0;
    for current in 1..=3 {
        for outstanding in &active {
            for request in &requests {
                for response in responses {
                    for now in 0..=4 {
                        transitions += 1;
                        let decision =
                            classify_callback(current, *outstanding, *request, response, now);
                        if response.is_some_and(|term| term > current) {
                            assert_eq!(decision, CallbackAdmission::HigherTerm(response.unwrap()));
                            higher_terms += 1;
                            continue;
                        }
                        match decision {
                            CallbackAdmission::Current => {
                                assert_eq!(*outstanding, Some(*request));
                                assert_eq!(request.term, current);
                                assert!(request.sent_at <= now);
                                // Consuming a request makes duplicate delivery inert.
                                assert_eq!(
                                    classify_callback(current, None, *request, response, now),
                                    CallbackAdmission::Ignore
                                );
                                admitted += 1;
                            }
                            CallbackAdmission::Ignore => {
                                assert!(
                                    outstanding != &Some(*request)
                                        || request.term != current
                                        || request.sent_at > now
                                );
                            }
                            CallbackAdmission::HigherTerm(_) => panic!("invented higher term"),
                        }
                    }
                }
            }
        }
    }
    assert!(admitted > 0 && higher_terms > 0, "non-vacuity");
    println!(
        "callback transitions={transitions}; admitted={admitted}; higher_terms={higher_terms}"
    );
}

#[test]
fn exhaustive_contact_and_progress_bounds() {
    let mut contacts = 0;
    for sent in 0u64..=4 {
        for now in 0u64..=6 {
            for old in std::iter::once(None).chain((0..=4).map(Some)) {
                let recorded = advance_contact(old, sent);
                assert_eq!(recorded, old.unwrap_or(sent).max(sent));
                for window in 0u64..=4 {
                    let age = now.checked_sub(recorded).map(Duration::from_millis);
                    let fresh = contact_is_fresh(age, Duration::from_millis(window));
                    assert_eq!(fresh, now >= recorded && now - recorded < window);
                    contacts += 1;
                }
            }
        }
    }
    assert!(!contact_is_fresh(None, Duration::MAX));
    assert!(!contact_is_fresh(Some(Duration::MAX), Duration::MAX));
    assert!(contact_is_fresh(Some(Duration::ZERO), Duration::MAX));
    let values = [0, 1, 2, 3, u64::MAX - 1, u64::MAX];
    let mut prefixes = 0;
    for known in values {
        for requested in values {
            for reported in values {
                let next = acknowledged_prefix(known, requested, reported);
                if reported < requested {
                    assert_eq!(next, None);
                } else {
                    let (matched, next) = next.unwrap();
                    assert!(matched >= known && matched >= requested);
                    assert_eq!(matched, known.max(requested));
                    assert!(next >= matched);
                    assert_eq!(next, matched.saturating_add(1));
                }
                prefixes += 1;
            }
        }
    }
    println!("contact cases={contacts}; prefix boundary cases={prefixes}");
}

#[test]
fn healthy_path_reaches_quorum_without_resurrecting_expired_evidence() {
    let request = RequestStamp {
        term: 2,
        sequence: 1,
        sent_at: 1u64,
    };
    let decision = classify_callback(2, Some(request), request, Some(2), 2);
    assert_eq!(decision, CallbackAdmission::Current);
    let contact = advance_contact(None, request.sent_at);
    // Three fixed members: self plus one fresh follower is a quorum.
    let fresh = contact_is_fresh(
        Some(Duration::from_millis(2 - contact)),
        Duration::from_millis(2),
    );
    assert!(1 + usize::from(fresh) >= 2);
    let expired = contact_is_fresh(
        Some(Duration::from_millis(3 - contact)),
        Duration::from_millis(2),
    );
    assert!(
        !expired,
        "an admitted but delayed response is not fresh evidence"
    );
    assert_eq!(acknowledged_prefix(0, 2, 2), Some((2, 3)));
}

#[test]
fn negative_controls_exhibit_generation_and_arrival_time_counterexamples() {
    let old = RequestStamp {
        term: 2,
        sequence: 1,
        sent_at: 0u64,
    };
    let active = RequestStamp {
        term: 2,
        sequence: 2,
        sent_at: 1u64,
    };
    assert_eq!(
        classify_callback(2, Some(active), old, Some(2), 3),
        CallbackAdmission::Ignore
    );
    // Mutant: term-only admission clears an unrelated newer outstanding RPC.
    let mutant_consumes = old.term == 2;
    assert!(mutant_consumes && Some(active) != Some(old));
    println!(
        "counterexample: send seq=1; consume; send seq=2; duplicate seq=1 must not clear seq=2"
    );

    let window = Duration::from_millis(2);
    let real_age = Duration::from_millis(3 - advance_contact(None, old.sent_at));
    assert!(!contact_is_fresh(Some(real_age), window));
    // Mutant: renewal at arrival (t=3) authorizes an already expired read lease.
    assert!(contact_is_fresh(Some(Duration::ZERO), window));
    println!("counterexample: request t=0; expiry t=2; delivery t=3 must not renew at t=3");
}
