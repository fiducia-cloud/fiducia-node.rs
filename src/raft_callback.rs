//! Pure admission and evidence rules for asynchronous Raft callbacks.
//!
//! The actor owns the outstanding request; transport responses do not mint
//! ownership or fresh time. This module is also compiled directly by the bounded
//! verification target, without a second implementation of these decisions.

use std::time::Duration;

/// Internal metadata, never serialized onto the Raft wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestStamp<T> {
    pub term: u64,
    pub sequence: u64,
    pub sent_at: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackAdmission {
    HigherTerm(u64),
    Current,
    Ignore,
}

/// Preserve newer-term evidence even from an obsolete request. Otherwise only
/// the exact outstanding request in this leadership term may consume a slot.
/// The caller must establish configured-peer identity before calling this.
pub fn classify_callback<T: Copy + Ord>(
    current_term: u64,
    active: Option<RequestStamp<T>>,
    request: RequestStamp<T>,
    response_term: Option<u64>,
    now: T,
) -> CallbackAdmission {
    if let Some(term) = response_term {
        if term > current_term {
            return CallbackAdmission::HigherTerm(term);
        }
    }
    if active != Some(request) || request.term != current_term || request.sent_at > now {
        CallbackAdmission::Ignore
    } else {
        CallbackAdmission::Current
    }
}

/// Missing or future evidence is not fresh. Expiry is exclusive.
pub fn contact_is_fresh(age: Option<Duration>, window: Duration) -> bool {
    age.is_some_and(|age| age < window)
}

/// An out-of-order reply cannot move contact backwards or renew it to receipt
/// time. The input is the request's dispatch instant, not callback delivery.
pub fn advance_contact<T: Copy + Ord>(previous: Option<T>, sent_at: T) -> T {
    previous.map_or(sent_at, |previous| previous.max(sent_at))
}

/// Credit only the prefix actually requested, never an unrelated remote tail.
/// Saturation prevents wrapping the next index; it is not an exhaustion policy
/// for the entire Raft log (other allocation paths have separate obligations).
pub fn acknowledged_prefix(known: u64, requested: u64, reported: u64) -> Option<(u64, u64)> {
    if reported < requested {
        return None;
    }
    let matched = known.max(requested);
    Some((matched, matched.saturating_add(1)))
}
