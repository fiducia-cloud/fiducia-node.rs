//! Request-scoped admission of replication replies (DEN-80).
//!
//! A response is evidence for the prefix sent by ONE RPC, not for an arbitrary
//! follower suffix. Keep this pure so the bounded model uses the production
//! decision rather than a second implementation. This is not a read-lease or
//! request-generation fence; the actor must still check its current role/term.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplyAdmission {
    /// Old request term, or a success that does not cover the offered prefix.
    Ignore,
    /// Preserve the newer term, but never carry replication success across terms.
    ObserveHigherTerm,
    /// Same-term negative reply or success covering at least the offered prefix.
    Deliver,
}

pub(super) fn admit_replication_reply(
    request_term: u64,
    requested_index: u64,
    response_term: u64,
    success: bool,
    reported_index: u64,
) -> ReplyAdmission {
    use std::cmp::Ordering;

    match response_term.cmp(&request_term) {
        Ordering::Less => ReplyAdmission::Ignore,
        Ordering::Greater => ReplyAdmission::ObserveHigherTerm,
        Ordering::Equal if success && reported_index < requested_index => ReplyAdmission::Ignore,
        Ordering::Equal => ReplyAdmission::Deliver,
    }
}
