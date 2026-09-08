#!/usr/bin/env python3
"""One-shot, exact-base source edit. Removed with its workflow before PR review."""
from pathlib import Path
import hashlib

path = Path('src/consensus.rs')
data = path.read_bytes()
assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == '0f19e1107bee1d958ba26cca44711713375ac722', 'consensus base changed; review instead of overwriting'
s = data.decode()

def replace(old, new, count=1):
    global s
    assert s.count(old) == count, (old[:100], s.count(old), count)
    s = s.replace(old, new)

replace('use crate::persist::{PersistedSnapshot, Recovered, ShardStore};', '''#[path = "raft_callback.rs"]
mod raft_callback;
pub use self::raft_callback::RequestStamp;
use self::raft_callback::{acknowledged_prefix, advance_contact, classify_callback, contact_is_fresh, CallbackAdmission};

#[cfg(test)]
#[path = "replication_callback_tests.rs"]
mod replication_callback_tests;

use crate::persist::{PersistedSnapshot, Recovered, ShardStore};''')
replace('    AppendReply {\n        from: String,', '    AppendReply {\n        from: String,\n        request: RequestStamp<Instant>,')
replace('    SnapshotReply {\n        from: String,', '    SnapshotReply {\n        from: String,\n        request: RequestStamp<Instant>,')
replace('    in_flight: HashMap<String, bool>,', '''    in_flight: HashMap<String, bool>,
    /// Exact actor-dispatched request allowed to consume each in-flight slot.
    pending_requests: HashMap<String, RequestStamp<Instant>>,
    /// Never reused within a leadership term; checked before dispatch.
    request_sequence: u64,''')
replace('    /// When we last received a reply from each peer *at our current term* — proof\n    /// the peer still acknowledges us as leader.', '    /// Dispatch instant of the latest acknowledged request in this term —\n    /// never reply arrival, which could renew already expired evidence.')
replace('    // --- candidate state ---\n    votes: HashSet<String>,', '    // --- candidate state ---\n    votes: HashSet<String>,\n    /// Conservative start of the real election; delayed votes cannot renew it.\n    campaign_started_at: Instant,')
replace('            votes: HashSet::new(),', '            votes: HashSet::new(),\n            campaign_started_at: Instant::now(),', 2)
replace('        self.current_term += 1;\n        self.role = Role::Candidate;', '        self.current_term += 1;\n        self.campaign_started_at = Instant::now();\n        self.role = Role::Candidate;')
replace('''        // The peers that just voted for us *are* fresh majority contact, so seed the
        // leader lease from them — otherwise the very first lease window would look
        // expired and we'd step down before the first heartbeat round returns.
        let voters = std::mem::take(&mut self.votes);
        let now = Instant::now();''', '''        // Votes establish contact no earlier than campaign dispatch. Never
        // renew a lease at delayed vote arrival; the first append round may
        // supply newer evidence, otherwise CheckQuorum safely steps us down.
        let voters = std::mem::take(&mut self.votes);
        let now = self.campaign_started_at;''')
replace('''            ShardMsg::AppendReply {
                from,
                up_to,
                rtt_ms,
                resp,
            } => self.handle_append_reply(from, up_to, rtt_ms, resp),
            ShardMsg::SnapshotReply { from, up_to, resp } => {
                self.handle_snapshot_reply(from, up_to, resp)
            }''', '''            ShardMsg::AppendReply { from, request, up_to, rtt_ms, resp } => {
                if self.accept_replication_callback(&from, request, resp.as_ref().map(|r| r.term)) {
                    self.handle_append_reply(request.sent_at, from, up_to, rtt_ms, resp);
                }
            }
            ShardMsg::SnapshotReply { from, request, up_to, resp } => {
                if self.accept_replication_callback(&from, request, resp.as_ref().map(|r| r.term)) {
                    self.handle_snapshot_reply(request.sent_at, from, up_to, resp);
                }
            }''')
replace('''    fn send_append_to(&mut self, peer: &str) {
        let Some(ls) = self.leader.as_mut() else {''', '''    fn send_append_to(&mut self, peer: &str) {
        if !self.peers.iter().any(|member| member == peer) {
            return;
        }
        let Some(ls) = self.leader.as_mut() else {''')
replace('''        let next = *ls.next_index.get(peer).unwrap_or(&1);''', '''        let Some(sequence) = ls.request_sequence.checked_add(1) else {
            tracing::warn!(shard = self.shard_id, "raft callback sequence exhausted; stepping down");
            self.step_down(self.current_term, None);
            return;
        };
        ls.request_sequence = sequence;
        let request = RequestStamp { term: self.current_term, sequence, sent_at: Instant::now() };
        ls.pending_requests.insert(peer.to_string(), request);
        let next = *ls.next_index.get(peer).unwrap_or(&1);''')
replace('''                    .send(ShardMsg::SnapshotReply {
                        from: peer_owned,
                        up_to,''', '''                    .send(ShardMsg::SnapshotReply {
                        from: peer_owned,
                        request,
                        up_to,''')
replace('''                .send(ShardMsg::AppendReply {
                    from: peer_owned,
                    up_to,''', '''                .send(ShardMsg::AppendReply {
                    from: peer_owned,
                    request,
                    up_to,''')
replace('''    fn handle_append_reply(
        &mut self,
        from: String,''', '''    /// Validate before touching any leader bookkeeping, even for timeouts.
    fn accept_replication_callback(
        &mut self, from: &str, request: RequestStamp<Instant>, response_term: Option<u64>,
    ) -> bool {
        if self.storage_fault.is_some() || !self.peers.iter().any(|peer| peer == from) {
            return false;
        }
        let active = if self.role == Role::Leader {
            self.leader.as_ref().and_then(|leader| leader.pending_requests.get(from)).copied()
        } else { None };
        match classify_callback(self.current_term, active, request, response_term, Instant::now()) {
            CallbackAdmission::HigherTerm(term) => { self.step_down(term, None); false }
            CallbackAdmission::Ignore => false,
            CallbackAdmission::Current => {
                if let Some(leader) = self.leader.as_mut() { leader.pending_requests.remove(from); }
                true
            }
        }
    }

    fn handle_append_reply(
        &mut self,
        sent_at: Instant,
        from: String,''')
replace('''    fn handle_snapshot_reply(
        &mut self,
        from: String,''', '''    fn handle_snapshot_reply(
        &mut self,
        sent_at: Instant,
        from: String,''')
replace('''            // Any reply at our term is proof this peer still sees us as leader —
            // refresh the lease clock regardless of log success/mismatch.
            ls.last_contact.insert(from.clone(), Instant::now());''', '''            // Bound evidence by dispatch, not arrival: transport and inbox
            // delay must never manufacture extra read-lease time.
            let contact = advance_contact(ls.last_contact.get(&from).copied(), sent_at);
            ls.last_contact.insert(from.clone(), contact);''')
replace('''                ls.next_index.insert(from.clone(), backoff.max(1));''', '''                let acknowledged_next = ls.match_index.get(&from).copied().unwrap_or(0).saturating_add(1);
                ls.next_index.insert(from.clone(), backoff.max(acknowledged_next));''')
replace('''        if resp.success {
            if let Some(leader) = self.leader.as_mut() {
                leader.match_index.insert(from.clone(), up_to);
                leader.next_index.insert(from.clone(), up_to + 1);
                leader.last_contact.insert(from.clone(), Instant::now());
            }
            self.send_append_to(&from);
        }''', '''        if resp.success {
            if let Some(leader) = self.leader.as_mut() {
                let known = leader.match_index.get(&from).copied().unwrap_or(0);
                let Some((matched, next)) = acknowledged_prefix(known, up_to, resp.match_index) else {
                    return;
                };
                leader.match_index.insert(from.clone(), matched);
                leader.next_index.insert(from.clone(), next);
                let contact = advance_contact(leader.last_contact.get(&from).copied(), sent_at);
                leader.last_contact.insert(from.clone(), contact);
            }
            self.send_append_to(&from);
        }''')
a = s.index('    fn leader_lease_held(&self) -> bool {')
b = s.index('    /// Step down because the leader lease lapsed', a)
s = s[:a] + '''    fn leader_lease_held(&self) -> bool {
        if !self.timing.check_quorum || self.members == 1 {
            return true;
        }
        let Some(ls) = self.leader.as_ref() else { return false; };
        let now = Instant::now();
        let window = Duration::from_millis(self.timing.election_min_ms);
        // Missing contacts are absent, never synthetic timestamps. Count only
        // configured peers with evidence younger than the exclusive deadline.
        let fresh_peers = self.peers.iter().filter(|peer| {
            let age = ls.last_contact.get(*peer).and_then(|sent| now.checked_duration_since(*sent));
            contact_is_fresh(age, window)
        }).count();
        1 + fresh_peers >= self.majority()
    }

''' + s[b:]
# Existing unit tests call the inner handlers deliberately. Preserve their
# fresh-response semantics; new regression tests exercise the guarded dispatcher.
marker = '#[cfg(test)]\nmod tests {'
assert s.count(marker) == 1
head, tail = s.split(marker)
for method in ['handle_append_reply', 'handle_snapshot_reply']:
    count = tail.count('.' + method + '(')
    print('adapt existing direct handler tests', method, count)
    tail = tail.replace('.' + method + '(', '.' + method + '(Instant::now(), ')
s = head + marker + tail
path.write_text(s)

p = Path('.github/workflows/formal-methods.yml')
w = p.read_text()
anchor = "      - 'src/raft_reply.rs'\n"
assert w.count(anchor) == 2
w = w.replace(anchor, anchor + "      - 'src/raft_callback.rs'\n      - 'src/replication_callback_tests.rs'\n")
anchor = '          rustfmt --edition 2021 --check src/raft_reply.rs tests/formal_raft_reply_refinement.rs\n'
assert w.count(anchor) == 1
w = w.replace(anchor, anchor + '''          sha256sum src/raft_callback.rs src/consensus.rs src/replication_callback_tests.rs \\
            tests/formal_raft_callback_refinement.rs formal/RAFT_CALLBACK_MODEL.md \\
            > .formal-artifacts/raft-reply-callback-inputs.sha256
          rustc --edition=2021 --deny warnings --test tests/formal_raft_callback_refinement.rs \\
            -o "$RUNNER_TEMP/fiducia-raft-callback-model"
          "$RUNNER_TEMP/fiducia-raft-callback-model" --nocapture 2>&1 \\
            | tee .formal-artifacts/raft-reply-callback-model.log
          rustfmt --edition 2021 --check src/raft_callback.rs tests/formal_raft_callback_refinement.rs
''')
p.write_text(w)
print('Applied exact-base callback hardening; full crate CI is still required.')
