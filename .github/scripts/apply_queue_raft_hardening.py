from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"{path}: expected one replacement target, found {count}; anchor={old[:240]!r}"
        )
    target.write_text(text.replace(old, new, 1))


def insert_before_once(path: str, marker: str, addition: str) -> None:
    target = Path(path)
    text = target.read_text()
    count = text.count(marker)
    if count != 1:
        raise SystemExit(
            f"{path}: expected one insertion marker, found {count}; marker={marker[:240]!r}"
        )
    target.write_text(text.replace(marker, addition + marker, 1))


# ---------------------------------------------------------------------------
# IndexedQueue: strict snapshot decoding, continuous debug invariants, bounded
# high-water memory, and deterministic differential churn tests.
# ---------------------------------------------------------------------------
replace_once(
    "src/indexed_queue.rs",
    "//! without replaying from the beginning of the log.\n\nuse std::collections::HashMap;\n",
    "//! without replaying from the beginning of the log.\n//!\n//! Snapshot decoding is deliberately strict. A duplicate key is malformed\n//! authority-bearing state and is rejected instead of being silently repaired:\n//! dropping either value would make recovery choose queue state that no committed\n//! command ever produced.\n\nuse std::collections::HashMap;\n",
)
replace_once(
    "src/indexed_queue.rs",
    "use serde::de::{Deserialize, Deserializer, SeqAccess, Visitor};\nuse serde::ser::{Serialize, SerializeSeq, Serializer};\n",
    "use serde::de::{self, Deserialize, Deserializer, SeqAccess, Visitor};\nuse serde::ser::{Serialize, SerializeSeq, Serializer};\n\n/// Release an empty queue's backing allocations after a large burst, while\n/// retaining small slabs so ordinary lock handoffs do not allocate on every use.\nconst RELEASE_EMPTY_CAPACITY_AT: usize = 1_024;\n",
)
replace_once(
    "src/indexed_queue.rs",
    "        self.tail = Some(idx);\n        self.index.insert(key, idx);\n        true\n",
    "        self.tail = Some(idx);\n        self.index.insert(key, idx);\n        self.debug_validate();\n        true\n",
)
replace_once(
    "src/indexed_queue.rs",
    """        self.index.remove(&node.key);
        self.free.push(idx);
        (node.key, node.value)
    }
""",
    """        let removed = self.index.remove(&node.key);
        debug_assert_eq!(removed, Some(idx));

        if self.index.is_empty() {
            self.head = None;
            self.tail = None;
            if self.slab.capacity() >= RELEASE_EMPTY_CAPACITY_AT {
                // A bursty lock key can grow a queue's slab and hash index to a
                // large high-water mark. Once fully drained there is no live slot
                // whose index must remain stable, so release large allocations.
                self.slab = Vec::new();
                self.free = Vec::new();
                self.index = HashMap::new();
            } else {
                self.free.push(idx);
            }
        } else {
            self.free.push(idx);
        }

        self.debug_validate();
        (node.key, node.value)
    }

    /// Assert every redundant representation agrees. This is compiled out of
    /// release builds, but runs after each mutation in tests/debug builds so
    /// randomized churn catches a broken link at the operation that caused it.
    fn debug_validate(&self) {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(self.index.is_empty(), self.head.is_none());
            debug_assert_eq!(self.index.is_empty(), self.tail.is_none());

            let mut visited = vec![false; self.slab.len()];
            let mut current = self.head;
            let mut previous = None;
            let mut count = 0usize;

            while let Some(slot) = current {
                debug_assert!(slot < self.slab.len(), "queue link outside slab");
                debug_assert!(!visited[slot], "queue contains a link cycle");
                visited[slot] = true;

                let node = self.slab[slot].as_ref().expect("linked slot occupied");
                debug_assert_eq!(node.prev, previous);
                debug_assert_eq!(self.index.get(&node.key), Some(&slot));

                previous = Some(slot);
                current = node.next;
                count += 1;
                debug_assert!(count <= self.index.len(), "queue traversal exceeded index");
            }

            debug_assert_eq!(previous, self.tail);
            debug_assert_eq!(count, self.index.len());

            let mut free_seen = vec![false; self.slab.len()];
            for &slot in &self.free {
                debug_assert!(slot < self.slab.len(), "free slot outside slab");
                debug_assert!(!free_seen[slot], "free list contains a duplicate slot");
                free_seen[slot] = true;
                debug_assert!(self.slab[slot].is_none(), "free slot is occupied");
            }

            for (slot, node) in self.slab.iter().enumerate() {
                match node {
                    Some(_) => debug_assert!(visited[slot], "occupied slot is unreachable"),
                    None => debug_assert!(free_seen[slot], "vacant slot is not reusable"),
                }
            }
        }
    }
""",
)
replace_once(
    "src/indexed_queue.rs",
    """            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a sequence of [key, value] pairs in FIFO order")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut queue = IndexedQueue::new();
                while let Some((key, value)) = seq.next_element::<(K, V)>()? {
                    // Last-writer-wins on a duplicate key would silently drop an
                    // element; a well-formed snapshot has none, so push_back's
                    // dedup simply ignores it.
                    queue.push_back(key, value);
                }
                Ok(queue)
            }
""",
    """            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a sequence of unique [key, value] pairs in FIFO order")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut queue = IndexedQueue::new();
                while let Some((key, value)) = seq.next_element::<(K, V)>()? {
                    if !queue.push_back(key, value) {
                        return Err(de::Error::custom(
                            "duplicate key in serialized IndexedQueue",
                        ));
                    }
                }
                queue.debug_validate();
                Ok(queue)
            }
""",
)
replace_once(
    "src/indexed_queue.rs",
    "mod tests {\n    use super::*;\n",
    "mod tests {\n    use std::collections::{HashMap, VecDeque};\n\n    use super::*;\n",
)
insert_before_once(
    "src/indexed_queue.rs",
    "    #[test]\n    fn iter_yields_fifo_order() {\n",
    """    #[test]
    fn a_fully_drained_queue_releases_its_large_high_water_mark() {
        let mut q = IndexedQueue::new();
        for n in 0..2_048 {
            q.push_back(n, n);
        }
        for _ in 0..2_048 {
            q.pop_front();
        }

        assert!(q.is_empty());
        assert!(q.slab.is_empty(), "drain releases the backing slab");
        assert_eq!(q.slab.capacity(), 0, "large slab allocation is released");
        assert!(q.free.is_empty(), "no stale free-list allocation remains");
        assert_eq!(q.index.capacity(), 0, "large hash allocation is released");

        assert!(q.push_back(7, 70));
        assert_eq!(q.pop_front(), Some((7, 70)));
    }

""",
)
insert_before_once(
    "src/indexed_queue.rs",
    "    #[test]\n    fn empty_queue_edges() {\n",
    """    #[test]
    fn snapshot_with_a_duplicate_key_is_rejected() {
        let error = serde_json::from_str::<IndexedQueue<String, i64>>(
            r#"[["same",1],["same",2]]"#,
        )
        .expect_err("duplicate queue identities are malformed recovery state");

        assert!(
            error.to_string().contains("duplicate key"),
            "error explains the rejected invariant: {error}"
        );
    }

    #[test]
    fn deterministic_churn_matches_a_simple_reference_queue() {
        let mut queue: IndexedQueue<u16, u64> = IndexedQueue::new();
        let mut order: VecDeque<u16> = VecDeque::new();
        let mut values: HashMap<u16, u64> = HashMap::new();
        let mut rng = 0x6a09_e667_f3bc_c909u64;

        for step in 0..25_000u64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;

            let key = (rng % 97) as u16;
            match (rng >> 8) % 5 {
                0 | 1 => {
                    let expected = if values.contains_key(&key) {
                        false
                    } else {
                        order.push_back(key);
                        values.insert(key, step);
                        true
                    };
                    assert_eq!(queue.push_back(key, step), expected);
                }
                2 => {
                    let expected = values.remove(&key);
                    if expected.is_some() {
                        let position = order
                            .iter()
                            .position(|candidate| *candidate == key)
                            .unwrap();
                        order.remove(position);
                    }
                    assert_eq!(queue.remove(&key), expected);
                }
                3 => {
                    let expected = order.pop_front().map(|front| {
                        let value = values.remove(&front).unwrap();
                        (front, value)
                    });
                    assert_eq!(queue.pop_front(), expected);
                }
                _ => {
                    assert_eq!(queue.contains(&key), values.contains_key(&key));
                    assert_eq!(queue.get(&key), values.get(&key));
                }
            }

            let actual: Vec<(u16, u64)> =
                queue.iter().map(|(key, value)| (*key, *value)).collect();
            let expected: Vec<(u16, u64)> = order
                .iter()
                .map(|key| (*key, *values.get(key).unwrap()))
                .collect();
            assert_eq!(actual, expected, "reference mismatch after step {step}");
            assert_eq!(queue.len(), values.len());
            for (position, key) in order.iter().enumerate() {
                assert_eq!(queue.position(key), Some(position));
            }

            if step % 257 == 0 {
                let bytes = serde_json::to_vec(&queue).unwrap();
                queue = serde_json::from_slice(&bytes).unwrap();
            }
        }
    }

""",
)


# ---------------------------------------------------------------------------
# State-machine recovery: every redundant authority index must be exact.
# ---------------------------------------------------------------------------
replace_once(
    "src/state.rs",
    """        for (key, token) in &self.locks.held {
            if !self.locks.grants.contains_key(token) {
                return Err(format!(
                    "held lock key '{key}' references missing grant token {token}"
                ));
            }
        }
""",
    """        for (key, token) in &self.locks.held {
            let Some(grant) = self.locks.grants.get(token) else {
                return Err(format!(
                    "held lock key '{key}' references missing grant token {token}"
                ));
            };
            if !grant.keys.iter().any(|grant_key| grant_key == key) {
                return Err(format!(
                    "held lock key '{key}' is absent from grant token {token}'s key set"
                ));
            }
        }
""",
)
replace_once(
    "src/state.rs",
    """            if grant.fencing_token != token {
                return Err(format!(
                    "lock grant indexed at token {token} carries fencing_token {}",
                    grant.fencing_token
                ));
            }
            for key in &grant.keys {
""",
    """            if token == 0 || grant.fencing_token != token {
                return Err(format!(
                    "lock grant indexed at token {token} carries fencing_token {}",
                    grant.fencing_token
                ));
            }
            if grant.holder.trim().is_empty()
                || grant.holder.len() > crate::validate::MAX_HOLDER_BYTES
                || grant.holder.chars().any(char::is_control)
            {
                return Err(format!("lock grant {token} carries an invalid holder"));
            }
            if grant.keys.is_empty()
                || grant.keys.len() > crate::validate::MAX_LOCK_KEYS
                || canonical_keys(&grant.keys).as_slice() != grant.keys.as_slice()
                || grant
                    .keys
                    .iter()
                    .any(|key| key.is_empty() || key.len() > crate::validate::MAX_KEY_BYTES)
            {
                return Err(format!(
                    "lock grant {token} carries a non-canonical or invalid key set"
                ));
            }
            if grant.request_id.as_ref().is_some_and(|request_id| {
                request_id.trim().is_empty()
                    || request_id.len() > crate::validate::MAX_REQUEST_ID_BYTES
                    || request_id.chars().any(char::is_control)
            }) {
                return Err(format!("lock grant {token} carries an invalid request id"));
            }
            for key in &grant.keys {
""",
)
replace_once(
    "src/state.rs",
    """        self.lock_cancellations.validate("lock")?;
        self.semaphore_cancellations.validate("semaphore")?;
        Ok(())
""",
    """        for ((indexed_holder, indexed_keys), queued) in self.locks.queue.iter() {
            if indexed_holder != &queued.holder || indexed_keys.as_slice() != queued.keys.as_slice() {
                return Err("lock queue index does not match its queued request".to_string());
            }
            if queued.holder.trim().is_empty()
                || queued.holder.len() > crate::validate::MAX_HOLDER_BYTES
                || queued.holder.chars().any(char::is_control)
                || queued.keys.is_empty()
                || queued.keys.len() > crate::validate::MAX_LOCK_KEYS
                || canonical_keys(&queued.keys).as_slice() != queued.keys.as_slice()
                || queued
                    .keys
                    .iter()
                    .any(|key| key.is_empty() || key.len() > crate::validate::MAX_KEY_BYTES)
                || queued.ttl_ms == 0
                || queued.ttl_ms > crate::validate::MAX_TTL_MS
                || queued
                    .wait_expires_ms
                    .is_some_and(|expires| expires < queued.requested_ms)
                || queued.request_id.as_ref().is_some_and(|request_id| {
                    request_id.trim().is_empty()
                        || request_id.len() > crate::validate::MAX_REQUEST_ID_BYTES
                        || request_id.chars().any(char::is_control)
                })
            {
                return Err("lock queue carries an invalid request".to_string());
            }
            if self.locks.grants.values().any(|grant| {
                grant.holder == queued.holder && grant.keys == queued.keys
            }) {
                return Err("the same union-lock identity is both granted and queued".to_string());
            }
        }

        for (key, semaphore) in &self.semaphores {
            if key.is_empty()
                || key.len() > crate::validate::MAX_KEY_BYTES
                || semaphore.limit == 0
                || semaphore.limit > crate::validate::MAX_SEMAPHORE_LIMIT
                || semaphore.holders.len() > semaphore.limit as usize
            {
                return Err(format!("semaphore '{key}' carries an invalid limit or key"));
            }

            let mut holders = std::collections::HashSet::<&str>::new();
            let mut tokens = std::collections::HashSet::<u64>::new();
            for slot in &semaphore.holders {
                if slot.holder.trim().is_empty()
                    || slot.holder.len() > crate::validate::MAX_HOLDER_BYTES
                    || slot.holder.chars().any(char::is_control)
                    || slot.fencing_token == 0
                    || !holders.insert(slot.holder.as_str())
                    || !tokens.insert(slot.fencing_token)
                    || slot.request_id.as_ref().is_some_and(|request_id| {
                        request_id.trim().is_empty()
                            || request_id.len() > crate::validate::MAX_REQUEST_ID_BYTES
                            || request_id.chars().any(char::is_control)
                    })
                {
                    return Err(format!("semaphore '{key}' carries an invalid holder"));
                }
            }

            for (indexed_holder, queued) in semaphore.queue.iter() {
                if indexed_holder != &queued.holder
                    || queued.holder.trim().is_empty()
                    || queued.holder.len() > crate::validate::MAX_HOLDER_BYTES
                    || queued.holder.chars().any(char::is_control)
                    || holders.contains(queued.holder.as_str())
                    || queued.ttl_ms == 0
                    || queued.ttl_ms > crate::validate::MAX_TTL_MS
                    || queued
                        .wait_expires_ms
                        .is_some_and(|expires| expires < queued.requested_ms)
                    || queued.request_id.as_ref().is_some_and(|request_id| {
                        request_id.trim().is_empty()
                            || request_id.len() > crate::validate::MAX_REQUEST_ID_BYTES
                            || request_id.chars().any(char::is_control)
                    })
                {
                    return Err(format!("semaphore '{key}' carries an invalid queued waiter"));
                }
            }
        }

        self.lock_cancellations.validate("lock")?;
        self.semaphore_cancellations.validate("semaphore")?;
        Ok(())
""",
)
insert_before_once(
    "src/state.rs",
    "    // Proves whether a lost-response RETRY of an acquire is safe server-side —\n",
    """    #[test]
    fn restore_rejects_a_ghost_held_key_missing_from_the_grant() {
        let mut store = Store::default();
        store.next_fencing_token = 1;
        store.locks.grants.insert(
            1,
            LockGrant {
                holder: "owner".to_string(),
                keys: vec!["real".to_string()],
                fencing_token: 1,
                lease_expires_ms: 10_000,
                request_id: None,
            },
        );
        store.locks.held.insert("real".to_string(), 1);
        store.locks.held.insert("ghost".to_string(), 1);

        let error = StateMachine::new()
            .restore(&serde_json::to_vec(&store).unwrap())
            .expect_err("a ghost reverse-index key would remain blocked forever");
        assert!(error.to_string().contains("absent from grant"));
    }

    #[test]
    fn restore_rejects_a_lock_queue_index_value_identity_mismatch() {
        let mut store = Store::default();
        assert!(store.locks.queue.push_back(
            ("indexed-holder".to_string(), vec!["a".to_string()]),
            QueuedLock {
                holder: "different-holder".to_string(),
                keys: vec!["a".to_string()],
                ttl_ms: 1_000,
                requested_ms: 1,
                wait_expires_ms: Some(2_000),
                request_id: None,
            },
        ));

        let error = StateMachine::new()
            .restore(&serde_json::to_vec(&store).unwrap())
            .expect_err("queue cancellation identity must survive recovery exactly");
        assert!(error.to_string().contains("queue index"));
    }

    #[test]
    fn restore_rejects_a_semaphore_with_more_holders_than_its_limit() {
        let mut store = Store::default();
        store.next_fencing_token = 2;
        store.semaphores.insert(
            "pool".to_string(),
            Semaphore {
                limit: 1,
                holders: vec![
                    SemaphoreSlot {
                        holder: "a".to_string(),
                        fencing_token: 1,
                        lease_expires_ms: 10_000,
                        request_id: None,
                    },
                    SemaphoreSlot {
                        holder: "b".to_string(),
                        fencing_token: 2,
                        lease_expires_ms: 10_000,
                        request_id: None,
                    },
                ],
                queue: IndexedQueue::new(),
            },
        );

        let error = StateMachine::new()
            .restore(&serde_json::to_vec(&store).unwrap())
            .expect_err("over-capacity semaphore state cannot be authoritative");
        assert!(error.to_string().contains("invalid limit"));
    }

""",
)


# ---------------------------------------------------------------------------
# Durable Raft recovery: exact hard-state term and monotonic log terms.
# ---------------------------------------------------------------------------
replace_once(
    "src/persist.rs",
    "        let mut previous_index: Option<u64> = None;\n",
    "        let mut previous_index: Option<u64> = None;\n        let mut previous_term: Option<u64> = None;\n",
)
replace_once(
    "src/persist.rs",
    """            if let Some(previous) = previous_index {
""",
    """            if let Some(previous) = previous_term {
                if entry.term < previous {
                    return Err(invalid_data(format!(
                        "raft log term descends at line {}: previous {}, found {}",
                        line_number + 1,
                        previous,
                        entry.term
                    )));
                }
            }
            previous_term = Some(entry.term);
            if let Some(previous) = previous_index {
""",
)
replace_once(
    "src/persist.rs",
    """        let durable_term = meta.current_term.max(1);
        if snapshot_term > durable_term || log.iter().any(|entry| entry.term > durable_term) {
""",
    """        let durable_term = meta.current_term;
        if snapshot_term > durable_term || log.iter().any(|entry| entry.term > durable_term) {
""",
)
# Existing low-level persistence tests intentionally write logs directly; make
# their hard-state setup explicit now that recovery no longer treats term zero as
# an implicit term-one store.
for old, new in {
    """        let (mut store, _) = ShardStore::open(&root, 0).unwrap();
        store.append_tail(&[entry(1, 1, "a")]).unwrap();
""": """        let (mut store, _) = ShardStore::open(&root, 0).unwrap();
        store.save_meta(1, None, 0).unwrap();
        store.append_tail(&[entry(1, 1, "a")]).unwrap();
""",
    """            let (mut store, _) = ShardStore::open(&root, 9).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
""": """            let (mut store, _) = ShardStore::open(&root, 9).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
""",
    """            let (mut store, _) = ShardStore::open(&root, 10).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
""": """            let (mut store, _) = ShardStore::open(&root, 10).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store.append_tail(&[entry(1, 1, "a")]).unwrap();
""",
    """            let (mut store, _) = ShardStore::open(&root, 11).unwrap();
            store
                .rewrite(&[entry(1, 1, "a"), entry(3, 1, "missing-two")])
""": """            let (mut store, _) = ShardStore::open(&root, 11).unwrap();
            store.save_meta(1, None, 0).unwrap();
            store
                .rewrite(&[entry(1, 1, "a"), entry(3, 1, "missing-two")])
""",
}.items():
    replace_once("src/persist.rs", old, new)

insert_before_once(
    "src/persist.rs",
    "    #[test]\n    fn snapshot_compacts_log_and_survives_reopen() {\n",
    """    #[test]
    fn descending_log_terms_are_rejected_as_impossible_raft_history() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 15).unwrap();
            store.save_meta(3, None, 0).unwrap();
            store
                .rewrite(&[entry(1, 3, "newer"), entry(2, 2, "older")])
                .unwrap();
        }

        let error = open_error(&root, 15);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("term descends"));
    }

    #[test]
    fn term_one_log_cannot_hide_behind_term_zero_hard_state() {
        let root = tmpdir();
        {
            let (mut store, _) = ShardStore::open(&root, 16).unwrap();
            store.append_tail(&[entry(1, 1, "future")]).unwrap();
        }

        let error = open_error(&root, 16);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("persisted current term 0"));
    }

""",
)
