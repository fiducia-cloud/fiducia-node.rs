//! `IndexedQueue` — a FIFO queue with O(1) lookup, insert, and removal of any
//! element **by key**, not just at the ends.
//!
//! A plain `VecDeque` gives O(1) push/pop at the ends but O(n) to find or remove
//! an element in the *middle* — which is exactly what a wait queue does when a
//! specific waiter cancels or its lease expires, and when it dedups "am I already
//! queued?". This pairs a doubly-linked list (order + O(1) unlink) with a hash
//! map `key → node` (O(1) find), the structure behind
//! [oresoftware/linked-queue](https://github.com/oresoftware/linked-queue):
//!
//! | op                         | `VecDeque` | `IndexedQueue` |
//! |----------------------------|-----------:|---------------:|
//! | `push_back` / `pop_front`  |       O(1) |           O(1) |
//! | `contains` / `get` by key  |       O(n) |           O(1) |
//! | `remove` by key (anywhere) |       O(n) |           O(1) |
//! | `position` by key          |       O(n) |           O(n) |
//!
//! The list lives in a slab (a `Vec` of slots + a free list) so there are no
//! per-node heap allocations and no `unsafe`: links are slab indices, not raw
//! pointers. Those indices are an internal detail — never observable, never
//! serialized — so two replicas that apply the same sequence of operations are
//! observably identical regardless of how slots happen to be reused.
//!
//! ## Durability / recovery
//!
//! In `fiducia-node` the lock/semaphore wait queues live inside the
//! Raft-replicated state machine, so they are **recreated by replaying the log**
//! when a node restarts: replaying the same `push_back` / `pop_front` / `remove`
//! sequence rebuilds the identical FIFO order (slot indices may differ; the
//! observable order does not). The structure is additionally
//! `Serialize`/`Deserialize` — as an ordered sequence of `[key, value]` pairs —
//! so it can also be captured in a state-machine snapshot and restored verbatim
//! without replaying from the beginning of the log.

use std::collections::HashMap;
use std::hash::Hash;

use serde::de::{Deserialize, Deserializer, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeSeq, Serializer};

/// One slab slot: a queued element and its neighbor links (slab indices).
#[derive(Debug, Clone)]
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

/// A FIFO queue with O(1) keyed lookup / insert / removal anywhere in the queue.
///
/// Keys are unique: [`IndexedQueue::push_back`] is a no-op for a key already
/// present (mirroring a wait queue's "already queued?" dedup). Iteration order is
/// insertion order (FIFO), independent of the keys' hashes.
#[derive(Debug, Clone)]
pub struct IndexedQueue<K, V> {
    /// Backing store; `None` = a free (recycled) slot.
    slab: Vec<Option<Node<K, V>>>,
    /// Recycled slot indices, ready for reuse (keeps the slab compact-ish).
    free: Vec<usize>,
    /// key → occupied slab index, for O(1) `contains`/`get`/`remove`.
    index: HashMap<K, usize>,
    head: Option<usize>, // front (oldest)
    tail: Option<usize>, // back (newest)
}

impl<K, V> Default for IndexedQueue<K, V> {
    fn default() -> Self {
        IndexedQueue {
            slab: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            head: None,
            tail: None,
        }
    }
}

impl<K: Eq + Hash + Clone, V> IndexedQueue<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of queued elements.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Is `key` currently queued? O(1).
    pub fn contains(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    /// Append `value` under `key` at the back (newest). Returns `false` — leaving
    /// the queue unchanged — if `key` is already present (FIFO dedup). O(1).
    pub fn push_back(&mut self, key: K, value: V) -> bool {
        if self.index.contains_key(&key) {
            return false;
        }
        let node = Node {
            key: key.clone(),
            value,
            prev: self.tail,
            next: None,
        };
        let idx = self.alloc(node);
        match self.tail {
            Some(t) => self.slab[t].as_mut().expect("tail occupied").next = Some(idx),
            None => self.head = Some(idx),
        }
        self.tail = Some(idx);
        self.index.insert(key, idx);
        true
    }

    /// Remove and return the front (oldest) element. O(1).
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        let head = self.head?;
        Some(self.unlink(head))
    }

    /// Remove the element keyed by `key` from wherever it sits in the queue,
    /// returning its value. O(1) — the whole point of the structure.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let idx = *self.index.get(key)?;
        Some(self.unlink(idx).1)
    }

    /// Borrow the value for `key`. O(1).
    pub fn get(&self, key: &K) -> Option<&V> {
        self.index
            .get(key)
            .map(|&i| &self.slab[i].as_ref().expect("indexed slot occupied").value)
    }

    /// Mutably borrow the value for `key`. O(1).
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let i = *self.index.get(key)?;
        Some(&mut self.slab[i].as_mut().expect("indexed slot occupied").value)
    }

    /// Borrow the front (oldest) `(key, value)`. O(1).
    pub fn front(&self) -> Option<(&K, &V)> {
        self.head.map(|i| {
            let n = self.slab[i].as_ref().expect("head occupied");
            (&n.key, &n.value)
        })
    }

    /// 0-based position of `key` from the front. O(n) — used only for the
    /// human-facing "your place in line", never on the hot path.
    pub fn position(&self, key: &K) -> Option<usize> {
        let target = *self.index.get(key)?;
        let mut cur = self.head;
        let mut i = 0;
        while let Some(c) = cur {
            if c == target {
                return Some(i);
            }
            cur = self.slab[c].as_ref().expect("link occupied").next;
            i += 1;
        }
        None
    }

    /// Iterate `(key, value)` in FIFO order (front → back).
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            queue: self,
            cur: self.head,
        }
    }

    /// Claim a slab slot for `node` (reusing a freed one if available).
    fn alloc(&mut self, node: Node<K, V>) -> usize {
        match self.free.pop() {
            Some(i) => {
                self.slab[i] = Some(node);
                i
            }
            None => {
                self.slab.push(Some(node));
                self.slab.len() - 1
            }
        }
    }

    /// Detach slot `idx`: splice its neighbors / head / tail, drop it from the
    /// key index, recycle the slot, and return its `(key, value)`.
    fn unlink(&mut self, idx: usize) -> (K, V) {
        let node = self.slab[idx].take().expect("unlink an occupied slot");
        match node.prev {
            Some(p) => self.slab[p].as_mut().expect("prev occupied").next = node.next,
            None => self.head = node.next,
        }
        match node.next {
            Some(n) => self.slab[n].as_mut().expect("next occupied").prev = node.prev,
            None => self.tail = node.prev,
        }
        self.index.remove(&node.key);
        self.free.push(idx);
        (node.key, node.value)
    }
}

/// FIFO-order iterator over `(&key, &value)`; see [`IndexedQueue::iter`].
pub struct Iter<'a, K, V> {
    queue: &'a IndexedQueue<K, V>,
    cur: Option<usize>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.cur?;
        let n = self.queue.slab[i].as_ref().expect("iter occupied slot");
        self.cur = n.next;
        Some((&n.key, &n.value))
    }
}

// ── Serialization ────────────────────────────────────────────────────────────
//
// Serialize as an ordered sequence of `[key, value]` pairs (front → back). This
// drops the internal slab indices entirely, so a snapshot is compact and a
// restore rebuilds a fresh, contiguous slab in the same FIFO order.

impl<K, V> Serialize for IndexedQueue<K, V>
where
    K: Eq + Hash + Clone + Serialize,
    V: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for (key, value) in self.iter() {
            seq.serialize_element(&(key, value))?;
        }
        seq.end()
    }
}

impl<'de, K, V> Deserialize<'de> for IndexedQueue<K, V>
where
    K: Eq + Hash + Clone + Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct QueueVisitor<K, V>(std::marker::PhantomData<(K, V)>);

        impl<'de, K, V> Visitor<'de> for QueueVisitor<K, V>
        where
            K: Eq + Hash + Clone + Deserialize<'de>,
            V: Deserialize<'de>,
        {
            type Value = IndexedQueue<K, V>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        }

        deserializer.deserialize_seq(QueueVisitor(std::marker::PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain_order<K: Eq + Hash + Clone, V>(mut q: IndexedQueue<K, V>) -> Vec<V> {
        let mut out = Vec::new();
        while let Some((_, v)) = q.pop_front() {
            out.push(v);
        }
        out
    }

    #[test]
    fn push_back_pop_front_is_fifo() {
        let mut q = IndexedQueue::new();
        for n in 0..5 {
            assert!(q.push_back(n, n * 10));
        }
        assert_eq!(q.len(), 5);
        assert_eq!(drain_order(q), vec![0, 10, 20, 30, 40]);
    }

    #[test]
    fn push_back_dedups_an_existing_key() {
        let mut q = IndexedQueue::new();
        assert!(q.push_back("a", 1));
        assert!(!q.push_back("a", 999), "duplicate key is rejected");
        assert_eq!(q.len(), 1);
        assert_eq!(q.get(&"a"), Some(&1), "the original value is kept");
    }

    #[test]
    fn contains_and_get_are_keyed() {
        let mut q = IndexedQueue::new();
        q.push_back("x", 7);
        q.push_back("y", 8);
        assert!(q.contains(&"x"));
        assert!(!q.contains(&"z"));
        assert_eq!(q.get(&"y"), Some(&8));
        *q.get_mut(&"y").unwrap() = 80;
        assert_eq!(q.get(&"y"), Some(&80));
    }

    #[test]
    fn remove_from_the_middle_keeps_the_rest_in_order() {
        let mut q = IndexedQueue::new();
        for n in 0..5 {
            q.push_back(n, n);
        }
        assert_eq!(q.remove(&2), Some(2), "removed the middle element");
        assert_eq!(q.remove(&2), None, "already gone");
        assert_eq!(q.len(), 4);
        assert_eq!(q.position(&3), Some(2), "3 shifted up after 2 left");
        assert_eq!(drain_order(q), vec![0, 1, 3, 4]);
    }

    #[test]
    fn remove_head_and_tail() {
        let mut q = IndexedQueue::new();
        for n in 0..4 {
            q.push_back(n, n);
        }
        assert_eq!(q.remove(&0), Some(0)); // head
        assert_eq!(q.remove(&3), Some(3)); // tail
        assert_eq!(q.front(), Some((&1, &1)));
        assert_eq!(drain_order(q), vec![1, 2]);
    }

    #[test]
    fn position_reports_place_in_line() {
        let mut q = IndexedQueue::new();
        for c in ['a', 'b', 'c'] {
            q.push_back(c, c);
        }
        assert_eq!(q.position(&'a'), Some(0));
        assert_eq!(q.position(&'c'), Some(2));
        assert_eq!(q.position(&'z'), None);
    }

    #[test]
    fn slots_are_recycled_and_order_survives_churn() {
        let mut q = IndexedQueue::new();
        for n in 0..100 {
            q.push_back(n, n);
        }
        // Drain half from the front, removing the structure's churn through the
        // free list, then push a fresh batch that must land after the survivors.
        for _ in 0..50 {
            q.pop_front();
        }
        for n in 100..150 {
            q.push_back(n, n);
        }
        let expected: Vec<i32> = (50..150).collect();
        assert_eq!(drain_order(q), expected, "FIFO order holds across slot reuse");
    }

    #[test]
    fn iter_yields_fifo_order() {
        let mut q = IndexedQueue::new();
        for n in [3, 1, 2] {
            q.push_back(n, n);
        }
        let seen: Vec<i32> = q.iter().map(|(_, v)| *v).collect();
        assert_eq!(seen, vec![3, 1, 2], "iteration follows links, not key hashes");
    }

    #[test]
    fn serde_round_trip_recreates_the_queue_in_fifo_order() {
        // Proves the queue "can be recreated if the node goes down": a snapshot
        // serializes to an ordered pair list and restores byte-for-byte order.
        let mut q: IndexedQueue<String, i64> = IndexedQueue::new();
        q.push_back("first".into(), 1);
        q.push_back("second".into(), 2);
        q.push_back("third".into(), 3);
        q.remove(&"second".into()); // leave a gap so the slab isn't trivial

        let json = serde_json::to_string(&q).unwrap();
        let restored: IndexedQueue<String, i64> = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.position(&"first".into()), Some(0));
        assert_eq!(restored.position(&"third".into()), Some(1));
        assert!(restored.contains(&"first".into()));
        let order: Vec<i64> = restored.iter().map(|(_, v)| *v).collect();
        assert_eq!(order, vec![1, 3], "restored queue preserves FIFO order");
    }

    #[test]
    fn empty_queue_edges() {
        let mut q: IndexedQueue<i32, i32> = IndexedQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.pop_front(), None);
        assert_eq!(q.front(), None);
        assert_eq!(q.remove(&1), None);
        assert_eq!(q.position(&1), None);
    }
}
