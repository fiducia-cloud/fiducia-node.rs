//! Lightweight in-process latency + outcome metrics, aggregated per operation.
//!
//! The `propose` / `query` hot paths already measure their own elapsed time (for
//! the per-call tracing event); this module accumulates those measurements into
//! per-op counters plus a small fixed-bucket latency histogram so
//! `/v1/observe/metrics` can report call volume, error rate, and a latency
//! distribution **without** pulling in a metrics backend.
//!
//! It is intentionally tiny — a `Mutex<HashMap>` whose lock is held only for the
//! handful of arithmetic operations of a `record`/`snapshot`, never across an
//! `.await`. A node coordinating a few thousand ops/sec is not bottlenecked by
//! one uncontended mutex; if it ever is, this is the obvious place to shard.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Serialize;

/// Upper bounds (ms) of the latency histogram buckets. A sample is counted in the
/// first bucket whose bound it is `<=`; anything slower lands in the trailing
/// `+Inf` bucket. Chosen to straddle a healthy single-region Raft commit (low
/// single-digit ms) through to a cross-cloud / contended outlier.
const BUCKET_BOUNDS_MS: [f64; 6] = [1.0, 5.0, 25.0, 100.0, 500.0, 2000.0];

/// Raw accumulator for one operation label. Bucket `i` counts samples that land
/// in `BUCKET_BOUNDS_MS[i]`; the final slot is the `+Inf` overflow.
#[derive(Default)]
struct OpStat {
    count: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
    buckets: [u64; BUCKET_BOUNDS_MS.len() + 1],
}

/// Process-wide metrics registry, keyed by operation label (e.g. `lock.acquire`,
/// `kv.put`, `read`).
#[derive(Default)]
pub struct Metrics {
    ops: Mutex<HashMap<String, OpStat>>,
}

/// One cumulative histogram bucket in a [`OpMetrics`] snapshot. `le_ms = None` is
/// the `+Inf` bucket; its `count` equals the op's total call count.
#[derive(Debug, Clone, Serialize)]
pub struct Bucket {
    /// Inclusive upper bound in ms, or `None` for the `+Inf` bucket.
    pub le_ms: Option<f64>,
    /// Cumulative count of samples at or below `le_ms` (Prometheus `le` semantics).
    pub count: u64,
}

/// A point-in-time view of one operation's metrics.
#[derive(Debug, Clone, Serialize)]
pub struct OpMetrics {
    pub op: String,
    pub count: u64,
    pub errors: u64,
    pub avg_ms: f64,
    pub max_ms: f64,
    /// Cumulative latency histogram (each bucket's count includes faster buckets).
    pub buckets: Vec<Bucket>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one completed operation: its label, wall-clock latency in ms, and
    /// whether it succeeded (a `NotLeader` redirect or an `Unavailable` counts as
    /// an error here — it is a call the node could not satisfy locally).
    pub fn record(&self, op: &str, elapsed_ms: f64, ok: bool) {
        // Guard against a NaN/negative clock reading poisoning `total_ms`.
        let elapsed_ms = if elapsed_ms.is_finite() && elapsed_ms >= 0.0 {
            elapsed_ms
        } else {
            0.0
        };
        let mut ops = self.ops.lock().unwrap();
        let stat = ops.entry(op.to_string()).or_default();
        stat.count += 1;
        if !ok {
            stat.errors += 1;
        }
        stat.total_ms += elapsed_ms;
        if elapsed_ms > stat.max_ms {
            stat.max_ms = elapsed_ms;
        }
        let slot = BUCKET_BOUNDS_MS
            .iter()
            .position(|bound| elapsed_ms <= *bound)
            .unwrap_or(BUCKET_BOUNDS_MS.len());
        stat.buckets[slot] += 1;
    }

    /// A sorted, serializable snapshot of every operation's metrics. Histogram
    /// buckets are emitted cumulatively so the `+Inf` bucket equals `count`.
    pub fn snapshot(&self) -> Vec<OpMetrics> {
        let ops = self.ops.lock().unwrap();
        let mut out: Vec<OpMetrics> = ops
            .iter()
            .map(|(op, stat)| {
                let mut cumulative = 0u64;
                let mut buckets = Vec::with_capacity(BUCKET_BOUNDS_MS.len() + 1);
                for (i, bound) in BUCKET_BOUNDS_MS.iter().enumerate() {
                    cumulative += stat.buckets[i];
                    buckets.push(Bucket {
                        le_ms: Some(*bound),
                        count: cumulative,
                    });
                }
                cumulative += stat.buckets[BUCKET_BOUNDS_MS.len()];
                buckets.push(Bucket {
                    le_ms: None,
                    count: cumulative,
                });
                OpMetrics {
                    op: op.clone(),
                    count: stat.count,
                    errors: stat.errors,
                    avg_ms: if stat.count > 0 {
                        stat.total_ms / stat.count as f64
                    } else {
                        0.0
                    },
                    max_ms: stat.max_ms,
                    buckets,
                }
            })
            .collect();
        out.sort_by(|a, b| a.op.cmp(&b.op));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_counts_errors_and_cumulative_buckets() {
        let m = Metrics::new();
        m.record("kv.put", 0.5, true); // bucket 0 (<=1ms)
        m.record("kv.put", 3.0, true); // bucket 1 (<=5ms)
        m.record("kv.put", 9000.0, false); // +Inf, error

        let snap = m.snapshot();
        assert_eq!(snap.len(), 1);
        let s = &snap[0];
        assert_eq!(s.op, "kv.put");
        assert_eq!(s.count, 3);
        assert_eq!(s.errors, 1);
        assert!((s.max_ms - 9000.0).abs() < 1e-9);

        // Cumulative: <=1ms has 1, <=5ms has 2, +Inf has all 3.
        assert_eq!(s.buckets.first().unwrap().count, 1);
        assert_eq!(s.buckets[1].count, 2);
        let inf = s.buckets.last().unwrap();
        assert_eq!(inf.le_ms, None);
        assert_eq!(inf.count, 3, "+Inf bucket must equal total count");
    }

    #[test]
    fn snapshot_is_sorted_by_op() {
        let m = Metrics::new();
        m.record("read", 1.0, true);
        m.record("lock.acquire", 1.0, true);
        let ops: Vec<String> = m.snapshot().into_iter().map(|o| o.op).collect();
        assert_eq!(ops, vec!["lock.acquire", "read"]);
    }

    #[test]
    fn non_finite_latency_is_clamped_not_propagated() {
        let m = Metrics::new();
        m.record("kv.put", f64::NAN, true);
        m.record("kv.put", -1.0, true);
        let s = &m.snapshot()[0];
        assert!(s.avg_ms.is_finite());
        assert_eq!(s.max_ms, 0.0);
    }
}
