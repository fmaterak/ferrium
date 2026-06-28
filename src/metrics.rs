//! Prometheus metrics.
//!
//! A minimal, dependency-free registry: a handful of named atomic counters and
//! gauges rendered in the Prometheus text exposition format. It's intentionally
//! tiny — ferrium exposes a fixed, known set of series rather than a general
//! metrics library.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Shared handle to the process-wide metrics.
pub type Metrics = Arc<MetricsInner>;

/// The metric series ferrium exports.
#[derive(Default)]
pub struct MetricsInner {
    /// Total client commands processed, by kind.
    pub gets_total: AtomicU64,
    pub sets_total: AtomicU64,
    pub dels_total: AtomicU64,
    /// Client requests rejected because this node isn't the leader.
    pub not_leader_total: AtomicU64,
    /// Raft leader elections this node has won.
    pub elections_won_total: AtomicU64,
    /// Current Raft term.
    pub current_term: AtomicU64,
    /// 1 if this node currently believes it is the leader, else 0.
    pub is_leader: AtomicI64,
    /// Highest committed log index.
    pub commit_index: AtomicU64,
    /// Number of entries currently in the in-memory log.
    pub log_len: AtomicU64,
}

/// Create a new, zeroed metrics handle.
pub fn new() -> Metrics {
    Arc::new(MetricsInner::default())
}

impl MetricsInner {
    #[inline]
    pub fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_u64(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    pub fn set_i64(gauge: &AtomicI64, value: i64) {
        gauge.store(value, Ordering::Relaxed);
    }

    /// Render all series in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(1024);
        let counters: [(&str, u64); 5] = [
            (
                "ferrium_gets_total",
                self.gets_total.load(Ordering::Relaxed),
            ),
            (
                "ferrium_sets_total",
                self.sets_total.load(Ordering::Relaxed),
            ),
            (
                "ferrium_dels_total",
                self.dels_total.load(Ordering::Relaxed),
            ),
            (
                "ferrium_not_leader_total",
                self.not_leader_total.load(Ordering::Relaxed),
            ),
            (
                "ferrium_elections_won_total",
                self.elections_won_total.load(Ordering::Relaxed),
            ),
        ];
        for (name, value) in counters {
            out.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
        }

        let gauges: [(&str, i64); 4] = [
            (
                "ferrium_current_term",
                self.current_term.load(Ordering::Relaxed) as i64,
            ),
            ("ferrium_is_leader", self.is_leader.load(Ordering::Relaxed)),
            (
                "ferrium_commit_index",
                self.commit_index.load(Ordering::Relaxed) as i64,
            ),
            (
                "ferrium_log_entries",
                self.log_len.load(Ordering::Relaxed) as i64,
            ),
        ];
        for (name, value) in gauges {
            out.push_str(&format!("# TYPE {name} gauge\n{name} {value}\n"));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_all_series() {
        let m = new();
        MetricsInner::incr(&m.sets_total);
        MetricsInner::set_i64(&m.is_leader, 1);
        let text = m.render();
        assert!(text.contains("ferrium_sets_total 1"));
        assert!(text.contains("ferrium_is_leader 1"));
        assert!(text.contains("ferrium_gets_total 0"));
    }
}
