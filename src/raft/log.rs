//! The replicated log.
//!
//! Log indices are 1-based (index 0 is the empty "before the beginning"
//! sentinel, matching the Raft paper). After a snapshot the low end of the log
//! is discarded; `snapshot_index`/`snapshot_term` remember the last entry that
//! the snapshot covers so index math still works.

use serde::{Deserialize, Serialize};

/// A single command replicated across the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Leader term in which the entry was created.
    pub term: u64,
    /// Opaque, already-serialized state-machine command.
    pub command: Vec<u8>,
}

/// An in-memory Raft log with snapshot bookkeeping.
#[derive(Debug, Clone, Default)]
pub struct RaftLog {
    /// Entries after the snapshot point. `entries[0]` has index
    /// `snapshot_index + 1`.
    entries: Vec<LogEntry>,
    /// Index of the last entry included in the most recent snapshot.
    snapshot_index: u64,
    /// Term of the last entry included in the most recent snapshot.
    snapshot_term: u64,
}

impl RaftLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index of the last entry (0 if the log is empty and un-snapshotted).
    pub fn last_index(&self) -> u64 {
        self.snapshot_index + self.entries.len() as u64
    }

    /// Term of the last entry.
    pub fn last_term(&self) -> u64 {
        match self.entries.last() {
            Some(entry) => entry.term,
            None => self.snapshot_term,
        }
    }

    /// The first index physically present in `entries` (i.e. not compacted).
    pub fn first_index(&self) -> u64 {
        self.snapshot_index + 1
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// Term of the entry at `index`, if known (present or the snapshot point).
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Borrow the entry at a 1-based log `index`, if present.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_index || index > self.last_index() {
            return None;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Append a batch of entries to the tail, returning the new last index.
    pub fn append(&mut self, mut new_entries: Vec<LogEntry>) -> u64 {
        self.entries.append(&mut new_entries);
        self.last_index()
    }

    /// All entries with index `>= from` (used to build AppendEntries).
    pub fn entries_from(&self, from: u64) -> Vec<LogEntry> {
        if from <= self.snapshot_index {
            return self.entries.clone();
        }
        let offset = (from - self.snapshot_index - 1) as usize;
        self.entries
            .get(offset..)
            .map(<[_]>::to_vec)
            .unwrap_or_default()
    }

    /// Whether the log contains an entry at `index` whose term matches `term`.
    /// Index 0 (and the snapshot point) always "matches" by convention.
    pub fn matches(&self, index: u64, term: u64) -> bool {
        match self.term_at(index) {
            Some(t) => t == term,
            None => index == 0,
        }
    }

    /// Merge leader-supplied entries starting at `prev_index + 1`, truncating
    /// any conflicting suffix (Raft §5.3). Returns the resulting last index.
    pub fn splice(&mut self, prev_index: u64, incoming: Vec<LogEntry>) -> u64 {
        let mut next = prev_index + 1;
        let mut iter = incoming.into_iter();

        // Skip over entries that already agree.
        for entry in iter.by_ref() {
            match self.term_at(next) {
                Some(existing_term) if existing_term == entry.term => {
                    next += 1;
                }
                _ => {
                    // Conflict (or new territory): drop everything from `next`
                    // and append this entry plus the remainder.
                    self.truncate_from(next);
                    self.entries.push(entry);
                    break;
                }
            }
        }
        self.entries.extend(iter);
        self.last_index()
    }

    /// Discard all entries with index `>= index`.
    fn truncate_from(&mut self, index: u64) {
        if index <= self.snapshot_index {
            self.entries.clear();
            return;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.truncate(offset);
    }

    /// Install a snapshot received from the leader: discard all buffered
    /// entries and reset the snapshot point to the given position.
    pub fn install_snapshot(&mut self, last_included_index: u64, last_included_term: u64) {
        self.entries.clear();
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
    }

    /// Compact the log up to and including `index`, recording the snapshot
    /// point. Entries at or below `index` are dropped.
    pub fn compact(&mut self, index: u64) {
        if index <= self.snapshot_index || index > self.last_index() {
            return;
        }
        let term = self.term_at(index).unwrap_or(self.snapshot_term);
        let offset = (index - self.snapshot_index) as usize;
        self.entries.drain(0..offset);
        self.snapshot_index = index;
        self.snapshot_term = term;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term,
            command: vec![],
        }
    }

    #[test]
    fn append_and_index_math() {
        let mut log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        log.append(vec![entry(1), entry(1), entry(2)]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(2), Some(1));
    }

    #[test]
    fn matches_respects_term() {
        let mut log = RaftLog::new();
        log.append(vec![entry(1), entry(3)]);
        assert!(log.matches(0, 0));
        assert!(log.matches(2, 3));
        assert!(!log.matches(2, 1));
    }

    #[test]
    fn splice_truncates_conflicts() {
        let mut log = RaftLog::new();
        log.append(vec![entry(1), entry(1), entry(2)]);
        // Leader says index 2 onward should be term 1 then term 4.
        log.splice(1, vec![entry(1), entry(4)]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(4));
    }

    #[test]
    fn splice_is_idempotent_for_matching_entries() {
        let mut log = RaftLog::new();
        log.append(vec![entry(1), entry(2)]);
        log.splice(0, vec![entry(1), entry(2)]);
        assert_eq!(log.last_index(), 2);
    }

    #[test]
    fn compaction_preserves_index_math() {
        let mut log = RaftLog::new();
        log.append(vec![entry(1), entry(1), entry(2), entry(2)]);
        log.compact(2);
        assert_eq!(log.first_index(), 3);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(4), Some(2));
        assert!(log.get(1).is_none());
    }
}
