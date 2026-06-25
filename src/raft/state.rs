//! Role and per-role volatile bookkeeping.

use std::collections::HashMap;

use super::message::NodeId;

/// The three Raft roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// State a leader keeps for each follower (Raft §5.3, Figure 2).
#[derive(Debug, Clone, Default)]
pub struct LeaderProgress {
    /// Next log index to send to each peer.
    pub next_index: HashMap<NodeId, u64>,
    /// Highest log index known to be replicated on each peer.
    pub match_index: HashMap<NodeId, u64>,
}

impl LeaderProgress {
    /// Initialize progress when first becoming leader: `next_index` optimistic
    /// (leader's last index + 1), `match_index` pessimistic (0).
    pub fn reset(&mut self, peers: &[NodeId], last_log_index: u64) {
        self.next_index.clear();
        self.match_index.clear();
        for peer in peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }
    }

    pub fn next_index(&self, peer: &str) -> u64 {
        self.next_index.get(peer).copied().unwrap_or(1)
    }

    pub fn set_next_index(&mut self, peer: &str, index: u64) {
        self.next_index.insert(peer.to_string(), index);
    }

    pub fn set_match_index(&mut self, peer: &str, index: u64) {
        self.match_index.insert(peer.to_string(), index);
    }
}

/// Tally of votes received during a candidacy.
#[derive(Debug, Clone, Default)]
pub struct VoteTally {
    granted: std::collections::HashSet<NodeId>,
}

impl VoteTally {
    pub fn clear(&mut self) {
        self.granted.clear();
    }

    /// Record a vote; returns the running count of distinct grants.
    pub fn record(&mut self, voter: NodeId) -> usize {
        self.granted.insert(voter);
        self.granted.len()
    }

    pub fn count(&self) -> usize {
        self.granted.len()
    }
}
