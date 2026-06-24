//! Raft RPC messages exchanged between nodes.
//!
//! These are serialized (JSON) over the inter-node transport. They mirror the
//! RPCs from the Raft paper: RequestVote and AppendEntries (heartbeats are just
//! AppendEntries with no entries), plus InstallSnapshot for catching up a peer
//! that has fallen behind the leader's compacted log.

use serde::{Deserialize, Serialize};

use super::log::LogEntry;
use crate::storage::KeyValue;

/// A node identifier (matches `Config::id`).
pub type NodeId = String;

/// Any message that can travel over the Raft transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotReply(InstallSnapshotReply),
}

impl Message {
    /// The sender's term, present on every message, used for the universal
    /// "step down if we see a higher term" rule.
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote(m) => m.term,
            Message::RequestVoteReply(m) => m.term,
            Message::AppendEntries(m) => m.term,
            Message::AppendEntriesReply(m) => m.term,
            Message::InstallSnapshot(m) => m.term,
            Message::InstallSnapshotReply(m) => m.term,
        }
    }
}

/// §5.2 — candidate solicits a vote.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

/// §5.3 — leader replicates entries (empty = heartbeat).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub follower_id: NodeId,
    pub success: bool,
    /// On success, the highest index now stored by the follower — lets the
    /// leader advance `match_index` without guessing.
    pub match_index: u64,
    /// On failure, a hint about where the follower's log diverges so the leader
    /// can back up `next_index` faster than one-at-a-time.
    pub conflict_index: u64,
}

/// §7 — leader ships a snapshot to a follower whose needed entries were
/// already compacted away.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<KeyValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub follower_id: NodeId,
}
