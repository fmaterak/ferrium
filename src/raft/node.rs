//! The Raft consensus state machine.
//!
//! [`RaftNode`] is deliberately *pure* and synchronous: it consumes inputs
//! (timer ticks, inbound [`Message`]s, client proposals) and accumulates
//! outputs (outbound messages, committed entries) that the caller drains via
//! [`RaftNode::take_ready`]. Keeping I/O out of here is what makes the
//! deterministic simulation tests (`--features sim`) possible — the same logic
//! runs identically whether driven by real sockets or a simulated network.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::log::{LogEntry, RaftLog};
use super::message::{
    AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, Message, NodeId,
    RequestVote, RequestVoteReply,
};
use super::state::{LeaderProgress, Role, VoteTally};
use crate::storage::KeyValue;

/// Timing and behaviour knobs, expressed in abstract "ticks".
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Ticks between leader heartbeats.
    pub heartbeat_ticks: u64,
    /// Lower bound of the randomized election timeout, in ticks.
    pub election_timeout_min: u64,
    /// Upper bound of the randomized election timeout, in ticks.
    pub election_timeout_max: u64,
    /// Compact the log once it grows beyond this many entries.
    pub snapshot_threshold: u64,
    /// Optional fixed RNG seed for deterministic tests.
    pub seed: Option<u64>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            heartbeat_ticks: 1,
            election_timeout_min: 5,
            election_timeout_max: 10,
            snapshot_threshold: 1024,
            seed: None,
        }
    }
}

/// A message addressed to a specific peer.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub to: NodeId,
    pub message: Message,
}

/// Something that has been committed and must be applied to the state machine.
#[derive(Debug, Clone)]
pub enum Committed {
    /// A committed log entry carrying an application command.
    Command { index: u64, command: Vec<u8> },
    /// A snapshot installed from the leader; the state machine should be
    /// wholesale replaced with this data.
    Snapshot(Vec<KeyValue>),
}

/// Outputs produced since the last drain.
#[derive(Debug, Default)]
pub struct Ready {
    pub messages: Vec<Envelope>,
    pub committed: Vec<Committed>,
}

/// A Raft peer's full consensus state.
pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,
    config: RaftConfig,
    rng: StdRng,

    role: Role,
    leader_id: Option<NodeId>,

    // Persistent state (would be fsync'd before responding in a durable build).
    current_term: u64,
    voted_for: Option<NodeId>,
    log: RaftLog,

    // Volatile state.
    commit_index: u64,
    last_applied: u64,
    progress: LeaderProgress,
    votes: VoteTally,

    // Timers, in ticks.
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    election_timeout: u64,

    // Accumulated outputs.
    outgoing: Vec<Envelope>,
    committed: Vec<Committed>,
}

impl RaftNode {
    /// Create a fresh follower.
    pub fn new(id: impl Into<NodeId>, peers: Vec<NodeId>, config: RaftConfig) -> Self {
        let mut rng = match config.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };
        let election_timeout =
            rng.gen_range(config.election_timeout_min..=config.election_timeout_max);
        RaftNode {
            id: id.into(),
            peers,
            config,
            rng,
            role: Role::Follower,
            leader_id: None,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            progress: LeaderProgress::default(),
            votes: VoteTally::default(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            outgoing: Vec::new(),
            committed: Vec::new(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Drain everything produced since the last call.
    pub fn take_ready(&mut self) -> Ready {
        self.apply_committed();
        Ready {
            messages: std::mem::take(&mut self.outgoing),
            committed: std::mem::take(&mut self.committed),
        }
    }

    /// Advance logical time by one tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.become_candidate();
                }
            }
        }
    }

    /// Propose a new command (leader only). Returns the assigned log index.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id.clone(),
            });
        }
        let index = self.log.append(vec![LogEntry {
            term: self.current_term,
            command,
        }]);
        // Replicate immediately, and re-check commit in case we're alone.
        self.broadcast_append();
        self.advance_commit();
        Ok(index)
    }

    /// Handle an inbound message from a peer.
    pub fn step(&mut self, from: NodeId, message: Message) {
        // Universal rule: any message with a higher term forces us to step down.
        if message.term() > self.current_term {
            self.become_follower(message.term(), None);
        }

        match message {
            Message::RequestVote(m) => self.handle_request_vote(m),
            Message::RequestVoteReply(m) => self.handle_request_vote_reply(m),
            Message::AppendEntries(m) => self.handle_append_entries(m),
            Message::AppendEntriesReply(m) => self.handle_append_entries_reply(from, m),
            Message::InstallSnapshot(m) => self.handle_install_snapshot(m),
            Message::InstallSnapshotReply(m) => self.handle_install_snapshot_reply(from, m),
        }
    }

    // ---- role transitions -------------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = leader;
        self.reset_election_timer();
    }

    fn become_candidate(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.record(self.id.clone());
        self.reset_election_timer();

        // A single-node cluster elects itself instantly.
        if self.votes.count() >= self.majority() {
            self.become_leader();
            return;
        }

        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for peer in self.peers.clone() {
            self.send(peer, Message::RequestVote(request.clone()));
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        self.progress.reset(&self.peers, self.log.last_index());
        self.heartbeat_elapsed = 0;
        self.broadcast_append();
        // In a single-node cluster the leader's own log is trivially committed.
        self.advance_commit();
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self
            .rng
            .gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
    }

    // ---- RequestVote ------------------------------------------------------

    fn handle_request_vote(&mut self, m: RequestVote) {
        let mut granted = false;
        if m.term >= self.current_term {
            let can_vote =
                self.voted_for.is_none() || self.voted_for.as_deref() == Some(&m.candidate_id);
            if can_vote && self.candidate_is_up_to_date(m.last_log_index, m.last_log_term) {
                granted = true;
                self.voted_for = Some(m.candidate_id.clone());
                self.reset_election_timer();
            }
        }
        let reply = RequestVoteReply {
            term: self.current_term,
            voter_id: self.id.clone(),
            vote_granted: granted,
        };
        self.send(m.candidate_id, Message::RequestVoteReply(reply));
    }

    /// §5.4.1 — a candidate's log must be at least as up-to-date as ours.
    fn candidate_is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let our_term = self.log.last_term();
        let our_index = self.log.last_index();
        last_term > our_term || (last_term == our_term && last_index >= our_index)
    }

    fn handle_request_vote_reply(&mut self, m: RequestVoteReply) {
        if self.role != Role::Candidate || m.term != self.current_term {
            return;
        }
        if m.vote_granted {
            let count = self.votes.record(m.voter_id);
            if count >= self.majority() {
                self.become_leader();
            }
        }
    }

    // ---- AppendEntries ----------------------------------------------------

    fn handle_append_entries(&mut self, m: AppendEntries) {
        // Stale leader — reject and advertise our term.
        if m.term < self.current_term {
            return self.reject_append(&m.leader_id, 0);
        }

        // Valid leader for this term: (re)become a follower and defer to it.
        self.become_follower(m.term, Some(m.leader_id.clone()));

        // Log-matching property (§5.3).
        if !self.log.matches(m.prev_log_index, m.prev_log_term) {
            let conflict = self.conflict_hint(m.prev_log_index);
            return self.reject_append(&m.leader_id, conflict);
        }

        let last_new_index = self.log.splice(m.prev_log_index, m.entries);
        if m.leader_commit > self.commit_index {
            self.commit_index = m.leader_commit.min(last_new_index);
        }

        let reply = AppendEntriesReply {
            term: self.current_term,
            follower_id: self.id.clone(),
            success: true,
            match_index: last_new_index,
            conflict_index: 0,
        };
        self.send(m.leader_id, Message::AppendEntriesReply(reply));
    }

    /// Cheap divergence hint so the leader can back up faster than one index
    /// per round-trip: point it at the start of the conflicting region.
    fn conflict_hint(&self, prev_log_index: u64) -> u64 {
        let last = self.log.last_index();
        if prev_log_index > last {
            last + 1
        } else {
            prev_log_index.max(self.log.first_index())
        }
    }

    fn reject_append(&mut self, leader_id: &str, conflict_index: u64) {
        let reply = AppendEntriesReply {
            term: self.current_term,
            follower_id: self.id.clone(),
            success: false,
            match_index: 0,
            conflict_index,
        };
        self.send(leader_id.to_string(), Message::AppendEntriesReply(reply));
    }

    fn handle_append_entries_reply(&mut self, from: NodeId, m: AppendEntriesReply) {
        if self.role != Role::Leader || m.term != self.current_term {
            return;
        }
        if m.success {
            self.progress.set_match_index(&from, m.match_index);
            self.progress.set_next_index(&from, m.match_index + 1);
            self.advance_commit();
        } else {
            let next = m.conflict_index.max(1);
            self.progress.set_next_index(&from, next);
            self.send_append(&from);
        }
    }

    // ---- replication driving ---------------------------------------------

    fn broadcast_append(&mut self) {
        for peer in self.peers.clone() {
            self.send_append(&peer);
        }
    }

    fn send_append(&mut self, peer: &str) {
        let next = self.progress.next_index(peer);

        // The needed entries have been compacted away — ship a snapshot instead.
        if next <= self.log.snapshot_index() {
            let snapshot = InstallSnapshot {
                term: self.current_term,
                leader_id: self.id.clone(),
                last_included_index: self.log.snapshot_index(),
                last_included_term: self.log.last_term(),
                data: Vec::new(), // filled in by the driver, which owns the state machine
            };
            self.send(peer.to_string(), Message::InstallSnapshot(snapshot));
            return;
        }

        let prev_index = next - 1;
        let prev_term = self.log.term_at(prev_index).unwrap_or(0);
        let entries = self.log.entries_from(next);
        let append = AppendEntries {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.send(peer.to_string(), Message::AppendEntries(append));
    }

    /// §5.3/§5.4 — advance `commit_index` to the highest N replicated on a
    /// majority, but only for entries from the current term.
    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        for n in (self.commit_index + 1..=last).rev() {
            if self.log.term_at(n) != Some(self.current_term) {
                continue;
            }
            let mut replicas = 1; // the leader itself
            for peer in &self.peers {
                if self.progress.match_index.get(peer).copied().unwrap_or(0) >= n {
                    replicas += 1;
                }
            }
            if replicas >= self.majority() {
                self.commit_index = n;
                break;
            }
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied) {
                if !entry.command.is_empty() {
                    self.committed.push(Committed::Command {
                        index: self.last_applied,
                        command: entry.command.clone(),
                    });
                }
            }
        }
        self.maybe_compact();
    }

    fn maybe_compact(&mut self) {
        if self.last_applied.saturating_sub(self.log.snapshot_index())
            >= self.config.snapshot_threshold
        {
            self.log.compact(self.last_applied);
        }
    }

    // ---- InstallSnapshot --------------------------------------------------

    fn handle_install_snapshot(&mut self, m: InstallSnapshot) {
        if m.term < self.current_term {
            return;
        }
        self.become_follower(m.term, Some(m.leader_id.clone()));
        self.log
            .install_snapshot(m.last_included_index, m.last_included_term);
        self.commit_index = m.last_included_index;
        self.last_applied = m.last_included_index;
        self.committed.push(Committed::Snapshot(m.data));

        let reply = InstallSnapshotReply {
            term: self.current_term,
            follower_id: self.id.clone(),
        };
        self.send(m.leader_id, Message::InstallSnapshotReply(reply));
    }

    fn handle_install_snapshot_reply(&mut self, from: NodeId, m: InstallSnapshotReply) {
        if self.role != Role::Leader || m.term != self.current_term {
            return;
        }
        let index = self.log.snapshot_index();
        self.progress.set_match_index(&from, index);
        self.progress.set_next_index(&from, index + 1);
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.outgoing.push(Envelope { to, message });
    }
}

/// Returned by [`RaftNode::propose`] when this node isn't the leader.
#[derive(Debug, Clone)]
pub struct NotLeader {
    pub leader: Option<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(seed: u64) -> RaftConfig {
        RaftConfig {
            seed: Some(seed),
            ..Default::default()
        }
    }

    #[test]
    fn single_node_elects_itself_and_commits() {
        let mut node = RaftNode::new("n1", vec![], cfg(1));
        // Tick past the election timeout.
        for _ in 0..node.config.election_timeout_max {
            node.tick();
        }
        assert!(node.is_leader());

        let idx = node.propose(b"SET k v".to_vec()).unwrap();
        assert_eq!(idx, 1);
        let ready = node.take_ready();
        assert!(matches!(
            ready.committed.as_slice(),
            [Committed::Command { index: 1, .. }]
        ));
    }

    #[test]
    fn follower_grants_vote_once_per_term() {
        let mut node = RaftNode::new("n1", vec!["n2".into(), "n3".into()], cfg(2));
        node.step(
            "n2".into(),
            Message::RequestVote(RequestVote {
                term: 1,
                candidate_id: "n2".into(),
                last_log_index: 0,
                last_log_term: 0,
            }),
        );
        let ready = node.take_ready();
        let granted = matches!(
            ready.messages.first().map(|e| &e.message),
            Some(Message::RequestVoteReply(r)) if r.vote_granted
        );
        assert!(granted);

        // A different candidate in the same term must be refused.
        node.step(
            "n3".into(),
            Message::RequestVote(RequestVote {
                term: 1,
                candidate_id: "n3".into(),
                last_log_index: 0,
                last_log_term: 0,
            }),
        );
        let ready = node.take_ready();
        let refused = matches!(
            ready.messages.first().map(|e| &e.message),
            Some(Message::RequestVoteReply(r)) if !r.vote_granted
        );
        assert!(refused);
    }

    #[test]
    fn higher_term_forces_step_down() {
        let mut node = RaftNode::new("n1", vec!["n2".into()], cfg(3));
        for _ in 0..node.config.election_timeout_max {
            node.tick();
        }
        let term_before = node.current_term();
        node.step(
            "n2".into(),
            Message::AppendEntries(AppendEntries {
                term: term_before + 5,
                leader_id: "n2".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), term_before + 5);
        assert_eq!(node.leader_id(), Some("n2"));
    }
}
