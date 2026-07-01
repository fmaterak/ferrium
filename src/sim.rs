//! Deterministic simulation testing.
//!
//! This drives a whole cluster of [`RaftNode`]s over a fully in-memory,
//! synchronous "network". Because the consensus core is pure and the message
//! bus here is deterministic, tests can reproduce elections, partitions, and
//! crashes exactly — the failures live in CI, not just in the README.
//!
//! Time is modelled as discrete steps. In each step every live node advances
//! one tick, its outputs are drained and applied, and the emitted messages are
//! delivered to reachable peers (arriving on the next step, i.e. one network
//! hop per step).

use std::collections::HashMap;

use crate::apply::WriteCommand;
use crate::raft::message::{Message, NodeId};
use crate::raft::node::{Committed, RaftConfig, RaftNode};
use crate::raft::Role;
use crate::storage::{MemoryEngine, StorageEngine};

/// An in-memory cluster used for deterministic testing.
pub struct SimCluster {
    nodes: Vec<RaftNode>,
    stores: Vec<MemoryEngine>,
    /// Whether a node is running (a crashed node neither ticks nor receives).
    alive: Vec<bool>,
    /// Whether a node can reach the network (partition modelling).
    connected: Vec<bool>,
    ids: Vec<NodeId>,
    id_to_idx: HashMap<NodeId, usize>,
    step: u64,
}

impl SimCluster {
    /// Build an `n`-node cluster with a fixed RNG seed for reproducibility.
    pub fn new(n: usize, seed: u64) -> Self {
        let ids: Vec<NodeId> = (0..n).map(|i| format!("n{i}")).collect();
        let id_to_idx = ids.iter().cloned().zip(0..).collect();

        let mut nodes = Vec::with_capacity(n);
        for (i, id) in ids.iter().enumerate() {
            let peers = ids.iter().filter(|p| *p != id).cloned().collect();
            let config = RaftConfig {
                heartbeat_ticks: 1,
                election_timeout_min: 5,
                election_timeout_max: 10,
                snapshot_threshold: 8,
                // Distinct seeds so election timeouts diverge and split votes
                // resolve.
                seed: Some(seed + i as u64),
            };
            nodes.push(RaftNode::new(id.clone(), peers, config));
        }

        SimCluster {
            stores: (0..n).map(|_| MemoryEngine::new()).collect(),
            nodes,
            alive: vec![true; n],
            connected: vec![true; n],
            ids,
            id_to_idx,
            step: 0,
        }
    }

    /// Advance the whole cluster by one step.
    pub fn step(&mut self) {
        self.step += 1;

        for i in 0..self.nodes.len() {
            if self.alive[i] {
                self.nodes[i].tick();
            }
        }

        // Drain outputs and buffer messages for delivery.
        let mut bus: Vec<(usize, NodeId, Message)> = Vec::new();
        for i in 0..self.nodes.len() {
            if !self.alive[i] {
                continue;
            }
            let ready = self.nodes[i].take_ready();
            for committed in ready.committed {
                self.apply(i, committed);
            }
            for env in ready.messages {
                if let Some(&to) = self.id_to_idx.get(&env.to) {
                    bus.push((to, self.ids[i].clone(), env.message));
                }
            }
        }

        // Deliver to reachable nodes.
        for (to, from, message) in bus {
            let from_idx = self.id_to_idx[&from];
            if self.deliverable(from_idx, to) {
                self.nodes[to].step(from, message);
            }
        }
    }

    /// Run for `steps` steps.
    pub fn run(&mut self, steps: u64) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// Step until `pred` holds or `max` steps elapse; returns whether it held.
    pub fn run_until(&mut self, max: u64, mut pred: impl FnMut(&SimCluster) -> bool) -> bool {
        for _ in 0..max {
            if pred(self) {
                return true;
            }
            self.step();
        }
        pred(self)
    }

    fn deliverable(&self, from: usize, to: usize) -> bool {
        self.alive[from] && self.alive[to] && self.connected[from] && self.connected[to]
    }

    fn apply(&mut self, node: usize, committed: Committed) {
        match committed {
            Committed::Command { command, .. } => {
                if let Ok(cmd) = WriteCommand::decode(&command) {
                    cmd.apply(&self.stores[node]);
                }
            }
            Committed::Snapshot(data) => self.stores[node].restore(data),
        }
    }

    // ---- inspection / control --------------------------------------------

    /// Index of the current leader, if a unique one exists at the highest term.
    pub fn leader(&self) -> Option<usize> {
        let max_term = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| self.alive[*i])
            .map(|(_, n)| n.current_term())
            .max()?;
        let leaders: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| {
                self.alive[i]
                    && self.nodes[i].role() == Role::Leader
                    && self.nodes[i].current_term() == max_term
            })
            .collect();
        match leaders.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// Propose a write to node `idx`. Returns the assigned index on success.
    pub fn propose(&mut self, idx: usize, command: WriteCommand) -> Option<u64> {
        self.nodes[idx].propose(command.encode()).ok()
    }

    /// Read a key from node `idx`'s applied state machine.
    pub fn get(&self, idx: usize, key: &str) -> Option<Vec<u8>> {
        self.stores[idx].get(key).map(|b| b.to_vec())
    }

    pub fn commit_index(&self, idx: usize) -> u64 {
        self.nodes[idx].commit_index()
    }

    pub fn term(&self, idx: usize) -> u64 {
        self.nodes[idx].current_term()
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    /// Crash a node: it stops ticking and receiving until restarted.
    pub fn crash(&mut self, idx: usize) {
        self.alive[idx] = false;
    }

    /// Restart a previously crashed node (its log/state persist).
    pub fn restart(&mut self, idx: usize) {
        self.alive[idx] = true;
    }

    /// Partition a node away from the rest of the cluster.
    pub fn disconnect(&mut self, idx: usize) {
        self.connected[idx] = false;
    }

    /// Heal a node's network partition.
    pub fn reconnect(&mut self, idx: usize) {
        self.connected[idx] = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(key: &str, value: &str) -> WriteCommand {
        WriteCommand::Set {
            key: key.into(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn cluster_elects_a_single_leader() {
        let mut cluster = SimCluster::new(3, 42);
        assert!(cluster.run_until(100, |c| c.leader().is_some()));
    }

    #[test]
    fn writes_replicate_to_all_nodes() {
        let mut cluster = SimCluster::new(3, 7);
        assert!(cluster.run_until(100, |c| c.leader().is_some()));
        let leader = cluster.leader().unwrap();

        cluster.propose(leader, set("hello", "world"));
        cluster.run(30);

        for i in 0..cluster.size() {
            assert_eq!(cluster.get(i, "hello").as_deref(), Some(&b"world"[..]));
        }
    }
}
