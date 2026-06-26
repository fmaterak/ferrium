//! The Raft consensus implementation (leader election, log replication,
//! commitment, and snapshotting), written from scratch.
//!
//! The core [`node::RaftNode`] is a pure state machine; the async driver that
//! wires it to real time and sockets lives in [`crate::net`].

pub mod log;
pub mod message;
pub mod node;
pub mod state;

pub use log::{LogEntry, RaftLog};
pub use message::{Message, NodeId};
pub use node::{Committed, Envelope, RaftConfig, RaftNode, Ready};
pub use state::Role;
