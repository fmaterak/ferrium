//! **ferrium** — a distributed, Raft-replicated key/value store.
//!
//! The crate is organized in layers, mirroring what a single node runs:
//!
//! - [`protocol`] — the RESP2 wire format and command parsing (client-facing).
//! - [`storage`] — the pluggable state-machine backend ([`storage::MemoryEngine`]).
//! - [`raft`] — the from-scratch consensus core: log, messages, and the pure
//!   [`raft::RaftNode`] state machine.
//! - [`apply`] — encoding committed commands between the log and the store.
//! - [`net`] — async I/O: the client server and the peer transport.
//! - [`server`] — the runtime driver that stitches it all together.
//! - [`metrics`] — Prometheus counters and gauges.
//!
//! The `sim` module (behind the `sim` feature) exercises the consensus core
//! against a fully in-memory, deterministic network.

pub mod apply;
pub mod config;
pub mod error;
pub mod metrics;
pub mod net;
pub mod protocol;
pub mod raft;
pub mod server;
pub mod storage;

#[cfg(feature = "sim")]
pub mod sim;

pub use config::Config;
pub use error::{Error, Result};
