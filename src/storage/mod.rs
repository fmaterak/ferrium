//! Storage engines.
//!
//! The Raft state machine applies committed commands to whatever backend
//! implements [`StorageEngine`]. An in-memory map is provided by default; an
//! LSM-tree backend lives behind the `lsm` feature.

pub mod engine;
pub mod memory;

pub use engine::{KeyValue, StorageEngine};
pub use memory::MemoryEngine;
