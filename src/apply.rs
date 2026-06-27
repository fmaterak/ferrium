//! Bridging committed Raft log entries and the storage engine.
//!
//! Client writes are encoded into a [`WriteCommand`], stored verbatim in the
//! replicated log, and decoded + applied once committed. Keeping this in one
//! place guarantees every node applies identical bytes in identical order —
//! the whole point of the replicated log.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::StorageEngine;

/// A state-machine mutation replicated through Raft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteCommand {
    Set { key: String, value: Vec<u8> },
    Del { keys: Vec<String> },
}

impl WriteCommand {
    /// Serialize for storage in the log.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("WriteCommand is always serializable")
    }

    /// Decode an entry pulled back out of the log.
    pub fn decode(bytes: &[u8]) -> Result<WriteCommand> {
        serde_json::from_slice(bytes).map_err(|e| Error::Storage(format!("corrupt log entry: {e}")))
    }

    /// Apply this command to the state machine, returning the integer reply
    /// (e.g. number of keys deleted) surfaced to the client.
    pub fn apply(&self, engine: &dyn StorageEngine) -> i64 {
        match self {
            WriteCommand::Set { key, value } => {
                engine.set(key.clone(), Bytes::from(value.clone()));
                1
            }
            WriteCommand::Del { keys } => keys.iter().filter(|k| engine.del(k)).count() as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryEngine;

    #[test]
    fn encode_decode_roundtrip() {
        let cmd = WriteCommand::Set {
            key: "k".into(),
            value: b"v".to_vec(),
        };
        assert_eq!(WriteCommand::decode(&cmd.encode()).unwrap(), cmd);
    }

    #[test]
    fn apply_mutates_engine() {
        let engine = MemoryEngine::new();
        WriteCommand::Set {
            key: "k".into(),
            value: b"v".to_vec(),
        }
        .apply(&engine);
        assert_eq!(engine.get("k"), Some(Bytes::from("v")));

        let deleted = WriteCommand::Del {
            keys: vec!["k".into(), "missing".into()],
        }
        .apply(&engine);
        assert_eq!(deleted, 1);
    }
}
