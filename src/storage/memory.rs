//! In-memory storage engine backed by a `HashMap` behind an `RwLock`.
//!
//! This is the default backend: fast, dependency-free, and lost on restart
//! (durability comes from the replicated Raft log + snapshots, not the map).

use std::collections::HashMap;
use std::sync::RwLock;

use bytes::Bytes;

use super::engine::{KeyValue, StorageEngine};

/// A thread-safe in-memory key/value map.
#[derive(Default)]
pub struct MemoryEngine {
    map: RwLock<HashMap<String, Bytes>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageEngine for MemoryEngine {
    fn get(&self, key: &str) -> Option<Bytes> {
        self.map.read().unwrap().get(key).cloned()
    }

    fn set(&self, key: String, value: Bytes) {
        self.map.write().unwrap().insert(key, value);
    }

    fn del(&self, key: &str) -> bool {
        self.map.write().unwrap().remove(key).is_some()
    }

    fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    fn snapshot(&self) -> Vec<KeyValue> {
        self.map
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn restore(&self, entries: Vec<KeyValue>) {
        let mut map = self.map.write().unwrap();
        map.clear();
        map.extend(entries);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_del_cycle() {
        let engine = MemoryEngine::new();
        assert!(engine.is_empty());
        engine.set("k".into(), Bytes::from("v"));
        assert_eq!(engine.get("k"), Some(Bytes::from("v")));
        assert_eq!(engine.len(), 1);
        assert!(engine.del("k"));
        assert!(!engine.del("k"));
        assert!(engine.get("k").is_none());
    }

    #[test]
    fn snapshot_and_restore_roundtrip() {
        let engine = MemoryEngine::new();
        engine.set("a".into(), Bytes::from("1"));
        engine.set("b".into(), Bytes::from("2"));
        let snap = engine.snapshot();

        let restored = MemoryEngine::new();
        restored.restore(snap);
        assert_eq!(restored.len(), 2);
        assert_eq!(restored.get("a"), Some(Bytes::from("1")));
    }
}
