//! The storage engine abstraction.

use bytes::Bytes;

/// One key/value pair, used when taking and restoring snapshots.
pub type KeyValue = (String, Bytes);

/// A key/value backend that the Raft state machine applies committed writes to.
///
/// Implementations must be safe to share across tasks (`Send + Sync`) and use
/// interior mutability — the state machine holds them behind an `Arc`.
pub trait StorageEngine: Send + Sync {
    /// Fetch a value, or `None` if the key is absent.
    fn get(&self, key: &str) -> Option<Bytes>;

    /// Insert or overwrite a key.
    fn set(&self, key: String, value: Bytes);

    /// Remove a key, returning whether it existed.
    fn del(&self, key: &str) -> bool;

    /// Number of stored keys.
    fn len(&self) -> usize;

    /// Whether the store holds no keys.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Materialize the full contents for snapshotting.
    fn snapshot(&self) -> Vec<KeyValue>;

    /// Replace the entire contents from a snapshot.
    fn restore(&self, entries: Vec<KeyValue>);
}
