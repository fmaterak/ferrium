//! Crate-wide error type.
//!
//! Everything fallible in ferrium funnels through [`Error`]. Protocol-level
//! failures are deliberately kept as strings so they can be echoed straight
//! back to the client as a RESP `-ERR ...` reply.

use thiserror::Error;

/// The set of errors ferrium can produce.
#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    /// A write was routed to a node that is not the current leader. The
    /// optional payload carries the address of the leader we know about, so
    /// the client can be redirected.
    #[error("not leader")]
    NotLeader(Option<String>),

    #[error("unknown command '{0}'")]
    UnknownCommand(String),

    #[error("wrong number of arguments for '{0}'")]
    WrongArity(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("cluster error: {0}")]
    Cluster(String),
}

impl Error {
    /// The single-line message sent to clients in a RESP error frame.
    pub fn client_message(&self) -> String {
        match self {
            Error::NotLeader(Some(leader)) => format!("MOVED {leader}"),
            Error::NotLeader(None) => "CLUSTERDOWN no known leader".to_string(),
            other => other.to_string(),
        }
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
