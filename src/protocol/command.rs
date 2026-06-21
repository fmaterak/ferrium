//! Parsing decoded [`Frame`]s into typed [`Command`]s.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::resp::Frame;

/// A client request understood by ferrium.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Bytes>),
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,
    },
    Del {
        keys: Vec<String>,
    },
    ClusterStatus,
    ClusterMembers,
    /// `COMMAND [DOCS]` — emitted by `redis-cli` on connect; we answer with an
    /// empty array so the CLI is happy.
    Command,
}

impl Command {
    /// Parse a request frame (always a RESP array of bulk strings) into a
    /// [`Command`].
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let items = match frame {
            Frame::Array(items) if !items.is_empty() => items,
            _ => return Err(Error::Protocol("expected non-empty array".into())),
        };

        let mut args = items.into_iter().map(bulk_to_bytes);
        let name = args
            .next()
            .transpose()?
            .ok_or_else(|| Error::Protocol("empty command".into()))?;
        let name = String::from_utf8_lossy(&name).to_ascii_uppercase();

        let rest: Vec<Bytes> = args.collect::<Result<_>>()?;
        Self::assemble(&name, rest)
    }

    fn assemble(name: &str, args: Vec<Bytes>) -> Result<Command> {
        match name {
            "PING" => match args.len() {
                0 => Ok(Command::Ping(None)),
                1 => Ok(Command::Ping(Some(args.into_iter().next().unwrap()))),
                _ => Err(Error::WrongArity("ping".into())),
            },
            "GET" => {
                let [key] = exact(args, "get")?;
                Ok(Command::Get {
                    key: to_string(key),
                })
            }
            "SET" => {
                let [key, value] = exact(args, "set")?;
                Ok(Command::Set {
                    key: to_string(key),
                    value,
                })
            }
            "DEL" => {
                if args.is_empty() {
                    return Err(Error::WrongArity("del".into()));
                }
                Ok(Command::Del {
                    keys: args.into_iter().map(to_string).collect(),
                })
            }
            "CLUSTER" => {
                let sub = args
                    .first()
                    .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
                    .unwrap_or_default();
                match sub.as_str() {
                    "STATUS" | "INFO" => Ok(Command::ClusterStatus),
                    "MEMBERS" | "NODES" => Ok(Command::ClusterMembers),
                    other => Err(Error::UnknownCommand(format!("CLUSTER {other}"))),
                }
            }
            "COMMAND" => Ok(Command::Command),
            other => Err(Error::UnknownCommand(other.to_string())),
        }
    }

    /// Whether this command mutates state and therefore must go through Raft.
    pub fn is_write(&self) -> bool {
        matches!(self, Command::Set { .. } | Command::Del { .. })
    }
}

fn bulk_to_bytes(frame: Frame) -> Result<Bytes> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
        other => Err(Error::Protocol(format!(
            "expected bulk string, got {other:?}"
        ))),
    }
}

fn to_string(b: Bytes) -> String {
    String::from_utf8_lossy(&b).into_owned()
}

/// Enforce an exact argument count, returning them as a fixed-size array.
fn exact<const N: usize>(args: Vec<Bytes>, cmd: &str) -> Result<[Bytes; N]> {
    <[Bytes; N]>::try_from(args).map_err(|_| Error::WrongArity(cmd.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(parts: &[&str]) -> Result<Command> {
        let frame = Frame::Array(parts.iter().map(|p| Frame::bulk(p.to_string())).collect());
        Command::from_frame(frame)
    }

    #[test]
    fn parses_set_get_del() {
        assert_eq!(
            cmd(&["SET", "k", "v"]).unwrap(),
            Command::Set {
                key: "k".into(),
                value: Bytes::from("v")
            }
        );
        assert_eq!(
            cmd(&["get", "k"]).unwrap(),
            Command::Get { key: "k".into() }
        );
        assert_eq!(
            cmd(&["DEL", "a", "b"]).unwrap(),
            Command::Del {
                keys: vec!["a".into(), "b".into()]
            }
        );
    }

    #[test]
    fn arity_is_enforced() {
        assert!(matches!(cmd(&["GET"]), Err(Error::WrongArity(_))));
        assert!(matches!(
            cmd(&["SET", "only-key"]),
            Err(Error::WrongArity(_))
        ));
    }

    #[test]
    fn cluster_subcommands() {
        assert_eq!(cmd(&["CLUSTER", "status"]).unwrap(), Command::ClusterStatus);
        assert_eq!(
            cmd(&["cluster", "MEMBERS"]).unwrap(),
            Command::ClusterMembers
        );
    }

    #[test]
    fn writes_are_flagged() {
        assert!(cmd(&["SET", "k", "v"]).unwrap().is_write());
        assert!(!cmd(&["GET", "k"]).unwrap().is_write());
    }
}
