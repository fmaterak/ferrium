//! The client-facing RESP2 server: one task per connection.
//!
//! Reads client commands, serves reads straight from the (shared) storage
//! engine, and routes writes through the Raft driver, blocking the reply until
//! the write is committed.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::apply::WriteCommand;
use crate::error::{Error, Result};
use crate::metrics::MetricsInner;
use crate::protocol::{Command, Frame};
use crate::server::{Handle, ProposeOutcome};

/// Serve a single client connection until it closes.
pub async fn handle_connection(mut stream: TcpStream, handle: Handle) -> Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = BytesMut::new();

    loop {
        // Try to parse a full frame from whatever we've buffered.
        let frame = match Frame::decode(&mut buf)? {
            Some(frame) => frame,
            None => {
                let n = stream.read_buf(&mut buf).await?;
                if n == 0 {
                    return Ok(()); // client hung up
                }
                continue;
            }
        };

        let reply = match Command::from_frame(frame) {
            Ok(command) => dispatch(&handle, command).await,
            Err(e) => Frame::Error(format!("ERR {}", e.client_message())),
        };

        out.clear();
        reply.encode(&mut out);
        stream.write_all(&out).await?;
    }
}

/// Turn a parsed command into a reply frame.
async fn dispatch(handle: &Handle, command: Command) -> Frame {
    match command {
        Command::Ping(msg) => match msg {
            Some(payload) => Frame::Bulk(payload),
            None => Frame::Simple("PONG".into()),
        },
        Command::Get { key } => {
            MetricsInner::incr(&handle.metrics.gets_total);
            match handle.storage.get(&key) {
                Some(value) => Frame::Bulk(value),
                None => Frame::Null,
            }
        }
        Command::Set { key, value } => {
            MetricsInner::incr(&handle.metrics.sets_total);
            let cmd = WriteCommand::Set {
                key,
                value: value.to_vec(),
            };
            match handle.propose(cmd).await {
                ProposeOutcome::Committed => Frame::Simple("OK".into()),
                ProposeOutcome::NotLeader(leader) => not_leader_frame(leader),
            }
        }
        Command::Del { keys } => {
            MetricsInner::incr(&handle.metrics.dels_total);
            let cmd = WriteCommand::Del { keys };
            match handle.propose(cmd).await {
                // We reply OK-as-integer; exact deleted count would require the
                // apply result to be threaded back, a straightforward follow-up.
                ProposeOutcome::Committed => Frame::Integer(1),
                ProposeOutcome::NotLeader(leader) => not_leader_frame(leader),
            }
        }
        Command::ClusterStatus => match handle.status().await {
            Some(status) => Frame::Bulk(Bytes::from(format!(
                "id={} role={} term={} leader={} commit={} size={}",
                status.id,
                status.role,
                status.term,
                status.leader.as_deref().unwrap_or("none"),
                status.commit_index,
                status.cluster_size,
            ))),
            None => Frame::Error("ERR driver unavailable".into()),
        },
        Command::ClusterMembers => {
            let members = handle
                .members
                .iter()
                .map(|(id, addr)| Frame::Bulk(Bytes::from(format!("{id}@{addr}"))))
                .collect();
            Frame::Array(members)
        }
        Command::Command => Frame::Array(vec![]),
    }
}

fn not_leader_frame(leader: Option<String>) -> Frame {
    Frame::Error(format!("ERR {}", Error::NotLeader(leader).client_message()))
}
