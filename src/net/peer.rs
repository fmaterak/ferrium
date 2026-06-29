//! Inter-node Raft transport.
//!
//! A simple length-prefixed JSON framing over TCP. Each node keeps one
//! persistent outbound connection per peer (lazily (re)connected) and one
//! inbound listener that fans received messages into the driver.
//!
//! Messages are wrapped in a [`Wire`] envelope so the receiver always knows the
//! sender's id, independent of the message body.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::raft::message::{Message, NodeId};

/// On-wire envelope: the message plus its sender.
#[derive(Debug, Serialize, Deserialize)]
struct Wire {
    from: NodeId,
    message: Message,
}

/// Outbound side: routes messages to the right peer link.
#[derive(Clone)]
pub struct Peers {
    txs: HashMap<NodeId, mpsc::UnboundedSender<Message>>,
}

impl Peers {
    /// Spawn one writer task per peer and return a router.
    pub fn start(my_id: NodeId, peers: Vec<(NodeId, SocketAddr)>) -> Peers {
        let mut txs = HashMap::new();
        for (peer_id, addr) in peers {
            let (tx, rx) = mpsc::unbounded_channel();
            txs.insert(peer_id.clone(), tx);
            tokio::spawn(peer_link(my_id.clone(), peer_id, addr, rx));
        }
        Peers { txs }
    }

    /// Best-effort send to a peer. Dropping a heartbeat is fine — Raft is built
    /// to tolerate lost messages, and the next tick will retry.
    pub fn send(&self, to: &str, message: Message) {
        if let Some(tx) = self.txs.get(to) {
            let _ = tx.send(message);
        }
    }
}

/// Persistent outbound connection to a single peer with lazy reconnect.
async fn peer_link(
    my_id: NodeId,
    peer_id: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::UnboundedReceiver<Message>,
) {
    let mut conn: Option<TcpStream> = None;
    while let Some(message) = rx.recv().await {
        let wire = Wire {
            from: my_id.clone(),
            message,
        };
        let bytes = match serde_json::to_vec(&wire) {
            Ok(b) => b,
            Err(_) => continue,
        };

        // (Re)establish the connection on demand.
        if conn.is_none() {
            conn = TcpStream::connect(addr).await.ok();
        }
        if let Some(stream) = conn.as_mut() {
            if write_frame(stream, &bytes).await.is_err() {
                tracing::debug!(peer = %peer_id, "peer connection dropped, will reconnect");
                conn = None;
            }
        } else {
            // Peer unreachable; brief back-off so we don't spin.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Accept inbound Raft connections and forward decoded messages to `inbound`.
pub async fn serve(listener: TcpListener, inbound: mpsc::UnboundedSender<(NodeId, Message)>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let tx = inbound.clone();
                tokio::spawn(read_connection(stream, tx));
            }
            Err(e) => {
                tracing::warn!(error = %e, "raft accept failed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

async fn read_connection(mut stream: TcpStream, inbound: mpsc::UnboundedSender<(NodeId, Message)>) {
    loop {
        match read_frame(&mut stream).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<Wire>(&bytes) {
                Ok(wire) => {
                    if inbound.send((wire.from, wire.message)).is_err() {
                        return; // driver gone
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "dropping malformed raft frame");
                }
            },
            Ok(None) => return, // clean EOF
            Err(_) => return,
        }
    }
}

async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let len = match stream.read_u32().await {
        Ok(len) => len as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}
