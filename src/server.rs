//! The node runtime: the async driver that owns the [`RaftNode`] and wires it
//! to time, the peer transport, the storage engine, and client requests.
//!
//! Everything funnels through a single driver task that exclusively owns the
//! (otherwise un-synchronized) [`RaftNode`]. Clients and the network talk to it
//! over channels, which keeps the consensus core lock-free and its execution
//! deterministic.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::apply::WriteCommand;
use crate::config::Config;
use crate::error::Result;
use crate::metrics::{self, Metrics, MetricsInner};
use crate::net::{client, peer};
use crate::raft::message::{InstallSnapshot, Message, NodeId};
use crate::raft::node::{Committed, RaftConfig, RaftNode};
use crate::raft::Role;
use crate::storage::{MemoryEngine, StorageEngine};

/// How much wall-clock time one Raft "tick" represents.
const TICK: Duration = Duration::from_millis(50);

/// A request sent to the driver task.
pub enum DriverMsg {
    /// An inbound Raft message from a peer.
    Peer(NodeId, Message),
    /// A client write to be replicated; resolved once committed.
    Propose {
        command: WriteCommand,
        done: oneshot::Sender<ProposeOutcome>,
    },
    /// A snapshot of cluster state for `CLUSTER STATUS`.
    Status(oneshot::Sender<ClusterStatus>),
}

/// Result of a proposed write.
pub enum ProposeOutcome {
    Committed,
    NotLeader(Option<String>),
}

/// Human-readable cluster state.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub id: String,
    pub role: &'static str,
    pub term: u64,
    pub leader: Option<String>,
    pub commit_index: u64,
    pub cluster_size: usize,
}

/// A cheap, cloneable handle used by client connections to reach the driver
/// and read from the (shared) storage engine.
#[derive(Clone)]
pub struct Handle {
    pub storage: Arc<dyn StorageEngine>,
    pub metrics: Metrics,
    pub node_id: String,
    pub members: Arc<Vec<(String, SocketAddr)>>,
    driver: mpsc::UnboundedSender<DriverMsg>,
}

impl Handle {
    /// Propose a write and await its commitment.
    pub async fn propose(&self, command: WriteCommand) -> ProposeOutcome {
        let (done, rx) = oneshot::channel();
        if self
            .driver
            .send(DriverMsg::Propose { command, done })
            .is_err()
        {
            return ProposeOutcome::NotLeader(None);
        }
        rx.await.unwrap_or(ProposeOutcome::NotLeader(None))
    }

    /// Fetch current cluster status.
    pub async fn status(&self) -> Option<ClusterStatus> {
        let (tx, rx) = oneshot::channel();
        self.driver.send(DriverMsg::Status(tx)).ok()?;
        rx.await.ok()
    }
}

/// Start a node and serve until shutdown (Ctrl-C).
pub async fn run(config: Config) -> Result<()> {
    let peers: Vec<(NodeId, SocketAddr)> = config
        .peers
        .iter()
        .map(|p| (p.id.clone(), p.addr))
        .collect();
    let peer_ids: Vec<NodeId> = peers.iter().map(|(id, _)| id.clone()).collect();

    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let metrics = metrics::new();

    // Inbound Raft messages from peers.
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let raft_listener = TcpListener::bind(config.raft_addr()).await?;
    tracing::info!(addr = %config.raft_addr(), "raft transport listening");
    tokio::spawn(peer::serve(raft_listener, inbound_tx));

    let peer_router = peer::Peers::start(config.id.clone(), peers.clone());

    // The driver task exclusively owns the RaftNode.
    let (driver_tx, driver_rx) = mpsc::unbounded_channel();
    let raft_cfg = RaftConfig {
        heartbeat_ticks: 2,
        election_timeout_min: 6,
        election_timeout_max: 12,
        snapshot_threshold: 1024,
        seed: None,
    };
    let cluster_size = peer_ids.len() + 1;
    let node = RaftNode::new(config.id.clone(), peer_ids, raft_cfg);
    let mut driver = Driver {
        node,
        storage: Arc::clone(&storage),
        metrics: Arc::clone(&metrics),
        peers: peer_router,
        waiters: HashMap::new(),
        was_leader: false,
        cluster_size,
    };
    tokio::spawn(async move { driver.run(inbound_rx, driver_rx).await });

    // Optional metrics endpoint.
    if let Some(metrics_addr) = config.metrics_addr {
        let m = Arc::clone(&metrics);
        tokio::spawn(async move { serve_metrics(metrics_addr, m).await });
    }

    // Membership advertised via CLUSTER MEMBERS.
    let mut members = vec![(config.id.clone(), config.addr)];
    members.extend(peers.iter().cloned());
    let handle = Handle {
        storage,
        metrics,
        node_id: config.id.clone(),
        members: Arc::new(members),
        driver: driver_tx,
    };

    let client_listener = TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "client server listening (RESP2)");
    serve_clients(client_listener, handle).await
}

async fn serve_clients(listener: TcpListener, handle: Handle) -> Result<()> {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, addr) = accepted?;
                let handle = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = client::handle_connection(stream, handle).await {
                        tracing::debug!(peer = %addr, error = %e, "client connection closed");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

/// The single-owner Raft driver.
struct Driver {
    node: RaftNode,
    storage: Arc<dyn StorageEngine>,
    metrics: Metrics,
    peers: peer::Peers,
    waiters: HashMap<u64, Vec<oneshot::Sender<ProposeOutcome>>>,
    was_leader: bool,
    cluster_size: usize,
}

impl Driver {
    async fn run(
        &mut self,
        mut inbound: mpsc::UnboundedReceiver<(NodeId, Message)>,
        mut requests: mpsc::UnboundedReceiver<DriverMsg>,
    ) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.node.tick(),
                Some((from, msg)) = inbound.recv() => self.node.step(from, msg),
                maybe = requests.recv() => match maybe {
                    Some(msg) => self.handle_request(msg),
                    None => return,
                },
            }
            self.process_ready();
        }
    }

    fn handle_request(&mut self, msg: DriverMsg) {
        match msg {
            DriverMsg::Peer(from, message) => self.node.step(from, message),
            DriverMsg::Propose { command, done } => match self.node.propose(command.encode()) {
                Ok(index) => self.waiters.entry(index).or_default().push(done),
                Err(not_leader) => {
                    MetricsInner::incr(&self.metrics.not_leader_total);
                    let _ = done.send(ProposeOutcome::NotLeader(not_leader.leader));
                }
            },
            DriverMsg::Status(reply) => {
                let _ = reply.send(self.status());
            }
        }
    }

    fn process_ready(&mut self) {
        let ready = self.node.take_ready();

        for env in ready.messages {
            let message = self.fill_snapshot(env.message);
            self.peers.send(&env.to, message);
        }

        for committed in ready.committed {
            match committed {
                Committed::Command { index, command } => {
                    if let Ok(cmd) = WriteCommand::decode(&command) {
                        cmd.apply(&*self.storage);
                    }
                    self.resolve(index, ProposeOutcome::Committed);
                }
                Committed::Snapshot(data) => self.storage.restore(data),
            }
        }

        self.update_metrics();

        // If we've lost (or never had) leadership, fail everything still
        // waiting so clients get redirected instead of hanging.
        if !self.node.is_leader() && !self.waiters.is_empty() {
            let leader = self.node.leader_id().map(str::to_string);
            for (_, waiters) in self.waiters.drain() {
                for w in waiters {
                    let _ = w.send(ProposeOutcome::NotLeader(leader.clone()));
                }
            }
        }
    }

    /// The Raft core emits `InstallSnapshot` with empty data (it doesn't own
    /// the state machine); the driver fills in the current snapshot here.
    fn fill_snapshot(&self, message: Message) -> Message {
        match message {
            Message::InstallSnapshot(mut snap) => {
                let InstallSnapshot { data, .. } = &mut snap;
                *data = self.storage.snapshot();
                Message::InstallSnapshot(snap)
            }
            other => other,
        }
    }

    fn resolve(&mut self, index: u64, outcome: ProposeOutcome) {
        if let Some(waiters) = self.waiters.remove(&index) {
            for w in waiters {
                // All waiters for one index get the same outcome; clone via match.
                let out = match &outcome {
                    ProposeOutcome::Committed => ProposeOutcome::Committed,
                    ProposeOutcome::NotLeader(l) => ProposeOutcome::NotLeader(l.clone()),
                };
                let _ = w.send(out);
            }
        }
    }

    fn update_metrics(&mut self) {
        let is_leader = self.node.is_leader();
        if is_leader && !self.was_leader {
            MetricsInner::incr(&self.metrics.elections_won_total);
        }
        self.was_leader = is_leader;
        MetricsInner::set_u64(&self.metrics.current_term, self.node.current_term());
        MetricsInner::set_i64(&self.metrics.is_leader, is_leader as i64);
        MetricsInner::set_u64(&self.metrics.commit_index, self.node.commit_index());
        MetricsInner::set_u64(&self.metrics.log_len, self.node.last_log_index());
    }

    fn status(&self) -> ClusterStatus {
        let role = match self.node.role() {
            Role::Leader => "leader",
            Role::Candidate => "candidate",
            Role::Follower => "follower",
        };
        ClusterStatus {
            id: self.node.id().to_string(),
            role,
            term: self.node.current_term(),
            leader: self.node.leader_id().map(str::to_string),
            commit_index: self.node.commit_index(),
            cluster_size: self.cluster_size,
        }
    }
}

/// A bare-bones HTTP endpoint that serves the Prometheus text format on `GET`.
async fn serve_metrics(addr: SocketAddr, metrics: Metrics) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, "failed to bind metrics endpoint");
            return;
        }
    };
    tracing::info!(%addr, "metrics endpoint listening");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let body = metrics.render();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut scratch = [0u8; 512];
            let _ = stream.read(&mut scratch).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// Helper used by the CLI/`--join` path to ping an address (placeholder for a
/// future dynamic-membership RPC).
pub async fn probe(addr: SocketAddr) -> bool {
    TcpStream::connect(addr).await.is_ok()
}
