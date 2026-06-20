//! Command-line configuration.
//!
//! A ferrium node is fully described by its [`Config`]. The CLI is parsed with
//! `clap`; the same struct is reused by tests to spin up in-process nodes.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Runtime configuration for a single ferrium node.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "ferrium",
    version,
    about = "A distributed, Raft-replicated key-value store"
)]
pub struct Config {
    /// Stable, unique identifier for this node within the cluster.
    #[arg(long)]
    pub id: String,

    /// Address the RESP2 client server listens on.
    #[arg(long, default_value = "127.0.0.1:6379")]
    pub addr: SocketAddr,

    /// Address the inter-node Raft transport listens on.
    ///
    /// Defaults to `addr` with the port offset by 10000 when omitted.
    #[arg(long)]
    pub raft_addr: Option<SocketAddr>,

    /// Directory used for persistent state (log, snapshots).
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Address of an existing cluster member to join. Omit to bootstrap a
    /// brand-new single-node cluster.
    #[arg(long)]
    pub join: Option<SocketAddr>,

    /// Static list of peer Raft addresses (`id=addr`), used when bootstrapping
    /// a cluster without dynamic membership.
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<Peer>,

    /// Optional Prometheus metrics endpoint (`host:port`).
    #[arg(long)]
    pub metrics_addr: Option<SocketAddr>,

    /// Enable TLS for client and inter-node traffic (requires the `tls`
    /// feature and certificate paths below).
    #[arg(long, default_value_t = false)]
    pub tls: bool,

    /// Path to the PEM-encoded certificate chain.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,

    /// Path to the PEM-encoded private key.
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
}

/// A peer entry parsed from `--peer id=host:port`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr,
}

impl Config {
    /// Effective Raft transport address, applying the default offset rule.
    pub fn raft_addr(&self) -> SocketAddr {
        self.raft_addr.unwrap_or_else(|| {
            let mut a = self.addr;
            a.set_port(self.addr.port().wrapping_add(10_000));
            a
        })
    }
}

fn parse_peer(raw: &str) -> Result<Peer, String> {
    let (id, addr) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected `id=host:port`, got `{raw}`"))?;
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|e| format!("invalid peer address `{addr}`: {e}"))?;
    Ok(Peer {
        id: id.to_string(),
        addr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_parses_id_and_addr() {
        let p = parse_peer("node2=127.0.0.1:16380").unwrap();
        assert_eq!(p.id, "node2");
        assert_eq!(p.addr.port(), 16380);
    }

    #[test]
    fn peer_rejects_missing_separator() {
        assert!(parse_peer("garbage").is_err());
    }

    #[test]
    fn raft_addr_offsets_client_port_by_default() {
        let cfg = Config {
            id: "n1".into(),
            addr: "127.0.0.1:6379".parse().unwrap(),
            raft_addr: None,
            data_dir: "./data".into(),
            join: None,
            peers: vec![],
            metrics_addr: None,
            tls: false,
            tls_cert: None,
            tls_key: None,
        };
        assert_eq!(cfg.raft_addr().port(), 16379);
    }
}
