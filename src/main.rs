//! ferrium node entry point.

use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use ferrium::config::Config;
use ferrium::server;

#[tokio::main]
async fn main() -> ferrium::Result<()> {
    let config = Config::parse();
    init_tracing();

    tracing::info!(
        id = %config.id,
        client = %config.addr,
        raft = %config.raft_addr(),
        peers = config.peers.len(),
        "starting ferrium node"
    );

    if let Some(join) = config.join {
        // Dynamic membership (joint consensus) is on the roadmap; for now a
        // cluster is formed from the static --peer list, so we just verify the
        // seed node is reachable.
        if server::probe(join).await {
            tracing::info!(%join, "seed node reachable");
        } else {
            tracing::warn!(%join, "seed node not reachable yet");
        }
    }

    server::run(config).await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("FERRIUM_LOG")
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    fmt().with_env_filter(filter).with_target(false).init();
}
