//! End-to-end test against a real single-node server over a TCP socket,
//! speaking the RESP2 wire protocol exactly as `redis-cli` would.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::time::Duration;

use ferrium::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Grab an ephemeral port by binding and immediately releasing it.
fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(client_port: u16, raft_port: u16) -> Config {
    Config {
        id: "solo".into(),
        addr: format!("127.0.0.1:{client_port}").parse().unwrap(),
        raft_addr: Some(format!("127.0.0.1:{raft_port}").parse().unwrap()),
        data_dir: std::env::temp_dir().join("ferrium-test"),
        join: None,
        peers: vec![],
        metrics_addr: None,
        tls: false,
        tls_cert: None,
        tls_key: None,
    }
}

async fn send(stream: &mut TcpStream, raw: &[u8]) -> String {
    stream.write_all(raw).await.unwrap();
    let mut buf = vec![0u8; 256];
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("reply timed out")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn single_node_serves_set_get_ping() {
    let client_port = free_port();
    let raft_port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{client_port}").parse().unwrap();

    tokio::spawn(async move {
        let _ = ferrium::server::run(config(client_port, raft_port)).await;
    });

    // Give the node time to bind and elect itself leader.
    sleep(Duration::from_millis(800)).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");

    assert_eq!(send(&mut stream, b"PING\r\n").await, "+PONG\r\n");

    // SET may briefly race the election; retry until the leader is ready.
    let mut set_reply = String::new();
    for _ in 0..20 {
        set_reply = send(&mut stream, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").await;
        if set_reply == "+OK\r\n" {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(set_reply, "+OK\r\n");

    let get_reply = send(&mut stream, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").await;
    assert_eq!(get_reply, "$1\r\nv\r\n");

    let missing = send(&mut stream, b"*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n").await;
    assert_eq!(missing, "$-1\r\n");
}
