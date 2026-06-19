# ferrium

> A distributed, Raft-replicated key-value store written in Rust — inspired by Redis and etcd.

[![CI](https://github.com/fmaterak/ferrium/actions/workflows/ci.yml/badge.svg)](https://github.com/fmaterak/ferrium/actions)
[![Crates.io](https://img.shields.io/crates/v/ferrium.svg)](https://crates.io/crates/ferrium)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.78%2B-orange.svg)](https://www.rust-lang.org)

**ferrium** is a small, self-hosted, strongly-consistent key-value store. It replicates data across a cluster using the **Raft consensus algorithm**, exposes a simple RESP-like TCP protocol (Redis-compatible clients work out of the box), and is built entirely on `tokio` for async I/O.

It's not trying to replace etcd or Redis — it's a from-scratch implementation meant to demonstrate how distributed consensus, replication, and a storage engine actually work under the hood.

---

## Features

- 🗳️ **Raft consensus** — leader election, log replication, and safety guarantees implemented from scratch (no `raft-rs`)
- 🔁 **Automatic failover** — cluster survives the loss of a minority of nodes
- 💾 **Pluggable storage engine** — in-memory by default, optional persistent LSM-tree backend
- 🌐 **Redis-compatible wire protocol (RESP2)** — connect with `redis-cli` or any Redis client library
- 📊 **Prometheus metrics endpoint** — latency, throughput, leader status, log size
- 🔒 **TLS support** for inter-node and client traffic
- 🧪 **Deterministic simulation testing** — network partitions and crashes are simulated in CI, not just described in the README

## Table of Contents

- [Quickstart](#quickstart)
- [Architecture](#architecture)
- [Usage](#usage)
- [Benchmarks](#benchmarks)
- [Roadmap](#roadmap)
- [Development](#development)
- [License](#license)

---

## Quickstart

```bash
# Clone and build
git clone https://github.com/fmaterak/ferrium.git
cd ferrium
cargo build --release

# Start a 3-node local cluster
./scripts/start-cluster.sh

# Talk to it with redis-cli
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
# => "world"

# Kill the leader and watch the cluster re-elect in ms
./scripts/kill-leader.sh
redis-cli -p 6381 GET hello
# => "world"   (still works, new leader took over)
```

## Architecture

```
                 ┌─────────────┐
        writes   │             │
   ┌────────────►│   Leader    │
   │             │  (Node A)   │
   │             └──────┬──────┘
   │                    │ AppendEntries (replicated log)
   │        ┌───────────┴───────────┐
   │        ▼                       ▼
   │  ┌───────────┐           ┌───────────┐
   │  │ Follower  │           │ Follower  │
   │  │ (Node B)  │           │ (Node C)  │
   │  └───────────┘           └───────────┘
   │
 clients (RESP2 protocol)
```

Each node runs:

1. **Raft core** — a *pure*, side-effect-free state machine handling leader election, heartbeats, log replication, and commitment (`src/raft/`). It consumes ticks/messages and emits messages/committed-entries; it never touches the clock or sockets, which is what makes the deterministic simulation possible.
2. **Storage engine** — applies committed log entries to an in-memory map (default) or an LSM-tree (`src/storage/`, behind the `lsm` feature).
3. **Network layer** — an async RESP2 TCP server for clients, plus a length-prefixed JSON channel for inter-node Raft traffic (`src/net/`).
4. **Runtime driver** — the single task that owns the Raft core, drives it with a tick timer, fans messages to/from peers, applies committed writes, and unblocks waiting clients (`src/server.rs`).

Writes always go through the leader and are only acknowledged once a majority of nodes have persisted them. Reads can be served from the leader (linearizable) or optionally from followers (eventually consistent, lower latency).

## Usage

Start a single node:

```bash
ferrium --id node1 --addr 127.0.0.1:6379 --data-dir ./data/node1
```

Join it to a cluster:

```bash
ferrium --id node2 --addr 127.0.0.1:6380 \
        --data-dir ./data/node2 \
        --join 127.0.0.1:6379
```

Supported commands:

| Command | Description |
|---|---|
| `SET key value` | Write a key, replicated via Raft |
| `GET key` | Read a key |
| `DEL key` | Delete a key |
| `PING [msg]` | Liveness check |
| `CLUSTER STATUS` | Show current leader, term, and node health |
| `CLUSTER MEMBERS` | List cluster members |

### Feature flags

| Flag | Effect |
|---|---|
| _(default)_ | In-memory storage, RESP2, in-process Raft |
| `sim` | Compiles the deterministic simulation harness + tests |
| `lsm` | Persistent LSM-tree storage backend |
| `tls` | TLS for client and inter-node traffic |

### Metrics

With `--metrics-addr 127.0.0.1:9100`, scrape the Prometheus endpoint:

```bash
curl -s 127.0.0.1:9100
# ferrium_sets_total 42
# ferrium_is_leader 1
# ferrium_current_term 3
# ferrium_commit_index 128
# ...
```

## Benchmarks

**End-to-end cluster numbers are coming soon.** Once the replication path
stabilizes, this section will include real `SET`/`GET` throughput along with the
exact hardware and setup used, so results are reproducible.

| Operation | p50 | p99 | Throughput |
|---|---|---|---|
| SET (leader) | — | — | — |
| GET (leader) | — | — | — |
| GET (follower, stale reads) | — | — | — |

Micro-benchmarks for the hot paths (RESP2 framing, command parsing, storage
lookups) already run today:

```bash
cargo bench
```

## Roadmap

- [x] Leader election + log replication
- [x] Snapshotting / log compaction
- [x] RESP2 client protocol
- [ ] Dynamic cluster membership changes (joint consensus)
- [ ] LSM-tree persistent storage backend
- [ ] Multi-raft / sharding support
- [ ] Client-side load balancing / smart routing

## Development

```bash
# Run unit + integration tests
cargo test

# Run the deterministic simulation tests (network partitions, node crashes)
cargo test --features sim -- --test-threads=1

# Lint
cargo clippy --all-targets --features sim -- -D warnings

# Format
cargo fmt --all
```

### Project layout

```
src/
├── protocol/   RESP2 framing + command parsing
├── storage/    StorageEngine trait + in-memory backend
├── raft/       consensus core (log, messages, pure state machine)
├── net/        client server + inter-node peer transport
├── apply.rs    encode/apply committed commands
├── server.rs   runtime driver (owns the Raft core)
├── metrics.rs  Prometheus counters/gauges
└── sim.rs      deterministic in-memory cluster (feature = "sim")
tests/          end-to-end + simulation tests
benches/        micro-benchmarks
```

Contributions are welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Why this project exists

This started as a way to deeply understand how systems like etcd, TiKV, and CockroachDB actually achieve consistency under failure. Rather than reading the Raft paper and moving on, I implemented it — including the annoying edge cases around log matching, term conflicts, and leader-completeness that are easy to get wrong.

## License

Licensed under the [MIT License](LICENSE).
