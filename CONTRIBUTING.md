# Contributing to ferrium

Thanks for your interest! ferrium is a from-scratch learning-oriented
distributed system, so clarity and correctness matter more than raw features.

## Getting started

```bash
git clone https://github.com/fmaterak/ferrium.git
cd ferrium
cargo build
cargo test --features sim
```

## Before you open a PR

Run the same checks CI does:

```bash
cargo fmt --all --check
cargo clippy --all-targets --features sim -- -D warnings
cargo test --features sim
```

## Where things live

| Area | Path |
|---|---|
| RESP2 protocol & command parsing | `src/protocol/` |
| Storage engines | `src/storage/` |
| Raft consensus core (pure logic) | `src/raft/` |
| Async I/O (client server + peer transport) | `src/net/` |
| Runtime driver | `src/server.rs` |
| Deterministic simulation | `src/sim.rs` |

## Guidelines

- **Keep the consensus core pure.** `src/raft/node.rs` must not touch the clock,
  sockets, or the filesystem — that's what makes the simulation tests possible.
  Anything with side effects belongs in the driver (`src/server.rs`).
- **Add a simulation test for consensus changes.** If you touch election,
  replication, or commitment, cover it in `tests/simulation.rs`.
- **Small, focused commits** with a clear message describing the *why*.
- **No new `unsafe`** without a comment justifying it.

## Reporting bugs

Open an issue with a minimal reproduction. For consensus bugs, a failing
`SimCluster` scenario is the gold standard — it's deterministic and lands
straight in the test suite.
