//! Lightweight micro-benchmarks for the hot paths that don't need a running
//! cluster: RESP2 framing, command parsing, and the in-memory storage engine.
//!
//! Deliberately dependency-free (no criterion) — it just times a fixed number
//! of iterations with `Instant` and prints ns/op. End-to-end cluster
//! throughput (the numbers promised in the README) is measured separately once
//! the replication path stabilizes.

use std::time::Instant;

use bytes::{Bytes, BytesMut};

use ferrium::protocol::{Command, Frame};
use ferrium::storage::{MemoryEngine, StorageEngine};

const ITERS: u32 = 2_000_000;

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    // Warm up.
    for _ in 0..(iters / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / iters as f64;
    let throughput = 1_000_000_000.0 / per_op;
    println!("{name:<28} {per_op:>8.1} ns/op   {throughput:>12.0} ops/s");
}

fn main() {
    println!("ferrium micro-benchmarks ({ITERS} iters)\n");

    let mut encoded = BytesMut::new();
    Frame::Array(vec![
        Frame::bulk("SET"),
        Frame::bulk("some:key"),
        Frame::bulk("a-reasonably-sized-value-payload"),
    ])
    .encode(&mut encoded);
    bench("resp_decode_set", ITERS, || {
        let mut buf = encoded.clone();
        std::hint::black_box(Frame::decode(&mut buf).unwrap());
    });

    let get_frame = Frame::Array(vec![Frame::bulk("GET"), Frame::bulk("some:key")]);
    bench("command_parse_get", ITERS, || {
        std::hint::black_box(Command::from_frame(get_frame.clone()).unwrap());
    });

    let engine = MemoryEngine::new();
    engine.set("key".into(), Bytes::from_static(b"value"));
    bench("memory_engine_get", ITERS, || {
        std::hint::black_box(engine.get("key"));
    });
}
