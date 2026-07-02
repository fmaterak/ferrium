//! Deterministic simulation tests: elections, failover, and partition
//! tolerance driven entirely in-memory. Requires the `sim` feature:
//!
//! ```bash
//! cargo test --features sim --test simulation
//! ```

#![cfg(feature = "sim")]

use ferrium::apply::WriteCommand;
use ferrium::sim::SimCluster;

fn set(key: &str, value: &str) -> WriteCommand {
    WriteCommand::Set {
        key: key.into(),
        value: value.as_bytes().to_vec(),
    }
}

#[test]
fn cluster_survives_leader_crash() {
    let mut cluster = SimCluster::new(5, 100);
    assert!(
        cluster.run_until(200, |c| c.leader().is_some()),
        "no leader"
    );

    let leader = cluster.leader().unwrap();
    cluster.propose(leader, set("before", "crash"));
    cluster.run(40);

    // Kill the leader and confirm the majority re-elects and stays available.
    cluster.crash(leader);
    let elected_new = cluster.run_until(300, |c| c.leader().map(|l| l != leader).unwrap_or(false));
    assert!(elected_new, "cluster failed to re-elect after leader crash");

    let new_leader = cluster.leader().unwrap();
    assert!(cluster
        .propose(new_leader, set("after", "failover"))
        .is_some());
    cluster.run(60);

    // Every live node must have both writes applied.
    for i in 0..cluster.size() {
        if i == leader {
            continue;
        }
        assert_eq!(cluster.get(i, "before").as_deref(), Some(&b"crash"[..]));
        assert_eq!(cluster.get(i, "after").as_deref(), Some(&b"failover"[..]));
    }
}

#[test]
fn minority_partition_cannot_make_progress() {
    let mut cluster = SimCluster::new(3, 55);
    assert!(cluster.run_until(200, |c| c.leader().is_some()));

    // Isolate a single node: the remaining majority keeps a leader...
    cluster.disconnect(0);
    let majority_has_leader =
        cluster.run_until(300, |c| c.leader().map(|l| l != 0).unwrap_or(false));
    assert!(majority_has_leader, "majority lost its leader");

    // ...and the isolated node, unable to win a majority, never becomes leader.
    assert_ne!(cluster.leader(), Some(0));

    // Healing the partition lets it rejoin without disturbing the leader.
    cluster.reconnect(0);
    cluster.run(60);
    assert!(cluster.leader().is_some());
}
