#!/usr/bin/env bash
#
# Find the current leader by asking each node for CLUSTER STATUS, then kill it
# so you can watch the remaining nodes re-elect. Requires redis-cli.

set -euo pipefail
cd "$(dirname "$0")/.."

command -v redis-cli >/dev/null || {
  echo "redis-cli not found (install redis-tools)"; exit 1;
}

declare -A PORTS=( [node1]=6380 [node2]=6381 [node3]=6382 )

for id in node1 node2 node3; do
  port="${PORTS[$id]}"
  status="$(redis-cli -p "$port" CLUSTER STATUS 2>/dev/null || true)"
  if [[ "$status" == *"role=leader"* ]]; then
    pidfile="data/${id}.pid"
    if [[ -f "$pidfile" ]]; then
      pid="$(cat "$pidfile")"
      echo "leader is ${id} (client port ${port}, pid ${pid}) — killing it"
      kill "$pid" 2>/dev/null || true
      rm -f "$pidfile"
    fi
    echo "watch the survivors re-elect:"
    echo "  redis-cli -p 6381 CLUSTER STATUS"
    exit 0
  fi
done

echo "no leader found — is the cluster running? (./scripts/start-cluster.sh)"
exit 1
