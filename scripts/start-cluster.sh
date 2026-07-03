#!/usr/bin/env bash
#
# Start a local 3-node ferrium cluster.
#
# Client ports:  6380 6381 6382
# Raft ports:   16380 16381 16382  (client port + 10000, ferrium's default)
#
# PIDs are written to ./data/<id>.pid so kill-leader.sh can find them.

set -euo pipefail
cd "$(dirname "$0")/.."

BIN="${FERRIUM_BIN:-./target/release/ferrium}"
if [[ ! -x "$BIN" ]]; then
  echo "building release binary..."
  cargo build --release
  BIN="./target/release/ferrium"
fi

mkdir -p data logs

# id -> client port
declare -A PORTS=( [node1]=6380 [node2]=6381 [node3]=6382 )

raft_addr() { echo "127.0.0.1:$(( $1 + 10000 ))"; }

# Every node is told about the other two via static --peer entries.
peers_for() {
  local self="$1"
  local args=()
  for id in node1 node2 node3; do
    [[ "$id" == "$self" ]] && continue
    args+=( --peer "${id}=$(raft_addr "${PORTS[$id]}")" )
  done
  echo "${args[@]}"
}

echo "starting 3-node cluster..."
for id in node1 node2 node3; do
  port="${PORTS[$id]}"
  # shellcheck disable=SC2046
  "$BIN" \
    --id "$id" \
    --addr "127.0.0.1:${port}" \
    --data-dir "./data/${id}" \
    $(peers_for "$id") \
    > "logs/${id}.log" 2>&1 &
  echo $! > "data/${id}.pid"
  echo "  ${id}  client=127.0.0.1:${port}  raft=$(raft_addr "$port")  pid=$(cat "data/${id}.pid")"
done

echo
echo "cluster up. try:"
echo "  redis-cli -p 6380 SET hello world"
echo "  redis-cli -p 6380 CLUSTER STATUS"
echo "  ./scripts/kill-leader.sh"
