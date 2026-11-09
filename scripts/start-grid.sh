#!/usr/bin/env bash
# Starts every grid node of this config on THIS host, in the background (a single-machine grid).
# For separate hosts use systemd (deploy/galaxy-grid-node@.service) and scripts/grid-cluster.sh.
#   scripts/start-grid.sh
# Environment: GALAXY_CONFIG, GALAXY_GRID (grid-1), GALAXY_GRID_NODES ("node-1 node-2 node-3"),
#              GALAXY_ADMIN_PORTS ("7201 7202 7203"), GALAXY_HEAP (1500m), GALAXY_GRID_TOKEN if the config uses it.
set -euo pipefail
source "$(dirname "$0")/common.sh"
JAR="${GALAXY_NODE_JAR:-$ROOT/galaxy-grid-node/target/galaxy-grid-node.jar}"
GRID="${GALAXY_GRID:-grid-1}"
NODES="${GALAXY_GRID_NODES:-node-1 node-2 node-3}"
ADMIN_PORTS="${GALAXY_ADMIN_PORTS:-7201 7202 7203}"
HEAP="${GALAXY_HEAP:-1500m}"
require_jar "$JAR"

pids=()
for node in $NODES; do
  pidfile="$RUN_DIR/$node.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "$node is already running (pid $(cat "$pidfile")); stop the grid first" >&2
    exit 1
  fi
  nohup java "-Xmx$HEAP" -XX:+ExitOnOutOfMemoryError \
    '-Djava.util.logging.SimpleFormatter.format=%1$tFT%1$tT.%1$tL %4$s %3$s %5$s%6$s%n' \
    -jar "$JAR" --config "$CONFIG" --grid "$GRID" --node "$node" \
    >"$RUN_DIR/$node.log" 2>"$RUN_DIR/$node.err.log" &
  echo $! >"$pidfile"
  pids+=("$!")
  echo "$node started (pid $!)"
done

# /health answers 200 only when the node is READY: every node up with the same configuration
for port in $ADMIN_PORTS; do
  ready=no
  for _ in $(seq 1 600); do
    for pid in "${pids[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "a grid node exited during startup; see $RUN_DIR/*.err.log" >&2
        exit 1
      fi
    done
    if curl -fs -m 2 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      ready=yes
      break
    fi
    sleep 0.5
  done
  if [ "$ready" != yes ]; then
    echo "node on admin port $port did not become READY; see $RUN_DIR/*.err.log" >&2
    exit 1
  fi
done
echo "grid READY"
