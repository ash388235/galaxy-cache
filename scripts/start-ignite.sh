#!/usr/bin/env bash
# Starts the Ignite server nodes of config/galaxy-ignite.yaml on THIS host, in the background, and waits until
# the cluster is READY (every server up with the same configuration, cluster active).
#   scripts/start-ignite.sh
# Environment: GALAXY_CONFIG (config/galaxy-ignite.yaml), GALAXY_IGNITE_NODES ("node-1 node-2 node-3"),
#              GALAXY_HEAP (1g; Ignite keeps data off-heap, see ignite.dataRegionMb).
set -euo pipefail
GALAXY_CONFIG="${GALAXY_CONFIG:-$(cd "$(dirname "$0")/.." && pwd)/config/galaxy-ignite.yaml}"
source "$(dirname "$0")/common.sh"
JAR="${GALAXY_IGNITE_JAR:-$ROOT/galaxy-ignite-node/target/galaxy-ignite-node.jar}"
NODES="${GALAXY_IGNITE_NODES:-node-1 node-2 node-3}"
HEAP="${GALAXY_HEAP:-1g}"
require_jar "$JAR"

pids=()
for node in $NODES; do
  pidfile="$RUN_DIR/ignite-$node.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "ignite $node is already running (pid $(cat "$pidfile")); stop it first" >&2
    exit 1
  fi
  # the jar's manifest carries the --add-opens Ignite needs on Java 21
  nohup java "-Xmx$HEAP" -XX:+ExitOnOutOfMemoryError -DIGNITE_QUIET=true -DIGNITE_UPDATE_NOTIFIER=false \
    '-Djava.util.logging.SimpleFormatter.format=%1$tFT%1$tT.%1$tL %4$s %3$s %5$s%6$s%n' \
    -jar "$JAR" --config "$CONFIG" --node "$node" \
    >"$RUN_DIR/ignite-$node.log" 2>"$RUN_DIR/ignite-$node.err.log" &
  echo $! >"$pidfile"
  pids+=("$!")
  echo "ignite $node started (pid $!)"
done

for node in $NODES; do
  ready=no
  for _ in $(seq 1 600); do
    for pid in "${pids[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "an ignite node exited during startup; see $RUN_DIR/ignite-*.err.log" >&2
        exit 1
      fi
    done
    if grep -q "ignite node $node READY" "$RUN_DIR/ignite-$node.log" 2>/dev/null; then
      ready=yes
      break
    fi
    sleep 0.5
  done
  if [ "$ready" != yes ]; then
    echo "ignite $node did not become READY; see $RUN_DIR/ignite-$node.err.log" >&2
    exit 1
  fi
done
echo "ignite cluster READY"
