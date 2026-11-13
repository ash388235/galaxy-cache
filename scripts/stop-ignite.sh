#!/usr/bin/env bash
# Stops the Ignite server nodes started on this host by start-ignite.sh (all together: fail-stop clusters
# are restarted as a whole). With persistence: true the data is kept for the next start.
#   scripts/stop-ignite.sh
# Environment: GALAXY_IGNITE_NODES ("node-1 node-2 node-3").
set -uo pipefail
source "$(dirname "$0")/common.sh"
NODES="${GALAXY_IGNITE_NODES:-node-1 node-2 node-3}"
for node in $NODES; do
  pidfile="$RUN_DIR/ignite-$node.pid"
  [ -f "$pidfile" ] || continue
  kill "$(cat "$pidfile")" 2>/dev/null || true
done
for node in $NODES; do
  pidfile="$RUN_DIR/ignite-$node.pid"
  [ -f "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null
    echo "force-stopped ignite $node ($pid)"
  else
    echo "stopped ignite $node ($pid)"
  fi
  rm -f "$pidfile"
done
