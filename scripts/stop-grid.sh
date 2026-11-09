#!/usr/bin/env bash
# Stops the grid nodes started on this host by start-grid.sh. Index pods and the dashboard keep running.
#   scripts/stop-grid.sh
# Environment: GALAXY_GRID_NODES ("node-1 node-2 node-3").
set -uo pipefail
source "$(dirname "$0")/common.sh"
NODES="${GALAXY_GRID_NODES:-node-1 node-2 node-3}"
for node in $NODES; do
  pidfile="$RUN_DIR/$node.pid"
  [ -f "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  if kill "$pid" 2>/dev/null; then
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null
      echo "force-stopped $node ($pid)"
    else
      echo "stopped $node ($pid)"
    fi
  else
    echo "$node ($pid) had already stopped"
  fi
  rm -f "$pidfile"
done
