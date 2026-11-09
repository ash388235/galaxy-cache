#!/usr/bin/env bash
# Stops background index pods and the dashboard started by the scripts.
source "$(dirname "$0")/common.sh"
for pidfile in "$RUN_DIR"/*.pid; do
  [ -e "$pidfile" ] || continue
  pid=$(cat "$pidfile")
  kill "$pid" 2>/dev/null && echo "stopped $(basename "$pidfile" .pid) ($pid)"
  rm -f "$pidfile"
done
