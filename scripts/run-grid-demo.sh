#!/usr/bin/env bash
# Local demo: starts a single-machine grid and the dashboard if needed, then runs writer processes and one reader
# that waits for them. Follow it at http://localhost:8080/?namespace=grid
#   scripts/run-grid-demo.sh [total-objects] [writers]        default 100000 and 2
set -euo pipefail
source "$(dirname "$0")/common.sh"
here="$(dirname "$0")"
TOTAL="${1:-100000}"
WRITERS="${2:-2}"
JAR="${GALAXY_TOOLS_JAR:-$ROOT/galaxy-examples/target/galaxy-examples.jar}"
MAIN=io.galaxycache.examples.sizing.GridDemo
require_jar "$JAR"

if ! "$here/grid-status.sh" >/dev/null 2>&1; then
  echo "grid is not up; starting it"
  "$here/start-grid.sh"
fi
if ! curl -s -m 3 -o /dev/null "http://127.0.0.1:8080/"; then
  echo "starting the dashboard"
  "$here/start-dashboard.sh"
fi

java -cp "$JAR" "$MAIN" --config "$CONFIG" --reset >"$RUN_DIR/demo-reset.log" 2>"$RUN_DIR/demo-reset.err.log"

client() { # client <name> <heap> <args...>
  local name="$1" heap="$2"
  shift 2
  java "-Xmx$heap" "-Dgalaxy.client.name=$name" -cp "$JAR" "$MAIN" --config "$CONFIG" "$@" \
    >"$RUN_DIR/demo-$name.log" 2>"$RUN_DIR/demo-$name.err.log" &
  pids+=("$!")
  echo "$name started (pid $!)"
}

pids=()
client reader 3g --read --writers "$WRITERS" --total "$TOTAL"
for w in $(seq 1 "$WRITERS"); do
  client "writer-$w" 1g --write "$w" --writers "$WRITERS" --total "$TOTAL"
done
echo "Dashboard: http://localhost:8080/?namespace=grid"

failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=1
done
echo
grep -h DONE "$RUN_DIR"/demo-writer-*.log || true
grep -h 'reader:' "$RUN_DIR/demo-reader.log" || true
if [ "$failed" -ne 0 ]; then
  echo "a client failed; see $RUN_DIR/demo-*.err.log" >&2
  exit 1
fi
