#!/usr/bin/env bash
# Starts the dashboard in the background on port 8080 (or $1).
set -euo pipefail
source "$(dirname "$0")/common.sh"
PORT="${1:-8080}"
JAR="$ROOT/galaxy-dashboard/target/galaxy-dashboard.jar"
require_jar "$JAR"
nohup java -jar "$JAR" --config "$CONFIG" --port "$PORT" > "$RUN_DIR/dashboard.log" 2>&1 &
echo $! > "$RUN_DIR/dashboard.pid"
echo "dashboard started on http://localhost:$PORT (pid $(cat "$RUN_DIR/dashboard.pid"))"
