#!/usr/bin/env bash
# Starts one index pod in the background: ./scripts/start-index.sh idx-a
set -euo pipefail
source "$(dirname "$0")/common.sh"
NODE="${1:?usage: start-index.sh <index node name from config>}"
JAR="$ROOT/galaxy-index-server/target/galaxy-index-server.jar"
require_jar "$JAR"
nohup java -jar "$JAR" --config "$CONFIG" --node "$NODE" > "$RUN_DIR/$NODE.log" 2>&1 &
echo $! > "$RUN_DIR/$NODE.pid"
echo "index pod $NODE started (pid $(cat "$RUN_DIR/$NODE.pid"), log $RUN_DIR/$NODE.log)"
