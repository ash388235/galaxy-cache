#!/usr/bin/env bash
# Starts one grid node on this host, in the foreground (use systemd or Docker to supervise it).
#   scripts/start-grid-node.sh <node-name> [config] [grid]
# Environment: GALAXY_HEAP (default 4g), GALAXY_GRID_TOKEN if the config uses ${env:GALAXY_GRID_TOKEN}.
set -euo pipefail
NODE="${1:?usage: start-grid-node.sh <node-name> [config] [grid]}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="${2:-$ROOT/config/galaxy.yaml}"
GRID="${3:-grid-1}"
JAR="${GALAXY_NODE_JAR:-$ROOT/galaxy-grid-node/target/galaxy-grid-node.jar}"
exec java "-Xmx${GALAXY_HEAP:-4g}" -XX:+ExitOnOutOfMemoryError \
  '-Djava.util.logging.SimpleFormatter.format=%1$tFT%1$tT.%1$tL %4$s %3$s %5$s%6$s%n' \
  -jar "$JAR" --config "$CONFIG" --grid "$GRID" --node "$NODE"