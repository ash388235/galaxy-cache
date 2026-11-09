#!/usr/bin/env bash
# Runs the end-to-end demo client. Extra args go to the demo, e.g.:
#   ./scripts/run-demo.sh --records 500000
#   ./scripts/run-demo.sh --embedded --keep-running     (index pods + dashboard inside the demo JVM)
set -euo pipefail
source "$(dirname "$0")/common.sh"
JAR="$ROOT/galaxy-examples/target/galaxy-examples.jar"
require_jar "$JAR"
java -jar "$JAR" --config "$CONFIG" "$@"
