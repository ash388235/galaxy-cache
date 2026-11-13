#!/usr/bin/env bash
# Builds all modules, runs the tests and produces the runnable jars.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
mvn -q -B install "$@"
echo "Built:"
ls -1 galaxy-ignite-node/target/galaxy-ignite-node.jar galaxy-index-server/target/galaxy-index-server.jar galaxy-dashboard/target/galaxy-dashboard.jar galaxy-examples/target/galaxy-examples.jar
