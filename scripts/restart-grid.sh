#!/usr/bin/env bash
# Restarts all grid nodes on this host. There is no single-node restart: under fail-stop the whole grid restarts.
#   scripts/restart-grid.sh
set -euo pipefail
here="$(dirname "$0")"
"$here/stop-grid.sh"
"$here/start-grid.sh"
