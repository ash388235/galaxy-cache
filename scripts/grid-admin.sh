#!/usr/bin/env bash
# Grid maintenance jobs. Run them with no clients writing.
#   scripts/grid-admin.sh rebalance    move records to their owners after the node list changed
#   scripts/grid-admin.sh compact      reclaim disk held by old versions and deleted records
# Environment: GALAXY_CONFIG, GALAXY_GRID (grid-1), GALAXY_TOOLS_JAR, GALAXY_GRID_TOKEN if the config uses it.
set -euo pipefail
source "$(dirname "$0")/common.sh"
JAR="${GALAXY_TOOLS_JAR:-$ROOT/galaxy-examples/target/galaxy-examples.jar}"
GRID="${GALAXY_GRID:-grid-1}"
require_jar "$JAR"
case "${1:-}" in
  rebalance) main=io.galaxycache.grid.client.GridRebalance ;;
  compact) main=io.galaxycache.grid.client.GridCompact ;;
  *) echo "usage: grid-admin.sh rebalance|compact" >&2; exit 2 ;;
esac
exec java -cp "$JAR" "$main" --config "$CONFIG" --grid "$GRID"
