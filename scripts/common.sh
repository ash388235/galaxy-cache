#!/usr/bin/env bash
# Shared settings for the helper scripts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${GALAXY_CONFIG:-$ROOT/config/galaxy.yaml}"
RUN_DIR="${GALAXY_RUN_DIR:-$ROOT/galaxy-data/run}"
mkdir -p "$RUN_DIR"
require_jar() {
  if [ ! -f "$1" ]; then
    echo "Missing $1 - run ./scripts/build.sh first" >&2
    exit 1
  fi
}
