#!/usr/bin/env bash
# Deletes all local demo data (storages, catalog, logs). Stop services first.
source "$(dirname "$0")/common.sh"
rm -rf "$ROOT/galaxy-data/fs1" "$ROOT/galaxy-data/fs2" "$ROOT/galaxy-data/catalog" "$ROOT/galaxy-data/logs"
echo "local data removed"
