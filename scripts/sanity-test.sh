#!/usr/bin/env bash
# Basic sanity test for a new setup: builds, then runs the same ProviderDemo on all three engines and checks that
# each one stores, queries and drops data with identical results.
#   1. direct : file storage + 2 index pods      (config/galaxy.yaml, namespace trading)
#   2. grid   : 3-node galaxy grid               (config/galaxy.yaml, namespace grid)
#   3. ignite : 3-node Apache Ignite cluster     (config/galaxy-ignite.yaml)
# Usage: scripts/sanity-test.sh [--skip-build] [--with-tests] [--only direct|grid|ignite]
# Everything it starts is stopped again. Takes about 3 minutes after the build. Needs about 4 GB of free memory.
set -uo pipefail
source "$(dirname "$0")/common.sh"
here="$(cd "$(dirname "$0")" && pwd)"
BUILD=yes; TESTS=no; ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build) BUILD=no ;;
    --with-tests) TESTS=yes ;;
    --only) ONLY="$2"; shift ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
  shift
done
JAR="$ROOT/galaxy-examples/target/galaxy-examples.jar"
EXPECT="count(symbol = MSFT) = 507"   # 5,000 trades from a fixed seed: identical on every engine
export GALAXY_HEAP="${GALAXY_HEAP:-768m}"
declare -A RESULT

if [ "$BUILD" = yes ]; then
  echo "== build"
  if [ "$TESTS" = yes ]; then (cd "$ROOT" && mvn -B -q verify) || { echo "build or tests FAILED"; exit 1; }
  else (cd "$ROOT" && mvn -B -q package -DskipTests) || { echo "build FAILED"; exit 1; }; fi
fi
require_jar "$JAR"

demo() { # demo <log> <extra JVM option or ""> <demo args...>
  local log="$RUN_DIR/sanity-$1.log" jvm="$2"; shift 2
  java -Xmx768m ${jvm:+"$jvm"} -cp "$JAR" io.galaxycache.examples.ProviderDemo --records 5000 "$@" >"$log" 2>&1
  local rc=$?
  grep -E '^   |^==' "$log" | grep -v '^\s*$' | sed 's/^/  /' | cut -c1-160
  [ $rc -eq 0 ] && grep -qF "$EXPECT" "$log" && grep -q "^   dropped" "$log"
}

wait_http() { for _ in $(seq 1 60); do curl -fs -m 2 "$1" >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }

if [ -z "$ONLY" ] || [ "$ONLY" = direct ]; then
  echo "== 1. direct: file storage + index pods"
  "$here/start-index.sh" idx-a && "$here/start-index.sh" idx-b
  if wait_http http://127.0.0.1:7070/health && wait_http http://127.0.0.1:7071/health \
      && demo direct "" --config "$ROOT/config/galaxy.yaml" --namespace trading; then RESULT[direct]=PASS; else RESULT[direct]=FAIL; fi
  for n in idx-a idx-b; do kill "$(cat "$RUN_DIR/$n.pid")" 2>/dev/null; rm -f "$RUN_DIR/$n.pid"; done
fi

if [ -z "$ONLY" ] || [ "$ONLY" = grid ]; then
  echo "== 2. grid: 3 galaxy grid nodes"
  if "$here/start-grid.sh" && demo grid "" --config "$ROOT/config/galaxy.yaml" --namespace grid; then RESULT[grid]=PASS; else RESULT[grid]=FAIL; fi
  "$here/stop-grid.sh" >/dev/null
fi

if [ -z "$ONLY" ] || [ "$ONLY" = ignite ]; then
  echo "== 3. ignite: 3 Apache Ignite nodes"
  if "$here/start-ignite.sh" && demo ignite "@$ROOT/config/ignite-jvm.options" --config "$ROOT/config/galaxy-ignite.yaml" --namespace trading; then
    RESULT[ignite]=PASS; else RESULT[ignite]=FAIL; fi
  "$here/stop-ignite.sh" >/dev/null
fi

echo
echo "== sanity test results (logs in $RUN_DIR/sanity-*.log)"
failed=0
for k in direct grid ignite; do
  [ -n "${RESULT[$k]:-}" ] || continue
  printf '  %-7s %s\n' "$k" "${RESULT[$k]}"
  [ "${RESULT[$k]}" = PASS ] || failed=1
done
exit $failed
