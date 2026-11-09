#!/usr/bin/env bash
# Operates a grid whose nodes run on separate hosts under systemd, from one admin machine over ssh.
#   scripts/grid-cluster.sh start|stop|restart|status
# Environment:
#   GALAXY_CLUSTER   "node-1=host1 node-2=host2 node-3=host3"  (required)
#   GALAXY_ADMIN_PORT  7201   admin port on each host, for status and readiness (protocol port + 100)
#   GALAXY_SSH         ssh    ssh command, e.g. "ssh -i ~/.ssh/galaxy"
#
# Stop and restart act on every node together: under fail-stop, losing one node stops them all anyway.
set -uo pipefail
ACTION="${1:?usage: grid-cluster.sh start|stop|restart|status}"
CLUSTER="${GALAXY_CLUSTER:?set GALAXY_CLUSTER, e.g. \"node-1=host1 node-2=host2 node-3=host3\"}"
PORT="${GALAXY_ADMIN_PORT:-7201}"
SSH="${GALAXY_SSH:-ssh}"

each() { # each <systemctl action>
  local failed=0
  for entry in $CLUSTER; do
    node="${entry%%=*}"
    host="${entry#*=}"
    if $SSH "$host" sudo systemctl "$1" "galaxy-grid-node@$node"; then
      echo "$1 $node on $host"
    else
      echo "FAILED: $1 $node on $host" >&2
      failed=1
    fi
  done
  return "$failed"
}

status() {
  local rc=0
  for entry in $CLUSTER; do
    host="${entry#*=}"
    body="$(curl -s -m 3 -w ' http=%{http_code}' "http://$host:$PORT/health")" || body="unreachable"
    echo "$host:$PORT  $body"
    case "$body" in
      *http=200*) ;;
      *) rc=1 ;;
    esac
  done
  return "$rc"
}

await_ready() {
  for _ in $(seq 1 600); do
    if status >/dev/null 2>&1; then
      status
      echo "grid READY"
      return 0
    fi
    sleep 1
  done
  status
  echo "grid did not become READY; check journalctl -u 'galaxy-grid-node@*' on each host" >&2
  return 1
}

case "$ACTION" in
  start) each start && await_ready ;;
  stop) each stop ;;
  restart) each stop; each start && await_ready ;;
  status) status ;;
  *) echo "unknown action $ACTION" >&2; exit 2 ;;
esac
