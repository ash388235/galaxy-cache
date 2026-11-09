#!/usr/bin/env bash
# Shows whether grid nodes are READY. Exit status 0 only if all of them are.
#   scripts/grid-status.sh [host:admin-port ...]      default: 127.0.0.1 on GALAXY_ADMIN_PORTS (7201 7202 7203)
set -uo pipefail
targets=("$@")
if [ "${#targets[@]}" -eq 0 ]; then
  for port in ${GALAXY_ADMIN_PORTS:-7201 7202 7203}; do
    targets+=("127.0.0.1:$port")
  done
fi
status=0
for t in "${targets[@]}"; do
  body="$(curl -s -m 3 -w ' http=%{http_code}' "http://$t/health")" || body="unreachable"
  echo "$t  $body"
  case "$body" in
    *http=200*) ;;
    *) status=1 ;;
  esac
done
exit "$status"
