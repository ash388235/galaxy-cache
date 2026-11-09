#!/usr/bin/env bash
# Creates what a secured grid needs: an authentication token, a private CA, one certificate per node, and a truststore.
#   scripts/generate-grid-secrets.sh <output-dir> <node>=<host> [<node>=<host> ...]
#   scripts/generate-grid-secrets.sh ./grid-secrets node-1=grid1.example.com node-2=grid2.example.com node-3=grid3.example.com
#
# Output (keep it out of version control; .gitignore covers these file types):
#   grid.env            GALAXY_GRID_TOKEN and GALAXY_TLS_PASSWORD, for /etc/galaxy/grid.env and client hosts
#   ca.p12              the CA's private key: keep it offline, it is not needed on any host
#   truststore.p12      the CA certificate, for every node and client
#   nodes/<node>.p12    one per node, holding that node's key and certificate
# Requires keytool (from the JDK) and openssl. Certificates last GALAXY_CERT_DAYS (default 825) days, the CA 10 years.
set -euo pipefail
OUT="${1:?usage: generate-grid-secrets.sh <output-dir> <node>=<host> ...}"
shift
[ "$#" -gt 0 ] || { echo "name at least one <node>=<host>" >&2; exit 2; }
DAYS="${GALAXY_CERT_DAYS:-825}"
if [ -e "$OUT/ca.p12" ]; then
  echo "$OUT already holds a CA; refusing to overwrite it" >&2
  exit 1
fi
umask 077
mkdir -p "$OUT/nodes"
TOKEN="$(openssl rand -hex 32)"
PW="$(openssl rand -hex 24)"
ks() { keytool -storepass "$PW" -storetype PKCS12 "$@"; }

ks -genkeypair -alias ca -keyalg RSA -keysize 3072 -dname "CN=Galaxy Grid CA" -ext bc:c -validity 3650 \
  -keypass "$PW" -keystore "$OUT/ca.p12" 2>/dev/null
ks -exportcert -alias ca -keystore "$OUT/ca.p12" -file "$OUT/ca.crt" 2>/dev/null
ks -importcert -noprompt -alias ca -keystore "$OUT/truststore.p12" -file "$OUT/ca.crt" 2>/dev/null

for entry in "$@"; do
  node="${entry%%=*}"
  host="${entry#*=}"
  store="$OUT/nodes/$node.p12"
  case "$host" in
    *[!0-9.]*) san="dns:$host" ;;
    *) san="ip:$host" ;;
  esac
  ks -genkeypair -alias grid -keyalg RSA -keysize 2048 -dname "CN=$host" -validity "$DAYS" -keypass "$PW" -keystore "$store" 2>/dev/null
  ks -certreq -alias grid -keystore "$store" -file "$OUT/$node.csr" 2>/dev/null
  ks -gencert -alias ca -keystore "$OUT/ca.p12" -infile "$OUT/$node.csr" -outfile "$OUT/$node.crt" \
    -ext "SAN=$san" -validity "$DAYS" 2>/dev/null
  ks -importcert -noprompt -alias ca -keystore "$store" -file "$OUT/ca.crt" 2>/dev/null
  ks -importcert -noprompt -alias grid -keystore "$store" -file "$OUT/$node.crt" 2>/dev/null
  rm -f "$OUT/$node.csr" "$OUT/$node.crt"
  echo "certificate for $node ($host)"
done
rm -f "$OUT/ca.crt"
{
  echo "GALAXY_GRID_TOKEN=$TOKEN"
  echo "GALAXY_TLS_PASSWORD=$PW"
} >"$OUT/grid.env"
echo "secrets written to $OUT"
