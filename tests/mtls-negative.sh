#!/usr/bin/env bash
# mTLS negative test.
#
# The broker runs with ssl.client.auth=required, so a client that presents no
# client certificate must be rejected — while a client with a valid
# certificate is accepted. Both checks run against a freshly started broker.
set -uo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.yml)

cleanup() { "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

if [ ! -f certs/truststore.p12 ]; then
  echo "[mtls-negative] ERROR: certs not generated — run certs/generate-certs.sh first" >&2
  exit 2
fi

echo "[mtls-negative] starting Kafka"
"${COMPOSE[@]}" up -d kafka kafka-setup
"${COMPOSE[@]}" wait kafka-setup >/dev/null 2>&1 || true

# Positive control: a client with the full mTLS config must connect.
echo "[mtls-negative] sanity: connecting WITH a client certificate"
if ! "${COMPOSE[@]}" exec -T kafka timeout 40 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --command-config /etc/kafka/secrets/client-ssl.properties --list >/dev/null 2>&1; then
  echo "[mtls-negative] ERROR: a valid mTLS client could not connect — stack is broken" >&2
  exit 2
fi
echo "[mtls-negative] sanity OK — valid client accepted"

# Negative: a client that trusts the broker but presents NO client certificate.
echo "[mtls-negative] connecting WITHOUT a client certificate (must be rejected)"
"${COMPOSE[@]}" exec -T kafka bash -c '
  cat > /tmp/no-cert.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=changeit
ssl.truststore.type=PKCS12
ssl.endpoint.identification.algorithm=
EOF
  timeout 40 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --command-config /tmp/no-cert.properties --list' >/dev/null 2>&1
result=$?

if [ "$result" -eq 0 ]; then
  echo "[mtls-negative] FAIL: broker accepted a client with no certificate" >&2
  exit 1
fi
echo "[mtls-negative] PASS: broker rejected the no-certificate client"
