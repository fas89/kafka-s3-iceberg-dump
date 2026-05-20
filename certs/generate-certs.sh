#!/usr/bin/env bash
# Generates a local CA and per-principal mTLS material for the Kafka broker and
# every engine.
#
# All keystores are PKCS12 (every engine here is a JVM/librdkafka client, so a
# single format works everywhere). Built with `openssl`; the truststore is built
# with `keytool` run inside a throwaway container (Java will not load an
# openssl `-nokeys` PKCS12 as a trust store), so Docker must be available.
#
# Outputs (next to this script):
#   ca.crt                       CA certificate (PEM)
#   truststore.p12               shared PKCS12 truststore containing the CA
#   <principal>.keystore.p12      PKCS12 keystore per principal
#   creds                        file holding the store password
#   client-ssl.properties        Kafka admin-client SSL config (kafka-topics.sh)
#
# Principals: broker, flink, spark, connect, duckdb, producer.
# Every store uses the password in CERT_STOREPASS (default: changeit).
set -euo pipefail
cd "$(dirname "$0")"

STOREPASS="${CERT_STOREPASS:-changeit}"
VALIDITY=3650
PRINCIPALS=(broker flink spark connect duckdb producer)
# Image used only for its bundled `keytool` (the project's Kafka image).
KEYTOOL_IMAGE="${KEYTOOL_IMAGE:-apache/kafka:4.0.0}"

echo ">> cleaning previous material"
rm -f ./*.p12 ./*.pem ./*.crt ./*.csr ./*.srl ./ca.key ./creds ./client-ssl.properties 2>/dev/null || true

echo ">> creating local CA"
openssl req -new -x509 -nodes -days "$VALIDITY" \
  -keyout ca.key -out ca.crt \
  -subj "/CN=KafkaIceberg-Local-CA/O=KafkaIceberg/C=US"

echo ">> building shared truststore (truststore.p12) via keytool"
rm -f truststore.p12
docker run --rm -v "$PWD":/work -w /work --entrypoint keytool "$KEYTOOL_IMAGE" \
  -importcert -noprompt -alias ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$STOREPASS"

make_keystore() {
  local name="$1"
  echo ">> keystore: ${name}.keystore.p12"
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "${name}.key.pem" -out "${name}.csr" \
    -subj "/CN=${name}/O=KafkaIceberg/C=US"
  openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial \
    -in "${name}.csr" -out "${name}.cert.pem" -days "$VALIDITY" \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:kafka,DNS:localhost,IP:127.0.0.1" "$name")
  openssl pkcs12 -export \
    -inkey "${name}.key.pem" -in "${name}.cert.pem" -certfile ca.crt \
    -name "$name" -out "${name}.keystore.p12" -passout "pass:${STOREPASS}"
  rm -f "${name}.csr" "${name}.key.pem" "${name}.cert.pem"
}

for p in "${PRINCIPALS[@]}"; do make_keystore "$p"; done

echo ">> writing keystore credentials file (creds)"
printf '%s' "$STOREPASS" > creds

echo ">> writing kafka admin-client SSL config (client-ssl.properties)"
cat > client-ssl.properties <<EOF
security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/broker.keystore.p12
ssl.keystore.password=${STOREPASS}
ssl.key.password=${STOREPASS}
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=${STOREPASS}
ssl.endpoint.identification.algorithm=
EOF

echo
echo ">> done. store type: PKCS12   store password: ${STOREPASS}"
ls -1 ./*.p12 ./creds ./client-ssl.properties
