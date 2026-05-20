# Kafka Connect worker image for the Iceberg sink engine.
#
# Connect ingests `events` straight into the Iceberg table `connect_db.events`
# (no Java module — the engine is pure configuration: see iceberg-sink.json).
# Maintenance for this table runs as a separate standalone `flink-maintenance`
# MaintenanceJob container.
ARG CONNECT_VERSION=7.8.0
FROM confluentinc/cp-kafka-connect:${CONNECT_VERSION}

# Apache Iceberg sink connector — published to Confluent Hub by the Iceberg
# community. Installs into the Connect plugin path.
ARG ICEBERG_CONNECT_VERSION=1.9.2
RUN confluent-hub install --no-prompt \
    iceberg/iceberg-kafka-connect:${ICEBERG_CONNECT_VERSION}

# The Iceberg JDBC catalog needs a JDBC driver on the *connector's* classpath
# (Connect isolates each plugin in its own classloader). The connector bundle
# ships only the S3/AWS dependencies, so add the Postgres driver to its lib dir.
ARG POSTGRES_VERSION=42.7.4
ADD --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar \
    /usr/share/confluent-hub-components/iceberg-iceberg-kafka-connect/lib/postgresql.jar
