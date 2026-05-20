# Connector Service: Kafka Connect + Custom SMT + Fork Iceberg Connector
FROM confluentinc/cp-kafka-connect-base:8.0.3

# Copy Iceberg connector plugin (fork with snapshot metadata injection)
COPY plugins/iceberg-kafka-connect /usr/share/java/iceberg-kafka-connect

# Copy Custom SMT plugin (CDC transform, version filter, routing)
COPY plugins/custom-smt /usr/share/java/custom-smt

# Plugin discovery path
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

EXPOSE 8083
