#!/bin/sh
# Creates the topics and grants each service only what it uses. Safe to re-run.
# The broker denies anything not granted here (allow.everyone.if.no.acl.found=false),
# and only admin, used by this script, is a superuser.
set -eu

cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF
B="--bootstrap-server kafka:9092 --command-config /tmp/admin.properties"

for topic in transactions clickstream inventory_updates; do
    kafka-topics $B --create --if-not-exists --topic "$topic" --partitions 5 --replication-factor 1
done

acl() { kafka-acls $B --add --force "$@" > /dev/null; }
SOURCES="--topic transactions --topic clickstream --topic inventory_updates"

# Simulator: writes the three source topics
acl --allow-principal User:streamcart_producer --operation Write --operation Describe $SOURCES
acl --allow-principal User:streamcart_producer --operation IdempotentWrite --cluster

# Bronze stream: reads the three source topics. Spark names its consumer groups itself.
acl --allow-principal User:streamcart_spark --operation Read --operation Describe $SOURCES
acl --allow-principal User:streamcart_spark --operation Read --resource-pattern-type prefixed --group spark-kafka-source-

# Serving sink: reads orders, stock and the two alert topics, as group realtime-sink
acl --allow-principal User:streamcart_sink --operation Read --operation Describe \
    --topic transactions --topic inventory_updates --topic fraud_alerts --topic restock_required
acl --allow-principal User:streamcart_sink --operation Read --group realtime-sink

# ksqlDB: reads orders and stock, owns its output topics and its own internal topics,
# groups and transactions (all prefixed with its service ID, streamcart_)
# (DescribeConfigs: ksqlDB checks a source topic's settings when a stream is created over it)
acl --allow-principal User:streamcart_ksql --operation Read --operation Describe --operation DescribeConfigs \
    --topic transactions --topic inventory_updates
acl --allow-principal User:streamcart_ksql --operation All \
    --topic TRANSACTIONS_LIVE --topic fraud_alerts --topic user_velocity_1h --topic restock_required
acl --allow-principal User:streamcart_ksql --operation All --resource-pattern-type prefixed \
    --topic _confluent-ksql-streamcart_ --topic streamcart_ --group _confluent-ksql-streamcart_ \
    --transactional-id streamcart_
acl --allow-principal User:streamcart_ksql --operation Describe --operation DescribeConfigs \
    --operation IdempotentWrite --cluster

# Schema Registry: stores schemas in _schemas, and elects its leader through a group
acl --allow-principal User:streamcart_registry --operation All --topic _schemas
acl --allow-principal User:streamcart_registry --operation All --resource-pattern-type prefixed --group schema-registry
acl --allow-principal User:streamcart_registry --operation Describe --operation DescribeConfigs --cluster

echo "Kafka topics and ACLs ready."
