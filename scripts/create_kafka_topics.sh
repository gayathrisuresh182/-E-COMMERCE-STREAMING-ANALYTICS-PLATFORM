#!/usr/bin/env bash
# Create Kafka topics for E-Commerce Streaming Analytics (Phase 1E)
# Run after: docker-compose up -d
# Usage: ./scripts/create_kafka_topics.sh   OR   bash scripts/create_kafka_topics.sh

set -e
KAFKA_BOOTSTRAP="localhost:9092"

echo "Waiting for Kafka to be ready..."
sleep 10

# Create topics with partitions and retention
# Replication factor 1 for local/single-broker

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic orders-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic clickstream \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic payments-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic shipments-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic deliveries-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic reviews-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic experiments-stream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete

echo "Listing topics..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo "Done. Topics created with intended partitions and retention."
