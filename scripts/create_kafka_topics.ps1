# Create Kafka topics for E-Commerce Streaming Analytics (Phase 1E)
# Run after: docker-compose up -d
# Usage: .\scripts\create_kafka_topics.ps1

$ErrorActionPreference = "Stop"
$KafkaContainer = "kafka"

Write-Host "Waiting for Kafka to be ready..."
Start-Sleep -Seconds 10

$topics = @(
    @{ Name = "orders-stream";     Partitions = 3; RetentionMs = 604800000 },   # 7 days
    @{ Name = "clickstream";       Partitions = 6; RetentionMs = 259200000 },   # 3 days
    @{ Name = "payments-stream";   Partitions = 3; RetentionMs = 604800000 },
    @{ Name = "shipments-stream";  Partitions = 3; RetentionMs = 604800000 },
    @{ Name = "deliveries-stream"; Partitions = 3; RetentionMs = 604800000 },
    @{ Name = "reviews-stream";    Partitions = 3; RetentionMs = 604800000 },
    @{ Name = "experiments-stream"; Partitions = 3; RetentionMs = 2592000000 }  # 30 days
)

foreach ($t in $topics) {
    Write-Host "Creating topic: $($t.Name)"
    docker exec $KafkaContainer kafka-topics --create --if-not-exists `
        --bootstrap-server localhost:9092 `
        --topic $t.Name `
        --partitions $t.Partitions `
        --replication-factor 1 `
        --config retention.ms=$($t.RetentionMs) `
        --config cleanup.policy=delete
}

Write-Host "Listing topics..."
docker exec $KafkaContainer kafka-topics --list --bootstrap-server localhost:9092
Write-Host "Done."
