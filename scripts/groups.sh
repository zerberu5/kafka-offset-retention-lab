#!/usr/bin/env bash
# Zeigt Offsets/Lag einer Consumer-Group (kafka-consumer-groups.sh --describe im Kafka-Container).
# Usage: scripts/groups.sh [group] [weitere Optionen, z. B. --state | --members]
set -euo pipefail
cd "$(dirname "$0")/.."
GROUP="${1:-${KAFKA_GROUP_ID:-demo-group}}"
shift || true
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group "$GROUP" "$@"
