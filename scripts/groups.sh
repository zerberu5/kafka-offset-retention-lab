#!/usr/bin/env bash
# Zeigt Offsets/Lag einer Consumer-Group (kafka-consumer-groups.sh --describe im Kafka-Container).
# Usage: scripts/groups.sh [group] [weitere Optionen, z. B. --state | --members]
set -euo pipefail
cd "$(dirname "$0")/.."
# docker-compose (v1, z. B. Docker 19.03) oder docker compose (v2)
if command -v docker-compose >/dev/null 2>&1; then DC=docker-compose; else DC="docker compose"; fi
GROUP="${1:-${KAFKA_GROUP_ID:-demo-group}}"
shift || true
$DC exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group "$GROUP" "$@"
