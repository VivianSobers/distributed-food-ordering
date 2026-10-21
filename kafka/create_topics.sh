#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
require_ip VM2_IP
require_ip VM3_IP

broker_output="$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP")" || true
broker1=0
broker2=0
broker_id_pattern='\(id:[[:space:]]*([0-9]+)[[:space:]]'
while IFS= read -r line; do
    if [[ "$line" == *"-> ("* && "$line" =~ $broker_id_pattern ]]; then
        case "${BASH_REMATCH[1]}" in
            1) broker1=1 ;;
            2) broker2=1 ;;
        esac
    fi
done <<< "$broker_output"
registered_count=$((broker1 + broker2))
[[ "$registered_count" -eq 2 ]] ||
    die "Only $registered_count of 2 brokers are up; start VM3 first"

for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists --topic "$topic" \
        --partitions "$TOPIC_PARTITIONS" --replication-factor "$TOPIC_RF"
    printf 'Created topic %s\n' "$topic"
done
