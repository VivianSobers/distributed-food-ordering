#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
require_ip VM2_IP
require_ip VM3_IP

wait_seconds="${FOODPIPE_TOPIC_DELETE_TIMEOUT:-60}"
[[ "$wait_seconds" =~ ^[0-9]+$ ]] || die "Topic deletion timeout must be a nonnegative integer"

for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    printf 'Deleting topic %s\n' "$topic"
    if ! delete_output="$("$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP" --delete --topic "$topic" 2>&1)"; then
        [[ "$delete_output" == *"does not exist"* ]] || die "$delete_output"
        printf 'Topic %s does not exist; continuing\n' "$topic"
    fi
done

deadline=$((SECONDS + 10#$wait_seconds))
while true; do
    topics="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list)"
    pending=0
    while IFS= read -r listed_topic; do
        if [[ "$listed_topic" == "$TOPIC_ORDERS" || "$listed_topic" == "$TOPIC_METRICS" ]]; then
            pending=1
        fi
    done <<< "$topics"
    [[ "$pending" -eq 0 ]] && break
    (( SECONDS < deadline )) || die "Timed out waiting for topics $TOPIC_ORDERS and $TOPIC_METRICS to disappear"
    printf 'Waiting for topic deletion to finish\n'
    sleep 1
done

printf 'Topics deleted; recreating both topics\n'
bash "$REPO_ROOT/kafka/create_topics.sh"
