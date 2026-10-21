#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
require_ip VM2_IP
require_ip VM3_IP

printf 'Topic status at %s | bootstrap %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$BOOTSTRAP"
descriptions=
for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    output="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
        --describe --topic "$topic")"
    printf '%s\n' "$output"
    descriptions+="$output"$'\n'
done

awk '
{
    topic = partition = leader = replicas = isr = ""
    for (i = 1; i <= NF; i++) {
        if ($i == "Topic:") topic = $(i + 1)
        if ($i == "Partition:") partition = $(i + 1)
        if ($i == "Leader:") leader = $(i + 1)
        if ($i == "Replicas:") replicas = $(i + 1)
        if ($i == "Isr:" && $(i + 1) ~ /^[0-9]+(,[0-9]+)*$/) isr = $(i + 1)
    }
    if (partition == "") next
    printf "%s p%s: leader %s, replicas %s, in sync %s\n", topic, partition, leader, replicas, (isr == "" ? "none" : isr)
    replica_count = split(replicas, replica_ids, ",")
    sync_count = (isr == "" ? 0 : split(isr, sync_ids, ","))
    if (sync_count < replica_count) {
        warnings[++warning_count] = sprintf("WARNING: %s p%s: %d of %d replicas in sync", topic, partition, sync_count, replica_count)
    }
}
END {
    for (i = 1; i <= warning_count; i++) print warnings[i]
}
' <<< "$descriptions"
