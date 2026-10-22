#!/usr/bin/env bash
# Prints the variables the demo terminal needs; load them with: eval "$(bash scripts/demo_env.sh)"
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
require_ip VM2_IP
[[ -x "$KAFKA_HOME/bin/kafka-topics.sh" ]] ||
    die "Kafka not found in $KAFKA_HOME; run bash kafka/install_kafka.sh"
printf 'export KAFKA_HOME=%q\n' "$KAFKA_HOME"
printf 'export BS=%q\n' "$VM2_IP:$VM2_KAFKA_PORT"
