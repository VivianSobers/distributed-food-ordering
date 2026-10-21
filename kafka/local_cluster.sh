#!/usr/bin/env bash
set -euo pipefail

export FOODPIPE_ENV=cluster.local
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
[[ "$#" -eq 1 ]] || die "Usage: local_cluster.sh up|down|status"

case "$1" in
    up)
        [[ -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]] ||
            die "Kafka missing at $KAFKA_HOME; run FOODPIPE_ENV=cluster.local bash kafka/install_kafka.sh"
        cd -- "$REPO_ROOT"
        for role in vm2 vm3; do
            bash "$REPO_ROOT/kafka/format_storage.sh" "$role"
        done
        for role in vm2 vm3; do
            bash "$REPO_ROOT/kafka/start_broker.sh" "$role"
        done
        deadline=$((SECONDS + 60))
        broker_id_pattern='\(id:[[:space:]]*([0-9]+)[[:space:]]'
        while (( SECONDS < deadline )); do
            remaining=$((deadline - SECONDS))
            broker_output="$(timeout "$remaining" "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
                --bootstrap-server "$BOOTSTRAP")" || true
            broker1=0
            broker2=0
            while IFS= read -r line; do
                if [[ "$line" == *"-> ("* && "$line" =~ $broker_id_pattern ]]; then
                    case "${BASH_REMATCH[1]}" in
                        1) broker1=1 ;;
                        2) broker2=1 ;;
                    esac
                fi
            done <<< "$broker_output"
            if (( broker1 && broker2 )); then
                printf 'Local cluster ready: brokers 1 and 2 registered at %s\n' "$BOOTSTRAP"
                exit 0
            fi
            printf 'Waiting for both rehearsal brokers to register\n'
            sleep 1
        done
        die "Timed out waiting for both rehearsal brokers to register"
        ;;
    down)
        bash "$REPO_ROOT/kafka/stop_broker.sh" vm3
        bash "$REPO_ROOT/kafka/stop_broker.sh" vm2
        ;;
    status)
        for role in vm2 vm3; do
            if pid="$(broker_pid "$role")"; then
                printf '%s rehearsal broker running (pid %s)\n' "$role" "$pid"
            else
                printf '%s rehearsal broker is not running\n' "$role"
            fi
        done
        printf 'Broker list for %s\n' "$BOOTSTRAP"
        "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" || true
        ;;
    *) die "Usage: local_cluster.sh up|down|status" ;;
esac
