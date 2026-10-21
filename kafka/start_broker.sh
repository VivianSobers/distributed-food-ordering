#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

[[ "$#" -ge 1 && "$#" -le 2 ]] || die "Usage: start_broker.sh <vm2|vm3> [--foreground]"
role="$1"
require_role "$role"
mode="${2:--daemon}"
[[ "$#" -eq 1 || "$mode" == --foreground ]] ||
    die "Usage: start_broker.sh <vm2|vm3> [--foreground]"
config_path="$(bash "$REPO_ROOT/kafka/render_config.sh" "$role")"
data_dir="$(role_data_dir "$role")"
[[ -f "$data_dir/meta.properties" ]] ||
    die "Missing $data_dir/meta.properties; run bash kafka/format_storage.sh $role first"

if pid="$(broker_pid "$role")"; then
    printf '%s broker already running (pid %s)\n' "$role" "$pid"
    exit 0
fi

log_dir="$(role_log_dir "$role")"
mkdir -p -- "$log_dir"
if [[ "$FOODPIPE_ENV" == cluster.local ]]; then
    # Kafka expands JVM logging options without quotes; use a relative log path.
    cd -- "$REPO_ROOT"
    log_dir="$(realpath --relative-to="$REPO_ROOT" "$log_dir")"
fi
if [[ "$mode" == --foreground ]]; then
    LOG_DIR="$log_dir" "$KAFKA_HOME/bin/kafka-server-start.sh" "$config_path" &
    launcher_pid="$!"
else
    LOG_DIR="$log_dir" "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$config_path"
fi

case "$role" in
    vm2) port="$VM2_KAFKA_PORT" ;;
    vm3) port="$VM3_KAFKA_PORT" ;;
esac
if [[ "${FOODPIPE_SKIP_PORT_WAIT:-0}" != 1 ]]; then
    deadline=$((SECONDS + 60))
    until (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
        if (( SECONDS >= deadline )); then
            printf '%s broker timed out waiting for port %s\n' "$role" "$port" >&2
            if [[ -f "$log_dir/server.log" ]]; then
                tail -n 20 -- "$log_dir/server.log" >&2
            fi
            exit 1
        fi
        sleep 1
    done
fi
if ! pid="$(broker_pid "$role")"; then
    [[ -n "${launcher_pid:-}" ]] || die "Port $port is open but no broker process uses $config_path"
    pid="$launcher_pid"
fi
printf '%s broker up on port %s (pid %s)\n' "$role" "$port" "$pid"
if [[ "$mode" == --foreground ]]; then
    wait "$launcher_pid"
fi
