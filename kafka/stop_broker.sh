#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

[[ "$#" -eq 1 ]] || die "Usage: stop_broker.sh <vm2|vm3>"
role="$1"
require_role "$role"
if ! pid="$(broker_pid "$role")"; then
    printf '%s broker is not running\n' "$role"
    exit 0
fi

printf 'Stopping %s broker (pid %s) with SIGTERM\n' "$role" "$pid"
if ! kill -TERM -- "$pid"; then
    kill -0 -- "$pid" 2>/dev/null && die "Unable to stop $role broker (pid $pid)"
fi
deadline=$((SECONDS + 30))
while kill -0 -- "$pid" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
        printf '%s broker still alive after 30 seconds; sending SIGKILL (pid %s)\n' "$role" "$pid"
        kill -KILL -- "$pid"
        break
    fi
    printf 'Waiting for %s broker to exit (pid %s)\n' "$role" "$pid"
    sleep 1
done
printf '%s broker stopped (pid %s)\n' "$role" "$pid"
