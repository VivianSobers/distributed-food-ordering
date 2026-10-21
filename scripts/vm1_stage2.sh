#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
require_ip VM2_IP
require_ip VM3_IP
cd -- "$REPO_ROOT"

python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Python interpreter missing at $python_bin; set up the venv with: python3.10 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"

step=consumer
trap 'step_status=$?; printf "VM1 stage 2: %s failed (exit %s)\n" "$step" "$step_status" >&2; exit "$step_status"' ERR

printf 'VM1 stage 2: consuming %s to output/order_metrics_landed.jsonl\n' "$TOPIC_METRICS"
"$python_bin" -m foodpipe.consumer --topic "$TOPIC_METRICS" \
    --out "output/order_metrics_landed.jsonl" --group "vm1-metrics-$(date +%s)" \
    --bootstrap "$BOOTSTRAP" --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" \
    --max-wait "${FOODPIPE_MAX_WAIT:-300}"

step='Job 2'
printf 'VM1 stage 2: analysing output/order_metrics_landed.jsonl\n'
"$python_bin" -m foodpipe.job2_analytics --input "output/order_metrics_landed.jsonl" \
    --restaurant-out "output/restaurant_summary.csv" --city-out "output/city_summary.csv"

for summary in restaurant city; do
    step="$summary CSV display"
    printf '\n%s summary\n' "${summary^}"
    if command -v column >/dev/null 2>&1; then
        column -s, -t "output/${summary}_summary.csv"
    else
        cat "output/${summary}_summary.csv"
    fi
done
