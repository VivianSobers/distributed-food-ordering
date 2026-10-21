#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
require_ip VM2_IP
require_ip VM3_IP
cd -- "$REPO_ROOT"

python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Python interpreter missing at $python_bin; set up the venv with: python3.10 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"

step=consumer
trap 'step_status=$?; printf "VM4 stage 1: %s failed (exit %s)\n" "$step" "$step_status" >&2; exit "$step_status"' ERR

printf 'VM4 stage 1: consuming %s to output/orders_landed.jsonl\n' "$TOPIC_ORDERS"
"$python_bin" -m foodpipe.consumer --topic "$TOPIC_ORDERS" \
    --out "output/orders_landed.jsonl" --group "vm4-orders-$(date +%s)" \
    --bootstrap "$BOOTSTRAP" --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" \
    --max-wait "${FOODPIPE_MAX_WAIT:-300}"

step='Job 1'
printf 'VM4 stage 1: enriching output/orders_landed.jsonl\n'
"$python_bin" -m foodpipe.job1_enrichment --input "output/orders_landed.jsonl" \
    --output "output/order_metrics.jsonl"

step=producer
printf 'VM4 stage 1: producing output/order_metrics.jsonl to %s\n' "$TOPIC_METRICS"
"$python_bin" -m foodpipe.producer --topic "$TOPIC_METRICS" \
    --file "output/order_metrics.jsonl" --bootstrap "$BOOTSTRAP"
