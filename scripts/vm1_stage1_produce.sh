#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
require_ip VM2_IP
require_ip VM3_IP
cd -- "$REPO_ROOT"

python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Python interpreter missing at $python_bin; set up the venv with: python3.10 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"

printf 'VM1 stage 1: producing data/order.jsonl to %s\n' "$TOPIC_ORDERS"
exec "$python_bin" -m foodpipe.producer --topic "$TOPIC_ORDERS" \
    --file "data/order.jsonl" --bootstrap "$BOOTSTRAP"
