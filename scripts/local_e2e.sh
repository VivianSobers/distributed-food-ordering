#!/usr/bin/env bash
set -euo pipefail

export FOODPIPE_ENV=cluster.local
source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
[[ "$#" -eq 0 || ( "$#" -eq 1 && "$1" == --down ) ]] ||
    die "Usage: local_e2e.sh [--down]"
down=0
[[ "${1:-}" != --down ]] || down=1
cd -- "$REPO_ROOT"
python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Run python3.10 -m venv .venv and install requirements.txt first"

work_dir="$(mktemp -d)"
vm4_pid=
cluster_started=0
vm3_stopped=0
check='initialisation'
cleanup() {
    local result="$?"
    trap - EXIT
    if [[ -n "$vm4_pid" ]]; then
        kill -TERM -- "-$vm4_pid" 2>/dev/null || true
        wait "$vm4_pid" 2>/dev/null || true
    fi
    if (( down && cluster_started )); then
        if ! bash "$REPO_ROOT/kafka/local_cluster.sh" down; then
            printf 'FAIL: rehearsal cleanup shutdown\n' >&2
            result=1
        fi
    elif (( vm3_stopped )); then
        if ! bash "$REPO_ROOT/kafka/start_broker.sh" vm3; then
            printf 'FAIL: restoring rehearsal broker vm3\n' >&2
            result=1
        fi
    fi
    rm -rf -- "$work_dir"
    exit "$result"
}
trap cleanup EXIT
trap 'check_status=$?; printf "FAIL: %s (exit %s)\n" "$check" "$check_status" >&2; exit "$check_status"' ERR

check='rehearsal cluster up'
cluster_started=1
bash "$REPO_ROOT/kafka/local_cluster.sh" up
printf 'PASS: rehearsal cluster up\n'
check='topic reset'
bash "$REPO_ROOT/kafka/reset_topics.sh"
printf 'PASS: topics reset\n'

check='VM4 background enrichment'
setsid bash "$REPO_ROOT/scripts/vm4_stage1.sh" > "$work_dir/vm4.log" 2>&1 &
vm4_pid="$!"
check='VM1 orders producer'
bash "$REPO_ROOT/scripts/vm1_stage1_produce.sh"
printf 'PASS: VM1 orders producer\n'
check='VM4 enrichment stage'
if wait "$vm4_pid"; then
    vm4_pid=
    cat "$work_dir/vm4.log"
else
    vm4_status="$?"
    vm4_pid=
    cat "$work_dir/vm4.log" >&2
    printf 'FAIL: VM4 enrichment stage (exit %s)\n' "$vm4_status" >&2
    exit "$vm4_status"
fi
printf 'PASS: VM4 enrichment stage\n'
check='VM1 analytics stage'
bash "$REPO_ROOT/scripts/vm1_stage2.sh"
printf 'PASS: VM1 analytics stage\n'

check='direct Job 1 reference run'
"$python_bin" -m foodpipe.job1_enrichment --input "data/order.jsonl" --output "$work_dir/order_metrics.jsonl"
check='direct Job 2 reference run'
"$python_bin" -m foodpipe.job2_analytics --input "$work_dir/order_metrics.jsonl" \
    --restaurant-out "$work_dir/restaurant_summary.csv" --city-out "$work_dir/city_summary.csv"
for output in order_metrics.jsonl restaurant_summary.csv city_summary.csv; do
    check="$output byte comparison"
    cmp -- "$work_dir/$output" "$REPO_ROOT/output/$output"
    printf 'PASS: %s byte-identical\n' "$output"
done

check='broker vm3 failure'
vm3_stopped=1
bash "$REPO_ROOT/kafka/stop_broker.sh" vm3
bash "$REPO_ROOT/kafka/describe_topics.sh"
check='orders consumption with broker vm3 stopped'
"$python_bin" -m foodpipe.consumer --topic "$TOPIC_ORDERS" --out "$work_dir/failure_orders.jsonl" \
    --group "e2e-failure-$(date +%s)-$$" --bootstrap "$BOOTSTRAP" \
    --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" --max-wait "${FOODPIPE_MAX_WAIT:-300}"
expected="$(awk 'NF { count++ } END { print count + 0 }' "data/order.jsonl")"
actual="$(wc -l < "$work_dir/failure_orders.jsonl")"
if [[ "$actual" -ne "$expected" ]]; then
    printf 'FAIL: broker failure recovery: expected %s orders, received %s\n' "$expected" "$actual" >&2
    exit 1
fi
printf 'PASS: broker vm3 stopped; recovered all %s orders\n' "$actual"

if (( down )); then
    check='rehearsal cluster shutdown'
    bash "$REPO_ROOT/kafka/local_cluster.sh" down
    cluster_started=0
    vm3_stopped=0
    printf 'PASS: rehearsal cluster down\n'
else
    check='rehearsal cluster recovery'
    bash "$REPO_ROOT/kafka/start_broker.sh" vm3
    deadline=$((SECONDS + 60))
    while true; do
        state="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe \
            --topic "$TOPIC_ORDERS|$TOPIC_METRICS")"
        if awk '
            /Partition:/ {
                count++
                for (i=1; i<=NF; i++) if ($i=="Isr:" && $(i+1) ~ /^(1,2|2,1)$/) healthy++
            }
            END { exit !(count == 4 && healthy == 4) }
        ' <<< "$state"; then
            break
        fi
        (( SECONDS < deadline )) || die "Timed out restoring both replicas to the in-sync sets"
        sleep 1
    done
    vm3_stopped=0
    printf 'PASS: rehearsal cluster restored healthy\n'
fi
printf 'E2E OK\n'
