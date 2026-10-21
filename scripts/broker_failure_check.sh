#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
[[ "$#" -eq 0 || ( "$#" -eq 1 && "$1" == --no-wait ) ]] ||
    die "Usage: broker_failure_check.sh [--no-wait]"
require_ip VM2_IP
require_ip VM3_IP
cd -- "$REPO_ROOT"
python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Run python3.10 -m venv .venv and install requirements.txt first"

work_dir="$(mktemp -d)"
trap 'rm -rf -- "$work_dir"' EXIT
step='initial topic description'
trap 'step_status=$?; printf "FAIL: %s (exit %s)\n" "$step" "$step_status" >&2; exit "$step_status"' ERR
declare -A before after

bash "$REPO_ROOT/kafka/describe_topics.sh"
for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    step="before-failure consumption of $topic"
    "$python_bin" -m foodpipe.consumer --topic "$topic" --out "$work_dir/before-$topic.jsonl" \
        --group "failure-before-$topic-$(date +%s)-$$" --bootstrap "$BOOTSTRAP" \
        --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" --max-wait "${FOODPIPE_MAX_WAIT:-300}"
    before["$topic"]="$(wc -l < "$work_dir/before-$topic.jsonl")"
done

if [[ "${1:-}" != --no-wait ]]; then
    step='VM3 stop confirmation'
    printf '\n============================================================\n'
    printf 'Now stop the broker on VM3: bash kafka/stop_broker.sh vm3 (on VM3) - then press Enter here\n'
    printf '============================================================\n'
    if { true < /dev/tty; } 2>/dev/null; then
        read -r < /dev/tty
    else
        read -r
    fi
fi

step='waiting for leader 1 and ISR exactly 1 on every partition'
deadline=$((SECONDS + 60))
while true; do
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || die "Timed out waiting for leader 1 and ISR exactly 1 on every partition"
    state="$(timeout "$remaining" bash "$REPO_ROOT/kafka/describe_topics.sh")" || state=
    if awk -v expected="$((TOPIC_PARTITIONS * 2))" -v orders="$TOPIC_ORDERS" -v metrics="$TOPIC_METRICS" '
        /Partition:/ {
            topic = partition = leader = isr = ""
            for (i=1; i<=NF; i++) {
                if ($i=="Topic:") topic=$(i+1)
                if ($i=="Partition:") partition=$(i+1)
                if ($i=="Leader:") leader=$(i+1)
                if ($i=="Isr:") isr=$(i+1)
            }
            if (topic!=orders && topic!=metrics) next
            seen[topic SUBSEP partition]=1
            if (leader!="1" || isr!="1") bad=1
        }
        END { for (key in seen) count++; exit !(count==expected && !bad) }
    ' <<< "$state"; then
        break
    fi
    printf 'Waiting for leader 1 and ISR exactly 1 on every partition\n'
    sleep 1
done
step='post-failure topic description'
bash "$REPO_ROOT/kafka/describe_topics.sh"

for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    step="after-failure consumption of $topic"
    "$python_bin" -m foodpipe.consumer --topic "$topic" --out "$work_dir/after-$topic.jsonl" \
        --group "failure-after-$topic-$(date +%s)-$$" --bootstrap "$BOOTSTRAP" \
        --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" --max-wait "${FOODPIPE_MAX_WAIT:-300}"
    after["$topic"]="$(wc -l < "$work_dir/after-$topic.jsonl")"
done

result=0
printf '\nTopic counts before and after broker VM3 stopped\n'
for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    if [[ "${before[$topic]}" -eq "${after[$topic]}" ]]; then
        printf 'PASS: %s before=%s after=%s\n' "$topic" "${before[$topic]}" "${after[$topic]}"
    else
        printf 'FAIL: %s before=%s after=%s\n' "$topic" "${before[$topic]}" "${after[$topic]}"
        result=1
    fi
done
exit "$result"
