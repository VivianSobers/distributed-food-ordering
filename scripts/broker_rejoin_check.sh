#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../kafka/common.sh"
[[ "$#" -eq 0 ]] || die "Usage: broker_rejoin_check.sh"
require_ip VM2_IP
require_ip VM3_IP
cd -- "$REPO_ROOT"
python_bin="${FOODPIPE_PYTHON:-.venv/bin/python}"
[[ -x "$python_bin" ]] || die "Run python3.10 -m venv .venv and install requirements.txt first"

work_dir="$(mktemp -d)"
trap 'rm -rf -- "$work_dir"' EXIT
step='waiting for both brokers in the ISR on every partition'
trap 'step_status=$?; printf "FAIL: %s (exit %s)\n" "$step" "$step_status" >&2; exit "$step_status"' ERR

deadline=$((SECONDS + ${FOODPIPE_REJOIN_WAIT:-120}))
while true; do
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || die "Timed out waiting for both brokers in the ISR on every partition"
    state="$(timeout "$remaining" bash "$REPO_ROOT/kafka/describe_topics.sh")" || state=
    if awk -v expected="$((TOPIC_PARTITIONS * 2))" -v orders="$TOPIC_ORDERS" -v metrics="$TOPIC_METRICS" '
        /Partition:/ {
            topic = partition = isr = ""
            for (i=1; i<=NF; i++) {
                if ($i=="Topic:") topic=$(i+1)
                if ($i=="Partition:") partition=$(i+1)
                if ($i=="Isr:") isr=$(i+1)
            }
            if (topic!=orders && topic!=metrics) next
            seen[topic SUBSEP partition]=1
            if (split(isr, members, ",") != 2) bad=1
        }
        END { for (key in seen) count++; exit !(count==expected && !bad) }
    ' <<< "$state"; then
        break
    fi
    printf 'Waiting for VM3 to rejoin the ISR on every partition\n'
    sleep 1
done
step='post-rejoin topic description'
bash "$REPO_ROOT/kafka/describe_topics.sh"

result=0
printf '\nTopics after broker VM3 rejoined\n'
for topic in "$TOPIC_ORDERS" "$TOPIC_METRICS"; do
    step="post-rejoin consumption of $topic"
    "$python_bin" -m foodpipe.consumer --topic "$topic" --out "$work_dir/$topic.jsonl" \
        --group "rejoin-$topic-$(date +%s)-$$" --bootstrap "$BOOTSTRAP" \
        --idle-timeout "${FOODPIPE_IDLE_TIMEOUT:-15}" --max-wait "${FOODPIPE_MAX_WAIT:-300}"
    count="$(wc -l < "$work_dir/$topic.jsonl")"
    if (( count > 0 )); then
        printf 'PASS: %s readable after rejoin, records=%s\n' "$topic" "$count"
    else
        printf 'FAIL: %s readable after rejoin, records=%s\n' "$topic" "$count"
        result=1
    fi
done
exit "$result"
