#!/usr/bin/env bash
set -euo pipefail

die() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

require_ip() {
    local variable="${1:?IP variable name required}"
    local value="${!variable:-}"
    [[ -n "$value" && "$value" != "CHANGE_ME" ]] ||
        die "$variable is empty or CHANGE_ME; configure its IP address"
}

require_role() {
    case "${1:-}" in
        vm2|vm3) ;;
        *) die "Invalid role '${1:-}'; expected vm2 or vm3" ;;
    esac
}

role_config_path() {
    require_role "${1:-}"
    printf '%s/kafka/generated/%s/server-%s.properties\n' "$REPO_ROOT" "$FOODPIPE_ENV" "$1"
}

role_data_dir() {
    require_role "${1:-}"
    printf '%s/%s\n' "$KAFKA_DATA_ROOT" "$1"
}

role_log_dir() {
    require_role "${1:-}"
    printf '%s/logs-%s\n' "$KAFKA_DATA_ROOT" "$1"
}

broker_pid() {
    local config_pattern character replacement pids
    config_pattern="$(role_config_path "${1:-}")" || return 1
    for character in '\' '.' '[' ']' '(' ')' '{' '}' '*' '+' '?' '^' '$' '|'; do
        replacement="\\$character"
        config_pattern="${config_pattern//"$character"/"$replacement"}"
    done
    config_pattern="(^|[[:space:]])${config_pattern}([[:space:]]|$)"
    pids="$(pgrep -f -- "$config_pattern")" || return 1
    printf '%s\n' "${pids%%$'\n'*}"
}

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
FOODPIPE_ENV="${FOODPIPE_ENV:-cluster}"
[[ -f "$REPO_ROOT/config/$FOODPIPE_ENV.env" ]] ||
    die "Missing config file: $REPO_ROOT/config/$FOODPIPE_ENV.env"
set -a
source "$REPO_ROOT/config/$FOODPIPE_ENV.env"
set +a

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"
KAFKA_DATA_ROOT="${KAFKA_DATA_ROOT:-$HOME/kafka-data}"
case "$KAFKA_DATA_ROOT" in
    /*) ;;
    *) KAFKA_DATA_ROOT="$REPO_ROOT/$KAFKA_DATA_ROOT" ;;
esac
BOOTSTRAP="$VM2_IP:$VM2_KAFKA_PORT,$VM3_IP:$VM3_KAFKA_PORT"

if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=java
fi

foodpipe_java_output="$("$JAVA" -version 2>&1)" ||
    die "Unable to run Java: $JAVA"
if [[ "$foodpipe_java_output" =~ version[[:space:]]+\"([^\"]+)\" ]]; then
    foodpipe_java_version="${BASH_REMATCH[1]}"
else
    die "Unable to determine Java version from: $foodpipe_java_output"
fi
if [[ "$foodpipe_java_version" == 1.* ]]; then
    foodpipe_java_version="${foodpipe_java_version#1.}"
fi
foodpipe_java_major="${foodpipe_java_version%%[._+-]*}"
[[ "$foodpipe_java_major" =~ ^[0-9]+$ ]] ||
    die "Invalid Java version: $foodpipe_java_version"
(( foodpipe_java_major >= 11 )) || die "Java 11 or newer required; found Java $foodpipe_java_major"
if (( foodpipe_java_major != 11 )); then
    printf 'Warning: Java %s detected; the course stack uses Java 11\n' "$foodpipe_java_major" >&2
fi

export REPO_ROOT FOODPIPE_ENV KAFKA_HOME KAFKA_DATA_ROOT BOOTSTRAP JAVA
