#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

[[ "$#" -eq 1 ]] || die "Usage: format_storage.sh <vm2|vm3>"
role="$1"
require_role "$role"
config_path="$(bash "$REPO_ROOT/kafka/render_config.sh" "$role")"
data_dir="$(role_data_dir "$role")"
metadata_path="$data_dir/meta.properties"

if [[ -f "$metadata_path" ]]; then
    stored_cluster_id="$(awk -F= '$1 == "cluster.id" { sub(/\r$/, "", $2); print $2; exit }' "$metadata_path")"
    if [[ "$stored_cluster_id" != "$CLUSTER_ID" ]]; then
        die "Cluster ID mismatch in $metadata_path: found '$stored_cluster_id', expected '$CLUSTER_ID'. To wipe deliberately, stop the broker, remove the data directory '$data_dir', then rerun this script."
    fi
    printf 'already formatted (cluster id %s)\n' "$stored_cluster_id"
    exit 0
fi

"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$config_path"
