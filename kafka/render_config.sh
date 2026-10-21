#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

[[ "$#" -eq 1 ]] || die "Usage: render_config.sh <vm2|vm3>"
role="$1"
require_role "$role"
require_ip VM2_IP

case "$role" in
    vm2)
        require_ip VM2_IP
        process_roles=broker,controller
        node_id=1
        listeners="PLAINTEXT://0.0.0.0:$VM2_KAFKA_PORT,CONTROLLER://0.0.0.0:$CONTROLLER_PORT"
        advertised_listeners="PLAINTEXT://$VM2_IP:$VM2_KAFKA_PORT"
        ;;
    vm3)
        require_ip VM3_IP
        process_roles=broker
        node_id=2
        listeners="PLAINTEXT://0.0.0.0:$VM3_KAFKA_PORT"
        advertised_listeners="PLAINTEXT://$VM3_IP:$VM3_KAFKA_PORT"
        ;;
esac

rendered="$(cat -- "$REPO_ROOT/kafka/server.properties.template")"
replace_token() {
    rendered="${rendered//"@$1@"/"$2"}"
}
replace_token PROCESS_ROLES "$process_roles"
replace_token NODE_ID "$node_id"
replace_token CONTROLLER_QUORUM_VOTERS "1@$VM2_IP:$CONTROLLER_PORT"
replace_token LISTENERS "$listeners"
replace_token ADVERTISED_LISTENERS "$advertised_listeners"
replace_token LOG_DIRS "$(role_data_dir "$role")"

config_path="$(role_config_path "$role")"
mkdir -p -- "$(dirname -- "$config_path")"
printf '%s\n' "$rendered" > "$config_path"
printf '%s\n' "$config_path"
