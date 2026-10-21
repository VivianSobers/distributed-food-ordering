#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

sha512_from_apache_file() {
    local checksum
    checksum="$(tr -d '[:space:]' < "$1")"
    [[ "$checksum" == *:* ]] || die "Missing checksum label in $1"
    checksum="${checksum#*:}"
    checksum="${checksum,,}"
    [[ "$checksum" =~ ^[0-9a-f]{128}$ ]] || die "Invalid SHA-512 checksum in $1"
    printf '%s\n' "$checksum"
}

install_kafka() (
    if [[ -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
        printf 'Kafka already installed at %s\n' "$KAFKA_HOME"
        exit 0
    fi

    download_dir="$(mktemp -d)"
    trap 'rm -rf -- "$download_dir"' EXIT
    archive_name="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    archive_url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${archive_name}"
    archive_path="$download_dir/$archive_name"
    checksum_path="$archive_path.sha512"

    curl -fL --retry 3 -C - -o "$archive_path" "$archive_url"
    curl -fL --retry 3 -C - -o "$checksum_path" "$archive_url.sha512"
    checksum="$(sha512_from_apache_file "$checksum_path")"
    printf '%s  %s\n' "$checksum" "$archive_path" | sha512sum -c -

    install_parent="$(dirname -- "$KAFKA_HOME")"
    mkdir -p -- "$install_parent"
    tar -xzf "$archive_path" -C "$install_parent"
    extracted_dir="$install_parent/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
    if [[ "$extracted_dir" != "$KAFKA_HOME" ]]; then
        mv -T -- "$extracted_dir" "$KAFKA_HOME"
    fi
    "$KAFKA_HOME/bin/kafka-topics.sh" --version
)

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    install_kafka
fi
