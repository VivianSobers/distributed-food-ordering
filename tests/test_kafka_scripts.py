import os
from pathlib import Path
import re
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON = REPO_ROOT / "kafka" / "common.sh"
INSTALL = REPO_ROOT / "kafka" / "install_kafka.sh"


def run_common(tmp_path, script, profile="cluster.local", **overrides):
    env = os.environ.copy()
    for key in ("FOODPIPE_ENV", "KAFKA_HOME", "KAFKA_DATA_ROOT"):
        env.pop(key, None)
    if profile is not None:
        env["FOODPIPE_ENV"] = profile
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = str(value)
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", 'source "$1"\n' + script,
         "test-common", str(COMMON)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10,
    )


def test_common_bash_syntax():
    result = subprocess.run(["bash", "-n", str(COMMON)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_common_local_settings_and_role_paths(tmp_path):
    result = run_common(tmp_path, '''
printf '%s\n' "$REPO_ROOT" "$FOODPIPE_ENV" "$BOOTSTRAP" "$KAFKA_DATA_ROOT"
role_config_path vm3
role_data_dir vm2
role_log_dir vm2
bash -c 'printf "%s\\n" "$VM2_IP"'
''')
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        str(REPO_ROOT), "cluster.local", "127.0.0.1:19092,127.0.0.1:19094",
        str(REPO_ROOT / ".kafka-local"),
        str(REPO_ROOT / "kafka/generated/cluster.local/server-vm3.properties"),
        str(REPO_ROOT / ".kafka-local/vm2"),
        str(REPO_ROOT / ".kafka-local/logs-vm2"), "127.0.0.1",
    ]


def test_common_real_defaults(tmp_path):
    result = run_common(tmp_path, '''
printf '%s\n' "$FOODPIPE_ENV" "$KAFKA_HOME" "$KAFKA_DATA_ROOT"
role_data_dir vm2
role_log_dir vm2
role_config_path vm2
''', profile=None)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "cluster", str(Path.home() / "kafka_2.12-3.9.2"), str(Path.home() / "kafka-data"),
        str(Path.home() / "kafka-data/vm2"), str(Path.home() / "kafka-data/logs-vm2"),
        str(REPO_ROOT / "kafka/generated/cluster/server-vm2.properties"),
    ]


@pytest.mark.parametrize("data_root", ["relative data", "/tmp/absolute data"])
def test_common_resolves_data_root_and_honours_kafka_home(tmp_path, data_root):
    result = run_common(
        tmp_path, 'printf "%s\\n" "$KAFKA_DATA_ROOT" "$KAFKA_HOME"',
        profile="cluster", KAFKA_DATA_ROOT=data_root, KAFKA_HOME="/tmp/kafka home",
    )
    assert result.returncode == 0, result.stderr
    expected = data_root if data_root.startswith("/") else str(REPO_ROOT / data_root)
    assert result.stdout.splitlines() == [expected, "/tmp/kafka home"]


@pytest.mark.parametrize("script, succeeds, message", [
    ("require_ip VM1_IP", False, "CHANGE_ME"),
    ("require_ip VM2_IP", True, ""),
    ("VM2_IP=; require_ip VM2_IP", False, "VM2_IP"),
    ("unset VM2_IP; require_ip VM2_IP", False, "VM2_IP"),
    ("require_role vm1", False, "vm1"),
    ("require_role vm2; require_role vm3", True, ""),
    ("role_config_path vm1", False, "vm1"),
    ("role_data_dir vm1", False, "vm1"),
    ("role_log_dir vm1", False, "vm1"),
])
def test_common_validates_ips_and_roles(tmp_path, script, succeeds, message):
    result = run_common(tmp_path, script, profile="cluster")
    assert (result.returncode == 0) is succeeds
    assert message in result.stderr


@pytest.mark.parametrize("version, succeeds, warning", [
    ("1.8.0_402", False, False), ("11.0.32", True, False), ("21.0.8", True, True),
])
def test_common_java_version_check(tmp_path, version, succeeds, warning):
    java_home = tmp_path / "java home"
    java = java_home / "bin" / "java"
    java.parent.mkdir(parents=True)
    java.write_text(f'#!/bin/sh\nprintf \'openjdk version "{version}"\\n\' >&2\n')
    java.chmod(0o755)
    result = run_common(tmp_path, 'printf "%s\\n" "$JAVA"', JAVA_HOME=java_home)
    assert (result.returncode == 0) is succeeds
    if succeeds:
        assert result.stdout.strip() == str(java)
    else:
        assert "11" in result.stderr
    assert ("warning" in result.stderr.lower()) is warning


def test_common_java_from_path_without_java_home(tmp_path):
    java = tmp_path / "java"
    java.write_text('#!/bin/sh\nprintf \'openjdk version "11.0.32"\\n\' >&2\n')
    java.chmod(0o755)
    result = run_common(
        tmp_path, 'printf "%s\\n" "$JAVA"', JAVA_HOME=None,
        PATH=str(tmp_path) + os.pathsep + os.environ["PATH"],
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "java"


def test_install_bash_syntax():
    result = subprocess.run(["bash", "-n", str(INSTALL)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_install_already_installed_skips_download(tmp_path):
    kafka_home = tmp_path / "kafka home"
    start = kafka_home / "bin" / "kafka-server-start.sh"
    start.parent.mkdir(parents=True)
    start.touch()
    curl = tmp_path / "curl"
    curl.write_text('#!/bin/sh\ntouch "$DOWNLOAD_MARKER"\nexit 99\n')
    curl.chmod(0o755)
    marker = tmp_path / "download attempted"
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/install_kafka.sh"',
        KAFKA_HOME=kafka_home, DOWNLOAD_MARKER=marker,
        PATH=str(tmp_path) + os.pathsep + os.environ["PATH"],
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"Kafka already installed at {kafka_home}"
    assert not marker.exists()


def test_sha512_from_apache_file_parses_spaced_uppercase(tmp_path):
    checksum = "ABCDEF0123456789" * 8
    path = tmp_path / "archive.tgz.sha512"
    groups = [checksum[i:i + 16] for i in range(0, len(checksum), 16)]
    path.write_text(
        "kafka_2.12-3.9.2.tgz: " + " \n  ".join(groups) + " \n", encoding="ascii",
    )
    result = run_common(tmp_path, '''
source "$REPO_ROOT/kafka/install_kafka.sh"
sha512_from_apache_file "$CHECKSUM_FILE"
''', CHECKSUM_FILE=path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == checksum.lower()


def test_sha512_from_apache_file_rejects_invalid_checksum(tmp_path):
    path = tmp_path / "archive.tgz.sha512"
    path.write_text("archive.tgz: not-a-checksum\n", encoding="ascii")
    result = run_common(tmp_path, '''
source "$REPO_ROOT/kafka/install_kafka.sh"
sha512_from_apache_file "$CHECKSUM_FILE"
''', CHECKSUM_FILE=path)
    assert result.returncode != 0
    assert "checksum" in result.stderr.lower()


def test_broker_template_placeholders_and_fixed_properties():
    template = (REPO_ROOT / "kafka/server.properties.template").read_text(encoding="utf-8")
    placeholders = {
        "process.roles": "@PROCESS_ROLES@",
        "node.id": "@NODE_ID@",
        "controller.quorum.voters": "@CONTROLLER_QUORUM_VOTERS@",
        "listeners": "@LISTENERS@",
        "advertised.listeners": "@ADVERTISED_LISTENERS@",
        "log.dirs": "@LOG_DIRS@",
    }
    assert sorted(re.findall(r"@[A-Z_]+@", template)) == sorted(placeholders.values())
    lines = [line for line in template.splitlines() if line and not line.startswith("#")]
    expected = {
        **placeholders,
        "controller.listener.names": "CONTROLLER",
        "inter.broker.listener.name": "PLAINTEXT",
        "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "num.partitions": "2",
        "default.replication.factor": "2",
        "offsets.topic.replication.factor": "2",
        "transaction.state.log.replication.factor": "2",
        "transaction.state.log.min.isr": "1",
        "min.insync.replicas": "1",
        "auto.create.topics.enable": "false",
    }
    assert sorted(lines) == sorted(f"{key}={value}" for key, value in expected.items())
