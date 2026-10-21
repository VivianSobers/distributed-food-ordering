import os
from pathlib import Path
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON = REPO_ROOT / "kafka" / "common.sh"


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
