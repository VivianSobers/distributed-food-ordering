import os
from pathlib import Path
import re
import shutil
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


@pytest.fixture
def generated_configs():
    root = REPO_ROOT / "kafka/generated"
    paths = [root / profile / f"server-{role}.properties"
             for profile in ("cluster", "cluster.local") for role in ("vm2", "vm3")]
    backups = {path: path.read_bytes() if path.exists() else None for path in paths}
    new_dirs = [path for path in (root / "cluster", root / "cluster.local", root)
                if not path.exists()]
    yield root
    for path, original in backups.items():
        if original is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(original)
    for directory in new_dirs:
        if directory.exists():
            directory.rmdir()


def test_render_config_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/render_config.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("role, node, port, roles", [
    ("vm2", "1", "19092", "broker,controller"), ("vm3", "2", "19094", "broker"),
])
def test_render_local_broker_config(tmp_path, generated_configs, role, node, port, roles):
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/render_config.sh" "$TEST_ROLE"', TEST_ROLE=role,
    )
    assert result.returncode == 0, result.stderr
    path = generated_configs / "cluster.local" / f"server-{role}.properties"
    assert result.stdout.strip() == str(path)
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"@[A-Z_]+@", text)
    properties = dict(line.split("=", 1) for line in text.splitlines()
                      if line and not line.startswith("#"))
    assert properties.items() >= {
        "process.roles": roles, "node.id": node,
        "controller.quorum.voters": "1@127.0.0.1:19093",
        "advertised.listeners": f"PLAINTEXT://127.0.0.1:{port}",
        "listeners": f"PLAINTEXT://0.0.0.0:{port}" + (
            ",CONTROLLER://0.0.0.0:19093" if role == "vm2" else ""
        ),
        "log.dirs": str(REPO_ROOT / ".kafka-local" / role),
        "offsets.topic.replication.factor": "2", "min.insync.replicas": "1",
    }.items()


def test_render_real_vm2_config(tmp_path, generated_configs):
    result = run_common(tmp_path, 'bash "$REPO_ROOT/kafka/render_config.sh" vm2', profile="cluster")
    assert result.returncode == 0, result.stderr
    path = generated_configs / "cluster/server-vm2.properties"
    assert result.stdout.strip() == str(path)
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"@[A-Z_]+@", text)
    assert "advertised.listeners=PLAINTEXT://192.168.193.217:9092" in text.splitlines()
    assert "controller.quorum.voters=1@192.168.193.217:9093" in text.splitlines()


def test_render_config_rejects_bad_role(tmp_path, generated_configs):
    result = run_common(tmp_path, 'bash "$REPO_ROOT/kafka/render_config.sh" vm1')
    assert result.returncode != 0
    assert "vm1" in result.stderr


@pytest.fixture
def storage_stub(tmp_path):
    kafka_home = tmp_path / "fake kafka"
    script = kafka_home / "bin/kafka-storage.sh"
    script.parent.mkdir(parents=True)
    script.write_text('#!/bin/bash\nset -euo pipefail\nprintf \'%s\\n\' "$@" > "$STORAGE_ARGS"\n')
    script.chmod(0o755)
    return kafka_home, tmp_path / "storage arguments"


def test_format_storage_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/format_storage.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("role", ["vm2", "vm3"])
@pytest.mark.parametrize("matching", [True, False])
def test_format_storage_preserves_existing_metadata(
    tmp_path, generated_configs, storage_stub, role, matching,
):
    kafka_home, arguments = storage_stub
    data_root = tmp_path / "data root"
    metadata = data_root / role / "meta.properties"
    metadata.parent.mkdir(parents=True)
    cluster_id = "jbLv2nD5Tf6eYZrYdejohg" if matching else "another-cluster"
    original = f"# Existing storage\nversion=1\ncluster.id={cluster_id}\nnode.id=1\n".encode()
    metadata.write_bytes(original)
    before = metadata.stat()
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/format_storage.sh" "$TEST_ROLE"',
        profile="cluster", TEST_ROLE=role, KAFKA_DATA_ROOT=data_root,
        KAFKA_HOME=kafka_home, STORAGE_ARGS=arguments,
    )
    assert metadata.read_bytes() == original
    assert metadata.stat().st_mtime_ns == before.st_mtime_ns
    assert metadata.stat().st_ino == before.st_ino
    assert not arguments.exists()
    if matching:
        assert result.returncode == 0, result.stderr
        assert f"already formatted (cluster id {cluster_id})" in result.stdout
    else:
        assert result.returncode != 0
        assert "cluster" in result.stderr.lower()
        assert "mismatch" in result.stderr.lower()
        assert "wipe" in result.stderr.lower()
        assert str(metadata.parent) in result.stderr


@pytest.mark.parametrize("role", ["vm2", "vm3"])
def test_format_storage_fresh_directory_command(tmp_path, generated_configs, storage_stub, role):
    kafka_home, arguments = storage_stub
    data_root = tmp_path / "fresh data"
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/format_storage.sh" "$TEST_ROLE"',
        profile="cluster", TEST_ROLE=role, KAFKA_DATA_ROOT=data_root,
        KAFKA_HOME=kafka_home, STORAGE_ARGS=arguments,
    )
    assert result.returncode == 0, result.stderr
    config_path = generated_configs / "cluster" / f"server-{role}.properties"
    assert arguments.read_text().splitlines() == [
        "format", "-t", "jbLv2nD5Tf6eYZrYdejohg", "-c", str(config_path),
    ]
    assert f"log.dirs={data_root / role}" in config_path.read_text().splitlines()


@pytest.fixture
def broker_start_stub(tmp_path):
    kafka_home = tmp_path / "fake kafka"
    script = kafka_home / "bin/kafka-server-start.sh"
    script.parent.mkdir(parents=True)
    script.write_text(
        '#!/bin/bash\nset -euo pipefail\n'
        'printf \'%s\\n\' "$@" > "$START_ARGS"\n'
        'printf \'%s\\n\' "$LOG_DIR" > "$START_LOG_DIR"\n'
    )
    script.chmod(0o755)
    pgrep = tmp_path / "pgrep"
    pgrep.write_text(
        '#!/bin/bash\nset -euo pipefail\n'
        'printf \'%s\\n\' "$@" > "$PGREP_ARGS"\n'
        'if [[ "${ALREADY_RUNNING:-0}" == 1 || -f "$START_ARGS" ]]; then\n'
        '  printf \'43210\\n\'\nelse\n  exit 1\nfi\n'
    )
    pgrep.chmod(0o755)
    return {
        "KAFKA_HOME": kafka_home, "KAFKA_DATA_ROOT": tmp_path / "broker data",
        "START_ARGS": tmp_path / "start arguments", "START_LOG_DIR": tmp_path / "log dir",
        "PGREP_ARGS": tmp_path / "pgrep arguments", "FOODPIPE_SKIP_PORT_WAIT": "1",
        "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
    }


def test_start_broker_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/start_broker.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_start_broker_refuses_missing_metadata(tmp_path, generated_configs, broker_start_stub):
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/start_broker.sh" vm2',
        profile="cluster", **broker_start_stub,
    )
    assert result.returncode != 0
    assert "format_storage.sh" in result.stderr
    assert not broker_start_stub["START_ARGS"].exists()


@pytest.mark.parametrize("role, port", [("vm2", "9092"), ("vm3", "9092")])
@pytest.mark.parametrize("foreground", [False, True])
def test_start_broker_command_and_log_dir(
    tmp_path, generated_configs, broker_start_stub, role, port, foreground,
):
    metadata = broker_start_stub["KAFKA_DATA_ROOT"] / role / "meta.properties"
    metadata.parent.mkdir(parents=True)
    metadata.write_text("cluster.id=jbLv2nD5Tf6eYZrYdejohg\n")
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/start_broker.sh" "$TEST_ROLE" $TEST_MODE',
        profile="cluster", TEST_ROLE=role, TEST_MODE="--foreground" if foreground else "",
        **broker_start_stub,
    )
    assert result.returncode == 0, result.stderr
    path = generated_configs / "cluster" / f"server-{role}.properties"
    assert broker_start_stub["START_ARGS"].read_text().splitlines() == (
        [] if foreground else ["-daemon"]
    ) + [str(path)]
    assert broker_start_stub["START_LOG_DIR"].read_text().strip() == str(
        broker_start_stub["KAFKA_DATA_ROOT"] / f"logs-{role}"
    )
    assert re.search(rf"{role} broker up on port {port} \(pid \d+\)", result.stdout)
    pgrep_args = broker_start_stub["PGREP_ARGS"].read_text().splitlines()
    assert pgrep_args[0] == "-f"
    pattern = pgrep_args[-1]
    for command, expected_code in [
        (f"java kafka.Kafka {path}", 0), (f"java kafka.Kafka {path}.other", 1),
    ]:
        match = subprocess.run(
            ["grep", "-E", "-q", "--", pattern], input=command, text=True,
        )
        assert match.returncode == expected_code


def test_start_broker_already_running_skips_start(tmp_path, generated_configs, broker_start_stub):
    metadata = broker_start_stub["KAFKA_DATA_ROOT"] / "vm2/meta.properties"
    metadata.parent.mkdir(parents=True)
    metadata.touch()
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/start_broker.sh" vm2',
        profile="cluster", ALREADY_RUNNING="1", **broker_start_stub,
    )
    assert result.returncode == 0, result.stderr
    assert "43210" in result.stdout
    assert "already running" in result.stdout
    assert not broker_start_stub["START_ARGS"].exists()


@pytest.fixture
def stop_sandbox(tmp_path):
    root = tmp_path / "isolated repo [stop]"
    (root / "kafka").mkdir(parents=True)
    (root / "config").mkdir()
    for name in ("common.sh", "stop_broker.sh"):
        shutil.copyfile(REPO_ROOT / "kafka" / name, root / "kafka" / name)
    shutil.copyfile(REPO_ROOT / "config/cluster.local.env", root / "config/cluster.local.env")
    env = os.environ.copy()
    env["FOODPIPE_ENV"] = "cluster.local"
    return root, env


def test_stop_broker_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/stop_broker.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_stop_broker_not_running(stop_sandbox):
    root, env = stop_sandbox
    result = subprocess.run(
        ["bash", str(root / "kafka/stop_broker.sh"), "vm3"], env=env,
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "vm3 broker is not running"


@pytest.mark.parametrize("other_config", ["suffix", "environment"])
def test_stop_broker_terminates_only_exact_config(stop_sandbox, other_config):
    root, env = stop_sandbox
    config = root / "kafka/generated/cluster.local/server-vm3.properties"
    other = str(config) + ".other" if other_config == "suffix" else str(
        root / "kafka/generated/cluster/server-vm3.properties"
    )
    processes = []
    stopper = None
    try:
        for path in (str(config), other):
            processes.append(subprocess.Popen(
                [f"dummy-broker {path}", "300"], executable=shutil.which("sleep"),
            ))
        target, unrelated = processes
        stopper = subprocess.Popen(
            ["bash", str(root / "kafka/stop_broker.sh"), "vm3"], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        assert target.wait(timeout=5) == -15
        stdout, stderr = stopper.communicate(timeout=5)
        assert stopper.returncode == 0, stderr
        assert f"vm3 broker stopped (pid {target.pid})" in stdout
        assert unrelated.poll() is None
    finally:
        if stopper is not None and stopper.poll() is None:
            stopper.kill()
            stopper.communicate()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait()


@pytest.fixture
def topic_stubs(tmp_path):
    kafka_home = tmp_path / "fake kafka"
    bin_dir = kafka_home / "bin"
    bin_dir.mkdir(parents=True)
    scripts = {
        "kafka-broker-api-versions.sh": (
            'printf \'%s\\n\' "$@" > "$BROKER_ARGS"\n'
            'cat "$BROKER_OUTPUT"\nexit "${BROKER_STATUS:-0}"\n'
        ),
        "kafka-topics.sh": 'printf \'%s\\n\' "$@" >> "$TOPIC_ARGS"\n',
    }
    for name, body in scripts.items():
        path = bin_dir / name
        path.write_text("#!/bin/bash\nset -euo pipefail\n" + body)
        path.chmod(0o755)
    return {
        "KAFKA_HOME": kafka_home, "BROKER_ARGS": tmp_path / "broker args",
        "BROKER_OUTPUT": tmp_path / "broker output", "TOPIC_ARGS": tmp_path / "topic args",
    }


def test_create_topics_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/create_topics.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_create_topics_exact_commands(tmp_path, topic_stubs):
    topic_stubs["BROKER_OUTPUT"].write_text(
        "127.0.0.1:19092 (id: 1 rack: null) -> (\n\tProduce(0): 0 to 11 [usable: 11],\n)\n"
        "127.0.0.1:19094 (id: 2 rack: null) -> (\n\tFetch(1): 0 to 17 [usable: 17]\n)\n"
    )
    result = run_common(tmp_path, 'bash "$REPO_ROOT/kafka/create_topics.sh"', **topic_stubs)
    assert result.returncode == 0, result.stderr
    bootstrap = "127.0.0.1:19092,127.0.0.1:19094"
    assert topic_stubs["BROKER_ARGS"].read_text().splitlines() == ["--bootstrap-server", bootstrap]
    assert topic_stubs["TOPIC_ARGS"].read_text().splitlines() == [
        "--bootstrap-server", bootstrap, "--create", "--if-not-exists", "--topic", "orders",
        "--partitions", "2", "--replication-factor", "2",
        "--bootstrap-server", bootstrap, "--create", "--if-not-exists", "--topic", "order_metrics",
        "--partitions", "2", "--replication-factor", "2",
    ]
    assert result.stdout.splitlines() == ["Created topic orders", "Created topic order_metrics"]


@pytest.mark.parametrize("output, status, count", [
    ("localhost:19092 (id: 1 rack: null) -> (\n)\n", "0", 1),
    ("localhost:19094 (id: 2 rack: null) -> (\n)\n", "0", 1),
    ("localhost:19092 (id: 1 rack: null) -> (\n)\n" * 2, "0", 1),
    ("localhost:19092 (id: 1 rack: null) -> (\n)\n"
     "localhost:19094 (id: 2 rack: null) -> ERROR: Connection refused\n", "0", 1),
    ("", "1", 0),
])
def test_create_topics_refuses_missing_brokers(tmp_path, topic_stubs, output, status, count):
    topic_stubs["BROKER_OUTPUT"].write_text(output)
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/create_topics.sh"', BROKER_STATUS=status, **topic_stubs,
    )
    assert result.returncode != 0
    assert f"Only {count} of 2 brokers are up; start VM3 first" in result.stderr
    assert not topic_stubs["TOPIC_ARGS"].exists()


@pytest.fixture
def reset_stubs(tmp_path, topic_stubs):
    topic_stubs["BROKER_OUTPUT"].write_text(
        "localhost:19092 (id: 1 rack: null) -> (\n)\n"
        "localhost:19094 (id: 2 rack: null) -> (\n)\n"
    )
    script = topic_stubs["KAFKA_HOME"] / "bin/kafka-topics.sh"
    script.write_text(r'''#!/bin/bash
set -euo pipefail
printf '%s\t' "$@" >> "$TOPIC_ARGS"
printf '\n' >> "$TOPIC_ARGS"
action=
topic=
while (( $# )); do
    case "$1" in
        --delete|--list|--create) action="$1" ;;
        --topic) topic="$2"; shift ;;
    esac
    shift
done
count=0
[[ ! -f "$LIST_COUNT" ]] || read -r count < "$LIST_COUNT"
case "$action" in
    --delete)
        if [[ "${DELETE_ERROR:-0}" == 1 ]]; then
            printf 'Permission denied\n' >&2
            exit 1
        fi
        if [[ "$topic" == order_metrics ]]; then
            printf 'Topic order_metrics does not exist\n' >&2
            exit 1
        fi
        ;;
    --list)
        count=$((count + 1))
        printf '%s\n' "$count" > "$LIST_COUNT"
        if (( count <= DISAPPEAR_AFTER )); then
            printf 'orders\n'
            if (( count == 1 )); then printf 'order_metrics\n'; fi
        else
            printf 'orders_archive\norder_metrics_backup\n'
        fi
        ;;
    --create) (( count > DISAPPEAR_AFTER )) || exit 99 ;;
esac
''')
    sleep = tmp_path / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    sleep.chmod(0o755)
    return {
        **topic_stubs, "LIST_COUNT": tmp_path / "list count", "DISAPPEAR_AFTER": "1",
        "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
    }


def test_reset_topics_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "kafka/reset_topics.sh")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("list_delay", [1, 2])
def test_reset_topics_waits_for_deletion_before_creation(tmp_path, reset_stubs, list_delay):
    reset_stubs["DISAPPEAR_AFTER"] = str(list_delay)
    result = run_common(tmp_path, 'bash "$REPO_ROOT/kafka/reset_topics.sh"', **reset_stubs)
    assert result.returncode == 0, result.stderr
    calls = [line.rstrip("\t").split("\t")
             for line in reset_stubs["TOPIC_ARGS"].read_text().splitlines()]
    bootstrap = ["--bootstrap-server", "127.0.0.1:19092,127.0.0.1:19094"]
    assert calls[:2] == [bootstrap + ["--delete", "--topic", topic]
                         for topic in ("orders", "order_metrics")]
    assert calls[2:3 + list_delay] == [bootstrap + ["--list"]] * (list_delay + 1)
    assert calls[3 + list_delay:] == [
        bootstrap + ["--create", "--if-not-exists", "--topic", topic,
                     "--partitions", "2", "--replication-factor", "2"]
        for topic in ("orders", "order_metrics")
    ]
    assert "Deleting topic orders" in result.stdout
    assert "Created topic order_metrics" in result.stdout


def test_reset_topics_timeout_does_not_create(tmp_path, reset_stubs):
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/reset_topics.sh"',
        FOODPIPE_TOPIC_DELETE_TIMEOUT="0", **reset_stubs,
    )
    assert result.returncode != 0
    assert "timed out" in result.stderr.lower()
    assert "--create" not in reset_stubs["TOPIC_ARGS"].read_text()


def test_reset_topics_does_not_ignore_other_delete_errors(tmp_path, reset_stubs):
    result = run_common(
        tmp_path, 'bash "$REPO_ROOT/kafka/reset_topics.sh"', DELETE_ERROR="1", **reset_stubs,
    )
    assert result.returncode != 0
    assert "Permission denied" in result.stderr
    assert not reset_stubs["LIST_COUNT"].exists()
