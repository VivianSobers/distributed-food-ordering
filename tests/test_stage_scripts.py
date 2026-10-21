import os
from pathlib import Path
import re
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
STAGE = REPO_ROOT / "scripts/vm1_stage1_produce.sh"
VM4_STAGE = REPO_ROOT / "scripts/vm4_stage1.sh"


@pytest.fixture
def python_stub(tmp_path):
    interpreter = tmp_path / "stub python"
    interpreter.write_text(
        '#!/bin/bash\nset -euo pipefail\n'
        'printf \'%s\\n\' "$@" > "$PYTHON_ARGS"\n'
        'printf \'%s\\n\' "$PWD" > "$PYTHON_CWD"\n'
        'exit "${PYTHON_STATUS:-0}"\n'
    )
    interpreter.chmod(0o755)
    return {
        **os.environ, "FOODPIPE_PYTHON": str(interpreter),
        "PYTHON_ARGS": str(tmp_path / "python args"),
        "PYTHON_CWD": str(tmp_path / "python cwd"),
    }


def test_vm1_producing_stage_bash_syntax():
    result = subprocess.run(["bash", "-n", str(STAGE)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("profile, bootstrap", [
    (None, "192.168.193.217:9092,192.168.193.247:9092"),
    ("cluster.local", "127.0.0.1:19092,127.0.0.1:19094"),
])
def test_vm1_producing_stage_command(tmp_path, python_stub, profile, bootstrap):
    python_stub.pop("FOODPIPE_ENV", None)
    if profile:
        python_stub["FOODPIPE_ENV"] = profile
    result = subprocess.run(["bash", str(STAGE)], cwd=tmp_path, env=python_stub,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert Path(python_stub["PYTHON_ARGS"]).read_text().splitlines() == [
        "-m", "foodpipe.producer", "--topic", "orders", "--file", "data/order.jsonl",
        "--bootstrap", bootstrap,
    ]
    assert Path(python_stub["PYTHON_CWD"]).read_text().strip() == str(REPO_ROOT)
    assert result.stdout.strip() == "VM1 stage 1: producing data/order.jsonl to orders"


def test_vm1_producing_stage_missing_python_has_setup_hint(tmp_path, python_stub):
    python_stub["FOODPIPE_PYTHON"] = str(tmp_path / "missing python")
    python_stub["FOODPIPE_ENV"] = "cluster.local"
    result = subprocess.run(["bash", str(STAGE)], cwd=tmp_path, env=python_stub,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert "python3.10 -m venv .venv" in result.stderr
    assert not Path(python_stub["PYTHON_ARGS"]).exists()


def test_vm1_producing_stage_propagates_failure(tmp_path, python_stub):
    python_stub.update(FOODPIPE_ENV="cluster.local", PYTHON_STATUS="7")
    result = subprocess.run(["bash", str(STAGE)], cwd=tmp_path, env=python_stub,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 7


@pytest.fixture
def pipeline_python_stub(python_stub):
    Path(python_stub["FOODPIPE_PYTHON"]).write_text(r'''#!/bin/bash
set -euo pipefail
printf '%s\t' "$@" >> "$PYTHON_ARGS"
printf '\n' >> "$PYTHON_ARGS"
printf '%s\n' "$PWD" >> "$PYTHON_CWD"
if [[ "$2" == "${FAIL_MODULE:-}" ]]; then exit 7; fi
''')
    python_stub.pop("FOODPIPE_IDLE_TIMEOUT", None)
    python_stub.pop("FOODPIPE_MAX_WAIT", None)
    return python_stub


def test_vm4_enrichment_stage_bash_syntax():
    result = subprocess.run(["bash", "-n", str(VM4_STAGE)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("profile, bootstrap, idle, max_wait", [
    ("cluster", "192.168.193.217:9092,192.168.193.247:9092", "15", "300"),
    ("cluster.local", "127.0.0.1:19092,127.0.0.1:19094", "7", "23"),
])
def test_vm4_enrichment_stage_commands(tmp_path, pipeline_python_stub, profile, bootstrap, idle, max_wait):
    env = pipeline_python_stub
    env["FOODPIPE_ENV"] = profile
    if profile == "cluster.local":
        env.update(FOODPIPE_IDLE_TIMEOUT=idle, FOODPIPE_MAX_WAIT=max_wait)
    result = subprocess.run(["bash", str(VM4_STAGE)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    calls = [line.rstrip("\t").split("\t") for line in Path(env["PYTHON_ARGS"]).read_text().splitlines()]
    assert len(calls) == 3
    group = calls[0][7]
    assert re.fullmatch(r"vm4-orders-\d+", group)
    assert calls == [
        ["-m", "foodpipe.consumer", "--topic", "orders", "--out", "output/orders_landed.jsonl",
         "--group", group, "--bootstrap", bootstrap, "--idle-timeout", idle, "--max-wait", max_wait],
        ["-m", "foodpipe.job1_enrichment", "--input", "output/orders_landed.jsonl",
         "--output", "output/order_metrics.jsonl"],
        ["-m", "foodpipe.producer", "--topic", "order_metrics", "--file", "output/order_metrics.jsonl",
         "--bootstrap", bootstrap],
    ]
    assert Path(env["PYTHON_CWD"]).read_text().splitlines() == [str(REPO_ROOT)] * 3
    assert len(result.stdout.splitlines()) == 3


@pytest.mark.parametrize("module, step, call_count", [
    ("foodpipe.consumer", "consumer", 1),
    ("foodpipe.job1_enrichment", "Job 1", 2),
    ("foodpipe.producer", "producer", 3),
])
def test_vm4_enrichment_stage_stops_on_failed_step(tmp_path, pipeline_python_stub, module, step, call_count):
    env = pipeline_python_stub
    env.update(FOODPIPE_ENV="cluster.local", FAIL_MODULE=module)
    result = subprocess.run(["bash", str(VM4_STAGE)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 7
    assert step in result.stderr
    assert "failed" in result.stderr
    calls = Path(env["PYTHON_ARGS"]).read_text().splitlines()
    assert len(calls) == call_count
    assert calls[-1].split("\t")[1] == module


def test_vm4_enrichment_stage_missing_python_has_setup_hint(tmp_path, pipeline_python_stub):
    env = pipeline_python_stub
    env.update(FOODPIPE_ENV="cluster.local", FOODPIPE_PYTHON=str(tmp_path / "missing python"))
    result = subprocess.run(["bash", str(VM4_STAGE)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert "python3.10 -m venv .venv" in result.stderr
    assert not Path(env["PYTHON_ARGS"]).exists()
