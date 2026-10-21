import os
from pathlib import Path
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
STAGE = REPO_ROOT / "scripts/vm1_stage1_produce.sh"


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
