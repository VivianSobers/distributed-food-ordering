from pathlib import Path

import pytest

from foodpipe.config import _parse_env, bootstrap_servers, load_env


def test_local_env_bootstrap():
    assert bootstrap_servers(load_env("cluster.local")) == "127.0.0.1:19092,127.0.0.1:19094"


@pytest.mark.parametrize("unset_ip", ["VM2_IP", "VM3_IP"])
def test_unset_ips_rejected(unset_ip):
    env = {"VM2_IP": "10.0.0.2", "VM2_KAFKA_PORT": "9092",
           "VM3_IP": "10.0.0.3", "VM3_KAFKA_PORT": "9092"}
    env[unset_ip] = "CHANGE_ME"
    with pytest.raises(ValueError, match="CHANGE_ME"):
        bootstrap_servers(env)


def test_cluster_env_bootstrap():
    assert bootstrap_servers(load_env("cluster")) == (
        "192.168.193.217:9092,192.168.193.247:9092"
    )


def test_foodpipe_env_selects_file(monkeypatch):
    monkeypatch.setenv("FOODPIPE_ENV", "cluster.local")
    assert bootstrap_servers(load_env()) == "127.0.0.1:19092,127.0.0.1:19094"


def test_explicit_name_overrides_foodpipe_env(monkeypatch):
    monkeypatch.setenv("FOODPIPE_ENV", "cluster.local")
    assert bootstrap_servers(load_env("cluster")) == (
        "192.168.193.217:9092,192.168.193.247:9092"
    )


def test_default_env_is_relative_to_repo_root(monkeypatch, tmp_path):
    monkeypatch.delenv("FOODPIPE_ENV", raising=False)
    monkeypatch.chdir(tmp_path)
    assert bootstrap_servers(load_env()) == (
        "192.168.193.217:9092,192.168.193.247:9092"
    )


def test_parser_ignores_comments_and_blank_lines_without_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("FOODPIPE_TEST_VALUE", "expanded")
    path = tmp_path / "sample.env"
    path.write_text(
        "# Settings\n\n   # Indented comment\n   \nVM2_IP=127.0.0.1\n"
        "VM2_KAFKA_PORT=19092\nLITERAL=$FOODPIPE_TEST_VALUE\n"
        "OTHER=${FOODPIPE_TEST_VALUE}\nVALUE=a=b#c\nEMPTY=\n",
        encoding="utf-8",
    )
    assert _parse_env(path) == {
        "VM2_IP": "127.0.0.1", "VM2_KAFKA_PORT": "19092",
        "LITERAL": "$FOODPIPE_TEST_VALUE", "OTHER": "${FOODPIPE_TEST_VALUE}",
        "VALUE": "a=b#c", "EMPTY": "",
    }


def test_missing_env_names_path():
    expected_path = Path(__file__).resolve().parents[1] / "config" / "missing-test.env"
    with pytest.raises(FileNotFoundError) as error:
        load_env("missing-test")
    assert str(expected_path) in str(error.value)
