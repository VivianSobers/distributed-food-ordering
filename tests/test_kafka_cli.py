from pathlib import Path
from types import SimpleNamespace

import pytest

from foodpipe.config import _parse_env, bootstrap_servers, load_env
from foodpipe.producer import main as producer_main, produce_file


class FakeFuture:
    def __init__(self, partition, error=None, events=None):
        self.partition = partition
        self.error = error
        self.waited = False
        self.events = events

    def get(self, timeout=None):
        if self.events is not None:
            self.events.append(("get", timeout))
        self.waited = True
        if self.error:
            raise self.error
        return SimpleNamespace(partition=self.partition)


class FakeProducer:
    def __init__(self, futures, events=None):
        self.futures = futures
        self.sent = []
        self.flushed = False
        self.closed = False
        self.events = events

    def send(self, topic, value, key):
        if self.events is not None:
            self.events.append(("send", key))
        self.sent.append((topic, value, key))
        return self.futures[len(self.sent) - 1]

    def flush(self):
        if self.events is not None:
            self.events.append(("flush",))
        self.flushed = True

    def close(self):
        self.closed = True


def test_produce_file_preserves_bytes_and_counts_partitions(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_bytes(b'  {"id": 1}  \n\n \t\r\nmalformed\xff\r\nlast\r\r\nend')
    futures = [FakeFuture(p) for p in (1, 0, 1, 1)]
    producer = FakeProducer(futures)

    assert produce_file(producer, "orders", path) == {0: 1, 1: 3}
    assert producer.sent == [
        ("orders", b'  {"id": 1}  ', b"1"),
        ("orders", b"malformed\xff", b"4"),
        ("orders", b"last\r", b"5"),
        ("orders", b"end", b"6"),
    ]
    assert all(future.waited for future in futures)
    assert producer.flushed


def test_produce_file_failed_future_raises(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_bytes(b"record\n")
    producer = FakeProducer([FakeFuture(0, RuntimeError("send failed"))])
    with pytest.raises(RuntimeError, match="send failed"):
        produce_file(producer, "orders", path)


def test_produce_file_sends_all_records_before_flushing_and_getting(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_bytes(b"first\n\nsecond\nthird\n")
    events = []
    futures = [FakeFuture(p, events=events) for p in (1, 0, 1)]
    producer = FakeProducer(futures, events=events)

    assert produce_file(producer, "orders", path) == {0: 1, 1: 2}
    assert events == [
        ("send", b"1"), ("send", b"3"), ("send", b"4"),
        ("flush",), ("get", 60), ("get", 60), ("get", 60),
    ]


def test_produce_file_blank_input(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_bytes(b"\n \t\r\n")
    producer = FakeProducer([])
    assert produce_file(producer, "orders", path) == {}
    assert producer.sent == []
    assert producer.flushed


@pytest.mark.parametrize("explicit_bootstrap", [False, True])
def test_producer_cli_prints_counts_and_closes(tmp_path, monkeypatch, capsys, explicit_bootstrap):
    path = tmp_path / "orders.jsonl"
    path.write_bytes(b"one\ntwo\nthree\n")
    producer = FakeProducer([FakeFuture(p) for p in (1, 0, 1)])
    options = {}

    def create_producer(**kwargs):
        options.update(kwargs)
        return producer

    monkeypatch.setattr("foodpipe.producer.KafkaProducer", create_producer)
    monkeypatch.setenv("FOODPIPE_ENV", "cluster.local")
    args = ["--topic", "orders", "--file", str(path)]
    if explicit_bootstrap:
        monkeypatch.setenv("FOODPIPE_ENV", "missing-test")
        args += ["--bootstrap", "broker:1234"]

    assert producer_main(args) == 0
    assert options == {
        "bootstrap_servers": "broker:1234" if explicit_bootstrap else (
            "127.0.0.1:19092,127.0.0.1:19094"
        ),
        "acks": "all", "retries": 5, "linger_ms": 10,
    }
    assert producer.closed
    assert capsys.readouterr().out.splitlines() == [
        "Produced 3 records to orders", "partition 0: 1", "partition 1: 2",
    ]


def test_producer_cli_failed_send_exits_one_and_closes(tmp_path, monkeypatch, capsys):
    path = tmp_path / "orders.jsonl"
    path.write_bytes(b"record\n")
    producer = FakeProducer([FakeFuture(0, RuntimeError("send failed"))])
    monkeypatch.setattr("foodpipe.producer.KafkaProducer", lambda **kwargs: producer)
    assert producer_main([
        "--topic", "orders", "--file", str(path), "--bootstrap", "broker:1234",
    ]) == 1
    assert producer.closed
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "send failed" in captured.err


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
