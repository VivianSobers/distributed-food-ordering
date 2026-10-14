import csv
import json
from pathlib import Path

import pytest
from pyspark.sql.readwriter import DataFrameWriter

from foodpipe.io_utils import write_single_file


def test_write_single_json_file(spark, tmp_path):
    df = spark.createDataFrame([("a", 1.5), ("b", 2.0)], ["k", "v"])
    out = tmp_path / "nested" / "out.jsonl"
    write_single_file(df, out, "json")
    assert out.is_file()
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert sorted(r["k"] for r in rows) == ["a", "b"]
    assert sorted(p.name for p in out.parent.iterdir()) == ["out.jsonl"]


def test_write_single_csv_file_with_header_and_overwrite(spark, tmp_path):
    out = tmp_path / "out.csv"
    out.write_text("stale\n")
    df = spark.createDataFrame([("a", 1.5)], ["k", "v"])
    write_single_file(df, out, "csv")
    with out.open() as f:
        rows = list(csv.reader(f))
    assert rows == [["k", "v"], ["a", "1.5"]]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.csv"]


def test_write_empty_json_creates_empty_file(spark, tmp_path):
    df = spark.createDataFrame([], "k string, v double")
    out = tmp_path / "out.jsonl"
    write_single_file(df, out, "json")
    assert out.read_bytes() == b""
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.jsonl"]


def test_write_empty_csv_writes_header_only(spark, tmp_path):
    df = spark.createDataFrame([], "k string, v double")
    out = tmp_path / "out.csv"
    write_single_file(df, out, "csv")
    with out.open() as f:
        assert list(csv.reader(f)) == [["k", "v"]]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.csv"]


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_write_without_part_file(spark, tmp_path, monkeypatch, fmt):
    def save_without_part(self, path):
        Path(path).mkdir()

    monkeypatch.setattr(DataFrameWriter, "save", save_without_part)
    df = spark.createDataFrame([], "k string, v double").toDF("a,b", 'c"d')
    out = tmp_path / "out"
    out.write_text("stale\n")
    write_single_file(df, out, fmt)
    if fmt == "json":
        assert out.read_bytes() == b""
    else:
        with out.open() as f:
            assert list(csv.reader(f)) == [["a,b", 'c"d']]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out"]


def test_write_multiple_part_files_raises_clear_error(spark, tmp_path, monkeypatch):
    def save_multiple_parts(self, path):
        directory = Path(path)
        directory.mkdir()
        (directory / "part-00000").write_text("first\n")
        (directory / "part-00001").write_text("second\n")

    monkeypatch.setattr(DataFrameWriter, "save", save_multiple_parts)
    df = spark.createDataFrame([], "k string, v double")
    out = tmp_path / "out.jsonl"
    out.write_text("stale\n")
    with pytest.raises(ValueError, match="Expected at most one part file, found 2"):
        write_single_file(df, out, "json")
    assert out.read_text() == "stale\n"
