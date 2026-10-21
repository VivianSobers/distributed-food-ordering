import json

import pytest

from foodpipe.job1_enrichment import build_order_metrics, main, run
from foodpipe.schema import METRICS_FIELDS, NUMERIC_FIELDS, ORDER_FIELDS, TIMESTAMP_FIELDS
from tests.test_job1_cleaning import BASE, rec


def metrics(spark, tmp_path, lines):
    path = tmp_path / "orders_landed.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return build_order_metrics(spark, path).collect()


def test_duration_metrics(spark, tmp_path):
    (row,) = metrics(spark, tmp_path, [rec()])
    assert row["preparation_time_min"] == 13.0
    assert row["delivery_time_min"] == 25.0
    assert row["fulfillment_time_min"] == 40.0
    assert row["delivery_speed_kmph"] == 12.0


def test_columns_and_original_values(spark, tmp_path):
    (row,) = metrics(spark, tmp_path, [rec()])
    assert list(row.asDict().keys()) == METRICS_FIELDS
    for field in NUMERIC_FIELDS:
        assert isinstance(row[field], float)
        assert row[field] == BASE[field]
    for field in TIMESTAMP_FIELDS:
        assert row[field] == BASE[field]


@pytest.mark.parametrize(
    "delivered, delivery, fulfillment",
    [
        ("2026-09-01 10:40:30", 25.5, 40.5),
        ("2026-09-01 10:40:31", 25.52, 40.52),
    ],
)
def test_duration_metrics_with_seconds(spark, tmp_path, delivered, delivery, fulfillment):
    (row,) = metrics(spark, tmp_path, [rec(delivered_timestamp=delivered)])
    assert row["preparation_time_min"] == 13.0
    assert row["delivery_time_min"] == delivery
    assert row["fulfillment_time_min"] == fulfillment
    assert row["delivered_timestamp"] == delivered


def test_speed_rounded_to_two_places(spark, tmp_path):
    # 7.3 km in 17 min = 25.7647... km/h
    (row,) = metrics(spark, tmp_path, [rec(distance_km=7.3, delivered_timestamp="2026-09-01 10:32:00")])
    assert row["delivery_speed_kmph"] == 25.76


def test_speed_uses_unrounded_delivery_minutes(spark, tmp_path):
    (row,) = metrics(spark, tmp_path, [rec(delivered_timestamp="2026-09-01 10:15:01")])
    assert row["delivery_time_min"] == 0.02
    assert row["delivery_speed_kmph"] == 18000.0


@pytest.mark.parametrize(
    "delivered, delay, status, category",
    [
        ("2026-09-01 10:30:00", 0.0, "ON_TIME", "ON_TIME"),
        ("2026-09-01 10:40:00", 0.0, "ON_TIME", "ON_TIME"),
        ("2026-09-01 10:40:01", 0.02, "DELAYED", "SLIGHT_DELAY"),
        ("2026-09-01 10:40:30", 0.5, "DELAYED", "SLIGHT_DELAY"),
        ("2026-09-01 10:41:00", 1.0, "DELAYED", "SLIGHT_DELAY"),
        ("2026-09-01 10:55:00", 15.0, "DELAYED", "SLIGHT_DELAY"),
        ("2026-09-01 10:55:30", 15.5, "DELAYED", "MODERATE_DELAY"),
        ("2026-09-01 10:56:00", 16.0, "DELAYED", "MODERATE_DELAY"),
        ("2026-09-01 11:10:00", 30.0, "DELAYED", "MODERATE_DELAY"),
        ("2026-09-01 11:11:00", 31.0, "DELAYED", "SEVERE_DELAY"),
    ],
)
def test_delay_categories(spark, tmp_path, delivered, delay, status, category):
    (row,) = metrics(spark, tmp_path, [rec(delivered_timestamp=delivered)])
    assert row["delay_min"] == delay
    assert row["delivery_status"] == status
    assert row["delay_category"] == category


def test_run_writes_single_jsonl(spark, tmp_path):
    src = tmp_path / "orders_landed.jsonl"
    src.write_text("\n".join([rec(order_id="T2"), rec(order_id="T1"), rec(status="CANCELLED")]) + "\n")
    out = tmp_path / "out" / "order_metrics.jsonl"
    assert run(spark, src, out) == 2
    lines = out.read_text().splitlines()
    assert [json.loads(line)["order_id"] for line in lines] == ["T1", "T2"]
    assert list(json.loads(lines[0]).keys()) == METRICS_FIELDS
    assert sorted(path.name for path in out.parent.iterdir()) == ["order_metrics.jsonl"]


def test_run_orders_same_id_by_all_fields(spark, tmp_path):
    records = [rec(order_amount=500.0), rec(order_amount=100.0)]
    expected = sorted(
        [json.loads(record) for record in records],
        key=lambda record: tuple(record[field] for field in ORDER_FIELDS),
    )
    outputs = []
    for index, lines in enumerate((records, records[::-1])):
        src = tmp_path / f"orders_{index}.jsonl"
        src.write_text("\n".join(lines) + "\n")
        out = tmp_path / f"metrics_{index}.jsonl"
        assert run(spark, src, out) == 2
        outputs.append(out.read_bytes())
        rows = [json.loads(line) for line in out.read_text().splitlines()]
        assert [{field: row[field] for field in ORDER_FIELDS} for row in rows] == expected
    assert outputs[0] == outputs[1]


def test_main_writes_file_and_prints_count(spark, tmp_path, capsys):
    src = tmp_path / "orders_landed.jsonl"
    src.write_text(rec() + "\n")
    out = tmp_path / "out" / "order_metrics.jsonl"
    main(["--input", str(src), "--output", str(out)])
    assert f"Job 1: 1 order metrics -> {out}" in capsys.readouterr().out.splitlines()
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["order_id"] == BASE["order_id"]
    assert list(rows[0]) == METRICS_FIELDS
    assert spark.range(1).count() == 1
