import pytest

from foodpipe.job1_enrichment import build_order_metrics
from foodpipe.schema import NUMERIC_FIELDS, ORDER_FIELDS, TIMESTAMP_FIELDS
from tests.test_job1_cleaning import BASE, rec


def metrics(spark, tmp_path, lines):
    path = tmp_path / "orders_landed.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return build_order_metrics(spark, path).collect()


def test_duration_metrics(spark, tmp_path):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    (row,) = metrics(spark, tmp_path, [rec()])
    assert row["preparation_time_min"] == 13.0
    assert row["delivery_time_min"] == 25.0
    assert row["fulfillment_time_min"] == 40.0
    assert list(row.asDict()) == ORDER_FIELDS + [
        "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
    ]
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
