import json

import pytest

from foodpipe.job1_enrichment import clean_orders, load_raw_orders
from foodpipe.schema import ORDER_FIELDS

BASE = {
    "order_id": "T1",
    "customer_id": "C1",
    "restaurant_id": "R1",
    "delivery_partner_id": "D1",
    "city": "Pune",
    "order_timestamp": "2026-09-01 10:00:00",
    "accepted_timestamp": "2026-09-01 10:02:00",
    "picked_up_timestamp": "2026-09-01 10:15:00",
    "expected_delivery_timestamp": "2026-09-01 10:40:00",
    "delivered_timestamp": "2026-09-01 10:40:00",
    "distance_km": 5.0,
    "order_amount": 500,
    "delivery_fee": 30,
    "status": "DELIVERED",
}


def rec(**overrides):
    r = dict(BASE)
    r.update(overrides)
    return json.dumps(r)


def clean(spark, tmp_path, lines):
    path = tmp_path / "orders_landed.jsonl"
    path.write_text("\n".join(lines) + "\n")
    raw = load_raw_orders(spark, path)
    try:
        return clean_orders(raw).collect()
    finally:
        raw.unpersist(blocking=True)


def test_valid_record_kept(spark, tmp_path):
    rows = clean(spark, tmp_path, [rec()])
    assert len(rows) == 1
    assert rows[0]["order_id"] == "T1"


@pytest.mark.parametrize("line", ['{"order_id": "T9", "city": ', "not json at all", "42"])
def test_malformed_json_dropped(spark, tmp_path, line):
    assert len(clean(spark, tmp_path, [line, rec()])) == 1


def test_extra_fields_ignored(spark, tmp_path):
    rows = clean(spark, tmp_path, [rec(note="x", record_type="y")])
    assert len(rows) == 1
    assert "note" not in rows[0].asDict()


def test_repeated_clean_reads_current_file_after_malformed_lines(spark, tmp_path):
    rows = clean(spark, tmp_path, ["not json at all", rec()])
    assert [row["order_id"] for row in rows] == ["T1"]
    assert list(rows[0].asDict()) == ORDER_FIELDS
    assert clean(spark, tmp_path, ["not json at all"]) == []
    rows = clean(spark, tmp_path, ["42", rec(order_id="T2")])
    assert [row["order_id"] for row in rows] == ["T2"]
