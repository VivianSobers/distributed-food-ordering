import json
from datetime import datetime, timezone

import pytest

from foodpipe.job1_enrichment import clean_orders, load_raw_orders
from foodpipe.schema import ORDER_FIELDS, TIMESTAMP_FIELDS

PARSED_TIMESTAMP_FIELDS = [
    "_ts_order", "_ts_accepted", "_ts_picked_up", "_ts_expected", "_ts_delivered",
]

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
    for field, parsed in zip(TIMESTAMP_FIELDS, PARSED_TIMESTAMP_FIELDS):
        assert rows[0][field] == BASE[field]
        expected = datetime.strptime(BASE[field], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        assert rows[0][parsed].timestamp() == expected.timestamp()


@pytest.mark.parametrize("line", ['{"order_id": "T9", "city": ', "not json at all", "42"])
def test_malformed_json_dropped(spark, tmp_path, line):
    assert len(clean(spark, tmp_path, [line, rec()])) == 1


@pytest.mark.parametrize("field", ORDER_FIELDS)
def test_missing_field_dropped(spark, tmp_path, field):
    r = dict(BASE)
    del r[field]
    assert clean(spark, tmp_path, [json.dumps(r)]) == []


@pytest.mark.parametrize("field", ORDER_FIELDS)
def test_null_field_dropped(spark, tmp_path, field):
    assert clean(spark, tmp_path, [rec(**{field: None})]) == []


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_string_dropped(spark, tmp_path, value):
    assert clean(spark, tmp_path, [rec(city=value)]) == []


@pytest.mark.parametrize(
    "field",
    ["order_timestamp", "accepted_timestamp", "picked_up_timestamp",
     "expected_delivery_timestamp", "delivered_timestamp"],
)
@pytest.mark.parametrize("value", ["bad", "not-a-timestamp", "2026-13-45 99:00:00"])
def test_invalid_timestamp_dropped(spark, tmp_path, field, value):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    rows = clean(spark, tmp_path, [rec(**{field: value}), rec(order_id="T2")])
    assert [r["order_id"] for r in rows] == ["T2"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"accepted_timestamp": "2026-09-01 09:59:00"},            # accepted before order
        {"picked_up_timestamp": "2026-09-01 10:01:00"},           # picked up before accepted
        {"delivered_timestamp": "2026-09-01 10:10:00"},           # delivered before picked up
        {"delivered_timestamp": "2026-09-01 10:15:00"},           # zero delivery time
    ],
)
def test_out_of_order_timestamps_dropped(spark, tmp_path, overrides):
    assert clean(spark, tmp_path, [rec(**overrides)]) == []


@pytest.mark.parametrize("value", [0, -2, -0.5])
def test_nonpositive_distance_dropped(spark, tmp_path, value):
    assert clean(spark, tmp_path, [rec(distance_km=value)]) == []


@pytest.mark.parametrize("value", [0, -10])
def test_nonpositive_amount_dropped(spark, tmp_path, value):
    assert clean(spark, tmp_path, [rec(order_amount=value)]) == []


@pytest.mark.parametrize("status", ["CANCELLED", "PENDING", "REFUNDED", "FAILED", "delivered"])
def test_non_delivered_status_dropped(spark, tmp_path, status):
    assert clean(spark, tmp_path, [rec(status=status)]) == []


def test_extra_fields_ignored(spark, tmp_path):
    rows = clean(spark, tmp_path, [rec(note="x", record_type="y")])
    assert len(rows) == 1
    assert "note" not in rows[0].asDict()
    assert list(rows[0].asDict()) == ORDER_FIELDS + PARSED_TIMESTAMP_FIELDS


def test_repeated_clean_reads_current_file_after_malformed_lines(spark, tmp_path):
    rows = clean(spark, tmp_path, ["not json at all", rec()])
    assert [row["order_id"] for row in rows] == ["T1"]
    assert list(rows[0].asDict()) == ORDER_FIELDS + PARSED_TIMESTAMP_FIELDS
    assert clean(spark, tmp_path, ["not json at all"]) == []
    rows = clean(spark, tmp_path, ["42", rec(order_id="T2")])
    assert [row["order_id"] for row in rows] == ["T2"]


@pytest.mark.parametrize("field", ["distance_km", "order_amount", "delivery_fee"])
def test_numeric_sent_as_string_dropped(spark, tmp_path, field):
    assert clean(spark, tmp_path, [rec(**{field: "5.0"})]) == []
