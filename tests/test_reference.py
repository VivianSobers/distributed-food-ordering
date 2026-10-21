import json

import pytest

from foodpipe.schema import (
    CITY_SUMMARY_COLUMNS,
    METRICS_FIELDS,
    NUMERIC_FIELDS,
    ORDER_FIELDS,
    RESTAURANT_SUMMARY_COLUMNS,
)
from verify.reference import reference_order_metrics, reference_summaries


def order(**changes):
    record = {
        "order_id": "O1",
        "customer_id": "C1",
        "restaurant_id": "R1",
        "delivery_partner_id": "D1",
        "city": "Bengaluru",
        "order_timestamp": "2026-10-01 12:00:00",
        "accepted_timestamp": "2026-10-01 12:02:00",
        "picked_up_timestamp": "2026-10-01 12:15:00",
        "expected_delivery_timestamp": "2026-10-01 12:40:00",
        "delivered_timestamp": "2026-10-01 12:40:00",
        "distance_km": 5,
        "order_amount": 250,
        "delivery_fee": 20,
        "status": "DELIVERED",
    }
    return {**record, **changes}


def load(tmp_path, *records, extra_lines=()):
    path = tmp_path / "orders.jsonl"
    path.write_text("\n".join([*(json.dumps(r) for r in records), *extra_lines]),
                    encoding="utf-8")
    return reference_order_metrics(path)


def test_base_metrics_and_output_fields(tmp_path):
    result, = load(tmp_path, order())
    assert list(result) == METRICS_FIELDS
    assert {field: result[field] for field in ORDER_FIELDS} == order()
    assert all(type(result[field]) is float for field in NUMERIC_FIELDS)
    assert [result[field] for field in METRICS_FIELDS[14:]] == [
        13.0, 25.0, 40.0, 12.0, 0.0, "ON_TIME", "ON_TIME",
    ]


def test_speed_uses_unrounded_delivery_duration(tmp_path):
    result, = load(tmp_path, order(distance_km=7.3,
                                  delivered_timestamp="2026-10-01 12:32:00"))
    assert result["delivery_time_min"] == 17.0
    assert result["delivery_speed_kmph"] == 25.76


@pytest.mark.parametrize("expected, delay, status, category", [
    ("12:41:00", 0.0, "ON_TIME", "ON_TIME"),
    ("12:40:00", 0.0, "ON_TIME", "ON_TIME"),
    ("12:39:59", 0.02, "DELAYED", "SLIGHT_DELAY"),
    ("12:25:00", 15.0, "DELAYED", "SLIGHT_DELAY"),
    ("12:24:30", 15.5, "DELAYED", "MODERATE_DELAY"),
    ("12:10:00", 30.0, "DELAYED", "MODERATE_DELAY"),
    ("12:09:59", 30.02, "DELAYED", "SEVERE_DELAY"),
])
def test_delay_boundaries(tmp_path, expected, delay, status, category):
    result, = load(tmp_path, order(expected_delivery_timestamp=f"2026-10-01 {expected}"))
    assert (result["delay_min"], result["delivery_status"], result["delay_category"]) == (
        delay, status, category,
    )


def test_rounding_is_half_up_and_speed_uses_raw_time(tmp_path):
    result, = load(tmp_path, order(distance_km=1.005,
                                  delivered_timestamp="2026-10-01 13:15:00"))
    assert result["delivery_speed_kmph"] == 1.01
    result, = load(tmp_path, order(delivered_timestamp="2026-10-01 12:32:01"))
    assert result["delivery_time_min"] == 17.02
    assert result["delivery_speed_kmph"] == 17.63


@pytest.mark.parametrize("field", ORDER_FIELDS)
@pytest.mark.parametrize("invalid", [None, "", "   "])
def test_invalid_required_fields_dropped(tmp_path, field, invalid):
    assert load(tmp_path, order(**{field: invalid})) == []


@pytest.mark.parametrize("field", ORDER_FIELDS)
def test_missing_required_fields_dropped(tmp_path, field):
    record = order()
    del record[field]
    assert load(tmp_path, record) == []


@pytest.mark.parametrize("changes", [
    {"order_timestamp": "bad timestamp"},
    {"accepted_timestamp": "2026-02-30 12:02:00"},
    {"picked_up_timestamp": "2026-10-01T12:15:00"},
    {"expected_delivery_timestamp": "2026-10-01 12:40:00 trailing"},
    {"delivered_timestamp": "2026-10-1 12:40:00"},
    {"order_timestamp": "2026-10-01 12:03:00"},
    {"accepted_timestamp": "2026-10-01 12:16:00"},
    {"delivered_timestamp": "2026-10-01 12:15:00"},
    {"delivered_timestamp": "2026-10-01 12:14:59"},
    {"distance_km": 0}, {"distance_km": -1},
    {"order_amount": 0}, {"order_amount": -1},
    {"status": "CANCELLED"},
])
def test_invalid_records_dropped(tmp_path, changes):
    assert load(tmp_path, order(**changes)) == []


@pytest.mark.parametrize("field", NUMERIC_FIELDS)
@pytest.mark.parametrize("value", ["5.6", True, [], {}, float("nan"), float("inf")])
def test_numeric_fields_require_finite_json_numbers(tmp_path, field, value):
    assert load(tmp_path, order(**{field: value})) == []


def test_malformed_lines_nonobjects_and_exact_duplicates(tmp_path):
    original = order()
    duplicate = dict(reversed(list(original.items())))
    duplicate["extra"] = "ignored"
    rows = load(tmp_path, original, duplicate, order(order_amount=251),
                extra_lines=["{broken", "[]", "null", "42", '"text"', ""])
    assert len(rows) == 2
    assert sorted(row["order_amount"] for row in rows) == [250.0, 251.0]


def test_equal_early_timestamps_and_nonpositive_fee_allowed(tmp_path):
    result, = load(tmp_path, order(accepted_timestamp="2026-10-01 12:00:00",
                                  picked_up_timestamp="2026-10-01 12:00:00",
                                  delivery_fee=-1))
    assert result["preparation_time_min"] == 0.0
    assert result["delivery_fee"] == -1.0


def metric_row(restaurant_id, city, amount, prep, delivery, status):
    return {"restaurant_id": restaurant_id, "city": city, "order_amount": amount,
            "preparation_time_min": prep, "delivery_time_min": delivery,
            "delivery_status": status}


SUMMARY_ROWS = [
    metric_row("R1", "Bengaluru", 100.0, 10.0, 20.0, "DELAYED"),
    metric_row("R1", "Bengaluru", 200.0, 20.0, 30.0, "ON_TIME"),
    metric_row("R2", "Pune", 301.0, 11.0, 21.0, "ON_TIME"),
    metric_row("R2", "Bengaluru", 50.0, 5.0, 25.0, "DELAYED"),
]


def test_reference_restaurant_summary():
    restaurants, _ = reference_summaries(SUMMARY_ROWS)
    assert list(restaurants.columns) == RESTAURANT_SUMMARY_COLUMNS
    assert restaurants.to_dict("records") == [
        {"restaurant_id": "R1", "order_count": 2, "avg_order_amount": 150.0,
         "avg_preparation_time_min": 15.0, "avg_delivery_time_min": 25.0,
         "delayed_order_count": 1, "delay_rate_pct": 50.0},
        {"restaurant_id": "R2", "order_count": 2, "avg_order_amount": 175.5,
         "avg_preparation_time_min": 8.0, "avg_delivery_time_min": 23.0,
         "delayed_order_count": 1, "delay_rate_pct": 50.0},
    ]


def test_reference_city_summary_rounds_half_up():
    _, cities = reference_summaries(SUMMARY_ROWS)
    assert list(cities.columns) == CITY_SUMMARY_COLUMNS
    assert cities.to_dict("records") == [
        {"city": "Bengaluru", "order_count": 3, "avg_order_amount": 116.67,
         "avg_delivery_time_min": 25.0, "delayed_order_count": 2, "delay_rate_pct": 66.67},
        {"city": "Pune", "order_count": 1, "avg_order_amount": 301.0,
         "avg_delivery_time_min": 21.0, "delayed_order_count": 0, "delay_rate_pct": 0.0},
    ]
