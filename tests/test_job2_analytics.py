import csv
import json

from foodpipe.job2_analytics import city_summary, load_order_metrics, restaurant_summary, run
from foodpipe.schema import CITY_SUMMARY_COLUMNS, METRICS_FIELDS, RESTAURANT_SUMMARY_COLUMNS


def metric(order_id, restaurant_id, city, amount, prep, deliv, status):
    return json.dumps({
        "order_id": order_id, "customer_id": "C", "restaurant_id": restaurant_id,
        "delivery_partner_id": "D", "city": city,
        "order_timestamp": "2026-09-01 10:00:00", "accepted_timestamp": "2026-09-01 10:02:00",
        "picked_up_timestamp": "2026-09-01 10:15:00", "expected_delivery_timestamp": "2026-09-01 10:40:00",
        "delivered_timestamp": "2026-09-01 10:40:00", "distance_km": 5.0, "order_amount": amount,
        "delivery_fee": 30.0, "status": "DELIVERED", "preparation_time_min": prep,
        "delivery_time_min": deliv, "fulfillment_time_min": 40.0, "delivery_speed_kmph": 12.0,
        "delay_min": 5.0 if status == "DELAYED" else 0.0, "delivery_status": status,
        "delay_category": "SLIGHT_DELAY" if status == "DELAYED" else "ON_TIME",
    })


LINES = [
    metric("A", "R1", "Bengaluru", 100.0, 10.0, 20.0, "DELAYED"),
    metric("A", "R1", "Bengaluru", 100.0, 10.0, 20.0, "DELAYED"),   # exact duplicate
    metric("B", "R1", "Bengaluru", 200.0, 20.0, 30.0, "ON_TIME"),
    metric("C", "R2", "Pune", 301.0, 11.0, 21.0, "ON_TIME"),
    metric("D", "R2", "Bengaluru", 50.0, 5.0, 25.0, "DELAYED"),
    '{"order_id": "broken", ',                                       # malformed
]


def load(spark, tmp_path):
    path = tmp_path / "order_metrics_landed.jsonl"
    path.write_text("\n".join(LINES) + "\n")
    return load_order_metrics(spark, path)


def test_load_order_metrics(spark, tmp_path):
    df = load(spark, tmp_path)
    assert df.columns == METRICS_FIELDS
    rows = df.collect()
    assert len(rows) == 4
    assert sorted(row["order_id"] for row in rows) == ["A", "B", "C", "D"]


def test_restaurant_summary(spark, tmp_path):
    rows = [r.asDict() for r in restaurant_summary(load(spark, tmp_path)).collect()]
    assert rows == [
        {"restaurant_id": "R1", "order_count": 2, "avg_order_amount": 150.0,
         "avg_preparation_time_min": 15.0, "avg_delivery_time_min": 25.0,
         "delayed_order_count": 1, "delay_rate_pct": 50.0},
        {"restaurant_id": "R2", "order_count": 2, "avg_order_amount": 175.5,
         "avg_preparation_time_min": 8.0, "avg_delivery_time_min": 23.0,
         "delayed_order_count": 1, "delay_rate_pct": 50.0},
    ]


def test_city_summary(spark, tmp_path):
    rows = [r.asDict() for r in city_summary(load(spark, tmp_path)).collect()]
    assert rows == [
        {"city": "Bengaluru", "order_count": 3, "avg_order_amount": 116.67,
         "avg_delivery_time_min": 25.0, "delayed_order_count": 2, "delay_rate_pct": 66.67},
        {"city": "Pune", "order_count": 1, "avg_order_amount": 301.0,
         "avg_delivery_time_min": 21.0, "delayed_order_count": 0, "delay_rate_pct": 0.0},
    ]


def test_run_writes_two_csvs(spark, tmp_path):
    src = tmp_path / "order_metrics_landed.jsonl"
    src.write_text("\n".join(LINES) + "\n")
    rest, city = tmp_path / "out" / "restaurant_summary.csv", tmp_path / "out" / "city_summary.csv"
    assert run(spark, src, rest, city) == (2, 2)
    with rest.open() as f:
        header, *body = list(csv.reader(f))
    assert header == RESTAURANT_SUMMARY_COLUMNS
    assert [r[0] for r in body] == ["R1", "R2"]
    with city.open() as f:
        assert next(csv.reader(f)) == CITY_SUMMARY_COLUMNS
    assert sorted(p.name for p in rest.parent.iterdir()) == ["city_summary.csv", "restaurant_summary.csv"]


def test_load_raw_order_metrics_preserves_all_rows(spark, tmp_path):
    from foodpipe.job2_analytics import load_raw_order_metrics

    src = tmp_path / "order_metrics_landed.jsonl"
    src.write_text("\n".join(LINES) + "\n")
    raw = load_raw_order_metrics(spark, src)
    try:
        assert raw.is_cached
        assert raw.count() == len(LINES)
        assert raw.columns == METRICS_FIELDS + ["_corrupt_record"]
    finally:
        raw.unpersist()


def test_run_reads_changed_contents_at_same_path(spark, tmp_path):
    src = tmp_path / "order_metrics_landed.jsonl"
    rest = tmp_path / "restaurant_summary.csv"
    city = tmp_path / "city_summary.csv"
    src.write_text("\n".join(LINES) + "\n")
    assert run(spark, src, rest, city) == (2, 2)

    src.write_text(metric("E", "R3", "Mumbai", 90.0, 3.0, 12.0, "ON_TIME") + "\n")
    assert run(spark, src, rest, city) == (1, 1)
    with rest.open() as f:
        rows = list(csv.DictReader(f))
    assert [row["restaurant_id"] for row in rows] == ["R3"]
    assert rows[0]["order_count"] == "1"
    with city.open() as f:
        rows = list(csv.DictReader(f))
    assert [row["city"] for row in rows] == ["Mumbai"]
    assert rows[0]["order_count"] == "1"
