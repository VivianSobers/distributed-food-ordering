import json

from foodpipe.job2_analytics import load_order_metrics
from foodpipe.schema import METRICS_FIELDS


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
