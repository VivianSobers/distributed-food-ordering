import json
from pathlib import Path

import pytest

from foodpipe import job1_enrichment, job2_analytics
from verify.compare import main


DATASET = Path(__file__).resolve().parents[1] / "data/order.jsonl"


@pytest.fixture
def comparison_files(tmp_path):
    order = {
        "order_id": "O1", "customer_id": "C1", "restaurant_id": "R1",
        "delivery_partner_id": "D1", "city": "Delhi",
        "order_timestamp": "2026-10-01 12:00:00", "accepted_timestamp": "2026-10-01 12:02:00",
        "picked_up_timestamp": "2026-10-01 12:15:00", "expected_delivery_timestamp": "2026-10-01 12:40:00",
        "delivered_timestamp": "2026-10-01 12:40:00", "distance_km": 5,
        "order_amount": 250, "delivery_fee": 20, "status": "DELIVERED",
    }
    metrics = {
        **order, "preparation_time_min": 13.0, "delivery_time_min": 25.0,
        "fulfillment_time_min": 40.0, "delivery_speed_kmph": 12.0, "delay_min": 0.0,
        "delivery_status": "ON_TIME", "delay_category": "ON_TIME",
    }
    paths = {name: tmp_path / filename for name, filename in {
        "dataset": "orders.jsonl", "metrics": "order_metrics.jsonl",
        "restaurant": "restaurant_summary.csv", "city": "city_summary.csv",
    }.items()}
    paths["dataset"].write_text(json.dumps(order) + "\n")
    paths["metrics"].write_text(json.dumps(metrics) + "\n")
    paths["restaurant"].write_text(
        "restaurant_id,order_count,avg_order_amount,avg_preparation_time_min,avg_delivery_time_min,"
        "delayed_order_count,delay_rate_pct\nR1,1,250.0,13.0,25.0,0,0.0\n"
    )
    paths["city"].write_text(
        "city,order_count,avg_order_amount,avg_delivery_time_min,delayed_order_count,delay_rate_pct\n"
        "Delhi,1,250.0,25.0,0,0.0\n"
    )
    return paths


def arguments(paths):
    return [part for key, path in paths.items() for part in (f"--{key}", str(path))]


def test_matching_outputs(comparison_files, capsys):
    assert main(arguments(comparison_files)) == 0
    assert capsys.readouterr().out.strip() == "MATCH"


def test_extra_metrics_row_is_reported(comparison_files, capsys):
    path = comparison_files["metrics"]
    path.write_text(path.read_text() * 2)
    assert main(arguments(comparison_files)) == 1
    output = capsys.readouterr().out
    assert str(path) in output and "extra" in output.lower() and "O1" in output
    assert "MATCH" not in output


def test_changed_csv_value_is_reported(comparison_files, capsys):
    path = comparison_files["restaurant"]
    path.write_text(path.read_text().replace("250.0", "251.0"))
    assert main(arguments(comparison_files)) == 1
    output = capsys.readouterr().out
    assert str(path) in output and "different" in output.lower()
    assert "avg_order_amount" in output and "250" in output and "251" in output


def test_numbers_are_compared_after_rounding(comparison_files, capsys):
    path = comparison_files["metrics"]
    row = json.loads(path.read_text())
    row["preparation_time_min"] = 13.004
    path.write_text(json.dumps(dict(reversed(list(row.items())))) + "\n")
    path = comparison_files["restaurant"]
    path.write_text(path.read_text().replace("13.0", "13.004"))
    assert main(arguments(comparison_files)) == 0
    assert capsys.readouterr().out.strip() == "MATCH"


def test_metrics_differences_are_limited_to_ten_rows(comparison_files, capsys):
    path = comparison_files["metrics"]
    row = json.loads(path.read_text())
    with path.open("a") as output:
        for index in range(20):
            output.write(json.dumps({**row, "order_id": f"EXTRA-{index}"}) + "\n")
    assert main(arguments(comparison_files)) == 1
    lines = capsys.readouterr().out.splitlines()
    assert len([line for line in lines if "extra row" in line.lower()]) == 10


def test_compare_spark_outputs_from_dataset(spark, tmp_path, capsys):
    metrics = tmp_path / "order_metrics.jsonl"
    restaurant, city = tmp_path / "restaurant_summary.csv", tmp_path / "city_summary.csv"
    job1_enrichment.run(spark, DATASET, metrics)
    job2_analytics.run(spark, metrics, restaurant, city)
    assert main(arguments({"dataset": DATASET, "metrics": metrics,
                           "restaurant": restaurant, "city": city})) == 0
    assert capsys.readouterr().out.strip() == "MATCH"
