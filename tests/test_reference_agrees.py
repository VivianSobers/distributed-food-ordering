import json
from pathlib import Path

import pandas as pd

from foodpipe import job1_enrichment, job2_analytics
from verify.reference import reference_order_metrics, reference_summaries

DATASET = Path(__file__).resolve().parent.parent / "data" / "order.jsonl"


def by_content(rows):
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


def test_spark_matches_reference_on_dataset(spark, tmp_path):
    metrics_out = tmp_path / "order_metrics.jsonl"
    job1_enrichment.run(spark, DATASET, metrics_out)
    rest_out, city_out = tmp_path / "restaurant_summary.csv", tmp_path / "city_summary.csv"
    job2_analytics.run(spark, metrics_out, rest_out, city_out)

    spark_metrics = [json.loads(line) for line in metrics_out.read_text().splitlines()]
    ref_metrics = reference_order_metrics(DATASET)
    assert len(spark_metrics) == len(ref_metrics) > 0
    assert by_content(spark_metrics) == by_content(ref_metrics)

    ref_rest, ref_city = reference_summaries(ref_metrics)
    pd.testing.assert_frame_equal(pd.read_csv(rest_out), ref_rest, check_dtype=False)
    pd.testing.assert_frame_equal(pd.read_csv(city_out), ref_city, check_dtype=False)


def run_both(spark, landed, out_dir):
    out_dir.mkdir()
    job1_enrichment.run(spark, landed, out_dir / "m.jsonl")
    job2_analytics.run(spark, out_dir / "m.jsonl", out_dir / "r.csv", out_dir / "c.csv")
    return [(out_dir / name).read_bytes() for name in ("m.jsonl", "r.csv", "c.csv")]


def test_rerun_with_duplicated_topic_is_stable(spark, tmp_path):
    # Producing the dataset twice leaves every record in the topic twice.
    text = DATASET.read_text().rstrip("\n") + "\n"
    single, doubled = tmp_path / "single.jsonl", tmp_path / "doubled.jsonl"
    single.write_text(text)
    doubled.write_text(text * 2)
    assert run_both(spark, doubled, tmp_path / "b") == run_both(spark, single, tmp_path / "a")
