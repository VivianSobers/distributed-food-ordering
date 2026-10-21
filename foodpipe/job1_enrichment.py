import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from foodpipe.io_utils import write_single_file
from foodpipe.spark_session import get_spark
from foodpipe.schema import (
    NUMERIC_FIELDS,
    ORDER_FIELDS,
    ORDER_SCHEMA,
    STRING_FIELDS,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FORMAT,
)


def load_raw_orders(spark: SparkSession, path: str | Path) -> DataFrame:
    return (
        spark.read.schema(ORDER_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(path))
        .cache()
    )


def clean_orders(raw: DataFrame) -> DataFrame:
    clean = raw.filter(F.col("_corrupt_record").isNull())
    for field in STRING_FIELDS:
        clean = clean.filter(F.col(field).isNotNull() & F.col(field).rlike(r"\S"))
    for field in NUMERIC_FIELDS:
        clean = clean.filter(F.col(field).isNotNull())
    parsed_columns = [
        "_ts_order", "_ts_accepted", "_ts_picked_up", "_ts_expected", "_ts_delivered",
    ]
    for field, parsed in zip(TIMESTAMP_FIELDS, parsed_columns):
        clean = clean.withColumn(
            parsed, F.to_timestamp(F.col(field), TIMESTAMP_FORMAT)
        )
        clean = clean.filter(F.col(parsed).isNotNull())
    clean = clean.filter(F.col("_ts_order") <= F.col("_ts_accepted"))
    clean = clean.filter(F.col("_ts_accepted") <= F.col("_ts_picked_up"))
    clean = clean.filter(F.col("_ts_picked_up") < F.col("_ts_delivered"))
    clean = clean.filter(F.col("distance_km") > 0)
    clean = clean.filter(F.col("order_amount") > 0)
    clean = clean.filter(F.col("status") == "DELIVERED")
    return clean.select(*ORDER_FIELDS, *parsed_columns).dropDuplicates(ORDER_FIELDS)


def enrich_orders(clean: DataFrame) -> DataFrame:
    delivery_minutes = (
        F.unix_timestamp(F.col("_ts_delivered"))
        - F.unix_timestamp(F.col("_ts_picked_up"))
    ) / 60.0
    delay_minutes = F.greatest(
        F.lit(0.0),
        (
            F.unix_timestamp(F.col("_ts_delivered"))
            - F.unix_timestamp(F.col("_ts_expected"))
        ) / 60.0,
    )
    durations = [
        ("preparation_time_min", "_ts_accepted", "_ts_picked_up"),
        ("delivery_time_min", "_ts_picked_up", "_ts_delivered"),
        ("fulfillment_time_min", "_ts_order", "_ts_delivered"),
    ]
    return clean.select(
        *[
            F.col(field).cast("double") if field in NUMERIC_FIELDS else F.col(field)
            for field in ORDER_FIELDS
        ],
        *[
            F.round(
                (F.unix_timestamp(F.col(end)) - F.unix_timestamp(F.col(start))) / 60.0,
                2,
            ).alias(name)
            for name, start, end in durations
        ],
        F.round(F.col("distance_km") / (delivery_minutes / 60.0), 2).alias(
            "delivery_speed_kmph"
        ),
        F.round(delay_minutes, 2).alias("delay_min"),
        F.when(delay_minutes == 0, "ON_TIME").otherwise("DELAYED").alias(
            "delivery_status"
        ),
        F.when(delay_minutes == 0, "ON_TIME")
        .when(delay_minutes <= 15, "SLIGHT_DELAY")
        .when(delay_minutes <= 30, "MODERATE_DELAY")
        .otherwise("SEVERE_DELAY")
        .alias("delay_category"),
    )


def build_order_metrics(spark: SparkSession, input_path: str | Path) -> DataFrame:
    return enrich_orders(clean_orders(load_raw_orders(spark, input_path)))


def run(
    spark: SparkSession, input_path: str | Path, output_path: str | Path
) -> int:
    raw = load_raw_orders(spark, input_path)
    try:
        metrics = enrich_orders(clean_orders(raw)).orderBy(*ORDER_FIELDS)
        count = metrics.count()
        write_single_file(metrics, output_path, "json")
        return count
    finally:
        raw.unpersist()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Enrich delivered order records.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    active_session = SparkSession.getActiveSession()
    spark = get_spark("order-enrichment")
    try:
        count = run(spark, args.input, args.output)
        print(f"Job 1: {count} order metrics -> {args.output}")
    finally:
        if active_session is None:
            spark.stop()


if __name__ == "__main__":
    main()
