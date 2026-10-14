from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

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
            parsed, F.try_to_timestamp(F.col(field), F.lit(TIMESTAMP_FORMAT))
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
    )


def build_order_metrics(spark: SparkSession, input_path: str | Path) -> DataFrame:
    return enrich_orders(clean_orders(load_raw_orders(spark, input_path)))
