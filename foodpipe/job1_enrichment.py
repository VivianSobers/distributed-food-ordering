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
    return clean.select(*ORDER_FIELDS, *parsed_columns)
