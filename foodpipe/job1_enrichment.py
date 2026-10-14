from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from foodpipe.schema import ORDER_FIELDS, ORDER_SCHEMA


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
    return clean.select(*ORDER_FIELDS)
